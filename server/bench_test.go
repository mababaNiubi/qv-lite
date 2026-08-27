package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

// benchDB 打开临时引擎并建表 "t"，供 server 层基准使用。
func benchDB(b *testing.B) *tsdb.DB {
	b.Helper()
	db, err := tsdb.Open(tsdb.Config{
		Path:       filepath.Join(b.TempDir(), "data"),
		AsyncFlush: true,
	}, context.Background())
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if err := db.CreateTable(tsdb.TableInfo{ColumnAttribute: tsdb.ColumnAttribute{Name: "t"}}); err != nil {
		b.Fatalf("CreateTable: %v", err)
	}
	return db
}

// benchServer 打开配置化 server（默认流水线开启）。
func benchServer(b *testing.B, cfg *Config) *Server {
	b.Helper()
	if cfg == nil {
		cfg = &Config{}
	}
	cfg.ApplyDefaults()
	cfg.DB.Path = filepath.Join(b.TempDir(), "data")
	srv, err := New(cfg, nil)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	if err := srv.DB().CreateTable(tsdb.TableInfo{ColumnAttribute: tsdb.ColumnAttribute{Name: "t"}}); err != nil {
		b.Fatalf("CreateTable: %v", err)
	}
	return srv
}

// buildLineBody 构造 n 行 Line Protocol 请求体。
func buildLineBody(n int) []byte {
	var sb strings.Builder
	sb.Grow(n * 40)
	base := time.Now().Add(-time.Hour).UnixMilli()
	for i := 0; i < n; i++ {
		// Equal timestamps are valid when dedup/min-interval are disabled. Keeping
		// them equal ensures repeated benchmark iterations still exercise actual
		// WAL writes instead of being discarded as older than the prior batch.
		fmt.Fprintf(&sb, "t,tag=cpu value=36.5 %d\n", base)
	}
	return []byte(sb.String())
}

// buildJSONBody 构造 n 点 JSON batch 请求体。
func buildJSONBody(n int) []byte {
	base := time.Now().Add(-time.Hour).UnixMilli()
	pts := make([]map[string]any, n)
	for i := range pts {
		pts[i] = map[string]any{
			"tag": "cpu", "timestamp": base, "value": 36.5,
		}
	}
	raw, err := json.Marshal(map[string]any{"points": pts})
	if err != nil {
		panic(err)
	}
	return raw
}

// buildBinaryBody 构造 n 个 float 点的二进制批量请求体（同一种值类型）。
func buildBinaryBody(n int) []byte {
	var buf bytes.Buffer
	buf.Write([]byte{0x54, 0x53, 1, batchValueFloat})
	table := "t"
	var tlen [2]byte
	binary.BigEndian.PutUint16(tlen[:], uint16(len(table)))
	buf.Write(tlen[:])
	buf.WriteString(table)
	var cnt [4]byte
	binary.BigEndian.PutUint32(cnt[:], uint32(n))
	buf.Write(cnt[:])
	base := time.Now().Add(-time.Hour).UnixMilli()
	for i := 0; i < n; i++ {
		tag := "cpu"
		binary.BigEndian.PutUint16(tlen[:], uint16(len(tag)))
		buf.Write(tlen[:])
		buf.WriteString(tag)
		var ts [8]byte
		binary.BigEndian.PutUint64(ts[:], uint64(base))
		buf.Write(ts[:])
		var val [8]byte
		binary.BigEndian.PutUint64(val[:], math.Float64bits(36.5))
		buf.Write(val[:])
	}
	return buf.Bytes()
}

// BenchmarkPipelineSubmitFlush 衡量流水线写入器的端到端吞吐：
// Submit（复制入池化缓冲）→ 后台合并 → 引擎 WriteBatch → Flush 返回。
// 即「解码侧提交 + 单写入器入库」完整周期，不含 HTTP 反序列化。
func BenchmarkPipelineSubmitFlush(b *testing.B) {
	db := benchDB(b)
	w := NewPipelinedWriter(db, 1000, 50_000)
	defer w.Close()
	src := testPoints(50_000)
	for i := range src {
		src[i].Timestamp = src[0].Timestamp
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 生产侧从池取缓冲填充一批，所有权交给写入器，Flush 后归还池。
		pts := pointBatchPool.Get().([]tsdb.TagPoint)
		pts = append(pts[:0], src...)
		w.Submit("t", pts)
		if err := w.Flush(); err != nil {
			b.Fatalf("Flush: %v", err)
		}
	}
}

// BenchmarkServerWriteLine 衡量 Line Protocol 流式写入口的端到端吞吐
// （解码 + Submit + Flush 入库）。流水线开启（默认配置）。
func BenchmarkServerWriteLine(b *testing.B) {
	const n = 50_000
	srv := benchServer(b, &Config{WriteBufferMs: 5, WriteBatchSize: n})
	h := srv.Handler()
	body := buildLineBody(n)
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write/line", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("line write: %d %s", rec.Code, rec.Body.String())
		}
		if err := srv.flushWrites(); err != nil {
			b.Fatalf("flush: %v", err)
		}
	}
}

// BenchmarkServerWriteLineNoPipeline 同上，但关闭流水线（立即写）。
func BenchmarkServerWriteLineNoPipeline(b *testing.B) {
	const n = 50_000
	srv := benchServer(b, &Config{WriteBufferMs: 0})
	h := srv.Handler()
	body := buildLineBody(n)
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write/line", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("line write: %d %s", rec.Code, rec.Body.String())
		}
	}
}

// BenchmarkServerWriteJSON 衡量 JSON 流式 batch 入口的端到端吞吐
// （JSON 反序列化 + Submit + Flush 入库）。
func BenchmarkServerWriteJSON(b *testing.B) {
	const n = 50_000
	srv := benchServer(b, &Config{WriteBufferMs: 5, WriteBatchSize: n})
	h := srv.Handler()
	body := buildJSONBody(n)
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/batch?table=t", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("json batch: %d %s", rec.Code, rec.Body.String())
		}
		if err := srv.flushWrites(); err != nil {
			b.Fatalf("flush: %v", err)
		}
	}
}

// BenchmarkServerWriteBinary 衡量二进制批量入口的端到端吞吐
// （手工解析 + Submit + Flush 入库）。
func BenchmarkServerWriteBinary(b *testing.B) {
	const n = 50_000
	srv := benchServer(b, &Config{WriteBufferMs: 5, WriteBatchSize: n})
	h := srv.Handler()
	body := buildBinaryBody(n)
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/batch", bytes.NewReader(body))
		req.Header.Set("Content-Type", BatchContentType)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("binary batch: %d %s", rec.Code, rec.Body.String())
		}
		if err := srv.flushWrites(); err != nil {
			b.Fatalf("flush: %v", err)
		}
	}
}

// BenchmarkServerWriteLineParallel 并发场景（多 worker 同时流式写 Line
// Protocol）：对比流水线开/关。流水线开启时解码（各 worker 并行）与入库
// （单写入器）重叠，多个请求的小批被合并；关闭时入库内联在 handler 里，
// 每个请求都要等引擎写完才返回。
//
// 按真实流式用法测量：流水线开启时不逐请求 flush（后台写入器持续合批排空），
// 计时结束后统一 flush 验证数据落库；关闭时写入本来就同步，无需 flush。
func BenchmarkServerWriteLineParallel(b *testing.B) {
	for _, tc := range []struct {
		name     string
		bufferMs int64
	}{
		{"pipeline_on", 5},
		{"pipeline_off", 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			const n = 10_000
			srv := benchServer(b, &Config{WriteBufferMs: tc.bufferMs, WriteBatchSize: n})
			h := srv.Handler()
			body := buildLineBody(n)
			b.SetBytes(int64(len(body)))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					req := httptest.NewRequest(http.MethodPost, "/api/v1/write/line", bytes.NewReader(body))
					rec := httptest.NewRecorder()
					h.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK {
						b.Fatalf("line write: %d %s", rec.Code, rec.Body.String())
					}
				}
			})
			b.StopTimer()
			// 统一 flush：验证后台写入器已把全部数据落库（不计时）。
			if err := srv.flushWrites(); err != nil {
				b.Fatalf("final flush: %v", err)
			}
		})
	}
}
