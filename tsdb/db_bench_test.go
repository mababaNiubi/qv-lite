package tsdb

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

func benchDir(b *testing.B) string {
	dir, err := os.MkdirTemp("", "tsdb_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

// benchOpen 打开一个可配置的 DB。walSize 越大越少触发段刷新,可隔离"纯 WAL 写路径"
// 与"含段编码压缩的完整写路径"两个场景。
// 返回 (db, dir):dir 是该 DB 的数据目录,调用方(如 E2E 测 size)必须用它而不是自建目录。
func benchOpen(b *testing.B, tableName string, walSize int64) (*DB, string) {
	dir := benchDir(b)
	db, err := Open(Config{
		Path:           dir,
		WalConfig:      WalConfig{MaxFileSize: walSize, CloseBuffer: true},
		AsyncFlush:     true,
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	if err := db.CreateTable(TableInfo{ColumnAttribute: ColumnAttribute{Name: tableName, FloatPrecision: 2}}); err != nil {
		b.Fatal(err)
	}
	return db, dir
}

// prebuiltTags 预生成 numTags 个 tag 字符串,避免计时循环里做 fmt。
func prebuiltTags(numTags int) []string {
	tags := make([]string, numTags)
	for i := range tags {
		tags[i] = fmt.Sprintf("tag_%d", i)
	}
	return tags
}

// ==================== 写场景 ====================

// BenchmarkWritePoint 隔离单点写路径(锁/缓存/tag解析/去重/追加)。
// 轴:值类型 × tag基数。大 WAL(不触发段刷新)聚焦 WAL 本身。
func BenchmarkWritePoint(b *testing.B) {
	// 字符串值池:low-card 循环 8 个(走 dict),high-card 循环 100000 个(超过
	// 字典阈值 4096 → 走 snappy)。预生成以隔离调用方 fmt 成本。
	lowCardStrings := make([]string, 8)
	highCardStrings := make([]string, 100000)
	for i := range lowCardStrings {
		lowCardStrings[i] = fmt.Sprintf("st_%d", i)
	}
	for i := range highCardStrings {
		highCardStrings[i] = fmt.Sprintf("st_%06d", i)
	}

	scenarios := []struct {
		name  string
		value func(i int) variant.Variant
	}{
		{"float", func(i int) variant.Variant { return variant.NewFloat64(float64(i) * 0.01) }},
		{"int", func(i int) variant.Variant { return variant.NewInt(i) }},
		{"string_lowcard", func(i int) variant.Variant { return variant.NewString(lowCardStrings[i%len(lowCardStrings)]) }},
		{"string_highcard", func(i int) variant.Variant { return variant.NewString(highCardStrings[i%len(highCardStrings)]) }},
	}
	for _, sc := range scenarios {
		for _, numTags := range []int{1, 100, 10000} {
			b.Run(fmt.Sprintf("%s/card%d", sc.name, numTags), func(b *testing.B) {
				db, _ := benchOpen(b, "t", 256<<20)
				tags := prebuiltTags(numTags)
				base := time.Now().UnixNano()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := db.Write("t", tags[i%numTags], base+int64(i)*1e6, sc.value(i)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkWriteWithFlush 用小 WAL 强制段刷新,隔离"完整写路径(追加+压缩编码)"。
// 与 BenchmarkWritePoint(大 WAL)对比即可看出压缩编码的成本。
func BenchmarkWriteWithFlush(b *testing.B) {
	for _, numTags := range []int{1, 100} {
		b.Run(fmt.Sprintf("card%d", numTags), func(b *testing.B) {
			db, _ := benchOpen(b, "t", 1<<20) // 1MB WAL,频繁刷新
			tags := prebuiltTags(numTags)
			base := time.Now().UnixNano()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.Write("t", tags[i%numTags], base+int64(i)*1e6, variant.NewFloat64(float64(i)*0.01)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkWriteBatch 隔离批量写的锁摊薄效果。轴:批大小 × tag基数。
func BenchmarkWriteBatch(b *testing.B) {
	for _, batchSize := range []int{100, 1000, 10000} {
		for _, numTags := range []int{1, 100} {
			b.Run(fmt.Sprintf("batch%d/card%d", batchSize, numTags), func(b *testing.B) {
				db, _ := benchOpen(b, "t", 256<<20)
				tags := prebuiltTags(numTags)
				base := time.Now().UnixNano()
				points := make([]TagPoint, batchSize)
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					off := n * batchSize
					for i := range points {
						points[i] = TagPoint{
							Tag:       tags[(off+i)%numTags],
							Timestamp: base + int64(off+i)*1e6,
							Value:     variant.NewFloat64(float64(off+i) * 0.01),
						}
					}
					if _, err := db.WriteBatch("t", points); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkWriteParallel 隔离锁竞争。轴:goroutine 数。
// 用全局原子序号保证时间戳唯一,避免并发写同 tag 被去重扭曲吞吐。
func BenchmarkWriteParallel(b *testing.B) {
	for _, goroutines := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("g%d", goroutines), func(b *testing.B) {
			db, _ := benchOpen(b, "t", 256<<20)
			base := time.Now().UnixNano()
			var next atomic.Int64
			var wg sync.WaitGroup
			errCh := make(chan error, goroutines)
			b.ResetTimer()
			wg.Add(goroutines)
			for range goroutines {
				go func() {
					defer wg.Done()
					for {
						i := next.Add(1) - 1
						if i >= int64(b.N) {
							return
						}
						if _, err := db.Write("t", "CPU", base+i*1e6, variant.NewFloat64(float64(i)*0.01)); err != nil {
							errCh <- err
							return
						}
					}
				}()
			}
			wg.Wait()
			select {
			case err := <-errCh:
				b.Fatal(err)
			default:
			}
		})
	}
}

// BenchmarkWriteStructure 隔离结构值写入:复用 map vs 每点新建 map。
// 两者差值 = 调用方建 map 的成本;reuse 项本身 = 库结构编码成本。
func BenchmarkWriteStructure(b *testing.B) {
	// 预生成 tag 字段字符串(低基数,走 dict),隔离 fmt。
	fieldStrings := make([]string, 1000)
	for i := range fieldStrings {
		fieldStrings[i] = fmt.Sprintf("AX%d", i)
	}
	for _, tc := range []struct {
		name  string
		fresh bool
	}{
		{"reuse_map", false},
		{"fresh_map", true},
	} {
		for _, numTags := range []int{1, 100} {
			b.Run(fmt.Sprintf("%s/card%d", tc.name, numTags), func(b *testing.B) {
				db, _ := benchOpen(b, "t", 256<<20)
				tags := prebuiltTags(numTags)
				base := time.Now().UnixNano()
				mp := make(map[string]any)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if tc.fresh {
						mp = make(map[string]any)
					}
					mp["value"] = float64(i) * 0.01
					mp["tag"] = fieldStrings[i%len(fieldStrings)]
					if _, err := db.Write("t", tags[i%numTags], base+int64(i)*1e6, variant.New(mp)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// ==================== 读场景 ====================

// writeAndReopen 写 points 个点并 close+reopen,得到"纯磁盘段"的读视图(不含
// WAL 缓存),用于隔离读路径。返回新 DB 和 time 基址。
func writeAndReopen(b *testing.B, points int, walSize int64) (*DB, int64) {
	dir := benchDir(b)
	openCfg := func() Config {
		return Config{
			Path:           dir,
			WalConfig:      WalConfig{MaxFileSize: walSize},
			AsyncFlush:     true,
			MaxStorageTime: 24 * 60 * 60 * 365,
		}
	}
	db, err := Open(openCfg(), context.Background())
	if err != nil {
		b.Fatal(err)
	}
	if err := db.CreateTable(TableInfo{ColumnAttribute: ColumnAttribute{Name: "t", FloatPrecision: 2}}); err != nil {
		b.Fatal(err)
	}
	base := time.Now().UnixNano()
	for i := 0; i < points; i++ {
		if _, err := db.Write("t", "CPU", base+int64(i)*1e6, variant.NewFloat64(float64(i)*0.01)); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	db2, err := Open(openCfg(), context.Background())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db2.Close() })
	return db2, base
}

// BenchmarkReadScan 全量顺序扫描读(QueryAll)。
func BenchmarkReadScan(b *testing.B) {
	for _, points := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("points%d", points), func(b *testing.B) {
			db, base := writeAndReopen(b, points, 16<<20)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				all, err := db.QueryAll("t", "CPU", base-1, base+int64(points)*1e6+1, nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(all) != points {
					b.Fatalf("scan: got %d want %d", len(all), points)
				}
			}
		})
	}
}

// BenchmarkWALReadTagScale measures the decoded in-memory WAL cache directly.
// It queries one tag while the same total point count is distributed across
// increasing tag cardinality, exposing key-scan and result-capacity costs
// without segment decode I/O.
func BenchmarkWALReadTagScale(b *testing.B) {
	const totalPoints = 1_000_000
	for _, numTags := range []int{1, 100, 10_000} {
		b.Run(fmt.Sprintf("tags%d", numTags), func(b *testing.B) {
			db, _ := benchOpen(b, "t", 256<<20)
			tags := prebuiltTags(numTags)
			base := time.Now().UnixNano()
			for i := 0; i < totalPoints; i++ {
				if _, err := db.Write("t", tags[i%numTags], base+int64(i), variant.NewFloat64(float64(i))); err != nil {
					b.Fatal(err)
				}
			}
			table, ok := db.ssTables.Load("t")
			if !ok {
				b.Fatal("table missing")
			}
			if err := table.batcher.Flush(); err != nil {
				b.Fatal(err)
			}

			target := tags[numTags-1]
			want := totalPoints / numTags
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				points, err := db.QueryAll("t", target, base-1, base+totalPoints, nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(points) != want {
					b.Fatalf("points=%d, want %d", len(points), want)
				}
			}
			b.ReportMetric(totalPoints, "wal-points-scanned/op")
		})
	}
}

// BenchmarkReadWindow 固定窗口聚合读(Query + windowSize)。
func BenchmarkReadWindow(b *testing.B) {
	for _, points := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("points%d", points), func(b *testing.B) {
			db, base := writeAndReopen(b, points, 16<<20)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 1 秒窗口,均值聚合
				_, err := db.Query("t", "CPU", base-1, base+int64(points)*1e6+1, int64(time.Second), AvgFusion, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReadCondition 条件过滤读(Query + 数值区间条件)。
func BenchmarkReadCondition(b *testing.B) {
	for _, points := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("points%d", points), func(b *testing.B) {
			db, base := writeAndReopen(b, points, 16<<20)
			// 命中约 10% 的区间。
			lo := float64(points) * 0.45
			hi := float64(points) * 0.55
			cond := LogicalCondition{
				Op: LogicalAnd,
				Cond: []any{
					Condition{ColumnAttributeName: "", Operator: OpGreaterThan, Value: variant.NewFloat64(lo)},
					Condition{ColumnAttributeName: "", Operator: OpLessThan, Value: variant.NewFloat64(hi)},
				},
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.QueryAll("t", "CPU", base-1, base+int64(points)*1e6+1, cond); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ==================== E2E: 写后即查(真实形态) ====================

// BenchmarkE2E_WriteAndQuery 标量写入 8640 万点 + 全量查询。单点逐写,大 WAL。
// 用 -benchtime=1x 确保只跑一轮。
func BenchmarkE2E_WriteAndQuery(b *testing.B) {
	const totalPoints = 24 * 60 * 60 * 1000 // 86,400,000
	tableName := "eu12"
	tag := "CPU"
	db, dir := benchOpen(b, tableName, 256*1024*1024)

	writeStart := time.Now()
	baseTime := writeStart.UnixNano()
	for i := 0; i < totalPoints; i++ {
		if _, err := db.Write(tableName, tag, baseTime+int64(i)*int64(time.Millisecond), variant.NewFloat64(123+float64(i)*0.01)); err != nil {
			b.Fatalf("write %d failed: %v", i, err)
		}
	}
	writeElapsed := time.Since(writeStart)

	queryStart := time.Now()
	all, err := db.QueryAll(tableName, tag, baseTime-100, baseTime+int64(totalPoints)*int64(time.Millisecond)+100, nil)
	if err != nil {
		b.Fatal(err)
	}
	queryElapsed := time.Since(queryStart)
	// 显式 Close 排空 async flush 未落的段,再量 size,得到真实落盘大小。
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	b.Logf("write: %v (%.0f pts/s), read: %v (%.0f pts/s), count: %d, size: %v",
		writeElapsed, float64(totalPoints)/writeElapsed.Seconds(),
		queryElapsed, float64(len(all))/queryElapsed.Seconds(),
		len(all), fileDirSize(dir, tableName))
}

// BenchmarkE2E_WriteAndColumnQuery 结构写入(复用 map,隔离调用方建 map 成本)
// + 条件窗口查询。用 -benchtime=1x 确保只跑一轮。
func BenchmarkE2E_WriteAndColumnQuery(b *testing.B) {
	const totalPoints = 12 * 60 * 60 * 1000 // 43,200,000
	tableName := "eu12"
	tag := "CPU"
	db, dir := benchOpen(b, tableName, 256*1024*1024)

	fieldStrings := make([]string, 100000)
	for i := range fieldStrings {
		fieldStrings[i] = fmt.Sprintf("AX%v", i)
	}
	writeStart := time.Now()
	baseTime := writeStart.UnixNano()
	mp := make(map[string]any)
	for i := 0; i < totalPoints; i++ {
		mp["value"] = float64(i) * 0.01
		mp["tag"] = fieldStrings[i%len(fieldStrings)]
		if _, err := db.Write(tableName, tag, baseTime+int64(i)*int64(time.Millisecond), variant.New(mp)); err != nil {
			b.Fatalf("write %d failed: %v", i, err)
		}
	}
	writeElapsed := time.Since(writeStart)

	queryStart := time.Now()
	all, err := db.Query(tableName, tag, baseTime-100, baseTime+int64(totalPoints)*int64(time.Millisecond)+100, int64(time.Second), 1, LogicalCondition{
		Op: LogicalAnd,
		Cond: []any{
			Condition{ColumnAttributeName: "value", Operator: OpGreaterThan, Value: variant.NewFloat64(60 * 60 * 10)},
			Condition{ColumnAttributeName: "value", Operator: OpLessThan, Value: variant.NewFloat64(60 * 60 * 10 * 2)},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	queryElapsed := time.Since(queryStart)
	// 显式 Close 排空 async flush 未落的段,再量 size,得到真实落盘大小。
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	b.Logf("write: %v (%.0f pts/s), read: %v (%.0f pts/s), count: %d, size: %v",
		writeElapsed, float64(totalPoints)/writeElapsed.Seconds(),
		queryElapsed, float64(len(all))/queryElapsed.Seconds(),
		len(all), fileDirSize(dir, tableName))
}

func fileDirSize(dir string, tableName string) string {
	var total int64
	_ = filepath.WalkDir(filepath.Join(dir, tableName, "data"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			fmt.Fprintf(os.Stderr, "访问 %s 出错: %v\n", path, err)
			return nil
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				fmt.Fprintf(os.Stderr, "获取 %s 信息出错: %v\n", path, err)
				return nil
			}
			total += info.Size()
		}
		return nil
	})
	if total < 1024 {
		return fmt.Sprintf("%v b", total)
	} else if total < 1024*1024 {
		return fmt.Sprintf("%.2fkB", float64(total)/1024)
	} else if total < 1024*1024*1024 {
		return fmt.Sprintf("%.2fMB", float64(total)/(1024*1024))
	} else {
		return fmt.Sprintf("%.2fGB", float64(total)/(1024*1024*1024))
	}
}

// BenchmarkE2E_TagScaleWriteAndQuery 多点写入版 E2E 基准:
// 固定总量、多 tag 均匀分布(轮询), 写后逐 tag 全量查询, 显式 Close 排空
// async flush 后量落盘大小 —— 与 BenchmarkE2E_WriteAndQuery(单 tag)对标,
// 用于观察"tag 数量 × 写路径 × 落盘体积"在真实形态下的表现。
// mode = single: 单点逐写(与 BenchmarkE2E_WriteAndQuery 一致, 仅 tag 数不同)
//
//	batch : WriteBatch 分批写(4096/批), 对比批量写路径。
//
// 总量固定不可重复,请用 -benchtime=1x 运行;调整 totalPoints 可测更大规模。
// 进度通过 fmt.Printf 流式输出([E2E] 前缀):长跑期间实时可见,不会被误判为
// 死循环(默认 benchtime 下单个配置可能静默数分钟)。
func BenchmarkE2E_TagScaleWriteAndQuery(b *testing.B) {
	go func() {
		http.ListenAndServe(":6060", nil)
	}()
	const totalPoints = 10_000_000 * 5 // 1000 万点(1ms 步长 ≈ 2.8h 跨度)
	tableName := "eu12"
	modes := []string{"single"}
	tagList := []int{1, 100, 1000, 10000}
	scaleBatchSize := 4096
	for _, numTags := range tagList {
		for _, mode := range modes {
			b.Run(fmt.Sprintf("tags%d_%s", numTags, mode), func(b *testing.B) {
				// 32MB 小 WAL:强制写入过程旋转落盘,Close 后 data 目录才有
				// 压缩段文件可量(与 BenchmarkE2E_WriteAndQuery 的大 WAL 不同,
				// 后者靠 8640 万点的体量自然触发旋转)。
				//
				// 进度直接 fmt.Printf 流式输出(不走 benchmark 输出缓冲):
				// 长跑(如 tags10000 读阶段可达数分钟)期间必须实时可见,否则
				// 静默期会被误判为死循环。每行带 [E2E] 前缀便于区分。
				fmt.Printf("[E2E] tags%d: writing %d points...\n", numTags, totalPoints)
				db, dir := benchOpen(b, tableName, 32*1024*1024)
				tags := prebuiltTags(numTags)

				writeStart := time.Now()
				baseTime := writeStart.UnixNano()
				written := 0
				if mode == "single" {
					for i := 0; i < totalPoints; i++ {
						if _, err := db.Write(tableName, tags[i%numTags], baseTime+int64(i)*int64(time.Millisecond), variant.NewFloat64(123+float64(i)*0.01)); err != nil {
							b.Fatalf("write %d failed: %v", i, err)
						}
					}
					written = totalPoints
				} else {
					batch := make([]TagPoint, 0, scaleBatchSize)
					for i := 0; i < totalPoints; i++ {
						batch = append(batch, TagPoint{
							Tag:       tags[i%numTags],
							Timestamp: baseTime + int64(i)*int64(time.Millisecond),
							Value:     variant.NewFloat64(123 + float64(i)*0.01),
						})
						if len(batch) == scaleBatchSize {
							n, err := db.WriteBatch(tableName, batch)
							if err != nil {
								b.Fatalf("WriteBatch failed: %v", err)
							}
							written += n
							batch = batch[:0]
						}
					}
					if len(batch) > 0 {
						n, err := db.WriteBatch(tableName, batch)
						if err != nil {
							b.Fatalf("WriteBatch tail failed: %v", err)
						}
						written += n
					}
				}
				writeElapsed := time.Since(writeStart)
				fmt.Printf("[E2E] tags%d: write done %v (%.0f pts/s), querying %d tags...\n",
					numTags, writeElapsed, float64(written)/writeElapsed.Seconds(), numTags)

				// 逐 tag 全量查询,累加总点数。进度每 10% 打一行。
				queryStart := time.Now()
				total := 0
				progressStep := numTags / 10
				if progressStep < 1 {
					progressStep = 1
				}
				for i, tag := range tags {
					all, err := db.QueryAll(tableName, tag, baseTime-100, baseTime+int64(totalPoints)*int64(time.Millisecond)+100, nil)
					if err != nil {
						b.Fatal(err)
					}
					total += len(all)
					if (i+1)%progressStep == 0 {
						elapsed := time.Since(queryStart)
						fmt.Printf("[E2E] tags%d: query %d/%d tags, %d pts (%v, %.0f pts/s)\n",
							numTags, i+1, numTags, total, elapsed, float64(total)/elapsed.Seconds())
					}
				}
				queryElapsed := time.Since(queryStart)
				if total != written {
					b.Fatalf("query count %d != written %d", total, written)
				}

				// 显式 Close 排空 async flush 未落的段,再量 size,得到真实落盘大小。
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				fmt.Printf("[E2E] tags%d: done — write %v, read %v, count %d/%d, size %v\n",
					numTags, writeElapsed, queryElapsed, total, written, fileDirSize(dir, tableName))
				b.Logf("write: %v (%.0f pts/s), read: %v (%.0f pts/s), count: %d, size: %v",
					writeElapsed, float64(written)/writeElapsed.Seconds(),
					queryElapsed, float64(total)/queryElapsed.Seconds(),
					total, fileDirSize(dir, tableName))
			})
		}
	}
}
