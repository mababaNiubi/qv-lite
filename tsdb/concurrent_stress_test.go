package tsdb

// 并发写查一致性回归测试套件。
//
// 在"高峰并发写入"场景（多 writer + 小 WAL 高频 flush）下，某 tag 第一个
// 点可能出现值错位：值 0 消失、值 1 重复。根因是 PointDiskPack.Next() 的
// 时间过滤分支跳过了 valueDecoder.Read()，导致 IntegerDecoder simple8b 的
// prev 累加器未同步。该缺陷已修复；本套件作为回归门禁。
//
// 运行方式（修复后应全过）：
//
//	go test ./tsdb -run TestConcurrentWriteQuery -count=10 -timeout 600s -v

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mababaNiubi/variant"
)

// ─── 复现器参数 ──────────────────────────────────────────────────────

// reproBase 是固定时间基准（避免与 time.Now() 交互，保证可复现的 ts 序列）。
// 每个 tag w 的数据范围为 [reproBase+w*perWriter, reproBase+(w+1)*perWriter)，
// 值 = i（第 i 个点），因此"索引 i 的点值应等于 i"是强断言。
const reproBase = int64(1_000_000_000_000_000_000)

// ─── 并发写查一致性回归 ──────────────────────────────────────────────

// TestConcurrentWriteQuery 是并发写查一致性回归：8 writer × 10 万点 +
// 8 查询 goroutine 并发（QueryAll/QueryIter/QueryWindow 混用），小 WAL 高频
// flush。写完停查询后做最终一致性校验：逐 tag 点数、ts 唯一、值 == 索引。
//
// 配置矩阵覆盖：int/float 值、async/sync flush、CloseBuffer、有无查询——
// 曾触发"值流错位一位"缺陷的组合（int + 多 writer + 小 WAL）全部在内。
func TestConcurrentWriteQuery(t *testing.T) {
	for _, cfg := range []struct {
		name        string
		valueKind   string // "int" 或 "float"
		async       bool
		closeBuffer bool
		noQueries   bool
	}{
		{"int_async", "int", true, false, false},
		{"int_sync", "int", false, false, false},
		{"int_async_closebuf", "int", true, true, false},
		{"int_async_noqueries", "int", true, false, true},
		{"float_async", "float", true, false, false},
		{"float_async_noqueries", "float", true, false, true},
	} {
		t.Run(cfg.name, func(t *testing.T) {
			runConcurrentWriteQuery(t, cfg.valueKind, cfg.async, cfg.closeBuffer, cfg.noQueries)
		})
	}
}

// runConcurrentWriteQuery 执行一轮并发写查回归。valueKind 取 "int" 或 "float"。
func runConcurrentWriteQuery(t *testing.T, valueKind string, async, closeBuffer, skipQueries bool) {
	dir := tempDir(t)
	db, err := Open(Config{
		Path:           dir,
		MaxStorageTime: 24 * 60 * 60 * 365,
		WalConfig: WalConfig{
			MaxFileSize:        64 * 1024, // 小 WAL：写入高峰中频繁 rotate + flush
			MaxBufferBatchSize: 1024,
			CloseBuffer:        closeBuffer,
		},
		AsyncFlush: async,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tableInfo := TableInfo{ColumnAttribute: ColumnAttribute{Name: "stress", Type: ColumnTypeInt}}
	if valueKind == "float" {
		tableInfo = TableInfo{ColumnAttribute: ColumnAttribute{Name: "stress", Type: ColumnTypeFloat, FloatPrecision: 2}}
	}
	if err := db.CreateTable(tableInfo); err != nil {
		t.Fatal(err)
	}

	const (
		numWriters = 8
		perWriter  = 100000 // 8 × 100K = 800K 点
		numQueries = 8
	)

	// 预创建所有 tag（避免查询碰到 ErrorTagNotFound 的时序噪音；预创建点
	// ts=base-2 在查询范围外，不影响断言）。
	for w := 0; w < numWriters; w++ {
		var v variant.Variant
		if valueKind == "float" {
			v = variant.NewFloat64(-1)
		} else {
			v = variant.NewInt64(-1)
		}
		if _, err := db.Write("stress", fmt.Sprintf("tag_%d", w), reproBase-2, v); err != nil {
			t.Fatal(err)
		}
	}

	// 每个 writer 独占一个 tag，写 [base+w*perWriter, base+(w+1)*perWriter)。
	// 值 = i（int）或 float64(i)（float），因此"索引 i 的点值应等于 i"是强断言。
	writeFn := func(w int, errCh chan<- error) {
		for i := 0; i < perWriter; i++ {
			ts := reproBase + int64(w*perWriter+i)
			var v variant.Variant
			if valueKind == "float" {
				v = variant.NewFloat64(float64(i))
			} else {
				v = variant.NewInt64(int64(i))
			}
			if _, err := db.Write("stress", fmt.Sprintf("tag_%d", w), ts, v); err != nil {
				errCh <- fmt.Errorf("writer %d point %d: %w", w, i, err)
				return
			}
		}
		errCh <- nil
	}

	var done atomic.Bool
	var queryErrs atomic.Int64
	var queryLoops atomic.Int64
	queryFn := func(q int) {
		rng := rand.New(rand.NewSource(int64(q) * 7919))
		for !done.Load() {
			w := rng.Intn(numWriters)
			tag := fmt.Sprintf("tag_%d", w)
			start := reproBase + int64(w*perWriter) - 1
			end := reproBase + int64((w+1)*perWriter) + 1
			switch q % 3 {
			case 0: // 全量物化查询
				pts, err := db.QueryAll("stress", tag, start, end, nil)
				if err != nil {
					queryErrs.Add(1)
					t.Logf("QueryAll(%s) error: %v", tag, err)
					continue
				}
				for i := 1; i < len(pts); i++ {
					if pts[i].Tms <= pts[i-1].Tms {
						queryErrs.Add(1)
						t.Logf("QueryAll(%s) unsorted at %d", tag, i)
						break
					}
				}
			case 1: // 流式查询 + limit
				it, err := db.QueryIter(context.Background(), "stress", tag, start, end, nil, &QueryOptions{Limit: 500})
				if err != nil {
					queryErrs.Add(1)
					t.Logf("QueryIter(%s) error: %v", tag, err)
					continue
				}
				prev := int64(-1)
				for {
					p, ok, err := it.Next()
					if err != nil {
						queryErrs.Add(1)
						t.Logf("QueryIter(%s) Next error: %v", tag, err)
						break
					}
					if !ok {
						break
					}
					if p.Tms <= prev {
						queryErrs.Add(1)
						t.Logf("QueryIter(%s) unsorted: %d <= %d", tag, p.Tms, prev)
						break
					}
					prev = p.Tms
				}
				if err := it.Close(); err != nil {
					queryErrs.Add(1)
					t.Logf("QueryIter(%s) Close error: %v", tag, err)
				}
			default: // 窗口聚合
				pts, err := db.Query("stress", tag, start, end, 1000, AvgFusion, nil)
				if err != nil {
					queryErrs.Add(1)
					t.Logf("Query(%s) window error: %v", tag, err)
				}
				_ = pts
			}
			queryLoops.Add(1)
		}
	}

	var writerWg, queryWg sync.WaitGroup
	errCh := make(chan error, numWriters)
	for w := 0; w < numWriters; w++ {
		writerWg.Add(1)
		go func(w int) {
			defer writerWg.Done()
			writeFn(w, errCh)
		}(w)
	}
	if !skipQueries {
		for q := 0; q < numQueries; q++ {
			queryWg.Add(1)
			go func(q int) {
				defer queryWg.Done()
				queryFn(q)
			}(q)
		}
	}
	// 先等写入高峰结束，再停查询，最后收尾。
	writerWg.Wait()
	done.Store(true)
	queryWg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := queryErrs.Load(); n != 0 {
		t.Fatalf("query goroutines reported %d errors", n)
	}
	t.Logf("config: kind=%s async=%v closeBuffer=%v queries=%v queryLoops=%d",
		valueKind, async, closeBuffer, !skipQueries, queryLoops.Load())

	// 最终一致性校验：逐 tag 点数、ts 唯一、值 == 索引偏移。
	for w := 0; w < numWriters; w++ {
		tag := fmt.Sprintf("tag_%d", w)
		start := reproBase + int64(w*perWriter) - 1
		end := reproBase + int64((w+1)*perWriter) + 1
		pts, err := db.QueryAll("stress", tag, start, end, nil)
		if err != nil {
			t.Fatalf("final query %s: %v", tag, err)
		}
		if len(pts) != perWriter {
			t.Fatalf("tag %s: expected %d points, got %d", tag, perWriter, len(pts))
		}
		seen := make(map[int64]bool, len(pts))
		bad := 0
		firstBad := ""
		for i, p := range pts {
			if seen[p.Tms] {
				t.Fatalf("tag %s: duplicate timestamp %d", tag, p.Tms)
			}
			seen[p.Tms] = true
			if valueKind == "float" {
				if f, _ := p.V.AsFloat64(); f != float64(i%perWriter) {
					if bad == 0 {
						firstBad = fmt.Sprintf("tag %s point %d: value %v, want %v (ts=%d)",
							tag, i, f, float64(i%perWriter), p.Tms)
					}
					bad++
				}
			} else {
				if v, _ := p.V.AsInt64(); v != int64(i%perWriter) {
					if bad == 0 {
						firstBad = fmt.Sprintf("tag %s point %d: value %d, want %d (ts=%d)",
							tag, i, v, i%perWriter, p.Tms)
					}
					bad++
				}
			}
		}
		if bad > 0 {
			// 输出结果头部（前 8 点）辅助定位后 Fail。
			for k := 0; k < min(8, len(pts)); k++ {
				disp := "-"
				if valueKind == "float" {
					if f, err := pts[k].V.AsFloat64(); err == nil {
						disp = fmt.Sprintf("%v", f)
					}
				} else {
					if v, err := pts[k].V.AsInt64(); err == nil {
						disp = fmt.Sprintf("%d", v)
					}
				}
				want := pts[k].Tms - start - 1
				mark := ""
				got := float64(-1)
				if valueKind == "float" {
					got, _ = pts[k].V.AsFloat64()
				} else {
					g, _ := pts[k].V.AsInt64()
					got = float64(g)
				}
				if got != float64(want) {
					mark = "  <== BAD"
				}
				t.Logf("head[%d] ts=%d v=%s want=%d%s", k, pts[k].Tms, disp, want, mark)
			}
			t.Fatalf("%s (total bad=%d of %d)", firstBad, bad, len(pts))
		}
	}
}
