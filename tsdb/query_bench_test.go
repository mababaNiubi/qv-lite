package tsdb

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

// 查询专项基准（-benchmem）：
//
//	go test ./tsdb -run '^$' -bench 'BenchmarkQuery' -benchtime=2s -benchmem
//
// 数据在 benchmark 内一次性写入（写路径不计时），随后重复查询。B/op 与
// allocs/op 是确定性指标，对比优化前后优先看它们。
//
// 规模轴：1K / 100K / 1M 点；位置轴：wal（大 WAL，数据全在内存读缓存）与
// disk（小 WAL，数据大部分落段）。

const (
	benchTable = "q"
	benchTag   = "cpu"
)

// benchQueryOpen 打开查询基准用 DB。walSize 控制数据落盘程度：
// 大值 → 数据全在 WAL；小值 → 频繁 flush 落段。
func benchQueryOpen(b *testing.B, walSize int64) *DB {
	b.Helper()
	db, _ := benchOpen(b, benchTable, walSize)
	return db
}

// writePointsN 写入 n 个等间隔浮点（写路径不计时），返回首个时间戳。
func writePointsN(b *testing.B, db *DB, n int, tag string) int64 {
	b.Helper()
	base := time.Now().UnixNano()
	batch := make([]TagPoint, 0, 4096)
	for i := 0; i < n; i++ {
		batch = append(batch, TagPoint{
			Tag:       tag,
			Timestamp: base + int64(i)*1e6, // 1ms 间隔
			Value:     variant.NewFloat64(float64(i) * 0.5),
		})
		if len(batch) == 4096 {
			if _, err := db.WriteBatch(benchTable, batch); err != nil {
				b.Fatalf("WriteBatch: %v", err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if _, err := db.WriteBatch(benchTable, batch); err != nil {
			b.Fatalf("WriteBatch: %v", err)
		}
	}
	return base
}

// queryAllOnce 跑一轮全量查询并丢弃结果。
func queryAllOnce(b *testing.B, db *DB, tag string, base, end int64) {
	b.Helper()
	pts, err := db.QueryAll(benchTable, tag, base-1, end+1, nil)
	if err != nil {
		b.Fatalf("QueryAll: %v", err)
	}
	runtime.KeepAlive(pts)
}

// BenchmarkQueryRaw 全量原始点查询：位置(wal/disk) × 规模(1K/100K/1M)。
func BenchmarkQueryRaw(b *testing.B) {
	for _, loc := range []struct {
		name string
		wal  int64
	}{{"wal", 256 << 20}, {"disk", 1 << 20}} {
		for _, n := range []int{1_000, 100_000, 1_000_000} {
			b.Run(loc.name+"/"+itoa(n), func(b *testing.B) {
				db := benchQueryOpen(b, loc.wal)
				base := writePointsN(b, db, n, benchTag)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					queryAllOnce(b, db, benchTag, base, base+int64(n)*1e6)
				}
			})
		}
	}
}

// BenchmarkQueryWindow 窗口聚合查询（1s 窗口）: 1M 点 → 1000 个窗口点。
func BenchmarkQueryWindow(b *testing.B) {
	for _, loc := range []struct {
		name string
		wal  int64
	}{{"wal", 256 << 20}, {"disk", 1 << 20}} {
		b.Run(loc.name+"/1M", func(b *testing.B) {
			db := benchQueryOpen(b, loc.wal)
			const n = 1_000_000
			base := writePointsN(b, db, n, benchTag)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pts, err := db.Query(benchTable, benchTag, base-1, base+int64(n)*1e6,
					int64(time.Second), AvgFusion, nil)
				if err != nil {
					b.Fatalf("Query: %v", err)
				}
				runtime.KeepAlive(pts)
			}
		})
	}
}

// BenchmarkQueryIter 流式查询：与 BenchmarkQueryRaw 同数据，但不物化结果，
// 直接体现流式接口的分配成本。
func BenchmarkQueryIter(b *testing.B) {
	for _, loc := range []struct {
		name string
		wal  int64
	}{{"wal", 256 << 20}, {"disk", 1 << 20}} {
		for _, n := range []int{1_000, 100_000, 1_000_000} {
			b.Run(loc.name+"/"+itoa(n), func(b *testing.B) {
				db := benchQueryOpen(b, loc.wal)
				base := writePointsN(b, db, n, benchTag)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					it, err := db.QueryIter(context.Background(), benchTable, benchTag,
						base-1, base+int64(n)*1e6, nil, nil)
					if err != nil {
						b.Fatalf("QueryIter: %v", err)
					}
					for {
						_, ok, err := it.Next()
						if err != nil {
							b.Fatalf("Next: %v", err)
						}
						if !ok {
							break
						}
					}
					if err := it.Close(); err != nil {
						b.Fatalf("Close: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkQueryCondition 条件过滤查询（map 结构值，disk 侧）: 1M 点。
func BenchmarkQueryCondition(b *testing.B) {
	db := benchQueryOpen(b, 1<<20)
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{
			Name: "qc", Type: ColumnTypeStructure,
			Structure: []ColumnAttribute{{Name: "v", Type: ColumnTypeFloat}},
		},
	}); err != nil {
		b.Fatal(err)
	}
	const n = 1_000_000
	base := time.Now().UnixNano()
	batch := make([]TagPoint, 0, 4096)
	for i := 0; i < n; i++ {
		batch = append(batch, TagPoint{
			Tag:       benchTag,
			Timestamp: base + int64(i)*1e6,
			Value:     variant.New(map[string]any{"v": float64(i) * 0.5}),
		})
		if len(batch) == 4096 {
			if _, err := db.WriteBatch("qc", batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if _, err := db.WriteBatch("qc", batch); err != nil {
			b.Fatal(err)
		}
	}
	cond := LogicalCondition{
		Op: LogicalAnd,
		Cond: []any{
			Condition{ColumnAttributeName: "v", Operator: OpGreaterThan, Value: variant.NewFloat64(n * 0.25)},
			Condition{ColumnAttributeName: "v", Operator: OpLessThan, Value: variant.NewFloat64(n * 0.75)},
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pts, err := db.QueryAll("qc", benchTag, base-1, base+int64(n)*1e6, cond)
		if err != nil {
			b.Fatalf("QueryAll: %v", err)
		}
		runtime.KeepAlive(pts)
	}
}

// BenchmarkQueryHighCardWAL 高 tag 基数 WAL：100K tag × 1 点，查单个 tag。
// 体现 WAL 读路径的扫描成本（写入用 WriteBatch，只查不写）。
func BenchmarkQueryHighCardWAL(b *testing.B) {
	db := benchQueryOpen(b, 256<<20)
	const numTags = 100_000
	tags := prebuiltTags(numTags)
	base := time.Now().UnixNano()
	batch := make([]TagPoint, 0, 4096)
	for i := 0; i < numTags; i++ {
		batch = append(batch, TagPoint{
			Tag:       tags[i],
			Timestamp: base + int64(i),
			Value:     variant.NewInt64(int64(i)),
		})
		if len(batch) == 4096 {
			if _, err := db.WriteBatch(benchTable, batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if _, err := db.WriteBatch(benchTable, batch); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pts, err := db.QueryAll(benchTable, tags[i%numTags], base-1, base+int64(numTags)+1, nil)
		if err != nil {
			b.Fatalf("QueryAll: %v", err)
		}
		runtime.KeepAlive(pts)
	}
}

func itoa(n int) string {
	const digits = "0123456789"
	var buf [10]byte
	i := len(buf)
	for {
		i--
		buf[i] = digits[n%10]
		n /= 10
		if n == 0 {
			break
		}
	}
	return string(buf[i:])
}

// ─── 多角度查询基准 ────────────────────────────────────────────────
//
// BenchmarkQueryRange 覆盖"全量 / 前 20% / 后 20% / 中间 50%"四个时间窗，
// 位置轴（wal/disk）与规模轴（100K/1M）。不同时间窗的块命中模式不同：
// 前 20% 命中少数早期块（索引随机访问），全量走顺序扫描，后 20% 落在
// 最新段与 WAL 尾部——用于观察块选择、解码与 WAL 读取的差异。
func BenchmarkQueryRange(b *testing.B) {
	ranges := []struct {
		name string
		lo   float64
		hi   float64
	}{
		{"all", 0, 1},
		{"first20", 0, 0.2},
		{"last20", 0.8, 1},
		{"mid50", 0.25, 0.75},
	}
	for _, loc := range []struct {
		name string
		wal  int64
	}{{"wal", 256 << 20}, {"disk", 1 << 20}} {
		for _, n := range []int{100_000, 1_000_000} {
			for _, r := range ranges {
				b.Run(loc.name+"/"+r.name+"/"+itoa(n), func(b *testing.B) {
					db := benchQueryOpen(b, loc.wal)
					base := writePointsN(b, db, n, benchTag)
					start := base + int64(float64(n)*1e6*r.lo) - 1
					end := base + int64(float64(n)*1e6*r.hi) + 1
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						pts, err := db.QueryAll(benchTable, benchTag, start, end, nil)
						if err != nil {
							b.Fatalf("QueryAll: %v", err)
						}
						runtime.KeepAlive(pts)
					}
				})
			}
		}
	}
}

// BenchmarkQueryRangeIter 与 BenchmarkQueryRange 同矩阵，但走流式
// QueryIter（不物化结果），单独体现范围选择对迭代路径的影响。
func BenchmarkQueryRangeIter(b *testing.B) {
	ranges := []struct {
		name string
		lo   float64
		hi   float64
	}{
		{"all", 0, 1},
		{"first20", 0, 0.2},
		{"last20", 0.8, 1},
		{"mid50", 0.25, 0.75},
	}
	for _, loc := range []struct {
		name string
		wal  int64
	}{{"wal", 256 << 20}, {"disk", 1 << 20}} {
		for _, r := range ranges {
			b.Run(loc.name+"/"+r.name+"/1M", func(b *testing.B) {
				db := benchQueryOpen(b, loc.wal)
				const n = 1_000_000
				base := writePointsN(b, db, n, benchTag)
				start := base + int64(float64(n)*1e6*r.lo) - 1
				end := base + int64(float64(n)*1e6*r.hi) + 1
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					it, err := db.QueryIter(context.Background(), benchTable, benchTag, start, end, nil, nil)
					if err != nil {
						b.Fatalf("QueryIter: %v", err)
					}
					for {
						_, ok, err := it.Next()
						if err != nil {
							b.Fatalf("Next: %v", err)
						}
						if !ok {
							break
						}
					}
					if err := it.Close(); err != nil {
						b.Fatalf("Close: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkQueryConcurrentIter 流式查询的并发吞吐（与
// BenchmarkQueryConcurrent 同矩阵，但不物化结果），对照物化路径在并发下
// 的 GC/分配竞争差异。
func BenchmarkQueryConcurrentIter(b *testing.B) {
	for _, loc := range []struct {
		name string
		wal  int64
	}{{"wal", 256 << 20}, {"disk", 1 << 20}} {
		for _, workers := range []int{1, 2, 4, 8} {
			b.Run(loc.name+"/conc"+itoa(workers), func(b *testing.B) {
				db := benchQueryOpen(b, loc.wal)
				const n = 1_000_000
				base := writePointsN(b, db, n, benchTag)
				start, end := base-1, base+int64(n)*1e6
				b.SetParallelism(workers - 1)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						it, err := db.QueryIter(context.Background(), benchTable, benchTag, start, end, nil, nil)
						if err != nil {
							b.Fatalf("QueryIter: %v", err)
						}
						for {
							_, ok, err := it.Next()
							if err != nil {
								b.Fatalf("Next: %v", err)
							}
							if !ok {
								break
							}
						}
						if err := it.Close(); err != nil {
							b.Fatalf("Close: %v", err)
						}
					}
				})
			})
		}
	}
}

// BenchmarkQueryConcurrent 并发物化查询吞吐：同一 tag 全量查询，并发度
// 1/2/4/8（1 表示 RunParallel 默认全核）。体现 queryMute 共享读锁、
// readerCache、解码器池与 WAL 快照在并发读下的表现（ns/op = 1/吞吐）。
func BenchmarkQueryConcurrent(b *testing.B) {
	for _, loc := range []struct {
		name string
		wal  int64
	}{{"wal", 256 << 20}, {"disk", 1 << 20}} {
		for _, workers := range []int{1, 2, 4, 8} {
			b.Run(loc.name+"/conc"+itoa(workers), func(b *testing.B) {
				db := benchQueryOpen(b, loc.wal)
				const n = 1_000_000
				base := writePointsN(b, db, n, benchTag)
				start, end := base-1, base+int64(n)*1e6
				b.SetParallelism(workers - 1)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						pts, err := db.QueryAll(benchTable, benchTag, start, end, nil)
						if err != nil {
							b.Fatalf("QueryAll: %v", err)
						}
						runtime.KeepAlive(pts)
					}
				})
			})
		}
	}
}
