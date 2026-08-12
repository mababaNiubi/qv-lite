package benchmark

import "time"

// flushWalSize returns a WAL MaxFileSize (bytes) chosen so that the scenario
// performs roughly `target` flush cycles. The current engine only converts WAL
// data into compressed segments when the WAL rotates (NeedFlush), so without a
// small-enough WalSize most data stays in WAL files and the compression ratio
// would reflect the uncompressed WAL instead of segment encoding. DefaultSuite
// therefore sets WalSize explicitly per scenario.
func flushWalSize(pts int, vt ValueType, target int) int64 {
	perPt := int64(24) // estimated raw bytes per point (12-byte header + variant binary)
	switch vt {
	case StringHighEntropy:
		perPt = 55
	case StringLowCard:
		perPt = 26
	case StructTwoCol:
		perPt = 45
	}
	raw := int64(pts) * perPt
	if target < 1 {
		target = 1
	}
	if w := raw / int64(target); w > 0 {
		return w
	}
	return 64 * 1024 // floor: 64KB
}

// QuickSuite is a fast smoke set used by `go test` and `-quick`. It keeps the
// total point count small so the whole suite runs in a few seconds while still
// exercising single-write, batch-write, and string scenarios.
func QuickSuite() []Scenario {
	return []Scenario{
		{Name: "small-float-1tag-20k", Points: 20_000, Cardinality: 1, ValueType: FloatSlow,
			WalSize: flushWalSize(20_000, FloatSlow, 4), Query: QueryFull},
		{Name: "small-float-100tag-20k-batch", Points: 20_000, Cardinality: 100, ValueType: FloatWave,
			BatchSize: 1000, WalSize: flushWalSize(20_000, FloatWave, 4), Query: QueryFull},
		{Name: "small-string-10tag-10k", Points: 10_000, Cardinality: 10, ValueType: StringLowCard,
			WalSize: flushWalSize(10_000, StringLowCard, 4), Query: QueryFull},
	}
}

// DefaultSuite is the full comparison matrix defined in doc/02_testing.md. It
// covers data volume (small/medium/large), cardinality (incl. high-cardinality
// 100k tags), multiple flushes, bursty timestamps, strings, structs, async
// flush, aggregation queries, and a memory-limited run.
//
// Runtime: large scenarios take 1-3 minutes each; the full suite ~5-15 minutes.
// Use `-quick` or `-scenarios small,medium` to validate the pipeline first.
func DefaultSuite() []Scenario {
	sc := func(name string, pts, card int, vt ValueType, target int) Scenario {
		return Scenario{Name: name, Points: pts, Cardinality: card, ValueType: vt,
			WalSize: flushWalSize(pts, vt, target), Query: QueryFull}
	}
	return []Scenario{
		// 数据量梯度：小 / 中 / 大（各 ~10 次 flush）
		sc("small-float-1tag-100k", 100_000, 1, FloatSlow, 10),
		sc("medium-float-10tag-1M", 1_000_000, 10, FloatSlow, 10),
		sc("large-float-100tag-10M", 10_000_000, 100, FloatSlow, 10),

		// 高基数：10M 点 × 10 万 tag，验证内存与基数解耦（近端查询，读 1%）
		{Name: "highcard-float-100ktag-10M", Points: 10_000_000, Cardinality: 100_000,
			ValueType: FloatSlow, WalSize: flushWalSize(10_000_000, FloatSlow, 10),
			Query: QueryRecent},

		// 多次 flush：目标 ~100 次落盘，观察段文件数与碎片化
		{Name: "multiflush-int-10tag-1M-wal256k", Points: 1_000_000, Cardinality: 10,
			ValueType: IntCounter, WalSize: flushWalSize(1_000_000, IntCounter, 100), Query: QueryFull},

		// 突发时间模型
		{Name: "bursty-float-10tag-1M", Points: 1_000_000, Cardinality: 10,
			ValueType: FloatWave, TimestampModel: TSBursty,
			WalSize: flushWalSize(1_000_000, FloatWave, 10), Query: QueryFull},

		// 字符串低基数（字典压缩收益）
		{Name: "string-lowcard-10tag-1M", Points: 1_000_000, Cardinality: 10,
			ValueType: StringLowCard, WalSize: flushWalSize(1_000_000, StringLowCard, 10), Query: QueryFull},

		// 结构体列 + 批量写
		{Name: "struct-10tag-1M", Points: 1_000_000, Cardinality: 10,
			ValueType: StructTwoCol, BatchSize: 1000,
			WalSize: flushWalSize(1_000_000, StructTwoCol, 10), Query: QueryFull},

		// 异步 flush 路径
		{Name: "async-float-10tag-1M", Points: 1_000_000, Cardinality: 10,
			ValueType: FloatSlow, AsyncFlush: true,
			WalSize: flushWalSize(1_000_000, FloatSlow, 10), Query: QueryFull},

		// 聚合查询
		{Name: "window-float-10tag-1M", Points: 1_000_000, Cardinality: 10,
			ValueType: FloatSlow, WalSize: flushWalSize(1_000_000, FloatSlow, 10),
			Query: QueryWindow, WindowSize: int64(time.Minute)},

		// 内存受限：GOMEMLIMIT=128MB（资源受限设备模拟）
		{Name: "memlimited-float-10tag-1M", Points: 1_000_000, Cardinality: 10,
			ValueType: FloatSlow, WalSize: flushWalSize(1_000_000, FloatSlow, 10),
			MemoryBudget: 128 * 1024 * 1024, Query: QueryFull},

		// 写路径隔离：默认 WAL（不强制 flush），数据全留在 WAL 内存，测纯写路径
		// 与 WAL 内存占用（bytes/pt 侧记 segment+WAL 总量）。
		{Name: "single-float-1tag-1M-defaultwal", Points: 1_000_000, Cardinality: 1,
			ValueType: FloatSlow, WalSize: 0, Query: QueryFull},
		{Name: "batch-float-10tag-1M-defaultwal", Points: 1_000_000, Cardinality: 10,
			ValueType: FloatWave, BatchSize: 1000, WalSize: 0, Query: QueryFull},

		// block 层压缩权衡：zstd（默认）vs snappy vs none（资源受限设备的 CPU/压缩率取舍）
		{Name: "medium-float-10tag-1M-snappy", Points: 1_000_000, Cardinality: 10,
			ValueType: FloatSlow, WalSize: flushWalSize(1_000_000, FloatSlow, 10),
			CompressionName: "snappy", Query: QueryFull},
		{Name: "medium-float-10tag-1M-none", Points: 1_000_000, Cardinality: 10,
			ValueType: FloatSlow, WalSize: flushWalSize(1_000_000, FloatSlow, 10),
			CompressionName: "none", Query: QueryFull},
	}
}
