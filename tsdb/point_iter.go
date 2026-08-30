package tsdb

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"os"
	"sort"

	"github.com/mababaNiubi/variant"
)

// ─────────────────────────────────────────────────────────────────────
// point_iter.go —— 查询读路径：两相流式迭代
// ─────────────────────────────────────────────────────────────────────
//
// 一次单 tag 查询 = 「磁盘段(disk) + WAL」两个各自时间有序的源，先盘后 WAL
// 依次流式输出（历史 Query 的结果顺序），全程不物化全量结果：
//
//   - diskPointIter：按段迭代，块级「索引随机访问 vs 顺序扫描」二选一，逐块解压；
//   - walPointIter：缓冲模式读内存 chunk（惰性 tag 索引优先）；CloseBuffer 模式
//     按快照长度直接重读 WAL 文件；
//   - mergePointIter：对外流式接口（QueryIter），两相顺接 + 过滤/分页/超时；
//   - forEachQueryPoint：物化路径（Query/QueryAll/QueryWindow），把同一条流逐点
//     灌入 fn。
//
// 并发与快照语义（本文件的正确性根基）：
//
//   - 查询持有 table.queryMute.RLock（QueryIter 直到 Close；物化路径覆盖整个
//     walk），而 flushCache 在整段 flush（读 WAL → 编码 → 提交段 → 截断 WAL）
//     期间持有 queryMute.Lock。因此查询的快照与读取绝不会与段提交/WAL 截断
//     交错：快照里出现的文件在读取时必然仍存在且内容不变。
//   - WAL 快照在 ws.mutex 下拷贝（文件名 + 逻辑长度 + chunk 切片头），之后无锁
//     迭代：写路径只往已捕获长度之后追加，旋转/截断被 queryMute 排除。
//   - 段列表快照在段列表 RLock 下拷贝（RangeFromTime），flush 的提交/回滚持有
//     段列表写锁。
//   - CloseBuffer 模式下查询按快照中的「逻辑长度」读取文件，不会观察到并发
//     写者尚未完成的半条尾部记录（写路径先落盘、后更新长度，两者都在 ws.mutex
//     下，快照必然看到一致状态）。
//
// 超时与上限：ctx 每 ctxCheckMask+1 个点批量检查一次（摊薄逐点原子读）；
// QueryTimeout 在各入口派生 deadline；MaxQueryPoints 由调用方（table.go 的
// resultLimitFn）包装。所有迭代器错误粘滞：一旦出错，后续 next 直接返回该错。

// PointIter 是单 tag 查询的拉取式点流（对外接口）。
//
// 每个源内部按时间序输出：先磁盘段、后 WAL（历史 disk-then-WAL 的结果顺序）。
// 单调工作负载下合并流全局时间有序；WAL 中保留的乱序（晚到）数据可能在
// disk/WAL 交界处造成局部倒序，与流式重构前的行为一致。
//
// 迭代器持有表查询锁与它打开的 reader，调用方即使提前结束（limit 达成或 ctx
// 取消）也必须 Close。返回的 Point 按值传递且自包含（解码值拥有自己的内存），
// 下一次 Next 之后依然有效。
type PointIter interface {
	// Next 返回下一个命中点；ok=false 表示流已耗尽（或达到配置的 limit）。
	Next() (Point, bool, error)
	// Close 释放查询锁与迭代器持有的全部 reader。
	Close() error
}

// QueryOptions 限制一次查询。零值 = 不限。
type QueryOptions struct {
	// Limit 限制返回的点数（在 Offset 与条件过滤之后生效）。<=0 表示不限。
	Limit int
	// Offset 按时间序跳过前 Offset 个点再开始返回。只在 Limit>0 时有意义。
	Offset int64
}

// pointSource 是 disk/WAL 两侧共用的内部拉取接口，两个实现都按时间序输出点。
type pointSource interface {
	next() (Point, bool, error)
	close()
}

// ─── WAL 侧：快照与迭代 ─────────────────────────────────────────────

// walTagChunkSnapshot 是某个 WAL 读 chunk 的点内视图。切片头在 WAL 互斥锁下
// 拷贝，之后可无锁迭代：写路径只往已捕获长度之后追加、绝不改写已捕获区间，
// 缓冲区旋转/重置只发生在 flush 期间（被表查询锁排除）。
type walTagChunkSnapshot struct {
	keys       []tagCode
	timestamps []int64
	values     []variant.Variant
}

// walTagFileSnapshot 是一次查询对单个 WAL 文件的点内视图。
type walTagFileSnapshot struct {
	fileName    string         // 文件路径
	closeBuffer bool           // true：CloseBuffer 模式，查询直接从该文件重读
	length      int64          // 快照时刻该文件的有效字节数（closeBuffer 时按它读取）
	needsSort   bool           // 该文件是否保留了乱序（晚到）数据
	rb          *walReadBuffer // 惰性 tag 索引的活缓冲区（closeBuffer 时为 nil）
	indexable   int            // 快照时刻完整（不可变）chunk 的个数
	chunks      []walTagChunkSnapshot
}

// walTagSnapshot 是单次查询对 WAL 读缓存的点内、无竞态视图。只拷贝切片头，
// 开销小且不复制任何点数据。
type walTagSnapshot struct {
	files        []walTagFileSnapshot
	anyNeedsSort bool // 任一文件 needsSort → 走「收集-排序」回退
}

// snapshotTagTime 在 WAL 互斥锁下捕获读缓存视图（查询侧的唯一取快照入口）。
func (ws *walFile) snapshotTagTime() walTagSnapshot {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	snap := walTagSnapshot{files: make([]walTagFileSnapshot, 0, len(ws.walFiles))}
	for i := range ws.walFiles {
		ent := &ws.walFiles[i]
		fs := walTagFileSnapshot{
			fileName:    ent.fileName,
			closeBuffer: ws.config.CloseBuffer,
			length:      ent.length,
			needsSort:   ent.needsSort,
		}
		if fs.needsSort {
			snap.anyNeedsSort = true
		}
		if !fs.closeBuffer && ent.readBuffer != nil {
			rb := ent.readBuffer
			fs.rb = rb
			fs.indexable = rb.activeChunks - 1
			fs.chunks = make([]walTagChunkSnapshot, 0, rb.activeChunks)
			for c := 0; c < rb.activeChunks; c++ {
				chunk := &rb.chunks[c]
				fs.chunks = append(fs.chunks, walTagChunkSnapshot{
					keys:       chunk.keys,
					timestamps: chunk.timestamps,
					values:     chunk.values,
				})
			}
		}
		snap.files = append(snap.files, fs)
	}
	return snap
}

// walPointIter 按时间序流式输出单 tag 的 WAL 点，不物化结果。当任一文件保留
// 了乱序数据时退化为「收集-排序」回退（与旧 ReadByTime 语义一致，内存上限 =
// 该查询命中的 WAL 点数）。
type walPointIter struct {
	snap      walTagSnapshot
	tag       tagCode
	startTime int64
	endTime   int64

	// ctx 每 ctxCheckMask+1 次调用批量检查一次（逐点原子读在 1M 点规模下约
	// 多花 5-10ms）。
	ctx     context.Context
	ctxTick int

	// needsSort 回退：全量收集并排序后的命中点。
	sorted    []Point
	sortedIdx int

	// snap.files 上的流式游标。
	fileIdx   int     // 当前文件
	chunkIdx  int     // 当前文件内的 chunk
	posIdx    int     // 当前 chunk 内的位置（索引模式指向 positions，扫描模式指向 keys）
	positions []int32 // 当前 chunk 的惰性索引位置表（nil = 扫描模式）

	// CloseBuffer（重读文件）流式状态。每个文件独立打开，文件切换时关闭旧句柄
	// 并重置 remaining —— 曾因复用句柄导致快照首文件之后的文件被静默跳过。
	f           *os.File
	br          *bufio.Reader
	curFileName string // 当前已打开的文件名
	remaining   int64  // 当前文件剩余待读字节数（按快照长度约束）
	dataBuf     []byte // 记录解码复用缓冲（跨文件/记录复用）

	err error // 粘滞错误：首次出错后所有 next 直接返回它
}

func newWalPointIter(snap walTagSnapshot, tag tagCode, startTime, endTime int64, ctx context.Context) *walPointIter {
	w := &walPointIter{
		snap:      snap,
		tag:       tag,
		startTime: startTime,
		endTime:   endTime,
		ctx:       ctx,
	}
	if !snap.anyNeedsSort {
		// 常规（全部有序）路径：直接流式迭代。
		return w
	}
	// 罕见乱序路径：与旧 ReadByTime 语义一致 —— 收集全部命中点后全局排序。
	for i := range snap.files {
		file := &snap.files[i]
		if file.closeBuffer {
			w.collectFile(file)
			continue
		}
		for c := range file.chunks {
			w.appendSortedChunk(file, c)
		}
	}
	sort.Slice(w.sorted, func(i, j int) bool { return w.sorted[i].Tms < w.sorted[j].Tms })
	return w
}

// next 返回下一个命中点；ok=false 表示流已耗尽。
func (w *walPointIter) next() (Point, bool, error) {
	if w.err != nil {
		return Point{}, false, w.err
	}
	if w.ctx != nil {
		if w.ctxTick&ctxCheckMask == 0 {
			if err := w.ctx.Err(); err != nil {
				w.err = err
				return Point{}, false, err
			}
		}
		w.ctxTick++
	}
	if w.sorted != nil {
		// 乱序回退：直接吐出排序缓冲。
		if w.sortedIdx < len(w.sorted) {
			p := w.sorted[w.sortedIdx]
			w.sortedIdx++
			return p, true, nil
		}
		return Point{}, false, nil
	}
	for w.fileIdx < len(w.snap.files) {
		file := &w.snap.files[w.fileIdx]
		if file.closeBuffer {
			p, ok, err := w.nextFromFile(file)
			if err != nil {
				w.err = err
				return Point{}, false, err
			}
			if ok {
				return p, true, nil
			}
			w.fileIdx++ // 当前文件读尽：换下一个
			continue
		}
		// 缓冲模式：逐 chunk 迭代（索引优先、扫描兜底）。
		if p, ok := w.nextBufferedFile(file); ok {
			return p, true, nil
		}
		// 当前文件耗尽：重置 chunk 游标并换下一个文件。
		w.fileIdx++
		w.chunkIdx = 0
		w.posIdx = 0
		w.positions = nil
	}
	return Point{}, false, nil
}

// nextBufferedFile 消费当前文件（CloseBuffer 关闭、走内存 chunk）。返回
// (点, true) 表示命中；返回 (_, false) 表示文件已耗尽，调用方推进 fileIdx。
func (w *walPointIter) nextBufferedFile(file *walTagFileSnapshot) (Point, bool) {
	for w.chunkIdx < len(file.chunks) {
		chunk := &file.chunks[w.chunkIdx]
		if w.positions != nil {
			// 索引模式：按惰性索引的位置表取点；表耗尽或整块在时间窗外则进入
			// 下一 chunk。
			if p, ok := w.nextIndexedChunk(file, chunk); ok {
				return p, true
			}
			w.endChunk()
			continue
		}
		// 完整 chunk 优先建立/复用惰性 tag 索引；快照时仍在增长的 active chunk
		// 永远直接扫描。
		if w.tryLazyIndex(file) {
			continue
		}
		// 扫描兜底：逐条比对 key 与时间窗。
		if p, ok := w.nextScannedChunk(chunk); ok {
			return p, true
		}
		w.endChunk()
	}
	return Point{}, false
}

// nextIndexedChunk 从惰性索引位置表取出当前 chunk 的下一个命中点。有序文件的
// 位置按时间单调：可先用首/尾位置的时间戳整块跳过，超出查询终点即提前结束
// 本块。
func (w *walPointIter) nextIndexedChunk(file *walTagFileSnapshot, chunk *walTagChunkSnapshot) (Point, bool) {
	ordered := !file.needsSort
	if ordered && len(w.positions) > 0 {
		if chunk.timestamps[w.positions[0]] > w.endTime ||
			chunk.timestamps[w.positions[len(w.positions)-1]] < w.startTime {
			return Point{}, false // 整块在查询窗外
		}
	}
	for w.posIdx < len(w.positions) {
		j := w.positions[w.posIdx]
		w.posIdx++
		ts := chunk.timestamps[j]
		if ts > w.endTime {
			if ordered {
				return Point{}, false // 单调：后续位置时间更大，提前结束本块
			}
			continue
		}
		if ts < w.startTime {
			continue
		}
		return Point{Tms: ts, V: chunk.values[j]}, true
	}
	return Point{}, false
}

// tryLazyIndex 为当前完整 chunk 建立（或复用）惰性 tag 索引。返回 true 表示
// 调用方应重启 chunk 循环：可能已切到索引模式，也可能因本块无该 tag 而前进到
// 下一 chunk。返回 false 表示无索引可用（转扫描兜底）。
func (w *walPointIter) tryLazyIndex(file *walTagFileSnapshot) bool {
	if file.rb == nil || w.chunkIdx >= file.indexable {
		return false
	}
	idx := file.rb.tagIndex(w.chunkIdx, file.chunks[w.chunkIdx].keys)
	if idx == nil {
		return false
	}
	if pos := idx[w.tag]; len(pos) > 0 {
		w.positions = pos
		w.posIdx = 0
		return true
	}
	w.chunkIdx++ // 已建索引且本块无该 tag：直接跳过
	w.posIdx = 0
	return true
}

// nextScannedChunk 顺序扫描 chunk 的 key/时间/值，返回下一个命中点。
func (w *walPointIter) nextScannedChunk(chunk *walTagChunkSnapshot) (Point, bool) {
	for w.posIdx < len(chunk.keys) {
		key := chunk.keys[w.posIdx]
		ts := chunk.timestamps[w.posIdx]
		val := chunk.values[w.posIdx]
		w.posIdx++
		if key != w.tag || ts < w.startTime || ts > w.endTime {
			continue
		}
		return Point{Tms: ts, V: val}, true
	}
	return Point{}, false
}

// endChunk 结束当前 chunk：清索引状态并前进到下一 chunk。
func (w *walPointIter) endChunk() {
	w.positions = nil
	w.chunkIdx++
	w.posIdx = 0
}

// nextFromFile 从 WAL 文件流式读记录（CloseBuffer 模式），只读到快照长度为止，
// 以免观察到并发写者的半条尾部记录。每个文件独立打开：文件切换时必须关闭旧
// 句柄并重置 remaining —— 曾因复用句柄导致快照首文件之后的文件被静默跳过
// （连续性尾部丢点、且不报任何错误）。
//
// 记录帧循环刻意内联（不抽公共函数）：实测每记录函数调用比内联慢约 3.5ns，
// 在 CloseBuffer 读路径（E2E ≈105ns/点）上约 3%。collectFile（冷路径）复用
// readWalRecord。
func (w *walPointIter) nextFromFile(file *walTagFileSnapshot) (Point, bool, error) {
	if w.f == nil || w.curFileName != file.fileName {
		if w.f != nil {
			_ = w.f.Close()
			w.f = nil
		}
		f, err := os.Open(file.fileName)
		if err != nil {
			return Point{}, false, err
		}
		w.f = f
		w.br = bufio.NewReader(f)
		w.remaining = file.length
		w.curFileName = file.fileName
		if cap(w.dataBuf) < 64*1024 {
			w.dataBuf = make([]byte, 64*1024)
		}
	}
	for w.remaining > 0 {
		var lenBuf [4]byte
		if _, err := io.ReadFull(w.br, lenBuf[:]); err != nil {
			return Point{}, false, err
		}
		w.remaining -= 4
		length := binary.BigEndian.Uint32(lenBuf[:])
		if length <= 12 {
			return Point{}, false, ErrorWALDataCorruption
		}
		if int(length) > cap(w.dataBuf) {
			w.dataBuf = make([]byte, length)
		}
		data := w.dataBuf[:length]
		if _, err := io.ReadFull(w.br, data); err != nil {
			return Point{}, false, err
		}
		w.remaining -= int64(length)
		key := tagCode(binary.BigEndian.Uint32(data[0:4]))
		ts := int64(binary.BigEndian.Uint64(data[4:12]))
		if key != w.tag || ts < w.startTime || ts > w.endTime {
			continue
		}
		value, _, err := unmarshalWalValue(data[12:])
		if err != nil {
			return Point{}, false, err
		}
		return Point{Tms: ts, V: value}, true, nil
	}
	return Point{}, false, nil
}

// collectFile 把单个 WAL 文件中命中 tag 与时间窗的点收集进 sorted 回退缓冲
// （CloseBuffer + 乱序路径）。
func (w *walPointIter) collectFile(file *walTagFileSnapshot) {
	f, err := os.Open(file.fileName)
	if err != nil {
		w.err = err
		return
	}
	defer f.Close()
	br := bufio.NewReader(f)
	remaining := file.length
	var data []byte
	for remaining > 0 {
		key, ts, payload, err := readWalRecord(br, &remaining, &data)
		if err != nil {
			w.err = err
			return
		}
		if key != w.tag || ts < w.startTime || ts > w.endTime {
			continue
		}
		value, _, err := unmarshalWalValue(payload)
		if err != nil {
			w.err = err
			return
		}
		w.sorted = append(w.sorted, Point{Tms: ts, V: value})
	}
}

// appendSortedChunk 把单个 chunk 中命中 tag 与时间窗的点追加进 sorted 缓冲
// （乱序回退路径；与流式路径共用惰性索引）。
func (w *walPointIter) appendSortedChunk(file *walTagFileSnapshot, c int) {
	chunk := &file.chunks[c]
	if file.rb != nil && c < file.indexable {
		if idx := file.rb.tagIndex(c, file.chunks[c].keys); idx != nil {
			for _, j := range idx[w.tag] {
				ts := chunk.timestamps[j]
				if ts >= w.startTime && ts <= w.endTime {
					w.sorted = append(w.sorted, Point{Tms: ts, V: chunk.values[j]})
				}
			}
			return
		}
	}
	for j := range chunk.keys {
		if chunk.keys[j] != w.tag {
			continue
		}
		ts := chunk.timestamps[j]
		if ts >= w.startTime && ts <= w.endTime {
			w.sorted = append(w.sorted, Point{Tms: ts, V: chunk.values[j]})
		}
	}
}

// readWalRecord 读取一条 WAL 记录帧：[4B 总长][4B key][8B ts][值负载]。读取量
// 受 *remaining 字节预算约束（调用方按快照长度初始化），保证不会越界读到并发
// 写者尚未完成的部分记录。成功时返回 key、ts 与负载切片；负载由 *dataBuf
// 复用，仅在下次调用前有效。
// 仅供冷路径使用（collectFile 乱序回退）：热路径 nextFromFile 刻意内联帧循环
// （实测函数调用每记录慢约 3.5ns，见 nextFromFile 注释）。
func readWalRecord(br *bufio.Reader, remaining *int64, dataBuf *[]byte) (tagCode, int64, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
		return 0, 0, nil, err
	}
	*remaining -= 4
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length <= 12 {
		return 0, 0, nil, ErrorWALDataCorruption
	}
	if int(length) > cap(*dataBuf) {
		*dataBuf = make([]byte, length)
	}
	data := (*dataBuf)[:length]
	if _, err := io.ReadFull(br, data); err != nil {
		return 0, 0, nil, err
	}
	*remaining -= int64(length)
	key := tagCode(binary.BigEndian.Uint32(data[0:4]))
	ts := int64(binary.BigEndian.Uint64(data[4:12]))
	return key, ts, data[12:], nil
}

// unmarshalWalValue 解码 WAL 值负载（二进制或 JSON 两种格式）。
func unmarshalWalValue(payload []byte) (variant.Variant, int, error) {
	if variant.IsBinaryFormat(payload) {
		return variant.UnmarshalBinary(payload)
	}
	v, err := variant.UnmarshalJSON(payload)
	return v, 0, err
}

// close 释放已打开的 WAL 文件句柄（如有）。
func (w *walPointIter) close() {
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
}

// ─── ctx 批量检查 ─────────────────────────────────────────────────────

// ctxCheckMask 控制 ctx 批量检查粒度：每 4096 次调用检查一次（逐点原子读在
// 1M 点规模下约多花 5-10ms），超时响应延迟 ≤ 一个检查周期（≈ 物化 4096 点）。
const ctxCheckMask = 4095

// 三个 next()（wal/disk/merge）都在逐点热路径上内联展开 ctx 批量检查：每
// ctxCheckMask+1 次调用检查一次 ctx 是否已取消/超时。必须保持内联 —— 抽成
// 独立函数实测每点慢约 1.4ns（该函数成本 89 超内联预算 80，编译器不会自动
// 内联）。

// ─── Disk 侧：段/块迭代 ─────────────────────────────────────────────

// diskPointIter 按段、块顺序拉取单 tag 的磁盘点，任意时刻只解压一个块。每个
// 查询使用独立的 FileReader（旧路径共享段内可变 reader，在 dataBuf 与
// readEffectiveOffset 上竞态，且永久滞留最大块缓冲）。
type diskPointIter struct {
	table     *ssTable
	code      tagCode
	startTime int64
	endTime   int64
	ctx       context.Context
	ctxTick   int // next() 调用计数，用于批量 ctx 检查

	segments []FileSegment // 查询范围命中的段（构造时在段列表锁下快照）
	segIdx   int

	// 当前段状态。
	fs           FileSegment
	reader       FileReader // 每查询一个，跨段 rebind 复用
	readerOpened bool
	idx          *FileIndex
	positions    []int // 索引模式：idx.Blocks 内的位置表
	posIdx       int
	scanning     bool // true = 顺序扫描；false = 索引随机访问

	pack *PointDiskPack // 块解压 + 解码器池
	err  error          // 粘滞错误
}

// next 返回下一个磁盘点；ok=false 表示所有命中段均已读完。
func (d *diskPointIter) next() (Point, bool, error) {
	if d.err != nil {
		return Point{}, false, d.err
	}
	for {
		if d.ctx != nil {
			if d.ctxTick&ctxCheckMask == 0 {
				if err := d.ctx.Err(); err != nil {
					d.err = err
					return Point{}, false, err
				}
			}
			d.ctxTick++
		}
		if d.pack.Next() {
			tms, value := d.pack.Read()
			return Point{Tms: tms, V: value}, true, nil
		}
		// 当前块解完：取下一个命中块（无则整体结束）。
		d.pack.Reset()
		head, td, vd, err := d.nextBlock()
		if err != nil {
			d.err = err
			return Point{}, false, err
		}
		if head == nil {
			return Point{}, false, nil
		}
		if e := d.pack.AddSegment(td, vd); e != nil {
			d.err = e
			return Point{}, false, e
		}
	}
}

// nextBlock 前进到下一个命中块（复刻 forEachBlock 的索引-vs-扫描决策），返回
// 该块解压后的时间/值字节。
func (d *diskPointIter) nextBlock() (*SegmentHeader, []byte, []byte, error) {
	for {
		if d.fs == nil && !d.nextSegment() {
			return nil, nil, nil, nil
		}
		if d.scanning {
			// 顺序扫描：逐块 NextReadFilter（内部按 tag/时间窗过滤）。
			if !d.readerOpened {
				if err := d.reader.OpenReader(); err != nil {
					return nil, nil, nil, err
				}
				d.readerOpened = true
			}
			head, td, vd, err := d.reader.NextReadFilter(d.code, d.startTime, d.endTime, &d.table.tableInfo)
			if err != nil {
				return nil, nil, nil, err
			}
			if head == nil {
				d.endSegment()
				continue
			}
			return head, td, vd, nil
		}
		// 索引模式：按块位置随机访问。
		if d.posIdx < len(d.positions) {
			block := &d.idx.Blocks[d.positions[d.posIdx]]
			d.posIdx++
			head, td, vd, err := d.reader.ReadAt(block.Offset, &d.table.tableInfo)
			if err != nil {
				return nil, nil, nil, err
			}
			if head == nil {
				continue
			}
			return head, td, vd, nil
		}
		d.endSegment()
	}
}

// nextSegment 前进到下一个含命中块的段；无则返回 false。
func (d *diskPointIter) nextSegment() bool {
	d.endSegment()
	for d.segIdx < len(d.segments) {
		fs := d.segments[d.segIdx]
		d.segIdx++
		if fs == nil {
			continue
		}
		if d.bindSegment(fs) {
			return true
		}
	}
	return false
}

// bindSegment 为当前段选定读取模式（索引随机访问 vs 顺序扫描）并把每查询
// reader 绑定到该段文件。返回 false 表示该段与查询范围无交集（调用方继续找
// 下一段）。
func (d *diskPointIter) bindSegment(fs FileSegment) bool {
	idx := fs.GetIndex()
	if idx != nil && len(idx.Blocks) > 0 {
		if d.startTime > idx.MaxTime || d.endTime < idx.MinTime {
			return false
		}
		matching := idx.matchingBlockPositions(d.code, d.startTime, d.endTime)
		if len(matching) == 0 {
			return false
		}
		// 命中块占大多数（或超过阈值）→ 顺序扫描更划算；否则按索引随机访问。
		if len(matching) > len(idx.Blocks)/2 || len(matching) > 100 {
			d.scanning = true
		} else {
			d.scanning = false
			d.idx = idx
			d.positions = matching
			d.posIdx = 0
		}
	} else {
		// 无索引：退化为顺序扫描。
		d.scanning = true
	}
	// 每查询一个 FileReader：首次创建，后续 rebind 到新段文件，块缓冲跨段复用
	//（历史共享 reader 路径跨查询滞留缓冲；逐块重建又实测拖慢大块重复查询）。
	if d.reader == nil {
		if seg, ok := fs.(*fileSegment); ok {
			d.reader = NewFileReader(seg.filePath, seg.compressor, d.table.fragmentation.readerCache)
		} else {
			// 非 fileSegment 实现：复用段对象自带的 reader。调用方必须保证该
			// reader 支持并发查询各自持有（生产代码仅 *fileSegment 走每查询
			// 新建路径，此分支用于其它 FileSegment 实现）。
			d.reader = fs.(FileReader)
		}
	} else if seg, ok := fs.(*fileSegment); ok {
		// 把每查询 reader 重绑定到本段文件。
		fr := d.reader.(*fileReader)
		fr.filePath = seg.filePath
		fr.compressor = seg.compressor
	}
	d.fs = fs
	return true
}

// endSegment 结束当前段：清理段级状态。reader 保持打开（OpenReader 下次会
// rebind 到新文件），只在 close() 统一释放。
func (d *diskPointIter) endSegment() {
	d.readerOpened = false
	d.fs = nil
	d.idx = nil
	d.positions = nil
	d.posIdx = 0
	d.scanning = false
}

// close 释放 reader 与解码器。
func (d *diskPointIter) close() {
	if d.reader != nil {
		d.reader.CloseReader()
	}
	d.readerOpened = false
	d.reader = nil
	d.fs = nil
	d.idx = nil
	d.positions = nil
	if d.pack != nil {
		d.pack.Reset()
	}
}

// ─── 流式消费（walk）─────────────────────────────────────────────────

// walkFiltered 是带条件过滤/offset/limit 的慢路径，disk/wal 两个源共用。
// 逐点要调用 evalCond（variant 比较）或走分页分支，分发开销可忽略，故可安全
// 经 pointSource 接口间接调用。
func walkFiltered(src pointSource, evalCond ConditionFilter, limit int, offset int64, emitted, skipped *int64, fn func(Point) bool) error {
	for {
		p, ok, err := src.next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if evalCond != nil {
			ok, err := evalCond(p.V)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}
		if *skipped < offset {
			*skipped++
			continue
		}
		if limit > 0 && *emitted >= int64(limit) {
			return nil
		}
		*emitted++
		if !fn(p) {
			return nil
		}
	}
}

// walk 把 disk 源逐点送入 fn，应用条件过滤与 offset/limit。
//
// 无过滤/无分页的常见路径必须在具体类型上直调 next（不经过接口/泛型分发）：
// 泛型字典分发实测约 10.8ns/点、直调约 0.55ns/点（同进程微基准），1M 点查询
// 相差 ~10ms。这也是 disk/wal 两个 walk 快路径刻意重复的原因。
func (d *diskPointIter) walk(evalCond ConditionFilter, limit int, offset int64, emitted, skipped *int64, fn func(Point) bool) error {
	if evalCond == nil && limit <= 0 && offset <= 0 {
		// 热路径：无分支循环，next 直调（编译器可内联）。
		for {
			p, ok, err := d.next()
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			if !fn(p) {
				return nil
			}
		}
	}
	return walkFiltered(d, evalCond, limit, offset, emitted, skipped, fn)
}

// walk 把 WAL 源逐点送入 fn，应用条件过滤与 offset/limit（与 disk 版本同构，
// 见其注释中关于快路径直调的理由）。
func (w *walPointIter) walk(evalCond ConditionFilter, limit int, offset int64, emitted, skipped *int64, fn func(Point) bool) error {
	if evalCond == nil && limit <= 0 && offset <= 0 {
		for {
			p, ok, err := w.next()
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			if !fn(p) {
				return nil
			}
		}
	}
	return walkFiltered(w, evalCond, limit, offset, emitted, skipped, fn)
}

// ─── 两相合并（对外流式接口）────────────────────────────────────────

// mergePointIter 先流 disk 源、再流 WAL 源，沿途应用条件过滤、offset/limit 与
// ctx 取消。两个源各自时间有序，单调工作负载下合并流全局时间有序，与历史
// disk→WAL 结果顺序一致，且不物化结果。曾尝试逐点 k 路归并，结果型查询实测
// 回退（每点约 30ns 状态机开销），故两相改为首尾顺接。
type mergePointIter struct {
	a, b pointSource
	cur  pointSource // 当前源：a 耗尽后切 b
	done bool        // b 已耗尽（或 limit 达成）

	evalCond ConditionFilter
	ctx      context.Context
	ctxTick  int
	err      error

	limit   int
	offset  int64
	emitted int64
	skipped int64

	release func()             // 释放表查询锁（queryMute.RUnlock）
	cancel  context.CancelFunc // 可选：QueryTimeout 派生的 cancel
}

// next 返回下一个命中点；ok=false 表示流耗尽或达到 limit。
func (m *mergePointIter) next() (Point, bool, error) {
	for {
		if m.err != nil {
			return Point{}, false, m.err
		}
		if m.limit > 0 && m.emitted >= int64(m.limit) {
			return Point{}, false, nil // 达 limit：正常结束
		}
		if m.ctx != nil {
			if m.ctxTick&ctxCheckMask == 0 {
				if err := m.ctx.Err(); err != nil {
					m.err = err
					return Point{}, false, err
				}
			}
			m.ctxTick++
		}
		if m.cur == nil {
			m.cur = m.a // 首次调用：从 disk 源开始
		}
		p, ok, err := m.cur.next()
		if err != nil {
			m.err = err
			return Point{}, false, err
		}
		if !ok {
			// 当前源耗尽：disk 之后切 WAL；WAL 耗尽即整体结束。
			if m.cur == m.b || m.done {
				m.done = true
				return Point{}, false, nil
			}
			m.cur = m.b
			continue
		}
		if m.evalCond != nil {
			ok, err := m.evalCond(p.V)
			if err != nil {
				m.err = err
				return Point{}, false, err
			}
			if !ok {
				continue
			}
		}
		if m.skipped < m.offset {
			m.skipped++
			continue
		}
		m.emitted++
		return p, true, nil
	}
}

// close 依次释放两个源、表查询锁与 timeout cancel。
func (m *mergePointIter) close() {
	m.a.close()
	m.b.close()
	if m.release != nil {
		m.release()
		m.release = nil
	}
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
}

// Next 实现 PointIter。
func (m *mergePointIter) Next() (Point, bool, error) {
	return m.next()
}

// Close 实现 PointIter。
func (m *mergePointIter) Close() error {
	m.close()
	return nil
}

// ─── 查询入口 ────────────────────────────────────────────────────────

// queryIter 为单 tag 创建时间有序、条件过滤的点流（QueryIter 使用）。迭代器
// 持有表查询锁直到 Close —— 同时保证并发 flush 不会在查询中途截断段/WAL。
// 调用方必须 Close。配置了 QueryTimeout 时从调用方 ctx 派生 deadline。
func (s *ssTable) queryIter(ctx context.Context, code tagCode, startTime, endTime int64, evalCond ConditionFilter, opts *QueryOptions) PointIter {
	var cancel context.CancelFunc
	if s.queryTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.queryTimeout)
	}
	s.queryMute.RLock()
	disk := newDiskIter(s, code, startTime, endTime, ctx)
	m := &mergePointIter{
		a:        disk,
		b:        newWalPointIter(s.walFile.snapshotTagTime(), code, startTime, endTime, ctx),
		evalCond: evalCond,
		ctx:      ctx,
		release:  s.queryMute.RUnlock,
		cancel:   cancel,
	}
	if opts != nil {
		m.limit = opts.Limit
		m.offset = opts.Offset
	}
	return m
}

// newDiskIter 构造 disk 源：在段列表锁下快照命中段（表查询锁持有期间始终有效）。
func newDiskIter(s *ssTable, code tagCode, startTime, endTime int64, ctx context.Context) *diskPointIter {
	disk := &diskPointIter{
		table:     s,
		code:      code,
		startTime: startTime,
		endTime:   endTime,
		ctx:       ctx,
		pack:      NewPointDiskPack(s.tableInfo.Structure, startTime, endTime),
	}
	s.fragmentation.RangeFromTime(startTime, endTime, func(fs FileSegment) bool {
		disk.segments = append(disk.segments, fs)
		return true
	})
	return disk
}

// compileCond 对 nil 条件返回 nil，使 walk 的逐点求值调用被整体跳过
// （CompileCondition(nil) 会返回恒真闭包，仍有点级调用开销）。
func compileCond(cond any) ConditionFilter {
	if cond == nil {
		return nil
	}
	return CompileCondition(cond)
}

// forEachQueryPoint 驱动两相点流（先 disk 后 WAL），持有表查询锁，应用条件
// 过滤、offset/limit，并把每个点送入 fn。这是 Query/QueryAll/QueryWindow 的
// 物化路径：阶段顺序与历史 disk→WAL 结果顺序一致，但全程不物化全量结果。
// QueryTimeout 配置时从 ctx（物化路径为表上下文）派生 deadline。
func (s *ssTable) forEachQueryPoint(ctx context.Context, code tagCode, startTime, endTime int64, evalCond ConditionFilter, opts *QueryOptions, fn func(Point) bool) error {
	if s.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.queryTimeout)
		defer cancel()
	}
	// 查询锁 + 双源快照：见文件头「并发与快照语义」。
	s.queryMute.RLock()
	defer s.queryMute.RUnlock()
	disk := newDiskIter(s, code, startTime, endTime, ctx)
	defer disk.close()
	wal := newWalPointIter(s.walFile.snapshotTagTime(), code, startTime, endTime, ctx)
	defer wal.close()
	limit := 0
	offset := int64(0)
	if opts != nil {
		limit = opts.Limit
		offset = opts.Offset
	}
	var emitted, skipped int64
	if err := disk.walk(evalCond, limit, offset, &emitted, &skipped, fn); err != nil {
		return err
	}
	if limit > 0 && emitted >= int64(limit) {
		return nil
	}
	return wal.walk(evalCond, limit, offset, &emitted, &skipped, fn)
}
