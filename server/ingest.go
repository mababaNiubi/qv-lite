package server

import (
	"unsafe"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

// streamBatchSize 是流式写入单次触发入库的总点数阈值。攒满即把当前各表的
// 累积点统一入库：内存恒定（≈一批），传输与入库并行，引擎每次锁时长短，
// 不随请求总点数增长而积压。
const streamBatchSize = 50_000

// StreamIngestor 实现「边解码边分批入库」。
//
// 应对不可控的客户端：单连接一次性上传统千万点（或任意多）时，不再等整个
// body 到达并整体反序列化后才写，而是每攒满 streamBatchSize 点立即入库。
// 从而：
//   - 内存峰值恒定（≈ streamBatchSize，不随请求总点数量级增长）；
//   - 第一个字节到达即开始入库，传输与写入并行；
//   - 引擎每次持锁时长有限，不长时间阻塞其他连接的写/读。
//
// 多表处理：按表分别累积（Line Protocol 支持多表），待入库总点数达到阈值
// 时统一按表入库。不再「表切换即刷」——连续交替的多表流（A B A B…）不会
// 退化成逐行小写，各表攒够一批再写，入库效率与单表流一致。
type StreamIngestor struct {
	ingest func(table string, points []tsdb.TagPoint) (int, error)
	size   int

	firstHint int                        // 首个表的初始容量提示（二进制路径已知点数时预分配，减少扩容拷贝）
	pending   map[string][]tsdb.TagPoint // 表 → 待入库点（各表独立累积）
	pendingN  int                        // 待入库总点数（触发阈值）
	written   int                        // 已入库点数（响应）

	internMap map[string]string // 请求内字符串驻留：内容相同的 tag/表名只保留一份

	// 上次命中缓存：连续同 tag/同表（二进制/单表流最常见的形态）时免哈希查找。
	lastKey   string // intern/internBytes 上次命中的规范键（真实字符串，非复用缓冲视图）
	lastVal   string // 对应的驻留副本
	lastTable string // pending 上次命中的表
	lastPts   []tsdb.TagPoint
}

// newStreamIngestor 创建流式入库器，ingest 为实际入库函数
// （s.ingest：直接写引擎或经流水线）。
func (s *Server) newStreamIngestor() *StreamIngestor {
	return &StreamIngestor{
		ingest:  s.ingest,
		size:    streamBatchSize,
		pending: make(map[string][]tsdb.TagPoint, 4),
	}
}

// Add 追加一个点；待入库总点数达到阈值时统一入库（按表分组）。
func (g *StreamIngestor) Add(table string, p tsdb.TagPoint) error {
	table = g.intern(table)
	p.Tag = g.intern(p.Tag)
	pts := g.pendingSlice(table)
	pts = append(pts, p)
	g.pending[table] = pts
	if table == g.lastTable {
		g.lastPts = pts // 追加可能扩容换新数组，保持快路径缓存一致
	}
	g.pendingN++
	if g.pendingN >= g.size {
		return g.flush()
	}
	return nil
}

// pendingSlice 返回 table 的待入库切片；连续同表时命中快路径，免 map 查找。
func (g *StreamIngestor) pendingSlice(table string) []tsdb.TagPoint {
	if table == g.lastTable {
		return g.lastPts
	}
	pts, ok := g.pending[table]
	if !ok {
		capHint := 1024
		if g.firstHint > 0 && len(g.pending) == 0 {
			capHint = g.firstHint // 首个表按已知请求规模预分配
		}
		pts = make([]tsdb.TagPoint, 0, capHint)
		g.pending[table] = pts
	}
	g.lastTable = table
	g.lastPts = pts
	return pts
}

// Finish 写入残余的一批并返回总写入点数。
func (g *StreamIngestor) Finish() (int, error) {
	if err := g.flush(); err != nil {
		return g.written, err
	}
	return g.written, nil
}

// flush 把当前各表累积的点统一入库（逐表调用 ingest）。各表写入互不影响，
// 顺序无关；map 遍历顺序随机不影响正确性。ingest 同步返回（流水线为入队、
// 立即返回），返回后缓冲切片可安全复用（Submit 复制语义；直接写引擎则
// WriteBatch 已完成）。
func (g *StreamIngestor) flush() error {
	if g.pendingN == 0 {
		return nil
	}
	for table, pts := range g.pending {
		if len(pts) == 0 {
			continue
		}
		n, err := g.ingest(table, pts)
		if err != nil {
			return err
		}
		g.written += n
		g.pendingN -= len(pts)
		pts = pts[:0] // 复用缓冲：已入队/已写完，安全
		g.pending[table] = pts
		if table == g.lastTable {
			g.lastPts = pts // 保持快路径缓存与 map 一致（[:0] 后仍复用同一数组）
		}
	}
	return nil
}

// intern 返回 s 的请求内规范副本：内容相同的 tag/表名只保留一份字符串，
// 大幅降低堆上重复字符串对象数量与 GC 标记扫描压力（时序流式写入中，
// 同一个请求的 tag/表名高度重复）。连续相同输入走 lastKey 快路径，免哈希。
func (g *StreamIngestor) intern(s string) string {
	if s == "" {
		return s
	}
	if s == g.lastKey {
		return g.lastVal
	}
	if v, ok := g.internMap[s]; ok {
		g.lastKey, g.lastVal = s, v
		return v
	}
	if g.internMap == nil {
		g.internMap = make(map[string]string, 16)
	}
	g.internMap[s] = s
	g.lastKey, g.lastVal = s, s
	return s
}

// internBytes 零拷贝驻留查找：以 b 的只读视图做比较/查找（不拷贝），
// 命中返回既有副本（零分配）；未命中才拷贝一次并驻留。供二进制路径复用
// 读缓冲时使用（读缓冲随后被覆写，必须在此复制）。
// 注意：lastKey 只存真实字符串副本（b 可能指向复用缓冲，不能存其视图）。
func (g *StreamIngestor) internBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if len(b) == len(g.lastKey) && unsafe.String(unsafe.SliceData(b), len(b)) == g.lastKey {
		return g.lastVal
	}
	view := unsafe.String(unsafe.SliceData(b), len(b))
	if v, ok := g.internMap[view]; ok {
		g.lastKey, g.lastVal = v, v
		return v
	}
	if g.internMap == nil {
		g.internMap = make(map[string]string, 16)
	}
	s := string(b)
	g.internMap[s] = s
	g.lastKey, g.lastVal = s, s
	return s
}
