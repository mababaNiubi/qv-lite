package server

import (
	"sync"
	"unsafe"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

// streamBatchSize 是流式写入单次触发入库的总点数阈值。攒满即把当前各表的
// 累积点统一入库：内存恒定（≈一批），传输与入库并行，引擎每次锁时长短，
// 不随请求总点数增长而积压。
const streamBatchSize = 50_000

// pointBatchPool 复用 StreamIngestor 的累积缓冲与 PipelinedWriter 的入队
// 缓冲。flush 时缓冲所有权直接移交给 ingest（流水线模式由写入器写完 clear
// 后归还；直写模式由 ingest 立即归还），避免入队时的整批 memcpy。池条目
// 容量固定为 streamBatchSize，稳态零分配、零整批拷贝。
var pointBatchPool = sync.Pool{
	New: func() any {
		return make([]tsdb.TagPoint, 0, streamBatchSize)
	},
}

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
	lastValid bool // lastTable/lastPts 是否有效（表名可能是 ""，不能拿它当哨兵）
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

// Add 追加一个点到待入库缓冲；待入库总点数达到阈值时统一入库（按表分组）。
// table 与 p.Tag 假定已是请求内驻留的规范串（各 handler 负责驻留：二进制由
// internBytes/intern 驻留，Line/JSON 在调用前显式 intern）。驻留只影响内存
// 去重，不影响正确性。
func (g *StreamIngestor) Add(table string, p tsdb.TagPoint) error {
	pts := g.pendingSlice(table)
	pts = append(pts, p)
	g.lastPts = pts // pendingSlice 已保证 table 是活动表；append 后及时刷新（可能换新数组）
	g.pendingN++
	if g.pendingN >= g.size {
		return g.flush()
	}
	return nil
}

// pendingSlice 返回 table 的待入库切片；连续同表时命中快路径，免 map 查找。
// 切表时才把上一个活动表的最新切片头（含最新 len）写回 map，整个 Add 热循环
// 不再每点做一次 map 写（pprof 中 aeshash/mapaccess 的每点开销来源）。
func (g *StreamIngestor) pendingSlice(table string) []tsdb.TagPoint {
	if g.lastValid && table == g.lastTable {
		return g.lastPts
	}
	if g.lastValid {
		g.pending[g.lastTable] = g.lastPts
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
	g.lastValid = true
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
// 顺序无关；map 遍历顺序随机不影响正确性。
// ingest 契约：调用方把缓冲所有权交给 ingest，返回后本表不再复用该缓冲——
// 流水线模式写入器异步消费并归还 pool，直写模式 ingest 立即归还。flush 随后
// 从池取新缓冲继续累积，稳态零分配、无整批拷贝。
func (g *StreamIngestor) flush() error {
	if g.pendingN == 0 {
		return nil
	}
	// 先把活动表写回 map，确保遍历覆盖最后还在累积的表。
	if g.lastValid {
		g.pending[g.lastTable] = g.lastPts
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
		// 所有权已移交：从池取新缓冲续写，决不复用刚交给 ingest 的数组。
		seen := pointBatchPool.Get().([]tsdb.TagPoint)
		g.pending[table] = seen
		if g.lastValid && table == g.lastTable {
			g.lastPts = seen
		}
	}
	// 迭代结束：所有表都已写回新缓冲，清空活动表标记。
	g.lastValid = false
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

// internSpan 把 line 上的一个 token 区间转成请求内驻留的规范串。无转义时走
// internBytes 零拷贝驻留（重复值不新分配）；含转义时才解码后驻留（少见路径）。
// line 可能指向随后被覆写的读缓冲，调用方必须在覆写前完成驻留。
func (g *StreamIngestor) internSpan(line []byte, s tokenSpan, esc bool) string {
	if esc {
		return g.intern(spanString(line, s, true))
	}
	return g.internBytes(line[s.start:s.end])
}
