package server

import (
	"github.com/mababaNiubi/qv-lite/tsdb"
)

// streamBatchSize 是流式写入每批落引擎的点数。攒满一批即 WriteBatch：
// 内存恒定（≈一批），传输与入库并行，引擎每次锁时长短，不随请求总点数
// 增长而积压。
const streamBatchSize = 50_000

// StreamIngestor 实现「边解码边分批入库」。
//
// 应对不可控的客户端：单连接一次性上传统千万点（或任意多）时，不再
// 等整个 body 到达并整体反序列化后才写，而是每攒满 streamBatchSize 点
// 立即入库一批。从而：
//   - 内存峰值恒定（≈ streamBatchSize，不随请求总点数量级增长）；
//   - 第一个字节到达即开始入库，传输与写入并行；
//   - 引擎每次持锁时长有限，不长时间阻塞其他连接的写/读。
//
// 多个表（Line Protocol 支持）混在同一个请求时按表分组，表切换或攒满
// 时写当前批。
type StreamIngestor struct {
	ingest func(table string, points []tsdb.TagPoint) (int, error)

	size    int
	table   string
	pending []tsdb.TagPoint
	total   int
}

// newStreamIngestor 创建流式入库器，ingest 为实际入库函数
// （s.ingest：直接写引擎或经流水线）。
func (s *Server) newStreamIngestor() *StreamIngestor {
	return &StreamIngestor{
		ingest:  s.ingest,
		size:    streamBatchSize,
		pending: make([]tsdb.TagPoint, 0, streamBatchSize),
	}
}

// Add 追加一个点，攒满一批时立即入库。
func (g *StreamIngestor) Add(table string, p tsdb.TagPoint) error {
	if len(g.pending) > 0 && table != g.table {
		if err := g.flush(); err != nil {
			return err
		}
	}
	g.table = table
	g.pending = append(g.pending, p)
	if len(g.pending) >= g.size {
		return g.flush()
	}
	return nil
}

// Finish 写入残余的一批并返回总写入点数。
func (g *StreamIngestor) Finish() (int, error) {
	if err := g.flush(); err != nil {
		return g.total, err
	}
	return g.total, nil
}

func (g *StreamIngestor) flush() error {
	if len(g.pending) == 0 {
		return nil
	}
	n, err := g.ingest(g.table, g.pending)
	if err != nil {
		return err
	}
	g.total += n
	g.pending = g.pending[:0]
	return nil
}
