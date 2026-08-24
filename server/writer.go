package server

import (
	"sync"
	"time"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

// PipelinedWriter 实现「编解码 / 入库」流水线，提高高并发小请求写入吞吐。
//
// 编解码（handler goroutine，CPU 密集）与入库（引擎 WriteBatch，持 WAL 锁）
// 是两类不同的工作。默认串行：一个请求 decode 完才入库。流水线把入库交给
// 一个后台 goroutine：handler 只需把解码后的点 Submit 进缓冲立即返回，后台
// 用 interval（WriteBufferMs）或缓冲达 batchSize 触发，把多个请求的小批在
// 内存中合并成大批再调用引擎 WriteBatch——引擎单锁下大批明显快于小批，
// 且 decode 新数据与旧数据入库并行。
//
// 一致性：Submit 的数据在 Flush（查询/单点写/关闭前调用）之后立即可见，
// 开启流水线后「写后立即可查」语义保持不变，只是写入有 ≤ interval 的
// 短暂延迟。写失败错误记录在 Err()。
//
// 背压：缓冲点数达到 maxQueue 时 Submit 阻塞，避免无限积压内存。
type PipelinedWriter struct {
	db        *tsdb.DB
	batchSize int
	interval  time.Duration

	mu    sync.Mutex
	cond  *sync.Cond
	buf   []batchChunk // 待合并缓冲
	count int          // buf 中的总点数
	busy  int          // 正在执行的写批数（供 Flush 等待）
	quit  bool
	err   error

	work    chan struct{} // 容量 1，缓冲满/关闭时的非阻塞唤醒信号
	quitCh  chan struct{}
	done    chan struct{}
	closeCh sync.Once  // 保证 quitCh 只关闭一次（Close 幂等）
	writeMu sync.Mutex // 串行化所有引擎写入
}

type batchChunk struct {
	table  string
	points []tsdb.TagPoint
	pooled bool // points 来自 pointBatchPool，写完后需 clear 并归还
}

// NewPipelinedWriter 启动流水线写入器。
// intervalMs 为合并触发周期（毫秒，>0）；batchSize 为触发合并的点数上限。
func NewPipelinedWriter(db *tsdb.DB, intervalMs int, batchSize int) *PipelinedWriter {
	if batchSize <= 0 {
		batchSize = 100_000
	}
	if intervalMs <= 0 {
		intervalMs = 5
	}
	w := &PipelinedWriter{
		db:        db,
		batchSize: batchSize,
		interval:  time.Duration(intervalMs) * time.Millisecond,
		work:      make(chan struct{}, 1),
		quitCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
	w.cond = sync.NewCond(&w.mu)
	go w.run()
	return w
}

// Submit 把一批点按所有权移交方式入队并立即返回，返回入队点数，且不做整批
// 拷贝。调用方（StreamIngestor.flush）在返回后必须放弃对缓冲的所有权：写入器
// 消费完会 clear 并归还 pointBatchPool。缓冲达到背压上限时降级为同步直接写入
// （既不无限积压，也不阻塞请求），随后仍归还池。
func (w *PipelinedWriter) Submit(table string, points []tsdb.TagPoint) int {
	if len(points) == 0 {
		return 0
	}
	// 队列上限：batchSize 的 32 倍，防止无限积压。
	maxQueue := w.batchSize * 32
	w.mu.Lock()
	if w.quit || w.count >= maxQueue {
		n := len(points)
		w.mu.Unlock()
		_ = w.writeChunks([]batchChunk{{table: table, points: points, pooled: true}})
		return n
	}
	w.buf = append(w.buf, batchChunk{table: table, points: points, pooled: true})
	w.count += len(points)
	notify := w.count >= w.batchSize
	w.mu.Unlock()
	if notify {
		w.signalWork()
	}
	return len(points)
}

// Flush 同步等待所有已提交数据入库（含后台进行中的写批），随后返回。
// 用于查询/单点写前保证可见性。
func (w *PipelinedWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.count > 0 || w.busy > 0 {
		w.cond.Wait()
	}
	return w.err
}

// Close 停止后台 goroutine 并等待全部数据入库。幂等，可多次调用。
func (w *PipelinedWriter) Close() error {
	w.mu.Lock()
	w.quit = true
	for w.count > 0 || w.busy > 0 {
		w.cond.Wait()
	}
	err := w.err
	w.mu.Unlock()
	// 唤醒可能阻塞在等待中的 Submit，并通知 run 退出。
	w.cond.Broadcast()
	w.closeCh.Do(func() { close(w.quitCh) })
	<-w.done
	return err
}

// Err 返回最近一次写入错误。
func (w *PipelinedWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *PipelinedWriter) signalWork() {
	select {
	case w.work <- struct{}{}:
	default:
	}
}

// run 是后台合并写入循环：收到 work 信号或 interval 到期时，取出缓冲
// 合并入库；收到 quitCh 后退出。
func (w *PipelinedWriter) run() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	defer close(w.done)
	for {
		select {
		case <-w.quitCh:
			return
		case <-w.work:
		case <-ticker.C:
		}
		w.flush()
	}
}

// flush 取出全部缓冲并入库。由 run 串行调用；Flush 通过 busy/count 感知。
func (w *PipelinedWriter) flush() {
	w.mu.Lock()
	if w.count == 0 {
		w.mu.Unlock()
		return
	}
	chunks := w.buf
	w.buf = nil
	w.count = 0
	w.busy++
	w.mu.Unlock()

	err := w.writeChunks(chunks)

	w.mu.Lock()
	w.busy--
	if err != nil && w.err == nil {
		w.err = err
	}
	w.mu.Unlock()
	w.cond.Broadcast()
}

// writeChunks 按表合并后调用引擎 WriteBatch（串行化）。
//
// 同一张表的多个 chunk 先拼接成大批再调用引擎：减少引擎 WAL 锁获取次数，
// 引擎单锁下大批明显快于小批。每批点数以 batchSize 为上限分批写出，避免
// 单次 WriteBatch 过长持锁阻塞其他写入。写完后归还池化缓冲。
func (w *PipelinedWriter) writeChunks(chunks []batchChunk) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	defer w.releaseChunks(chunks)

	// 按表聚合，保持每个表内 chunk 的到达顺序。
	type tableChunks struct {
		table string
		chunk []batchChunk
		total int
	}
	var groups []tableChunks
	idx := make(map[string]int, 8)
	for _, c := range chunks {
		if i, ok := idx[c.table]; ok {
			g := &groups[i]
			g.chunk = append(g.chunk, c)
			g.total += len(c.points)
		} else {
			idx[c.table] = len(groups)
			groups = append(groups, tableChunks{table: c.table, chunk: []batchChunk{c}, total: len(c.points)})
		}
	}

	for i := range groups {
		g := &groups[i]
		if len(g.chunk) == 1 && len(g.chunk[0].points) <= w.batchSize {
			// 单 chunk 且不超上限：直接写，避免合并拷贝。
			if _, err := w.db.WriteBatch(g.table, g.chunk[0].points); err != nil {
				return err
			}
			continue
		}
		// 多 chunk（或单 chunk 超上限）：合并成 ≤ batchSize 的批次。
		merged := make([]tsdb.TagPoint, 0, min(w.batchSize, g.total))
		for _, c := range g.chunk {
			for len(c.points) > 0 {
				room := w.batchSize - len(merged)
				if room == 0 {
					if _, err := w.db.WriteBatch(g.table, merged); err != nil {
						return err
					}
					merged = merged[:0]
					room = w.batchSize
				}
				n := min(room, len(c.points))
				merged = append(merged, c.points[:n]...)
				c.points = c.points[n:]
			}
		}
		if len(merged) > 0 {
			if _, err := w.db.WriteBatch(g.table, merged); err != nil {
				return err
			}
		}
	}
	return nil
}

// releaseChunks 清空并归还池化缓冲。先 clear 置零（避免 GC 扫描残留的
// variant 指针导致对象无法回收），再以 len=0 放回池中复用。
func (w *PipelinedWriter) releaseChunks(chunks []batchChunk) {
	for _, c := range chunks {
		if c.pooled {
			clear(c.points)
			pointBatchPool.Put(c.points[:0])
		}
	}
}
