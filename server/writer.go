package server

import (
	"sync"
	"time"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

// PipelinedWriter 是可选的「编解码 / 入库」流水线，主要用于让超大流式请求的
// 解码与引擎入库重叠。引擎已有分片异步攒批，因此服务端默认不启用本层。
//
// 编解码（handler goroutine，CPU 密集）与引擎 WriteBatch 是两类不同的工作。
// 流水线把入库交给
// 一个后台 goroutine：handler 只需把解码后的点 Submit 进缓冲立即返回，后台
// 用 interval（WriteBufferMs）或缓冲达 batchSize 触发，把多个请求的小批
// 交给引擎。引擎侧自带分片异步攒批（按表累积、冻结换批、后台 worker 写
// WAL），server 层不再合批——多个 chunk 直接按提交顺序逐批 WriteBatch，
// 避免一次大分配 + 全量 memcpy，也不改变引擎的冻结粒度。提交与写入并行，
// 使 decode 新数据与旧数据入库重叠。
//
// 一致性：Submit 的数据在 Flush（查询/单点写/关闭前调用）之后立即可见，
// 开启流水线后「写后立即可查」语义保持不变，只是写入有 ≤ interval 的
// 短暂延迟。写失败错误记录在 Err()。
//
// 背压：缓冲点数达到 maxQueue（batchSize×32）时 Submit 阻塞（有界队列式
// 背压），把引擎侧排空速度逐级传导到 handler/TCP，避免无限积压内存；
// Close 后降级为同步直写兜底（计入 busy，保证 Close 等待其完成）。
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
	closeCh sync.Once // 保证 quitCh 只关闭一次（Close 幂等）
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
// 消费完会 clear 并归还 pointBatchPool。
//
// 背压：缓冲达到 maxQueue 上限时阻塞（条件变量等待写入器排空），把引擎侧
// 背压逐级传导到请求；Close 置位后阻塞的 Submit 直接放行入队，随 Close 的
// 排空完成。调用方不得在 Close 之后继续 Submit。
func (w *PipelinedWriter) Submit(table string, points []tsdb.TagPoint) int {
	if len(points) == 0 {
		return 0
	}
	// 队列上限：batchSize 的 32 倍，防止无限积压。
	maxQueue := w.batchSize * 32
	w.mu.Lock()
	for !w.quit && w.count >= maxQueue {
		w.cond.Wait()
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
// 调用方不得在 Close 之后继续 Submit。
func (w *PipelinedWriter) Close() error {
	w.mu.Lock()
	w.quit = true
	// 立即唤醒写入器排空，不依赖 interval：置位后新入队的数据照常消费，
	// 背压阻塞的 Submit 也随本轮排空解除。
	w.signalWork()
	for w.count > 0 || w.busy > 0 {
		w.cond.Wait()
	}
	err := w.err
	w.mu.Unlock()
	// 唤醒可能阻塞在等待中的 Submit（quit 已置位，它们会直接入队）。
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
	// 先广播：缓冲已让出，背压阻塞的 Submit 可立即续写，与本次入库并行。
	w.cond.Broadcast()

	err := w.writeChunks(chunks)

	w.mu.Lock()
	w.busy--
	if err != nil && w.err == nil {
		w.err = err
	}
	w.mu.Unlock()
	w.cond.Broadcast()
}

// writeChunks 按提交顺序逐 chunk 直写引擎。
//
// 引擎侧的分片异步攒批按自己的阈值冻结换批，server 层把多个 chunk 合并成
// 大切片既不能提高引擎的批大小，还要付出一次大分配 + 全量 memcpy，因此
// 不合并：逐 chunk 入库（同一表的到达顺序保持为缓冲顺序）。超 batchSize
// 的 chunk 按 batchSize 切分（纯切片拆段，无拷贝），控制单次移交的内存
// 上限。写完后归还池化缓冲。
func (w *PipelinedWriter) writeChunks(chunks []batchChunk) error {
	defer w.releaseChunks(chunks)

	for _, c := range chunks {
		pts := c.points
		for len(pts) > 0 {
			n := min(w.batchSize, len(pts))
			if _, err := w.db.WriteBatch(c.table, pts[:n]); err != nil {
				return err
			}
			pts = pts[n:]
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
