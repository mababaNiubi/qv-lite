package tsdb

import (
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mababaNiubi/qv-lite/container"
	"github.com/mababaNiubi/variant"
)

// rawWritePoint deliberately carries the string tag rather than tagCode. Tag
// resolution is deferred until a frozen batch reaches the background worker.
type rawWritePoint struct {
	timestamp int64
	value     variant.Variant
}

// rawTagBuffer keeps one tag's points together while accumulating. Most time
// series arrive in order, allowing the background worker to skip sorting for
// that tag entirely.
type rawTagBuffer struct {
	series       *ingestSeries
	points       []rawWritePoint
	lastTms      int64
	ordered      bool
	hasReference bool
}

type ingestSeries struct {
	tag     string
	hash    uint64
	code    tagCode
	buffers []rawTagBuffer
}

type ingestTagCacheEntry struct {
	tag    string
	series *ingestSeries
}

type ingestBuffer struct {
	id   int
	used []*rawTagBuffer
}

type ingestShard struct {
	mu          sync.Mutex
	series      map[string]*ingestSeries
	tagCache    []ingestTagCacheEntry
	active      ingestBuffer
	free        []ingestBuffer
	activeCount int
}

type frozenWriteBatch struct {
	shards []ingestBuffer
	count  int
	done   chan struct{}
	err    error
}

type batcherError struct {
	err error
}

// tableBatcher is a sharded, bounded-buffer MemTable in front of one table's
// WAL. Writers only hash the string tag and append under one small shard lock.
// Buffer swaps are short; tagCode resolution, sorting and WAL I/O happen on a
// single ordered background worker while writers continue into a free buffer.
type tableBatcher struct {
	table *ssTable

	shards       []ingestShard
	shardMask    uint64
	shardShift   uint
	tagCacheMask uint64
	maxBatchSize int64
	maxActive    int64
	interval     time.Duration

	activePoints atomic.Int64
	closed       atomic.Bool

	signal      chan struct{}
	queue       chan *frozenWriteBatch
	stopTrigger chan struct{}
	triggerDone chan struct{}
	workerDone  chan struct{}

	freezeMu sync.Mutex
	last     *frozenWriteBatch

	// waitMu/spaceCond are cold-path only. Normal writes append immediately;
	// callers wait only if a stalled trigger lets the active maps grow past the
	// two-batch emergency high-water mark.
	waitMu    sync.Mutex
	spaceCond *sync.Cond
	waiters   atomic.Int32
	err       atomic.Pointer[batcherError]

	// preparedRuns is worker-owned. Each entry references one pooled per-tag
	// point slice, avoiding a second flat copy of every point before WAL encode.
	preparedRuns []walDataRun
}

func newTableBatcher(table *ssTable, config IngestConfig) *tableBatcher {
	config.setDefaultValues()
	maxActive := int64(config.MaxBatchSize)
	if maxActive <= (1<<62)-1 {
		maxActive *= 2
	} else {
		maxActive = int64(^uint64(0) >> 1)
	}
	b := &tableBatcher{
		table:        table,
		shards:       make([]ingestShard, config.Shards),
		shardMask:    uint64(config.Shards - 1),
		shardShift:   uint(bits.TrailingZeros64(uint64(config.Shards))),
		maxBatchSize: int64(config.MaxBatchSize),
		maxActive:    maxActive,
		interval:     time.Duration(config.FlushIntervalMs) * time.Millisecond,
		signal:       make(chan struct{}, 1),
		queue:        make(chan *frozenWriteBatch, config.QueueSize),
		stopTrigger:  make(chan struct{}),
		triggerDone:  make(chan struct{}),
		workerDone:   make(chan struct{}),
	}
	// Use the existing table-level tag cache budget as the sizing hint, divided
	// across shards. Cap the direct-mapped cache because the stable per-shard
	// series registry remains the collision-safe fallback.
	cacheSlots := table.tagCacheSlots / config.Shards
	if cacheSlots < 64 {
		cacheSlots = 64
	}
	cacheSlots = nextPowerOfTwo(cacheSlots)
	if cacheSlots > 2048 {
		cacheSlots = 2048
	}
	b.tagCacheMask = uint64(cacheSlots - 1)
	// A frozen batch owns one buffer from every shard until the WAL worker has
	// committed it. Keep enough buffers for the active batch, the worker, the
	// entire bounded queue, and one trigger blocked while enqueueing. This
	// prevents a temporary worker stall from discarding warm high-cardinality
	// maps and entering a rebuild/GC feedback loop.
	buffersPerShard := config.QueueSize + 3
	for i := range b.shards {
		shard := &b.shards[i]
		shard.series = make(map[string]*ingestSeries)
		shard.tagCache = make([]ingestTagCacheEntry, cacheSlots)
		shard.active = newIngestBuffer(0)
		shard.free = make([]ingestBuffer, 0, buffersPerShard-1)
		for j := 1; j < buffersPerShard; j++ {
			shard.free = append(shard.free, newIngestBuffer(j))
		}
	}
	b.spaceCond = sync.NewCond(&b.waitMu)
	go b.runTrigger()
	go b.runWorker()
	return b
}

func newIngestBuffer(id int) ingestBuffer {
	return ingestBuffer{id: id}
}

func (b *tableBatcher) add(tag string, timestamp int64, value variant.Variant) error {
	if err := b.terminalError(); err != nil {
		return err
	}
	active := b.appendPoint(tag, timestamp, value)
	b.maybeSignal(active)
	return b.waitIfOverloaded(active)
}

func (b *tableBatcher) addBatch(points []TagPoint) error {
	if len(points) == 0 {
		return nil
	}
	if err := b.terminalError(); err != nil {
		return err
	}
	var active int64
	for i := range points {
		point := &points[i]
		active = b.appendPoint(point.Tag, point.Timestamp, point.Value)
	}
	b.maybeSignal(active)
	return b.waitIfOverloaded(active)
}

// waitIfOverloaded is outside the normal write path. The bounded frozen queue
// caps committed batches; this guard only stops active-map growth when the
// trigger itself cannot swap quickly enough (normally because that queue is
// full). Points have already been copied into a shard before waiting.
func (b *tableBatcher) waitIfOverloaded(observed int64) error {
	if observed <= b.maxActive {
		return nil
	}
	b.waitMu.Lock()
	b.waiters.Add(1)
	defer func() {
		b.waiters.Add(-1)
		b.waitMu.Unlock()
	}()
	for {
		if err := b.terminalError(); err != nil {
			return err
		}
		if b.activePoints.Load() <= b.maxActive {
			return nil
		}
		b.spaceCond.Wait()
	}
}

func (b *tableBatcher) appendPoint(tag string, timestamp int64, value variant.Variant) int64 {
	hash := container.HashString(tag)
	shard := &b.shards[hash&b.shardMask]
	shard.mu.Lock()
	ingest := &shard.active
	cacheEntry := &shard.tagCache[(hash>>b.shardShift)&b.tagCacheMask]
	series := cacheEntry.series
	if series == nil || cacheEntry.tag != tag {
		series = shard.series[tag]
		if series == nil {
			series = &ingestSeries{
				tag:     tag,
				hash:    hash,
				buffers: make([]rawTagBuffer, cap(shard.free)+1),
			}
			for i := range series.buffers {
				series.buffers[i].series = series
			}
			shard.series[tag] = series
		}
		cacheEntry.tag = tag
		cacheEntry.series = series
	}
	tagBuffer := &series.buffers[ingest.id]
	if len(tagBuffer.points) == 0 {
		ingest.used = append(ingest.used, tagBuffer)
		tagBuffer.ordered = true
	} else if timestamp < tagBuffer.lastTms {
		tagBuffer.ordered = false
	}
	tagBuffer.points = append(tagBuffer.points, rawWritePoint{timestamp: timestamp, value: value})
	switch value.Type() {
	case variant.TypeString, variant.TypeList, variant.TypeMap:
		tagBuffer.hasReference = true
	}
	tagBuffer.lastTms = timestamp
	shard.activeCount++
	active := b.activePoints.Add(1)
	shard.mu.Unlock()
	return active
}

func (b *tableBatcher) maybeSignal(active int64) {
	if active < b.maxBatchSize {
		return
	}
	select {
	case b.signal <- struct{}{}:
	default:
	}
}

func (b *tableBatcher) runTrigger() {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	defer close(b.triggerDone)
	for {
		select {
		case <-b.stopTrigger:
			return
		case <-b.signal:
			b.freezeAndEnqueue()
		case <-ticker.C:
			b.freezeAndEnqueue()
		}
	}
}

func (b *tableBatcher) runWorker() {
	defer close(b.workerDone)
	for batch := range b.queue {
		_, batch.err = b.table.commitRawWriteBatch(batch)
		if batch.err != nil {
			b.setError(batch.err)
		}
		clear(b.preparedRuns)
		b.preparedRuns = b.preparedRuns[:0]
		b.recycleBatch(batch)
		close(batch.done)
	}
}

// freezeAndEnqueue swaps every active shard map for a free pooled one. It serializes
// swaps so queue order is also WAL commit order. The queue is bounded, providing
// natural backpressure when disk preparation cannot keep up.
func (b *tableBatcher) freezeAndEnqueue() *frozenWriteBatch {
	b.freezeMu.Lock()
	defer b.freezeMu.Unlock()

	// activePoints is incremented before a writer releases its shard lock, so
	// zero is a safe idle fast path for completed writes and avoids replacing
	// every empty shard map on each timer tick.
	if b.activePoints.Load() == 0 {
		return b.last
	}

	batch := &frozenWriteBatch{
		shards: make([]ingestBuffer, len(b.shards)),
		done:   make(chan struct{}),
	}
	for i := range b.shards {
		shard := &b.shards[i]
		shard.mu.Lock()
		active := shard.active
		activeCount := shard.activeCount
		freeIndex := len(shard.free) - 1
		shard.active = shard.free[freeIndex]
		shard.free = shard.free[:freeIndex]
		shard.active.used = shard.active.used[:0]
		shard.activeCount = 0
		shard.mu.Unlock()
		batch.shards[i] = active
		batch.count += activeCount
	}
	if batch.count == 0 {
		b.recycleBatch(batch)
		return b.last
	}
	b.activePoints.Add(-int64(batch.count))
	b.notifyWaiters()
	b.queue <- batch
	b.last = batch
	return batch
}

// recycleBatch resets only the tag buffers used by this batch, then returns the
// whole persistent tag map to its shard's bounded free list. Maps and point
// capacity survive queue pressure, so cardinality warm-up is paid once per
// configured buffer rather than once per frozen batch.
func (b *tableBatcher) recycleBatch(batch *frozenWriteBatch) {
	for i, frozen := range batch.shards {
		for _, tagBuffer := range frozen.used {
			// Numeric Variants have a nil complexValue and are overwritten by the
			// next append, so clearing their whole backing slice only burns memory
			// bandwidth. Reference-bearing values must be cleared to avoid keeping
			// old strings/lists/maps alive while this pooled buffer is idle.
			if tagBuffer.hasReference {
				clear(tagBuffer.points)
			}
			tagBuffer.points = tagBuffer.points[:0]
			tagBuffer.lastTms = 0
			tagBuffer.ordered = true
			tagBuffer.hasReference = false
		}
		frozen.used = frozen.used[:0]
		shard := &b.shards[i]
		shard.mu.Lock()
		shard.free = append(shard.free, frozen)
		shard.mu.Unlock()
	}
	batch.shards = nil
}

// Flush establishes a visibility barrier for everything frozen before it and
// waits until the ordered WAL worker has committed that batch.
func (b *tableBatcher) Flush() error {
	target := b.freezeAndEnqueue()
	if target != nil {
		<-target.done
		if target.err != nil {
			return target.err
		}
	}
	return b.getError()
}

func (b *tableBatcher) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return b.getError()
	}
	b.notifyWaiters()
	close(b.stopTrigger)
	<-b.triggerDone
	target := b.freezeAndEnqueue()
	if target != nil {
		<-target.done
	}
	close(b.queue)
	<-b.workerDone
	return b.getError()
}

func (b *tableBatcher) setError(err error) {
	if err == nil {
		return
	}
	b.err.CompareAndSwap(nil, &batcherError{err: err})
	b.notifyWaiters()
}

func (b *tableBatcher) notifyWaiters() {
	if b.waiters.Load() == 0 {
		return
	}
	b.waitMu.Lock()
	b.spaceCond.Broadcast()
	b.waitMu.Unlock()
}

func (b *tableBatcher) getError() error {
	if stored := b.err.Load(); stored != nil {
		return stored.err
	}
	return nil
}

func (b *tableBatcher) terminalError() error {
	if b.closed.Load() {
		return ErrorWALClose
	}
	return b.getError()
}

// prepareRawWriteBatch resolves a persistent tag buffer only once, sorts only
// buffers that observed out-of-order timestamps, and emits one contiguous run
// per tag. Tag-code order is irrelevant to the grouped WAL fast path, so
// high-cardinality batches avoid a second slice and O(tags log tags) sort.
func (b *tableBatcher) prepareRawWriteBatch(batch *frozenWriteBatch) ([]walDataRun, error) {
	runs := b.preparedRuns[:0]
	for _, shard := range batch.shards {
		for _, tagBuffer := range shard.used {
			series := tagBuffer.series
			code := series.code
			if code == 0 {
				var ok bool
				code, ok = b.table.Meta.loadHash(series.tag, series.hash)
				if !ok {
					var err error
					code, err = b.table.CreateColumn(series.tag)
					if err != nil {
						return nil, err
					}
				}
				series.code = code
			}
			if !tagBuffer.ordered {
				sort.SliceStable(tagBuffer.points, func(i, j int) bool {
					return tagBuffer.points[i].timestamp < tagBuffer.points[j].timestamp
				})
			}
			runs = append(runs, walDataRun{Key: code, Points: tagBuffer.points})
		}
	}
	b.preparedRuns = runs
	return runs, nil
}
