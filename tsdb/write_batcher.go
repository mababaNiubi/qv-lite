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

const (

	// Dense series are warmed on the WAL worker before segment flush. The small
	// threshold spreads encoder allocation across ingest batches under a tight
	// memory limit, while one-shot/sparse tags keep their metadata-only form.
	denseSeriesEncoderWarmupPoints = 8

	// Each shard needs slots for its active buffer, the worker, every queued
	// batch, and a trigger that may be blocked while publishing to a full queue.
	// The final spare prevents the active buffer from being discarded while the
	// blocked trigger still owns its replacement.
	ingestBufferHeadroom = 3

	minDirectTagCacheSlots = 64
	maxDirectTagCacheSlots = 2048
)

type batcherError struct {
	err error
}

func overloadPointLimit(maxBatchSize int) int64 {
	limit := int64(maxBatchSize)
	if limit <= (1<<62)-1 {
		return limit * 2
	}
	return int64(^uint64(0) >> 1)
}

func directTagCacheSlots(tableSlots, shardCount int) int {
	// The direct-mapped cache is only a fast path. Hash collisions and tables
	// larger than the cache always fall back to the exact per-shard map, so the
	// cache can remain small and power-of-two without affecting correctness.
	slots := tableSlots / shardCount
	if slots < minDirectTagCacheSlots {
		slots = minDirectTagCacheSlots
	}
	slots = nextPowerOfTwo(slots)
	if slots > maxDirectTagCacheSlots {
		slots = maxDirectTagCacheSlots
	}
	return slots
}

// tableBatcher is the bounded, sharded MemTable in front of one table's WAL.
//
// The write path is deliberately split into four stages:
//
//  1. Callers hash the string tag and append to one shard-local buffer.
//  2. A trigger swaps all active buffers without copying their points.
//  3. A bounded queue provides backpressure between callers and storage.
//  4. One worker resolves tagCode, repairs out-of-order runs and commits WAL
//     batches in acceptance order.
//
// Keeping tag resolution, sorting and I/O off the caller is the main latency
// optimization. Keeping exactly one ordered worker is also a correctness
// property: WAL deduplication and last-point tracking must observe batches in
// the same order in which freezeAndEnqueue publishes them.
type tableBatcher struct {
	table *ssTable

	// Immutable routing and sizing state used by the per-point hot path.
	shards          []ingestShard
	shardMask       uint64
	shardShift      uint
	tagCacheMask    uint64
	buffersPerShard int
	maxBatchSize    int64
	maxActive       int64
	interval        time.Duration

	// activePoints counts points not yet moved to the frozen queue. The atomic
	// value is shared by all shards and lets the trigger avoid taking every
	// shard lock on idle timer ticks.
	activePoints atomic.Int64
	closed       atomic.Bool

	// signal is edge-triggered: capacity one is sufficient because a freeze
	// drains every shard. queue is intentionally bounded; increasing it can
	// improve burst tolerance but also multiplies retained high-cardinality
	// buffers.
	signal      chan struct{}
	queue       chan *frozenWriteBatch
	stopTrigger chan struct{}
	triggerDone chan struct{}
	workerDone  chan struct{}

	// freezeMu establishes batch publication order and protects last. last is
	// the visibility barrier used when Flush finds no new active points.
	freezeMu sync.Mutex
	last     *frozenWriteBatch

	// waitMu/spaceCond are cold-path only. A normal write never touches the
	// condition variable; callers wait only when the trigger cannot swap fast
	// enough and active memory grows beyond the two-batch emergency limit.
	waitMu    sync.Mutex
	spaceCond *sync.Cond
	waiters   atomic.Int32
	err       atomic.Pointer[batcherError]

	// preparedRuns is owned exclusively by runWorker. Runs reference pooled
	// per-tag point slices, avoiding a second flat copy before WAL encoding.
	preparedRuns []walDataRun
}

func newTableBatcher(table *ssTable, config IngestConfig) *tableBatcher {
	config.setDefaultValues()

	b := &tableBatcher{
		table:           table,
		shards:          make([]ingestShard, config.Shards),
		shardMask:       uint64(config.Shards - 1),
		shardShift:      uint(bits.TrailingZeros64(uint64(config.Shards))),
		buffersPerShard: config.QueueSize + ingestBufferHeadroom,
		maxBatchSize:    int64(config.MaxBatchSize),
		maxActive:       overloadPointLimit(config.MaxBatchSize),
		interval:        time.Duration(config.FlushIntervalMs) * time.Millisecond,
		signal:          make(chan struct{}, 1),
		queue:           make(chan *frozenWriteBatch, config.QueueSize),
		stopTrigger:     make(chan struct{}),
		triggerDone:     make(chan struct{}),
		workerDone:      make(chan struct{}),
	}

	cacheSlots := directTagCacheSlots(table.tagCacheSlots, config.Shards)
	b.tagCacheMask = uint64(cacheSlots - 1)
	b.initializeShards(cacheSlots)
	b.spaceCond = sync.NewCond(&b.waitMu)

	go b.runTrigger()
	go b.runWorker()
	return b
}

func (b *tableBatcher) initializeShards(cacheSlots int) {
	for i := range b.shards {
		shard := &b.shards[i]
		shard.series = make(map[string]*ingestSeries)
		shard.tagCache = make([]ingestTagCacheEntry, cacheSlots)
		shard.active = newIngestBuffer(0)
		shard.free = make([]ingestBuffer, 0, b.buffersPerShard-1)
		for id := 1; id < b.buffersPerShard; id++ {
			shard.free = append(shard.free, newIngestBuffer(id))
		}
	}
}

// add accepts one point into memory. A nil result means accepted by the
// batcher, not necessarily durable; Flush and Close are the durability and
// visibility barriers.
func (b *tableBatcher) add(tag string, timestamp int64, value variant.Variant) error {
	if err := b.terminalError(); err != nil {
		return err
	}
	active := b.appendPoint(tag, timestamp, value)
	b.maybeSignal(active)
	return b.waitIfOverloaded(active)
}

// addBatch uses the same point-level append path so ordering, reference
// tracking and backpressure semantics stay identical to add. It signals only
// once after the caller's batch, reducing channel traffic for large batches.
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

// appendPoint is the only per-point mutation path. Keep work here bounded:
// one hash, one shard lock, a direct-cache lookup in the common case, one slice
// append and one atomic counter increment. No metadata I/O or sorting occurs on
// the caller goroutine.
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
				tag:             tag,
				hash:            hash,
				primaryBufferID: -1,
			}
			shard.series[tag] = series
		}
		cacheEntry.tag = tag
		cacheEntry.series = series
	}

	tagBuffer := series.bufferFor(ingest.id, b.buffersPerShard)
	if len(tagBuffer.points) == 0 {
		ingest.used = append(ingest.used, tagBuffer)
		tagBuffer.ordered = true
	} else if timestamp < tagBuffer.lastTms {
		tagBuffer.ordered = false
	}
	tagBuffer.points = append(tagBuffer.points, rawWritePoint{timestamp: timestamp, value: value})
	if !tagBuffer.hasReference && variantRetainsHeap(value) {
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
		// A pending signal already represents all currently active shards.
	}
}

// waitIfOverloaded is an emergency memory bound, not the usual queue
// backpressure path. The point is already accepted into a shard before this
// method waits, so returning nil still means that exact point will be drained.
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

// Flush freezes current points and waits for the latest published batch. When
// no points are active, freezeAndEnqueue returns last so Flush still waits for a
// batch that was already queued but not yet committed.
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

// Close stops new writes, stops the periodic trigger, freezes its final active
// points, drains the ordered worker, and only then returns. Calling it more than
// once is harmless and returns the first terminal worker error, if any.
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
	// Preserve the first error: it is normally closest to the root cause, and
	// later batches must not obscure it with derivative failures.
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

// runTrigger coalesces threshold notifications with the latency timer. Both
// paths call the same serialized freeze operation, so timer and size triggers
// cannot reorder batches.
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

// runWorker is intentionally single-threaded. Besides preserving WAL order,
// this gives it exclusive ownership of ingestSeries.code, encoder warm-up
// counters and preparedRuns, avoiding per-series locks in high-cardinality
// workloads.
func (b *tableBatcher) runWorker() {
	defer close(b.workerDone)
	for batch := range b.queue {
		_, batch.err = b.table.commitRawWriteBatch(batch)
		if batch.err != nil {
			b.setError(batch.err)
		}
		// walDataRun holds a point slice. Clear descriptors before retaining the
		// backing array or completed frozen buffers would remain reachable.
		clear(b.preparedRuns)
		b.preparedRuns = b.preparedRuns[:0]
		b.recycleBatch(batch)
		close(batch.done)
	}
}

// prepareRawWriteBatch converts shard-local string-tag buffers into the grouped
// form consumed by walFile.WriteRuns. The conversion deliberately preserves a
// slice per tag instead of flattening all points:
//
//   - tagCode lookup/creation is paid once per series, not once per point;
//   - already ordered series bypass sorting;
//   - tagCode order is irrelevant, avoiding O(tags log tags) batch sorting;
//   - WAL serialization reads the pooled point slices directly.
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
				// Stable sorting preserves caller order for equal timestamps, which
				// keeps last-value/dedup behavior deterministic.
				sort.SliceStable(tagBuffer.points, func(i, j int) bool {
					return tagBuffer.points[i].timestamp < tagBuffer.points[j].timestamp
				})
			}

			if !series.encoderWarmed {
				series.preparedPoints += len(tagBuffer.points)
				if series.preparedPoints >= denseSeriesEncoderWarmupPoints {
					b.table.columns[code-1].ensureCompressors()
					series.encoderWarmed = true
				}
			}
			runs = append(runs, walDataRun{Key: code, Points: tagBuffer.points})
		}
	}
	b.preparedRuns = runs
	return runs, nil
}

// freezeAndEnqueue performs a generation swap, not a point copy. freezeMu
// serializes concurrent threshold, timer and explicit Flush triggers so queue
// order remains WAL order. The queue send may block and is the normal bounded
// backpressure mechanism when storage is slower than ingestion.
func (b *tableBatcher) freezeAndEnqueue() *frozenWriteBatch {
	b.freezeMu.Lock()
	defer b.freezeMu.Unlock()

	// activePoints is incremented before a writer releases its shard lock, so
	// zero is a safe idle fast path and avoids locking every shard each timer
	// tick. Returning last preserves Flush's pending-batch barrier.
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

// recycleBatch resets only tag buffers recorded in ingestBuffer.used and then
// returns each generation to its shard. Point and map capacity survive queue
// pressure, so cardinality warm-up is paid once per configured buffer rather
// than once per frozen batch.
func (b *tableBatcher) recycleBatch(batch *frozenWriteBatch) {
	for i, frozen := range batch.shards {
		for _, tagBuffer := range frozen.used {
			// Clearing every numeric point is pure memory bandwidth: numeric
			// Variants hold no heap references and the next append overwrites them.
			// Reference-bearing buffers must be cleared before becoming idle.
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
	// Drop point-slice references from a completed barrier. The worker retains
	// only the reusable preparedRuns backing array, which is cleared separately.
	batch.shards = nil
}
