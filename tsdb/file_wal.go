package tsdb

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/mababaNiubi/variant"
)

var batchWritePool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 512*1024)
		return &buf
	},
}

type walDataEntry struct {
	EndPosition int64
	Key         tagCode
	Timestamp   int64
	Value       variant.Variant
}

// walDataRun is the prepared unit handed from tableBatcher to the WAL. Points
// stay in their pooled per-tag buffer; only this small run descriptor is built.
type walDataRun struct {
	Key    tagCode
	Points []rawWritePoint
}

// Meta allocates tagCode densely from 1, so the normal lookup path can avoid
// Go map hashing. Codes above this bound retain the map fallback: besides
// supporting exceptionally large tables, that prevents a corrupt WAL key from
// forcing an unbounded slice allocation during recovery.
const maxDenseWALTagCode = tagCode(1 << 20)

type walTagState struct {
	maxTimestamp int64
	lastValue    variant.Variant
	known        bool
}

type flushEntry struct {
	tag       tagCode
	timestamp int64
	value     variant.Variant
}

type walReadChunk struct {
	keys       []tagCode
	timestamps []int64
	values     []variant.Variant
}

// walReadChunkIndex maps a tag to entry positions within one immutable WAL
// read chunk. It is built lazily on the first read that needs it and kept
// until the buffer is reset, so repeated queries of a sparse tag skip the
// per-entry key scan of every chunk. Only full (immutable) chunks are
// indexed; the active chunk keeps growing and is always scanned directly.
type walReadChunkIndex map[tagCode][]int32

// maxWALTagIndexBytes caps the total estimated size of per-chunk tag indexes
// in one read buffer. Beyond it, no new indexes are built and queries fall
// back to scanning, keeping the read-side memory cost bounded.
const maxWALTagIndexBytes = 8 << 20

// minIndexedChunkEntries skips indexing tiny chunks where a scan is cheaper
// than the map overhead.
const minIndexedChunkEntries = 512

// walReadBuffer is an in-memory, already-decoded read cache for values appended
// to the WAL. Its struct-of-arrays layout avoids walDataEntry's alignment and
// write-only EndPosition cost. It also lets a tag query scan compact keys before
// touching timestamps and Variants. Chunks only bound slice growth; write
// batching remains owned by tableBatcher.
type walReadBuffer struct {
	chunks        []walReadChunk
	activeChunks  int
	total         int
	chunkCap      int
	hasReferences bool

	// tagIndexes/tagIndexSize are read-path caches (see tagIndex). All access
	// is guarded by tagIndexMu; the write path never touches them.
	tagIndexMu   sync.Mutex
	tagIndexes   []walReadChunkIndex
	tagIndexSize int
}

// Keep WAL rotation reuse useful without pinning an entire decoded default
// (64 MiB) WAL per table. CloseBuffer remains the zero-cache option.
const maxRetainedWALReadBufferBytes int64 = 12 << 20

func newWalReadBuffer(chunkCap int) *walReadBuffer {
	return &walReadBuffer{chunkCap: chunkCap}
}

func (b *walReadBuffer) append(e walDataEntry) {
	b.appendValue(e.Key, e.Timestamp, e.Value)
}

func (b *walReadBuffer) appendValue(key tagCode, timestamp int64, value variant.Variant) {
	if b.activeChunks == 0 || len(b.chunks[b.activeChunks-1].keys) >= b.chunkCap {
		if b.activeChunks == len(b.chunks) {
			b.chunks = append(b.chunks, walReadChunk{
				keys:       make([]tagCode, 0, b.chunkCap),
				timestamps: make([]int64, 0, b.chunkCap),
				values:     make([]variant.Variant, 0, b.chunkCap),
			})
		}
		b.activeChunks++
	}
	chunk := &b.chunks[b.activeChunks-1]
	chunk.keys = append(chunk.keys, key)
	chunk.timestamps = append(chunk.timestamps, timestamp)
	chunk.values = append(chunk.values, value)
	if !b.hasReferences {
		switch value.Type() {
		case variant.TypeString, variant.TypeList, variant.TypeMap:
			b.hasReferences = true
		}
	}
	b.total++
}

// tagIndex returns the lazy tag→positions index for the immutable chunk at c,
// building it from the caller's snapshot of the chunk's keys (the caller
// passes its own copy, so the build never touches write-path state). Returns
// nil for chunks below the size threshold or when the byte budget is spent —
// callers then scan the chunk directly. Positions are valid within the
// caller's snapshot of the chunk, which is immutable once full.
func (b *walReadBuffer) tagIndex(c int, keys []tagCode) walReadChunkIndex {
	b.tagIndexMu.Lock()
	defer b.tagIndexMu.Unlock()
	if c < len(b.tagIndexes) && b.tagIndexes[c] != nil {
		return b.tagIndexes[c]
	}
	if len(keys) < minIndexedChunkEntries || b.tagIndexSize >= maxWALTagIndexBytes {
		return nil
	}
	// No map prealloc: a chunk with few distinct tags (the common case) keeps
	// a tiny map, while high-cardinality chunks grow geometrically. A large
	// fixed hint would waste ~4KB per chunk for single-tag buffers.
	idx := make(map[tagCode][]int32)
	for j, key := range keys {
		idx[key] = append(idx[key], int32(j))
	}
	for len(b.tagIndexes) <= c {
		b.tagIndexes = append(b.tagIndexes, nil)
	}
	b.tagIndexes[c] = idx
	// Rough estimate: map entry overhead plus 4 bytes per position.
	b.tagIndexSize += len(idx)*56 + len(keys)*4
	return idx
}

// resetForReuse releases values that may own heap data and keeps a bounded
// prefix of fixed-size struct-of-arrays chunks for the next WAL rotation.
func (b *walReadBuffer) resetForReuse(maxRetainedBytes int64) {
	b.tagIndexMu.Lock()
	clear(b.tagIndexes)
	b.tagIndexes = nil
	b.tagIndexSize = 0
	b.tagIndexMu.Unlock()
	for i := 0; i < b.activeChunks; i++ {
		chunk := &b.chunks[i]
		if b.hasReferences {
			clear(chunk.values)
		}
		chunk.keys = chunk.keys[:0]
		chunk.timestamps = chunk.timestamps[:0]
		chunk.values = chunk.values[:0]
	}
	chunkBytes := int64(b.chunkCap) * int64(unsafe.Sizeof(tagCode(0))+unsafe.Sizeof(int64(0))+unsafe.Sizeof(variant.Variant{}))
	maxChunks := 0
	if chunkBytes > 0 && maxRetainedBytes > 0 {
		maxChunks = min(len(b.chunks), int(maxRetainedBytes/chunkBytes))
	}
	for i := maxChunks; i < len(b.chunks); i++ {
		b.chunks[i] = walReadChunk{}
	}
	b.chunks = b.chunks[:maxChunks:maxChunks]
	b.activeChunks = 0
	b.total = 0
	b.hasReferences = false
}

func (b *walReadBuffer) forEach(fn func(key tagCode, timestamp int64, value variant.Variant) bool) bool {
	for i := 0; i < b.activeChunks; i++ {
		chunk := &b.chunks[i]
		for j := range chunk.keys {
			if !fn(chunk.keys[j], chunk.timestamps[j], chunk.values[j]) {
				return false
			}
		}
	}
	return true
}

func (b *walReadBuffer) appendMatching(dst []Point, tag tagCode, startTime, endTime int64) []Point {
	for i := 0; i < b.activeChunks; i++ {
		chunk := &b.chunks[i]
		for j, key := range chunk.keys {
			if key != tag {
				continue
			}
			timestamp := chunk.timestamps[j]
			if timestamp >= startTime && timestamp <= endTime {
				dst = append(dst, Point{Tms: timestamp, V: chunk.values[j]})
			}
		}
	}
	return dst
}

type walFileEnty struct {
	fileName   string
	length     int64
	readBuffer *walReadBuffer
	// needsSort records that this file retained a timestamp older than the
	// previously accepted timestamp for the same tag. Ordered files can be
	// streamed directly into segment encoders without a flush-time copy/sort.
	needsSort bool
}

// WalFile is the File-like interface for the write-ahead log cache.
type WalFile interface {
	updateWalConfig(walConfig WalConfig)
	Write(key tagCode, timestamp int64, value variant.Variant) (bool, int, error)
	WriteBatch(entries []walDataEntry) (int, error)
	WriteRuns(runs []walDataRun) (int, error)
	ReadByTime(tag tagCode, starTime int64, endTime int64) ([]Point, error)
	snapshotTagTime() walTagSnapshot
	GetTagMaxTimestamp(key tagCode) (int64, variant.Variant, bool)
	SetLastPoint(key tagCode, ts int64, value variant.Variant)
	NeedFlush() bool
	FlushPending() error
	completeFileState() (count int, needsSort bool)
	forEachCompleteFile(limit int, fc func(fileIndex int, tag tagCode, timestamp int64, value variant.Variant, offset int64) bool) (int, error)
	retainWalFilePrefix(index int, truncateSize int64) error
	truncate(n int)
	removeOrphanedFiles()
	Close() error
}

type walFile struct {
	mutex           sync.Mutex
	walFiles        []walFileEnty
	spareReadBuffer *walReadBuffer
	tagStates       []walTagState
	tagMaxTimestamp map[tagCode]int64
	tagLastValue    map[tagCode]variant.Variant
	tagStateCount   int

	writeFile   *os.File
	writeBuffer *bufio.Writer

	filePath      string
	dedupWindowMs int64
	minIntervalMs int64
	config        WalConfig
	policyEntries []walDataEntry
}

func NewWalFile(dirPath string, dedupWindowMs, minIntervalMs int64, walConfig WalConfig) (WalFile, error) {
	walConfig.setDefaultValues()
	filePath := filepath.Join(dirPath, "wal")
	tms, err := GetWalFileTms(filePath)
	if err != nil {
		return nil, err
	}
	if len(tms) == 0 {
		n := time.Now().UnixNano()
		tms = []int64{n}
		createF, err := os.Create(filepath.Join(filePath, strconv.FormatInt(n, 10)+".wal"))
		if err != nil {
			return nil, err
		}
		_ = createF.Close()
	}
	walFiles := make([]walFileEnty, len(tms))
	tagMaxTimestamp := make(map[tagCode]int64)
	tagLastValue := make(map[tagCode]variant.Variant)
	for i, t := range tms {
		walFiles[i].fileName = filepath.Join(filePath, strconv.FormatInt(t, 10)+".wal")
		walFiles[i].readBuffer, walFiles[i].length, err = getAllData(walFiles[i].fileName, walConfig.MaxBufferBatchSize)
		if err != nil {
			err = os.Truncate(walFiles[i].fileName, walFiles[i].length)
			if err != nil {
				return nil, err
			}
		}
		walFiles[i].readBuffer.forEach(func(key tagCode, timestamp int64, value variant.Variant) bool {
			maxTs, ok := tagMaxTimestamp[key]
			if ok && timestamp < maxTs {
				walFiles[i].needsSort = true
			}
			if !ok || timestamp > maxTs {
				tagMaxTimestamp[key] = timestamp
				tagLastValue[key] = value
			}
			return true
		})
		if walConfig.CloseBuffer {
			walFiles[i].readBuffer = newWalReadBuffer(walConfig.MaxBufferBatchSize)
		}
	}

	file, err := os.OpenFile(walFiles[len(walFiles)-1].fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	wls := &walFile{
		writeFile:       file,
		writeBuffer:     bufio.NewWriter(file),
		walFiles:        walFiles,
		tagMaxTimestamp: tagMaxTimestamp,
		tagLastValue:    tagLastValue,
		filePath:        filePath,
		dedupWindowMs:   dedupWindowMs,
		minIntervalMs:   minIntervalMs,
		config:          walConfig,
	}
	// Move normal dense codes out of the recovery maps once. Subsequent grouped
	// batches use direct indexed state reads and writes.
	for key, maxTs := range tagMaxTimestamp {
		if key > maxDenseWALTagCode {
			continue
		}
		wls.storeTagState(key, maxTs, tagLastValue[key])
		delete(tagMaxTimestamp, key)
		delete(tagLastValue, key)
	}
	wls.tagStateCount += len(tagMaxTimestamp)
	return wls, err
}

func (ws *walFile) loadTagState(key tagCode) (int64, variant.Variant, bool) {
	if key <= maxDenseWALTagCode {
		index := int(key)
		if index < len(ws.tagStates) {
			state := &ws.tagStates[index]
			if state.known {
				return state.maxTimestamp, state.lastValue, true
			}
		}
	}
	maxTs, ok := ws.tagMaxTimestamp[key]
	if !ok {
		return 0, variant.Variant{}, false
	}
	return maxTs, ws.tagLastValue[key], true
}

func (ws *walFile) storeTagState(key tagCode, maxTs int64, lastValue variant.Variant) {
	if key <= maxDenseWALTagCode {
		index := int(key)
		known := index < len(ws.tagStates) && ws.tagStates[index].known
		if index >= len(ws.tagStates) {
			newLength := len(ws.tagStates) * 2
			if newLength < 64 {
				newLength = 64
			}
			for newLength <= index {
				newLength *= 2
			}
			limit := int(maxDenseWALTagCode) + 1
			if newLength > limit {
				newLength = limit
			}
			states := make([]walTagState, newLength)
			copy(states, ws.tagStates)
			ws.tagStates = states
		}
		ws.tagStates[index] = walTagState{
			maxTimestamp: maxTs,
			lastValue:    lastValue,
			known:        true,
		}
		if !known {
			ws.tagStateCount++
		}
		return
	}
	if ws.tagMaxTimestamp == nil {
		ws.tagMaxTimestamp = make(map[tagCode]int64)
		ws.tagLastValue = make(map[tagCode]variant.Variant)
	}
	if _, known := ws.tagMaxTimestamp[key]; !known {
		ws.tagStateCount++
	}
	ws.tagMaxTimestamp[key] = maxTs
	ws.tagLastValue[key] = lastValue
}

func (s *walFile) updateWalConfig(walConfig WalConfig) {
	walConfig.setDefaultValues()
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.config = walConfig
	if walConfig.CloseBuffer || (s.spareReadBuffer != nil && s.spareReadBuffer.chunkCap != walConfig.MaxBufferBatchSize) {
		s.spareReadBuffer = nil
	}
}

func (ws *walFile) Write(key tagCode, timestamp int64, value variant.Variant) (bool, int, error) {
	entry := []walDataEntry{{Key: key, Timestamp: timestamp, Value: value}}
	written, err := ws.WriteBatch(entry)
	ws.mutex.Lock()
	fileIndex := len(ws.walFiles) - 1
	ws.mutex.Unlock()
	return written == 1, fileIndex, err
}

// WriteBatch appends a prepared batch directly to the physical WAL. The caller
// guarantees one contiguous, timestamp-ordered run per tag; runs do not need
// to be ordered by tagCode. tableBatcher owns buffering, tag resolution and
// batch-internal sorting.
func (ws *walFile) WriteBatch(entries []walDataEntry) (int, error) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	return ws.writeBatchLocked(entries)
}

func (ws *walFile) writeBatchLocked(entries []walDataEntry) (int, error) {
	if ws.writeFile == nil || ws.writeBuffer == nil {
		return 0, ErrorWALClose
	}
	if len(entries) == 0 {
		return 0, nil
	}

	fileIndex := len(ws.walFiles) - 1
	if ws.config.MaxFileNumber > 0 &&
		ws.walFiles[fileIndex].length >= ws.config.MaxFileSize &&
		len(ws.walFiles) >= ws.config.MaxFileNumber {
		return 0, ErrorWALCacheFull
	}

	ent := &ws.walFiles[fileIndex]
	startLength := ent.length
	for i := range entries {
		entries[i].EndPosition = 0
	}
	batchPtr := batchWritePool.Get().(*[]byte)
	batchBuf := (*batchPtr)[:0]
	defer func() {
		*batchPtr = batchBuf[:0]
		batchWritePool.Put(batchPtr)
	}()
	newLength := startLength
	var err error
	batchBuf, newLength, err = ws.dedupRuns(entries, batchBuf, startLength, &ent.needsSort)
	if err != nil {
		return 0, err
	}

	if len(batchBuf) > 0 {
		if _, err := ws.writeBuffer.Write(batchBuf); err != nil {
			return 0, err
		}
		if err := ws.writeBuffer.Flush(); err != nil {
			return 0, err
		}
	}

	ent.length = newLength
	written := 0
	for i := range entries {
		if entries[i].EndPosition <= startLength {
			continue
		}
		written++
		if !ws.config.CloseBuffer {
			ent.readBuffer.append(entries[i])
		}
	}
	if err := ws.rotateIfFull(); err != nil {
		return written, err
	}
	return written, nil
}

// WriteRuns is the tableBatcher fast path. With no sampling/dedup policy (the
// normal configuration), every prepared point is accepted, so the WAL can
// serialize pooled tag runs directly and append them to the decoded read cache
// after the file write succeeds. Explicit policies retain WriteBatch's exact
// filtering semantics through a reusable compatibility buffer.
func (ws *walFile) WriteRuns(runs []walDataRun) (int, error) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	if len(runs) == 0 {
		return 0, nil
	}
	if ws.minIntervalMs > 0 || ws.dedupWindowMs > 0 {
		entries := ws.policyEntries[:0]
		for i := range runs {
			run := &runs[i]
			for j := range run.Points {
				point := &run.Points[j]
				entries = append(entries, walDataEntry{Key: run.Key, Timestamp: point.timestamp, Value: point.value})
			}
		}
		written, err := ws.writeBatchLocked(entries)
		clear(entries)
		ws.policyEntries = entries[:0]
		return written, err
	}
	if ws.writeFile == nil || ws.writeBuffer == nil {
		return 0, ErrorWALClose
	}

	fileIndex := len(ws.walFiles) - 1
	if ws.config.MaxFileNumber > 0 &&
		ws.walFiles[fileIndex].length >= ws.config.MaxFileSize &&
		len(ws.walFiles) >= ws.config.MaxFileNumber {
		return 0, ErrorWALCacheFull
	}

	ent := &ws.walFiles[fileIndex]
	startLength := ent.length
	batchPtr := batchWritePool.Get().(*[]byte)
	batchBuf := (*batchPtr)[:0]
	defer func() {
		*batchPtr = batchBuf[:0]
		batchWritePool.Put(batchPtr)
	}()

	newLength := startLength
	written := 0
	for i := range runs {
		run := &runs[i]
		maxTimestamp, lastValue, known := ws.loadTagState(run.Key)
		for j := range run.Points {
			point := &run.Points[j]
			var dataLen int64
			var err error
			batchBuf, dataLen, err = appendSerialized(batchBuf, run.Key, point.timestamp, point.value)
			if err != nil {
				return 0, err
			}
			newLength += dataLen
			written++
			if known && point.timestamp < maxTimestamp {
				ent.needsSort = true
			}
			if !known || point.timestamp >= maxTimestamp {
				maxTimestamp, lastValue, known = point.timestamp, point.value, true
			}
		}
		if known {
			ws.storeTagState(run.Key, maxTimestamp, lastValue)
		}
	}

	if len(batchBuf) > 0 {
		if _, err := ws.writeBuffer.Write(batchBuf); err != nil {
			return 0, err
		}
		if err := ws.writeBuffer.Flush(); err != nil {
			return 0, err
		}
	}
	ent.length = newLength
	if !ws.config.CloseBuffer {
		for i := range runs {
			run := &runs[i]
			for j := range run.Points {
				point := &run.Points[j]
				ent.readBuffer.appendValue(run.Key, point.timestamp, point.value)
			}
		}
	}
	if err := ws.rotateIfFull(); err != nil {
		return written, err
	}
	return written, nil
}

// dedupRuns processes the prepared shape directly: each tag occupies one
// contiguous timestamp-ordered run. Tag-code order itself is not required, so
// the running (maxTimestamp, lastValue) lives in locals and global state is
// written once per run instead of once per entry. Must be called with ws.mutex
// held.
func (ws *walFile) dedupRuns(chunk []walDataEntry, batchBuf []byte, length int64, needsSort *bool) ([]byte, int64, error) {
	var curKey tagCode
	var curMax int64
	var curLast variant.Variant
	prevKnown := false
	haveRun := false
	for i := range chunk {
		e := &chunk[i]
		if !haveRun || e.Key != curKey {
			// Publish the previous run's final state once.
			if haveRun {
				ws.storeTagState(curKey, curMax, curLast)
			}
			curKey = e.Key
			curMax, curLast, prevKnown = ws.loadTagState(curKey)
			haveRun = true
		}
		if prevKnown {
			if e.Timestamp < curMax {
				// With no sampling/dedup policy configured, retain late data.
				// Mark the file so only this uncommon case pays for a global
				// flush-time reorder. Under an explicit policy, preserve the
				// historical behavior and reject older points.
				if ws.minIntervalMs > 0 || ws.dedupWindowMs > 0 {
					continue
				}
			} else {
				if ws.minIntervalMs > 0 && e.Timestamp-curMax < ws.minIntervalMs {
					continue
				}
				if ws.dedupWindowMs > 0 && e.Timestamp-curMax <= ws.dedupWindowMs && curLast.IsEqual(e.Value) {
					continue
				}
			}
		}
		var dataLen int64
		var err error
		batchBuf, dataLen, err = appendSerialized(batchBuf, e.Key, e.Timestamp, e.Value)
		if err != nil {
			return batchBuf, length, err
		}
		length += dataLen
		e.EndPosition = length
		if prevKnown && e.Timestamp < curMax && needsSort != nil {
			*needsSort = true
		}
		if !prevKnown || e.Timestamp >= curMax {
			curMax, curLast, prevKnown = e.Timestamp, e.Value, true
		}
	}
	if haveRun {
		ws.storeTagState(curKey, curMax, curLast)
	}
	return batchBuf, length, nil
}

// appendSerialized serializes (key, timestamp, value) and appends to dst.
// Returns the updated slice and the byte length of the appended record.
//
// Record layout (big-endian): [4B totalLen][4B key][8B ts][value binary].
// The value is appended directly via Variant.AppendBinary (which appends to dst
// and never allocates when capacity allows) and the length header is backfilled,
// avoiding a per-entry allocation in the hot flushPending loop.
func appendSerialized(dst []byte, key tagCode, timestamp int64, value variant.Variant) ([]byte, int64, error) {
	start := len(dst)
	// Reserve the 16-byte header (4 len + 4 key + 8 ts).
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(dst[start+4:start+8], uint32(key))
	binary.BigEndian.PutUint64(dst[start+8:start+16], uint64(timestamp))
	dst = value.AppendBinary(dst)
	totalDataLen := len(dst) - start - 4
	binary.BigEndian.PutUint32(dst[start:start+4], uint32(totalDataLen))
	return dst, int64(4 + totalDataLen), nil
}

// rotateIfFull flushes and rotates the WAL file when the size threshold is reached.
func (ws *walFile) rotateIfFull() error {
	fileIndex := len(ws.walFiles) - 1
	if ws.walFiles[fileIndex].length >= ws.config.MaxFileSize {
		if err := ws.writeBuffer.Flush(); err != nil {
			return err
		}
		// Keep the full last file in place when the configured file limit has
		// been reached. The next batch receives ErrorWALCacheFull, allowing the
		// table to drain complete files before retrying without exceeding the
		// configured bound.
		if ws.config.MaxFileNumber > 0 && len(ws.walFiles) >= ws.config.MaxFileNumber {
			return nil
		}
		if err := ws.addWalFile(); err != nil {
			return err
		}
	}
	return nil
}

// flushPending only flushes bytes already accepted by WriteBatch. WAL no
// longer owns an active accumulation chunk; tableBatcher is the sole batching
// layer.
func (ws *walFile) flushPending() error {
	if ws.writeBuffer == nil {
		return ErrorWALClose
	}
	return ws.writeBuffer.Flush()
}

func (ws *walFile) FlushPending() error {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	return ws.flushPending()
}

func (ws *walFile) GetTagMaxTimestamp(key tagCode) (int64, variant.Variant, bool) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	maxTs, maxVal, ok := ws.loadTagState(key)

	// Also scan the unflushed last chunk for newer data.
	fileIndex := len(ws.walFiles) - 1
	readBuffer := ws.walFiles[fileIndex].readBuffer
	if readBuffer.activeChunks > 0 {
		lastChunk := &readBuffer.chunks[readBuffer.activeChunks-1]
		for i, entryKey := range lastChunk.keys {
			if entryKey == key && lastChunk.timestamps[i] >= maxTs {
				maxTs = lastChunk.timestamps[i]
				maxVal = lastChunk.values[i]
				ok = true
			}
		}
	}

	if !ok {
		return 0, variant.Variant{}, false
	}
	return maxTs, maxVal, true
}

func (ws *walFile) SetLastPoint(key tagCode, ts int64, value variant.Variant) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	if lp, _, ok := ws.loadTagState(key); !ok || ts > lp {
		ws.storeTagState(key, ts, value)
	}
}

func (ws *walFile) addWalFile() error {
	tm := time.Now().UnixNano()
	// Ensure unique filename: on Windows rapid successive calls
	// may get the same timestamp. Increment until unused.
	var fileName string
	for {
		fileName = filepath.Join(ws.filePath, strconv.FormatInt(tm, 10)+".wal")
		if _, err := os.Stat(fileName); os.IsNotExist(err) {
			break
		}
		tm++
	}
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	err = ws.writeFile.Close()
	if err != nil {
		return err
	}
	readBuffer := ws.spareReadBuffer
	if readBuffer == nil || readBuffer.chunkCap != ws.config.MaxBufferBatchSize {
		readBuffer = newWalReadBuffer(ws.config.MaxBufferBatchSize)
	} else {
		ws.spareReadBuffer = nil
	}
	ws.walFiles = append(ws.walFiles, walFileEnty{
		fileName:   fileName,
		length:     0,
		readBuffer: readBuffer,
	})
	ws.writeFile = file
	ws.writeBuffer.Reset(file)
	return err
}

func (ws *walFile) NeedFlush() bool {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	return len(ws.walFiles) > 1
}

func (ws *walFile) ReadByTime(tag tagCode, starTime int64, endTime int64) ([]Point, error) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	if ws.config.CloseBuffer && len(ws.walFiles) > 0 {
		_ = ws.flushPending()
		if ws.writeBuffer != nil {
			_ = ws.writeBuffer.Flush()
		}
	}
	estCap := 512
	if !ws.config.CloseBuffer {
		total := 0
		for i := range ws.walFiles {
			total += ws.walFiles[i].readBuffer.total
		}
		if ws.tagStateCount > 0 {
			perTag := total / ws.tagStateCount
			if perTag > estCap {
				estCap = perTag
			}
		}
	}
	var err error
	entries := make([]Point, 0, estCap)
	for i := range ws.walFiles {
		if !ws.config.CloseBuffer {
			entries = ws.walFiles[i].readBuffer.appendMatching(entries, tag, starTime, endTime)
		} else {
			err = forEachWalFile(ws.walFiles[i].fileName, func(key tagCode, timestamp int64, value variant.Variant, offset int64) bool {
				if key == tag && timestamp >= starTime && timestamp <= endTime {
					entries = append(entries, Point{Tms: timestamp, V: value})
				}
				return true
			})
		}
	}
	if len(entries) > 0 {
		sort.Slice(entries, func(i, j int) bool { return entries[i].Tms < entries[j].Tms })
	}
	return entries, err
}

// completeFileState snapshots the number of immutable WAL files and whether
// any of them retained late data. The caller passes count back to
// forEachCompleteFile so files rotated concurrently belong to the next flush.
func (ws *walFile) completeFileState() (count int, needsSort bool) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	count = len(ws.walFiles) - 1
	for i := 0; i < count; i++ {
		if ws.walFiles[i].needsSort {
			return count, true
		}
	}
	return count, false
}

func (ws *walFile) forEachCompleteFile(limit int, fc func(fileIndex int, tag tagCode, timestamp int64, data variant.Variant, offset int64) bool) (int, error) {
	ws.mutex.Lock()
	available := len(ws.walFiles) - 1
	if limit < 0 || limit > available {
		limit = available
	}
	snapshot := ws.walFiles[:limit]
	closeBuffer := ws.config.CloseBuffer
	ws.mutex.Unlock()

	for i := 0; i < len(snapshot); i++ {
		if closeBuffer {
			stopped := false
			err := forEachWalFile(snapshot[i].fileName, func(tag tagCode, timestamp int64, data variant.Variant, offset int64) bool {
				if !fc(i, tag, timestamp, data, offset) {
					stopped = true
					return false
				}
				return true
			})
			if err != nil {
				return i, err
			}
			if stopped {
				return i, nil
			}
		} else {
			if !snapshot[i].readBuffer.forEach(func(tag tagCode, timestamp int64, value variant.Variant) bool {
				// In-memory entries cannot fail while being traversed, so repair
				// never needs a byte offset for this path.
				return fc(i, tag, timestamp, value, 0)
			}) {
				return i, nil
			}
		}
	}
	return len(snapshot), nil
}

func (ws *walFile) retainWalFilePrefix(index int, truncateSize int64) error {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	if index == len(ws.walFiles)-1 {
		err := ws.writeBuffer.Flush()
		if err != nil {
			return err
		}
		err = ws.writeFile.Truncate(truncateSize)
		if err != nil {
			return err
		}
	} else {
		err := os.Truncate(ws.walFiles[index].fileName, truncateSize)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ws *walFile) truncate(n int) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	if n <= 0 {
		return
	}
	if n >= len(ws.walFiles) {
		n = len(ws.walFiles) - 1
	}
	if n <= 0 {
		return
	}
	if !ws.config.CloseBuffer && ws.spareReadBuffer == nil {
		// The newest consumed file is normally the fullest candidate. Keep only
		// a bounded prefix from one buffer; older files remain available to GC.
		ws.spareReadBuffer = ws.walFiles[n-1].readBuffer
		ws.spareReadBuffer.resetForReuse(maxRetainedWALReadBufferBytes)
	}
	for i := 0; i < n; i++ {
		if err := os.Remove(ws.walFiles[i].fileName); err != nil {
			_ = os.Rename(ws.walFiles[i].fileName, ws.walFiles[i].fileName+".deleted")
		}
	}
	ws.walFiles = ws.walFiles[n:]
}

// removeOrphanedFiles deletes .wal files from disk that are no longer in the
// walFiles list. On some platforms (notably Windows) os.Remove inside
// truncate can fail when a file handle is not yet fully released, leaving
// consumed files on disk. This is called from Close after all handles are
// closed, so removal is guaranteed to succeed.
func (ws *walFile) removeOrphanedFiles() {
	ws.mutex.Lock()
	keep := make(map[string]bool, len(ws.walFiles))
	for i := range ws.walFiles {
		keep[ws.walFiles[i].fileName] = true
	}
	ws.mutex.Unlock()

	entries, err := os.ReadDir(ws.filePath)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		fullPath := filepath.Join(ws.filePath, entry.Name())
		if !keep[fullPath] {
			_ = os.Remove(fullPath)
		}
	}
}

func (ws *walFile) Close() error {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	if ws.writeFile == nil {
		return nil
	}
	if err := ws.flushPending(); err != nil {
		return err
	}
	if err := ws.writeBuffer.Flush(); err != nil {
		return err
	}
	err := ws.writeFile.Close()
	if err != nil {
		return err
	}
	ws.writeFile = nil
	ws.spareReadBuffer = nil
	return err
}

func GetWalFileTms(filePath string) ([]int64, error) {
	if err := os.Mkdir(filePath, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	entries, err := os.ReadDir(filePath)
	if err != nil {
		return nil, err
	}
	var walFileTms = make([]int64, 0)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".wal") {
			tsStr := strings.TrimSuffix(entry.Name(), ".wal")
			ts, err := strconv.ParseInt(tsStr, 10, 64)
			if err != nil {
				continue
			}
			walFileTms = append(walFileTms, ts)
		}
	}
	sort.Slice(walFileTms, func(i, j int) bool {
		return walFileTms[i] < walFileTms[j]
	})
	return walFileTms, nil
}

func getAllData(filePath string, chunkCap int) (*walReadBuffer, int64, error) {
	if len(filePath) == 0 {
		return nil, 0, ErrorFilePathEmpty
	}
	cacheBuffer := newWalReadBuffer(chunkCap)
	offset := int64(0)
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, offset, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	lengthByte := make([]byte, 4)
	var data []byte

	for {
		if _, err = io.ReadFull(reader, lengthByte); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return cacheBuffer, offset, err
		}
		length := binary.BigEndian.Uint32(lengthByte)
		if length <= 12 {
			return cacheBuffer, offset, err
		}

		if int(length) > cap(data) {
			data = make([]byte, length)
		} else {
			data = data[:length]
		}

		if _, err = io.ReadFull(reader, data); err != nil {
			return cacheBuffer, offset, err
		}
		offset += int64(length) + 4

		var value variant.Variant
		if variant.IsBinaryFormat(data[12:]) {
			value, _, err = variant.UnmarshalBinary(data[12:])
		} else {
			value, err = variant.UnmarshalJSON(data[12:])
		}
		if err != nil {
			return cacheBuffer, offset, err
		}
		cacheBuffer.append(walDataEntry{
			Key:         tagCode(binary.BigEndian.Uint32(data[0:4])),
			Timestamp:   int64(binary.BigEndian.Uint64(data[4:12])),
			EndPosition: offset,
			Value:       value,
		})
	}
	return cacheBuffer, offset, err
}

func forEachWalFile(fileName string, fc func(tag tagCode, timestamp int64, data variant.Variant, offset int64) bool) error {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	lengthByte := make([]byte, 4)
	var data []byte

	offset := int64(0)
	for {
		_, err := io.ReadFull(reader, lengthByte)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		length := binary.BigEndian.Uint32(lengthByte)
		if length <= 12 {
			return ErrorWALDataCorruption
		}

		if int(length) > cap(data) {
			data = make([]byte, length)
		} else {
			data = data[:length]
		}

		_, err = io.ReadFull(reader, data)
		if err != nil {
			return err
		}

		offset += 4 + int64(length)
		var value variant.Variant
		if variant.IsBinaryFormat(data[12:]) {
			value, _, err = variant.UnmarshalBinary(data[12:])
		} else {
			value, err = variant.UnmarshalJSON(data[12:])
		}
		if err != nil {
			return err
		}
		if !fc(tagCode(binary.BigEndian.Uint32(data[0:4])), int64(binary.BigEndian.Uint64(data[4:12])), value, offset) {
			break
		}
	}
	return nil
}
