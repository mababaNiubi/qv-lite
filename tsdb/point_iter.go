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

// PointIter is a pull-based point stream for a single-tag query.
//
// Points are emitted in time order within each source: disk segments first,
// then the WAL (the historical disk-then-WAL result order). For monotonic
// workloads the combined stream is globally time-ordered; late (out-of-order)
// data retained in the WAL may invert points at the disk/WAL boundary, exactly
// as before the streaming rewrite.
//
// The iterator owns the table's query lock and any readers it opened; the
// caller must call Close when done, even after early termination (limit or
// context cancellation). Points are returned by value and are self-contained
// (decoded values own their memory), so a returned Point stays valid after
// the next Next call.
type PointIter interface {
	// Next returns the next point matching the query, or ok=false when the
	// stream is exhausted (or the configured limit was reached).
	Next() (Point, bool, error)
	// Close releases the query lock and all readers held by the iterator.
	Close() error
}

// QueryOptions bounds a query. Zero values mean "unbounded".
type QueryOptions struct {
	// Limit caps the number of points returned (after Offset, and after
	// condition filtering). Values <= 0 mean unlimited.
	Limit int
	// Offset skips the first Offset points in time order before returning.
	// It only matters together with a positive Limit.
	Offset int64
}

// pointSource is the internal pull interface implemented by the disk and WAL
// sides of a query. Both produce time-ordered points.
type pointSource interface {
	next() (Point, bool, error)
	close()
}

// ─── WAL side ─────────────────────────────────────────────────────────

// walTagChunkSnapshot is a point-in-time view of one WAL read chunk. The slice
// headers are copied under the WAL mutex so iteration afterwards is lock-free:
// the write path only appends past the captured lengths and never mutates the
// captured range, and buffer rotation/reset happens only during flush, which
// the table query lock excludes for the duration of a query.
type walTagChunkSnapshot struct {
	keys       []tagCode
	timestamps []int64
	values     []variant.Variant
}

type walTagFileSnapshot struct {
	fileName    string
	closeBuffer bool
	length      int64 // bytes valid in fileName when closeBuffer is true
	needsSort   bool
	rb          *walReadBuffer // live buffer for lazy tag indexes (nil for closeBuffer)
	indexable   int            // number of full (immutable) chunks at snapshot time
	chunks      []walTagChunkSnapshot
}

// walTagSnapshot is a point-in-time, race-free view of the WAL read caches for
// one query. Only slice headers are copied, so it is cheap and does not
// duplicate point data.
type walTagSnapshot struct {
	files        []walTagFileSnapshot
	anyNeedsSort bool
}

// snapshotTagTime captures the WAL read caches under the WAL mutex.
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

// walPointIter streams one tag's WAL points in time order without
// materializing the result. When any WAL file retained late (out-of-order)
// data, it falls back to the historical collect-and-sort behavior, bounded by
// one query's matching WAL points.
type walPointIter struct {
	snap      walTagSnapshot
	tag       tagCode
	startTime int64
	endTime   int64

	// needsSort fallback: fully materialized, sorted matching points.
	sorted    []Point
	sortedIdx int

	// streaming state over snap.files.
	fileIdx   int
	chunkIdx  int
	posIdx    int
	positions []int32 // indexed positions within the current chunk (nil = scan mode)

	// closeBuffer (file re-read) streaming state.
	f         *os.File
	br        *bufio.Reader
	remaining int64
	dataBuf   []byte

	err error
}

func newWalPointIter(snap walTagSnapshot, tag tagCode, startTime, endTime int64) *walPointIter {
	w := &walPointIter{
		snap:      snap,
		tag:       tag,
		startTime: startTime,
		endTime:   endTime,
	}
	if !snap.anyNeedsSort {
		return w
	}
	// Rare late-data case: same semantics as the old ReadByTime — collect all
	// matching points and sort them globally.
	for i := range snap.files {
		file := &snap.files[i]
		if file.closeBuffer {
			w.collectFile(file)
			continue
		}
		for c := range file.chunks {
			chunk := &file.chunks[c]
			if file.rb != nil && c < file.indexable {
				if idx := file.rb.tagIndex(c, file.chunks[c].keys); idx != nil {
					for _, j := range idx[tag] {
						ts := chunk.timestamps[j]
						if ts >= startTime && ts <= endTime {
							w.sorted = append(w.sorted, Point{Tms: ts, V: chunk.values[j]})
						}
					}
					continue // indexed chunk handled
				}
			}
			for j := range chunk.keys {
				if chunk.keys[j] != tag {
					continue
				}
				ts := chunk.timestamps[j]
				if ts >= startTime && ts <= endTime {
					w.sorted = append(w.sorted, Point{Tms: ts, V: chunk.values[j]})
				}
			}
		}
	}
	sort.Slice(w.sorted, func(i, j int) bool { return w.sorted[i].Tms < w.sorted[j].Tms })
	return w
}

func (w *walPointIter) next() (Point, bool, error) {
	if w.err != nil {
		return Point{}, false, w.err
	}
	if w.sorted != nil {
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
			w.fileIdx++
			continue
		}
		for w.chunkIdx < len(file.chunks) {
			if w.positions != nil {
				// Iterating positions from the lazy tag index of the current
				// chunk. posIdx only advances here; it is reset when moving to
				// a different chunk, never at the top of next().
				chunk := &file.chunks[w.chunkIdx]
				// For an ordered file the tag's positions in this chunk are
				// timestamp-monotonic, so the whole chunk can be skipped by
				// its first/last timestamp and the per-position loop can stop
				// early past the query end.
				ordered := !file.needsSort
				if ordered && len(w.positions) > 0 {
					if chunk.timestamps[w.positions[0]] > w.endTime ||
						chunk.timestamps[w.positions[len(w.positions)-1]] < w.startTime {
						w.positions = nil
						w.chunkIdx++
						w.posIdx = 0
						continue
					}
				}
				for w.posIdx < len(w.positions) {
					j := w.positions[w.posIdx]
					w.posIdx++
					ts := chunk.timestamps[j]
					if ts > w.endTime {
						if ordered {
							break // monotonic: later positions are larger
						}
						continue
					}
					if ts < w.startTime {
						continue
					}
					return Point{Tms: ts, V: chunk.values[j]}, true, nil
				}
				w.positions = nil
				w.chunkIdx++
				w.posIdx = 0
				continue
			}
			// Full chunks try the lazy per-tag index; the chunk that was still
			// active at snapshot time is always scanned directly.
			if file.rb != nil && w.chunkIdx < file.indexable {
				if idx := file.rb.tagIndex(w.chunkIdx, file.chunks[w.chunkIdx].keys); idx != nil {
					if pos := idx[w.tag]; len(pos) > 0 {
						w.positions = pos
						w.posIdx = 0
						continue
					}
					w.chunkIdx++ // indexed chunk without this tag
					continue
				}
			}
			// Fallback: scan the chunk's keys.
			chunk := &file.chunks[w.chunkIdx]
			for w.posIdx < len(chunk.keys) {
				key := chunk.keys[w.posIdx]
				ts := chunk.timestamps[w.posIdx]
				val := chunk.values[w.posIdx]
				w.posIdx++
				if key != w.tag || ts < w.startTime || ts > w.endTime {
					continue
				}
				return Point{Tms: ts, V: val}, true, nil
			}
			w.chunkIdx++
			w.posIdx = 0
		}
		w.fileIdx++
		w.chunkIdx = 0
		w.posIdx = 0
		w.positions = nil
	}
	return Point{}, false, nil
}

// walk streams this source into fn (see diskPointIter.walk for the rationale
// of the duplicated concrete loop and its branch-free fast path).
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
	for {
		p, ok, err := w.next()
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

// nextFromFile streams records from a WAL file (CloseBuffer mode), reading
// only up to the snapshot length so a concurrent writer's partial tail record
// is never observed.
func (w *walPointIter) nextFromFile(file *walTagFileSnapshot) (Point, bool, error) {
	if w.f == nil {
		f, err := os.Open(file.fileName)
		if err != nil {
			return Point{}, false, err
		}
		w.f = f
		w.br = bufio.NewReader(f)
		w.remaining = file.length
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

// collectFile reads all matching records of one WAL file into the sorted
// fallback buffer (CloseBuffer + late-data path).
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
		var lenBuf [4]byte
		if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
			w.err = err
			return
		}
		remaining -= 4
		length := binary.BigEndian.Uint32(lenBuf[:])
		if length <= 12 {
			w.err = ErrorWALDataCorruption
			return
		}
		if int(length) > cap(data) {
			data = make([]byte, length)
		}
		data = data[:length]
		if _, err := io.ReadFull(br, data); err != nil {
			w.err = err
			return
		}
		remaining -= int64(length)
		key := tagCode(binary.BigEndian.Uint32(data[0:4]))
		ts := int64(binary.BigEndian.Uint64(data[4:12]))
		if key != w.tag || ts < w.startTime || ts > w.endTime {
			continue
		}
		value, _, err := unmarshalWalValue(data[12:])
		if err != nil {
			w.err = err
			return
		}
		w.sorted = append(w.sorted, Point{Tms: ts, V: value})
	}
}

// unmarshalWalValue decodes a WAL value payload in either binary or JSON form.
func unmarshalWalValue(payload []byte) (variant.Variant, int, error) {
	if variant.IsBinaryFormat(payload) {
		return variant.UnmarshalBinary(payload)
	}
	v, err := variant.UnmarshalJSON(payload)
	return v, 0, err
}

func (w *walPointIter) close() {
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
}

// ─── Disk side ────────────────────────────────────────────────────────

// diskPointIter pulls a tag's points from disk segments in block order,
// decoding at most one block at a time. Each segment is read through a
// per-query FileReader, so concurrent queries never share the mutable reader
// state embedded in the shared fileSegment (the old query path raced on
// dataBuf/readEffectiveOffset and permanently retained the largest block
// buffer per reader).
type diskPointIter struct {
	table     *ssTable
	code      tagCode
	startTime int64
	endTime   int64
	ctx       context.Context

	segments []FileSegment
	segIdx   int

	// per-segment state
	fs           FileSegment
	reader       FileReader
	readerOpened bool
	idx          *FileIndex
	positions    []int // index-mode positions into idx.Blocks
	posIdx       int
	scanning     bool

	pack *PointDiskPack
	err  error
}

func (d *diskPointIter) next() (Point, bool, error) {
	if d.err != nil {
		return Point{}, false, d.err
	}
	for {
		if d.ctx != nil {
			if err := d.ctx.Err(); err != nil {
				d.err = err
				return Point{}, false, err
			}
		}
		if d.pack.Next() {
			tms, value := d.pack.Read()
			return Point{Tms: tms, V: value}, true, nil
		}
		// Current block exhausted: load the next matching block.
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

// nextBlock advances to the next matching block, replicating forEachBlock's
// index-vs-scan decision, and returns its decompressed time/value bytes.
func (d *diskPointIter) nextBlock() (*SegmentHeader, []byte, []byte, error) {
	for {
		if d.fs == nil && !d.nextSegment() {
			return nil, nil, nil, nil
		}
		if d.scanning {
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
		// Index mode: random access by block position.
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

// nextSegment advances to the next segment with at least one matching block.
// One fileReader is created per query and rebound to each segment's file, so
// the block buffer (dataBuf) is allocated once per query and reused across
// blocks/segments instead of being reallocated per segment — the historical
// shared-reader path reused its retained buffer across queries, and per-block
// reallocation measurably regressed repeat queries over large blocks.
func (d *diskPointIter) nextSegment() bool {
	d.endSegment()
	for d.segIdx < len(d.segments) {
		fs := d.segments[d.segIdx]
		d.segIdx++
		if fs == nil {
			continue
		}
		idx := fs.GetIndex()
		if idx != nil && len(idx.Blocks) > 0 {
			if d.startTime > idx.MaxTime || d.endTime < idx.MinTime {
				continue
			}
			matching := idx.matchingBlockPositions(d.code, d.startTime, d.endTime)
			if len(matching) == 0 {
				continue
			}
			if len(matching) > len(idx.Blocks)/2 || len(matching) > 100 {
				d.scanning = true
			} else {
				d.scanning = false
				d.idx = idx
				d.positions = matching
				d.posIdx = 0
			}
		} else {
			d.scanning = true
		}
		if d.reader == nil {
			if seg, ok := fs.(*fileSegment); ok {
				d.reader = NewFileReader(seg.filePath, seg.compressor, d.table.fragmentation.readerCache)
			} else {
				d.reader = fs.(FileReader)
			}
		} else if seg, ok := fs.(*fileSegment); ok {
			// Rebind the per-query reader to this segment's file.
			fr := d.reader.(*fileReader)
			fr.filePath = seg.filePath
			fr.compressor = seg.compressor
		}
		d.fs = fs
		return true
	}
	return false
}

func (d *diskPointIter) endSegment() {
	// The per-query reader stays open across segment switches (OpenReader
	// rebinds it to the next file); only the final close() releases it.
	d.readerOpened = false
	d.fs = nil
	d.idx = nil
	d.positions = nil
	d.posIdx = 0
	d.scanning = false
}

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

// ─── Two-phase streaming ─────────────────────────────────────────────

// mergePointIter streams the disk source first and then the WAL source,
// applying the condition filter, offset/limit and context cancellation on the
// fly. Both sources are individually time-ordered, so for monotonic workloads
// (the normal case) the combined stream is globally time-ordered and matches
// the historical disk-then-WAL query result order — without materializing the
// result. A per-point k-way merge was tried here and measurably regressed
// result-heavy queries (~30 ns/point of state-machine overhead), so the two
// phases are streamed back to back instead.
type mergePointIter struct {
	a, b pointSource
	cur  pointSource // a until exhausted, then b
	done bool

	evalCond ConditionFilter
	ctx      context.Context
	err      error

	limit   int
	offset  int64
	emitted int64
	skipped int64

	release func() // releases the table query lock
}

func (m *mergePointIter) next() (Point, bool, error) {
	for {
		if m.err != nil {
			return Point{}, false, m.err
		}
		if m.limit > 0 && m.emitted >= int64(m.limit) {
			return Point{}, false, nil
		}
		if m.ctx != nil {
			if err := m.ctx.Err(); err != nil {
				m.err = err
				return Point{}, false, err
			}
		}
		if m.cur == nil {
			m.cur = m.a
		}
		p, ok, err := m.cur.next()
		if err != nil {
			m.err = err
			return Point{}, false, err
		}
		if !ok {
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

func (m *mergePointIter) close() {
	m.a.close()
	m.b.close()
	if m.release != nil {
		m.release()
		m.release = nil
	}
}

// Next implements PointIter.
func (m *mergePointIter) Next() (Point, bool, error) {
	return m.next()
}

// Close implements PointIter.
func (m *mergePointIter) Close() error {
	m.close()
	return nil
}

// queryIter creates a time-ordered, condition-filtered point stream for one
// tag. It holds the table query lock until the returned iterator is closed,
// which also keeps concurrent flushes from truncating segments mid-query.
// The caller must close the returned iterator.
func (s *ssTable) queryIter(ctx context.Context, code tagCode, startTime, endTime int64, evalCond ConditionFilter, opts *QueryOptions) PointIter {
	s.queryMute.RLock()
	disk := newDiskIter(s, code, startTime, endTime, ctx)
	m := &mergePointIter{
		a:        disk,
		b:        newWalPointIter(s.walFile.snapshotTagTime(), code, startTime, endTime),
		evalCond: evalCond,
		ctx:      ctx,
		release:  s.queryMute.RUnlock,
	}
	if opts != nil {
		m.limit = opts.Limit
		m.offset = opts.Offset
	}
	return m
}

// newDiskIter builds a disk source for one tag query, snapshotting the
// matching segments under the segment-list lock (they stay valid while the
// table query lock is held).
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

// compileCond returns nil for a nil condition so the per-point evaluation call
// is skipped entirely in walk (CompileCondition(nil) would return an
// always-true closure that still costs a call per point).
func compileCond(cond any) ConditionFilter {
	if cond == nil {
		return nil
	}
	return CompileCondition(cond)
}

// walk streams this source into fn, applying the condition filter and
// offset/limit. The loop is a per-concrete-type method (also implemented on
// walPointIter) so the per-point next call is direct instead of going through
// the generic shape dictionary. When no filter/limit/offset is configured the
// common case runs a branch-free loop.
func (d *diskPointIter) walk(evalCond ConditionFilter, limit int, offset int64, emitted, skipped *int64, fn func(Point) bool) error {
	if evalCond == nil && limit <= 0 && offset <= 0 {
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
	for {
		p, ok, err := d.next()
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

// forEachQueryPoint drives the two-phase point stream — disk first, then WAL —
// with the condition filter, offset/limit and the table query lock held. This
// is the materialization path for Query/QueryAll/QueryWindow; the phases match
// the historical disk-then-WAL result order without materializing the result.
func (s *ssTable) forEachQueryPoint(code tagCode, startTime, endTime int64, evalCond ConditionFilter, opts *QueryOptions, fn func(Point) bool) error {
	s.queryMute.RLock()
	defer s.queryMute.RUnlock()
	disk := newDiskIter(s, code, startTime, endTime, nil)
	defer disk.close()
	wal := newWalPointIter(s.walFile.snapshotTagTime(), code, startTime, endTime)
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
