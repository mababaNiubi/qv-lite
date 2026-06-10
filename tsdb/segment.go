package tsdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var segmentHeaderSize = int64(binary.Size(SegmentHeader{}))

type SegmentHeader struct {
	Attribute tagCode `json:"attribute"`
	MinTime   int64   `json:"min_time"`
	MaxTime   int64   `json:"max_time"`
	DataSize  int64   `json:"data_size"`
	Crc       uint32  `json:"crc"`
}

type FileSegment interface {
	FileWriter
	FileReader
	GetMinTms() int64
	GetIndex() *FileIndex
	PersistIndex() error
	InspectBlockIndex(tableInfo *TableInfo) error
	Remove() error
}

type fileSegment struct {
	filePath string
	FileReader
	FileWriter
	compressor BlockCompressor
	timestamp  int64
	index      *FileIndex
}

func newFileSegment(filePath string, tm int64, compressor BlockCompressor, index *FileIndex, cache *readerCache) *fileSegment {
	return &fileSegment{
		FileWriter: NewFileWriter(filePath, compressor),
		FileReader: NewFileReader(filePath, compressor, cache),
		filePath:   filePath,
		compressor: compressor,
		timestamp:  tm,
		index:      index,
	}
}
func (w *fileSegment) Remove() error {
	err := w.Cleanup()
	if err != nil {
		return err
	}
	_ = os.Remove(indexFilePath(w.filePath))
	return os.Remove(w.filePath)
}

func (w *fileSegment) GetMinTms() int64 { return w.timestamp }

// GetIndex returns the index, building it from the file if not already loaded.
func (w *fileSegment) GetIndex() *FileIndex {
	if w.index != nil {
		return w.index
	}
	// Try loading from the .idx file first.
	if idx := readIndexFile(indexFilePath(w.filePath)); idx != nil {
		w.index = idx
		return idx
	}
	return w.index
}

func (w *fileSegment) PersistIndex() error {
	if w.index == nil || len(w.index.Blocks) == 0 {
		return nil
	}
	return writeIndexFile(indexFilePath(w.filePath), w.index)
}

func (w *fileSegment) InspectBlockIndex(tableInfo *TableInfo) error {
	_ = w.OpenReader()
	idx := &FileIndex{
		Blocks: make([]BlockIndexEntry, 0, 5),
	}
	for {
		lastOffset := w.GetReadEffectiveSize()
		head, compressedTimeData, compressedValueData, err := w.NextRead(func(header SegmentHeader) bool { return true }, tableInfo)
		if err != nil || head == nil {
			break
		}
		_, err = GetAllPointByBytes(tableInfo.Structure, compressedTimeData, compressedValueData, nil)
		if err != nil {
			break
		}
		if idx.MinTime == 0 || head.MinTime < idx.MinTime {
			idx.MinTime = head.MinTime
		}
		if head.MaxTime > idx.MaxTime {
			idx.MaxTime = head.MaxTime
		}
		idx.Blocks = append(idx.Blocks, BlockIndexEntry{
			Attribute: head.Attribute,
			MinTime:   head.MinTime,
			MaxTime:   head.MaxTime,
			Offset:    lastOffset,
			DataSize:  head.DataSize,
		})
	}
	effectiveSize := w.GetReadEffectiveSize()
	w.CloseReader()

	if effectiveSize > 0 {
		bf, err := OpenBlockFile(w.filePath, w.compressor, BlockSizeDef)
		if err != nil {
			return err
		}
		if effectiveSize < bf.Size() {
			if err := bf.Truncate(effectiveSize); err != nil {
				bf.Close()
				return err
			}
		}
		bf.Close()
	}
	if w.index == nil {
		w.index = idx
	}
	return nil
}

// ─── Segment list (array-based, time-sorted) ────────────────────────

type fileSegmentList struct {
	segments                   []FileSegment
	activeIdx                  int // Index of the latest (currently writing) segment, -1 if empty.
	openStartWriterSegmentIdx  int // Segment index at the start of the current transaction.
	tableFragmentationFilePath string
	compressor                 BlockCompressor
	readerCache                *readerCache
	mutex                      sync.RWMutex
}

func (s *fileSegmentList) len() int { return len(s.segments) }

func (s *fileSegmentList) GetLastFragmentation() FileSegment {
	if s.activeIdx < 0 || s.activeIdx >= len(s.segments) {
		return nil
	}
	return s.segments[s.activeIdx]
}

func (s *fileSegmentList) InspectLastBlockIndex(tableInfo *TableInfo) error {
	f := s.GetLastFragmentation()
	if f == nil {
		return nil
	}
	return f.InspectBlockIndex(tableInfo)
}
func (s *fileSegmentList) PersistLastIndex() error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if s.activeIdx < 0 || s.activeIdx >= len(s.segments) {
		return nil
	}
	return s.segments[s.activeIdx].PersistIndex()
}

func (s *fileSegmentList) OpenTransaction() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.activeIdx < 0 {
		timestamp := time.Now().UnixNano()
		path := filepath.Join(s.tableFragmentationFilePath, fmt.Sprintf("%v%s", timestamp, dataSuffix))
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		f.Close()
		fs := newFileSegment(path, timestamp, s.compressor, &FileIndex{Blocks: make([]BlockIndexEntry, 0, 5)}, s.readerCache)
		s.segments = append(s.segments, fs)
		s.activeIdx = len(s.segments) - 1
	}
	s.openStartWriterSegmentIdx = s.activeIdx
	return s.segments[s.activeIdx].OpenWriter()
}

func (s *fileSegmentList) CommitTransactionFileSegment() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.openStartWriterSegmentIdx < 0 {
		return nil
	}
	var err error
	for i := s.openStartWriterSegmentIdx; i <= s.activeIdx && i < len(s.segments); i++ {
		if errC := s.segments[i].Commit(); errC != nil {
			err = errors.Join(err, errC)
		}
		// Evict cached reader for this segment — the file has new data.
		if s.readerCache != nil {
			if fs, ok := s.segments[i].(*fileSegment); ok {
				s.readerCache.evict(fs.filePath)
			}
		}
	}
	return err
}

func (s *fileSegmentList) AddTransactionSegment() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.openStartWriterSegmentIdx < 0 {
		return ErrorNoTransaction
	}
	// Cleanup the current writer before switching to a new segment.
	if s.activeIdx >= 0 && s.activeIdx < len(s.segments) {
		if err := s.segments[s.activeIdx].Cleanup(); err != nil {
			return err
		}
	}
	timestamp := time.Now().UnixNano()
	path := filepath.Join(s.tableFragmentationFilePath, fmt.Sprintf("%v%s", timestamp, dataSuffix))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	f.Close()
	fs := newFileSegment(path, timestamp, s.compressor, &FileIndex{Blocks: make([]BlockIndexEntry, 0, 5)}, s.readerCache)
	s.segments = append(s.segments, fs)
	s.activeIdx = len(s.segments) - 1
	return fs.OpenWriter()
}

func (s *fileSegmentList) RollbackLastCommitTransaction() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.openStartWriterSegmentIdx < 0 {
		return ErrorNoTransaction
	}
	var err error
	for i := s.openStartWriterSegmentIdx; i <= s.activeIdx && i < len(s.segments); i++ {
		if i == s.openStartWriterSegmentIdx {
			if errC := s.segments[i].RollbackLastCommit(); errC != nil {
				err = errors.Join(err, errC)
			}
		} else {
			if errC := s.segments[i].Remove(); errC != nil {
				err = errors.Join(err, errC)
			}
		}
		if s.readerCache != nil {
			if fs, ok := s.segments[i].(*fileSegment); ok {
				s.readerCache.evict(fs.filePath)
			}
		}
	}
	// Remove segments added during the transaction.
	if s.openStartWriterSegmentIdx+1 < len(s.segments) {
		s.segments = s.segments[:s.openStartWriterSegmentIdx+1]
		s.activeIdx = s.openStartWriterSegmentIdx
	}
	return err
}

// Range iterates over all segments in order (backward compatible).
func (s *fileSegmentList) Range(f func(FileSegment, FileSegment) bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	for i := 0; i < len(s.segments); i++ {
		var next FileSegment
		if i+1 < len(s.segments) {
			next = s.segments[i+1]
		}
		if !f(s.segments[i], next) {
			break
		}
	}
}

// RangeFromTime binary-searches for the starting segment, then iterates forward
// through segments whose time ranges overlap [startTime, endTime].
func (s *fileSegmentList) RangeFromTime(startTime, endTime int64, f func(FileSegment) bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	n := len(s.segments)
	if n == 0 {
		return
	}
	first := sort.Search(n, func(i int) bool {
		idx := s.segments[i].GetIndex()
		if idx != nil && idx.MaxTime != 0 {
			return idx.MaxTime >= startTime
		}
		return true
	})
	if first > 0 {
		first--
	}
	for i := first; i < n; i++ {
		seg := s.segments[i]
		// Stop if the segment's start time is beyond the query range.
		idx := seg.GetIndex()
		if idx != nil && idx.MinTime != 0 {
			if idx.MaxTime < startTime {
				continue
			}
			if idx.MinTime > endTime {
				break
			}
		} else if s.segments[i].GetMinTms() > endTime {
			break
		}
		if !f(seg) {
			break
		}
	}
}

func (s *fileSegmentList) Remove(timestamp int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	n := len(s.segments)
	if n < 2 {
		return
	}
	cut := -1
	for i := 0; i < n-1; i++ {
		if s.segments[i].GetMinTms() <= timestamp && s.segments[i+1].GetMinTms() < timestamp {
			cut = i

		}
	}
	if cut < 0 {
		return
	}
	// Remove segments in the range [0, cut].
	for i := 0; i <= cut; i++ {
		s.segments[i].Remove()
		if s.readerCache != nil {
			s.readerCache.evict(s.segments[i].(*fileSegment).filePath)
		}
	}
	s.segments = s.segments[cut+1:]
	s.activeIdx -= cut + 1
	if s.activeIdx < 0 {
		s.activeIdx = -1
	}
	s.openStartWriterSegmentIdx -= cut + 1
	if s.openStartWriterSegmentIdx < 0 {
		s.openStartWriterSegmentIdx = -1
	}
}

func (s *fileSegmentList) BuildFragmentation(dirPath string, compressor BlockCompressor) error {
	s.compressor = compressor
	s.tableFragmentationFilePath = filepath.Join(dirPath, dataPath)
	s.activeIdx = -1
	s.openStartWriterSegmentIdx = -1
	if err := os.MkdirAll(s.tableFragmentationFilePath, os.ModePerm); err != nil {
		return err
	}
	entries, err := os.ReadDir(s.tableFragmentationFilePath)
	if err != nil {
		return err
	}
	timeSet := make(map[int64]bool)
	var timestamps []int64
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, dataSuffix) {
			ts, err := strconv.ParseInt(strings.TrimSuffix(name, dataSuffix), 10, 64)
			if err == nil {
				timeSet[ts] = true
				timestamps = append(timestamps, ts)
			}
		}
	}
	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })
	for _, ts := range timestamps {
		path := filepath.Join(s.tableFragmentationFilePath, fmt.Sprintf("%v%s", ts, dataSuffix))
		index := &FileIndex{Blocks: make([]BlockIndexEntry, 0, 5)}
		if idx := readIndexFile(indexFilePath(path)); idx != nil {
			index = idx
		}
		fs := newFileSegment(path, ts, s.compressor, index, s.readerCache)
		s.segments = append(s.segments, fs)
	}
	if len(s.segments) > 0 {
		s.activeIdx = len(s.segments) - 1
	}
	return nil
}
