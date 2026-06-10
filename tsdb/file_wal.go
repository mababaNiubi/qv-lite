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

// walReadBuffer stores WAL entries in chunks.
// All chunks except the last are flushed (immutable).
// chunks[last] is the active unflushed chunk where new writes land.
type walReadBuffer struct {
	chunks   [][]walDataEntry
	total    int
	chunkCap int
}

func newWalReadBuffer(chunkCap int) *walReadBuffer {
	return &walReadBuffer{
		chunkCap: chunkCap,
		chunks:   [][]walDataEntry{make([]walDataEntry, 0, chunkCap)},
	}
}

func (b *walReadBuffer) append(e walDataEntry) {
	n := len(b.chunks)
	if n == 0 || len(b.chunks[n-1]) >= b.chunkCap {
		b.chunks = append(b.chunks, make([]walDataEntry, 0, b.chunkCap))
	}
	b.chunks[len(b.chunks)-1] = append(b.chunks[len(b.chunks)-1], e)
	b.total++
}

func (b *walReadBuffer) forEach(fn func(entry walDataEntry) bool) {
	for _, chunk := range b.chunks {
		for i := range chunk {
			if !fn(chunk[i]) {
				return
			}
		}
	}
}

func (b *walReadBuffer) unflushedCount() int {
	return len(b.chunks[len(b.chunks)-1])
}

type walFileEnty struct {
	fileName   string
	length     int64
	readBuffer *walReadBuffer
}

// WalFile is the File-like interface for the write-ahead log cache.
type WalFile interface {
	Write(key tagCode, timestamp int64, value variant.Variant) (bool, int, error)
	ReadByTime(tag tagCode, starTime int64, endTime int64) ([]int64, []variant.Variant, error)
	GetTagMaxTimestamp(key tagCode) (int64, variant.Variant, bool)
	NeedFlush() bool
	FlushPending() error
	forEachCompleteFile(fc func(fileIndex int, tag tagCode, timestamp int64, value variant.Variant, offset int64) bool) error
	retainWalFilePrefix(index int, truncateSize int64) error
	truncate()
	Close() error
}

type walFile struct {
	mutex           sync.RWMutex
	walFiles        []walFileEnty
	tagMaxTimestamp map[tagCode]int64
	tagLastValue    map[tagCode]variant.Variant

	writefile   *os.File
	writeBuffer *bufio.Writer

	filePath        string
	maxWalCacheSize int64
	maxWalCount     int
	closeBuffer     bool
	dedupWindowMs   int64
	minIntervalMs   int64
	maxWalBatchSize int
}

func NewWalFile(dirPath string, closeBuffer bool, maxWalCacheSize int64, maxWalCount int, dedupWindowMs, minIntervalMs int64, maxWalBatchSize int) (WalFile, error) {
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
	for i, t := range tms {
		walFiles[i].fileName = filepath.Join(filePath, strconv.FormatInt(t, 10)+".wal")
		walFiles[i].readBuffer, walFiles[i].length, err = getAllData(walFiles[i].fileName, maxWalBatchSize)
		if err != nil {
			err = os.Truncate(walFiles[i].fileName, walFiles[i].length)
			if err != nil {
				return nil, err
			}
		}
		if closeBuffer {
			walFiles[i].readBuffer = newWalReadBuffer(maxWalBatchSize)
		}
	}
	lastRB := walFiles[len(walFiles)-1].readBuffer
	lastRB.chunks = append(lastRB.chunks, make([]walDataEntry, 0, maxWalBatchSize))

	tagMaxTimestamp := make(map[tagCode]int64)
	tagLastValue := make(map[tagCode]variant.Variant)
	for i := range walFiles {
		walFiles[i].readBuffer.forEach(func(entry walDataEntry) bool {
			if maxTs, ok := tagMaxTimestamp[entry.Key]; !ok || entry.Timestamp > maxTs {
				tagMaxTimestamp[entry.Key] = entry.Timestamp
				tagLastValue[entry.Key] = entry.Value
			}
			return true
		})
	}

	file, err := os.OpenFile(walFiles[len(walFiles)-1].fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	wls := &walFile{
		writefile:       file,
		writeBuffer:     bufio.NewWriter(file),
		walFiles:        walFiles,
		tagMaxTimestamp: tagMaxTimestamp,
		tagLastValue:    tagLastValue,
		filePath:        filePath,
		closeBuffer:     closeBuffer,
		maxWalCacheSize: maxWalCacheSize,
		maxWalCount:     maxWalCount,
		dedupWindowMs:   dedupWindowMs,
		minIntervalMs:   minIntervalMs,
		maxWalBatchSize: maxWalBatchSize,
	}
	return wls, err
}

func (ws *walFile) Write(key tagCode, timestamp int64, value variant.Variant) (bool, int, error) {
	if ws.writefile == nil || ws.writeBuffer == nil {
		return false, 0, ErrorWALClose
	}

	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	fileIndex := len(ws.walFiles) - 1
	if ws.maxWalCount > 0 && ws.walFiles[fileIndex].length >= ws.maxWalCacheSize && len(ws.walFiles) >= ws.maxWalCount {
		return false, 0, ErrorWALCacheFull
	}

	if ws.closeBuffer {
		// Immediate write: require monotonic timestamps, write directly to disk.
		maxTimestamp, ok := ws.tagMaxTimestamp[key]
		if ok && timestamp < maxTimestamp {
			return false, 0, ErrorWALClose
		}
		if ws.skipDedup(key, timestamp, value, false) {
			return false, 0, nil
		}

		buf, dataLen, err := appendSerialized(nil, key, timestamp, value)
		if err != nil {
			return false, 0, err
		}
		if _, err = ws.writeBuffer.Write(buf); err != nil {
			return false, 0, err
		}

		ent := &ws.walFiles[fileIndex]
		ent.length += dataLen
		ws.tagMaxTimestamp[key] = timestamp
		ws.tagLastValue[key] = value

		if err := ws.rotateIfFull(); err != nil {
			return false, 0, err
		}
		return true, fileIndex, nil
	}

	// Batched mode: buffer in memory, flush when threshold reached.
	if ws.walFiles[fileIndex].readBuffer.unflushedCount() >= ws.maxWalBatchSize {
		if err := ws.flushPending(); err != nil {
			return false, 0, err
		}
	}

	ws.walFiles[fileIndex].readBuffer.append(walDataEntry{
		Key:       key,
		Timestamp: timestamp,
		Value:     value,
	})

	return true, fileIndex, nil
}

// flushPending sorts the last chunk by (Key, Timestamp), batch-writes
// all entries to the physical WAL file, then promotes the chunk to flushed.
// Must be called with ws.mutex held.
func (ws *walFile) flushPending() error {
	fileIndex := len(ws.walFiles) - 1
	ent := &ws.walFiles[fileIndex]
	rb := ent.readBuffer
	chunkIdx := len(rb.chunks) - 1
	chunk := rb.chunks[chunkIdx]

	if len(chunk) == 0 {
		return nil
	}

	// Skip sort if already ordered by (Key, Timestamp).
	sorted := true
	for i := 1; i < len(chunk); i++ {
		ci, pi := &chunk[i], &chunk[i-1]
		if ci.Key < pi.Key || (ci.Key == pi.Key && ci.Timestamp < pi.Timestamp) {
			sorted = false
			break
		}
	}
	if !sorted {
		sort.Slice(chunk, func(i, j int) bool {
			if chunk[i].Key != chunk[j].Key {
				return chunk[i].Key < chunk[j].Key
			}
			return chunk[i].Timestamp < chunk[j].Timestamp
		})
	}

	// Batch-accumulate serialized data, write once.
	batchPtr := batchWritePool.Get().(*[]byte)
	batchBuf := (*batchPtr)[:0]

	for i := range chunk {
		e := &chunk[i]

		if ws.skipDedup(e.Key, e.Timestamp, e.Value, false) {
			continue
		}

		var err error
		var dataLen int64
		batchBuf, dataLen, err = appendSerialized(batchBuf, e.Key, e.Timestamp, e.Value)
		if err != nil {
			return err
		}

		ent.length += dataLen
		e.EndPosition = ent.length
		ws.tagMaxTimestamp[e.Key] = e.Timestamp
		ws.tagLastValue[e.Key] = e.Value
	}

	if len(batchBuf) > 0 {
		if _, err := ws.writeBuffer.Write(batchBuf); err != nil {
			return err
		}
	}
	*batchPtr = batchBuf[:0]
	batchWritePool.Put(batchPtr)

	if ws.closeBuffer {
		rb.chunks[chunkIdx] = rb.chunks[chunkIdx][:0]
	} else {
		rb.chunks = append(rb.chunks, make([]walDataEntry, 0, ws.maxWalBatchSize))
	}

	return ws.rotateIfFull()
}

// appendSerialized serializes (key, timestamp, value) and appends to dst.
// Returns the updated slice and the byte length of the appended record.
func appendSerialized(dst []byte, key tagCode, timestamp int64, value variant.Variant) ([]byte, int64, error) {
	binaryValue, err := value.MarshalBinary()
	if err != nil {
		return dst, 0, err
	}
	totalDataLen := 12 + len(binaryValue)

	var dataArr [12]byte
	binary.BigEndian.PutUint32(dataArr[0:4], uint32(key))
	binary.BigEndian.PutUint64(dataArr[4:], uint64(timestamp))
	var lenArr [4]byte
	binary.BigEndian.PutUint32(lenArr[:], uint32(totalDataLen))

	dst = append(dst, lenArr[:]...)
	dst = append(dst, dataArr[:]...)
	dst = append(dst, binaryValue...)
	return dst, int64(4 + totalDataLen), nil
}

// skipDedup returns true if the entry should be skipped due to minIntervalMs
// or dedupWindowMs. For normal (batched) mode, only checks when ts >= prevTs.
func (ws *walFile) skipDedup(key tagCode, ts int64, value variant.Variant, requireOrder bool) bool {
	prevTs, ok := ws.tagMaxTimestamp[key]
	if !ok {
		return false
	}
	if requireOrder && ts < prevTs {
		return false
	}
	if ts < prevTs {
		return false
	}
	if ws.minIntervalMs > 0 && ts-prevTs < ws.minIntervalMs {
		return true
	}
	if ws.dedupWindowMs > 0 && ts-prevTs <= ws.dedupWindowMs {
		if prevVal, ok := ws.tagLastValue[key]; ok && prevVal.IsEqual(value) {
			return true
		}
	}
	return false
}

// rotateIfFull flushes and rotates the WAL file when the size threshold is reached.
func (ws *walFile) rotateIfFull() error {
	fileIndex := len(ws.walFiles) - 1
	if ws.walFiles[fileIndex].length >= ws.maxWalCacheSize {
		if err := ws.writeBuffer.Flush(); err != nil {
			return err
		}
		if err := ws.addWalFile(); err != nil {
			return err
		}
	}
	return nil
}

func (ws *walFile) FlushPending() error {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	return ws.flushPending()
}

func (ws *walFile) GetTagMaxTimestamp(key tagCode) (int64, variant.Variant, bool) {
	ws.mutex.RLock()
	defer ws.mutex.RUnlock()

	maxTs, ok := ws.tagMaxTimestamp[key]
	maxVal := ws.tagLastValue[key]

	// Also scan the unflushed last chunk for newer data.
	fileIndex := len(ws.walFiles) - 1
	lastChunk := ws.walFiles[fileIndex].readBuffer.chunks[len(ws.walFiles[fileIndex].readBuffer.chunks)-1]
	for i := range lastChunk {
		e := &lastChunk[i]
		if e.Key == key && e.Timestamp >= maxTs {
			maxTs = e.Timestamp
			maxVal = e.Value
			ok = true
		}
	}

	if !ok {
		return 0, variant.Variant{}, false
	}
	return maxTs, maxVal, true
}

func (ws *walFile) addWalFile() error {
	tm := time.Now().UnixNano()
	fileName := filepath.Join(ws.filePath, strconv.FormatInt(tm, 10)+".wal")
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	err = ws.writefile.Close()
	if err != nil {
		return err
	}
	ws.walFiles = append(ws.walFiles, walFileEnty{
		fileName:   fileName,
		length:     0,
		readBuffer: newWalReadBuffer(ws.maxWalBatchSize),
	})
	ws.writefile = file
	ws.writeBuffer.Reset(file)
	return err
}

func (ws *walFile) NeedFlush() bool {
	return len(ws.walFiles) > 1
}

func (ws *walFile) ReadByTime(tag tagCode, starTime int64, endTime int64) ([]int64, []variant.Variant, error) {
	if ws.closeBuffer {
		ws.mutex.RLock()
		needFlush := len(ws.walFiles) > 0
		ws.mutex.RUnlock()
		if needFlush {
			ws.mutex.Lock()
			_ = ws.flushPending()
			if ws.writeBuffer != nil {
				_ = ws.writeBuffer.Flush()
			}
			ws.mutex.Unlock()
		}
	}

	ws.mutex.RLock()
	estCap := 512
	if !ws.closeBuffer {
		total := 0
		for i := range ws.walFiles {
			total += ws.walFiles[i].readBuffer.total
		}
		if total > estCap {
			estCap = total
		}
	}
	tmsAll := make([]int64, 0, estCap)
	tmsAllValue := make([]variant.Variant, 0, estCap)
	defer ws.mutex.RUnlock()
	for i := range ws.walFiles {
		isFirstData := true
		if !ws.closeBuffer {
			ws.walFiles[i].readBuffer.forEach(func(p walDataEntry) bool {
				if p.Key == tag {
					if isFirstData && endTime < p.Timestamp {
						return true
					}
					isFirstData = false
					if p.Timestamp >= starTime && p.Timestamp <= endTime {
						tmsAll = append(tmsAll, p.Timestamp)
						tmsAllValue = append(tmsAllValue, p.Value)
					}
				}
				return true
			})
		} else {
			err := forEachWalFile(ws.walFiles[i].fileName, func(key tagCode, timestamp int64, value variant.Variant, offset int64) bool {
				if key == tag && timestamp >= starTime && timestamp <= endTime {
					tmsAll = append(tmsAll, timestamp)
					tmsAllValue = append(tmsAllValue, value)
				}
				return true
			})
			if err != nil {
				return tmsAll, tmsAllValue, err
			}
		}
	}
	return tmsAll, tmsAllValue, nil
}

func (ws *walFile) forEachCompleteFile(fc func(fileIndex int, tag tagCode, timestamp int64, data variant.Variant, offset int64) bool) error {
	for i := 0; i < len(ws.walFiles)-1; i++ {
		if ws.closeBuffer {
			err := forEachWalFile(ws.walFiles[i].fileName, func(tag tagCode, timestamp int64, data variant.Variant, offset int64) bool {
				return fc(i, tag, timestamp, data, offset)
			})
			if err != nil {
				return err
			}
		} else {
			ws.walFiles[i].readBuffer.forEach(func(e walDataEntry) bool {
				return fc(i, e.Key, e.Timestamp, e.Value, e.EndPosition)
			})
		}
	}
	return nil
}

func (ws *walFile) retainWalFilePrefix(index int, truncateSize int64) error {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	if index == len(ws.walFiles)-1 {
		err := ws.writeBuffer.Flush()
		if err != nil {
			return err
		}
		err = ws.writefile.Truncate(truncateSize)
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

func (ws *walFile) truncate() {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	for i := 0; i <= len(ws.walFiles)-1; i++ {
		_ = os.Remove(ws.walFiles[i].fileName)
	}
	ws.walFiles = ws.walFiles[len(ws.walFiles)-1:]
}

func (ws *walFile) Close() error {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	if ws.writefile == nil {
		return nil
	}
	if err := ws.flushPending(); err != nil {
		return err
	}
	if err := ws.writeBuffer.Flush(); err != nil {
		return err
	}
	err := ws.writefile.Close()
	if err != nil {
		return err
	}
	ws.writefile = nil
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
	cacheBuffer := &walReadBuffer{chunkCap: chunkCap}
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
