package tsdb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/golang/snappy"
)

const (
	Magic        = 0x434C424B
	Version      = 1
	HeaderSize   = 39 // binary.Write writes fields without alignment padding
	BlockSizeDef = 64 * 1024

	TxDone   = 0
	TxActive = 1
)

// FileHeader is the file header metadata (48 bytes, LittleEndian).
type FileHeader struct {
	Magic       uint32
	Version     uint8
	TxState     uint8
	CodecID     uint8
	BlockSize   uint32
	TotalSize   int64
	DataEnd     int64
	IndexOffset int64
	HeaderCrc   uint32
}

// IndexEntry is a single block index entry (32 bytes, LittleEndian).
type IndexEntry struct {
	RawOff  int64
	CompOff int64
	CompLen int64
	Crc32   uint32
	_       uint32
}

// BlockFile is a transparently compressed file layer implementing io.ReadWriteSeeker.
// It presents a normal file interface externally while internally handling
// compression/decompression and crash recovery automatically.
type BlockFile struct {
	mu              sync.RWMutex
	file            *os.File
	filePath        string
	blockCompressor BlockCompressor
	header          FileHeader
	blockSize       int
	buf             []byte
	readBuf         []byte
	decodeBuf       []byte
	pos             int64
	totalSize       int64
	index           []IndexEntry
	lastRawOff      int64
	dataWritePos    int64
	lastPhysicalPos int64
	closed          atomic.Bool
}

// OpenBlockFile opens or creates a compressed file with automatic crash recovery.
func OpenBlockFile(path string, blockCompressor BlockCompressor, blockSize int) (*BlockFile, error) {
	if blockSize <= 0 {
		blockSize = BlockSizeDef
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	bf := &BlockFile{
		file:            f,
		filePath:        path,
		blockCompressor: blockCompressor,
		blockSize:       blockSize,
		buf:             make([]byte, 0, blockSize),
	}
	if err := bf.loadAndRecover(); err != nil {
		if err == io.EOF {
			if err := bf.initNew(); err != nil {
				_ = f.Close()
				return nil, err
			}
			return bf, nil
		}
		_ = f.Close()
		return nil, err
	}
	return bf, nil
}

func (b *BlockFile) loadAndRecover() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, err := b.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := binary.Read(b.file, binary.LittleEndian, &b.header); err != nil {
		return err
	}
	if b.header.Magic != Magic {
		return ErrorInvalidFileFormat
	}

	if b.header.TxState == TxActive {
		// Crash recovery: truncate to the last valid data position.
		if err := b.file.Truncate(b.header.DataEnd); err != nil {
			return err
		}
		b.header.TxState = TxDone
		b.header.IndexOffset = 0
		// Scan compressed blocks to rebuild the index.
		if err := b.rebuildIndex(); err != nil {
			b.header.TotalSize = 0
		}
		if err := b.writeHeader(); err != nil {
			return err
		}
	} else if b.header.IndexOffset > 0 {
		// Try to load the index; if it fails, scan and rebuild.
		if _, err := b.file.Seek(b.header.IndexOffset, io.SeekStart); err != nil {
			_ = b.rebuildIndex()
			b.header.IndexOffset = 0
			return b.writeHeader()
		}
		var count uint32
		if err := binary.Read(b.file, binary.LittleEndian, &count); err != nil {
			_ = b.rebuildIndex()
			b.header.IndexOffset = 0
			return b.writeHeader()
		}
		b.index = make([]IndexEntry, 0, count)
		for i := uint32(0); i < count; i++ {
			var ent IndexEntry
			if err := binary.Read(b.file, binary.LittleEndian, &ent); err != nil {
				_ = b.rebuildIndex()
				b.header.IndexOffset = 0
				return b.writeHeader()
			}
			b.index = append(b.index, ent)
		}
	}

	b.dataWritePos = b.header.DataEnd
	b.totalSize = b.header.TotalSize
	b.pos = 0
	return nil
}

func (b *BlockFile) initNew() error {
	b.header = FileHeader{
		Magic:     Magic,
		Version:   Version,
		TxState:   TxDone,
		BlockSize: uint32(b.blockSize),
		DataEnd:   HeaderSize,
	}
	b.dataWritePos = HeaderSize
	b.pos = 0
	return b.writeHeader()
}

func (b *BlockFile) writeHeader() error {
	if _, err := b.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	b.lastPhysicalPos = -1
	return binary.Write(b.file, binary.LittleEndian, &b.header)
}

func (b *BlockFile) markActive() error {
	b.header.TxState = TxActive
	return b.writeHeader()
}

func (b *BlockFile) flushBlock() error {
	if len(b.buf) == 0 {
		return nil
	}
	b.lastPhysicalPos = -1
	if _, err := b.file.Seek(b.dataWritePos, io.SeekStart); err != nil {
		return err
	}

	encoded := b.blockCompressor.Encode(b.buf)
	var prefix [4]byte
	binary.LittleEndian.PutUint32(prefix[:], uint32(len(encoded)))
	if _, err := b.file.Write(prefix[:]); err != nil {
		return err
	}
	if _, err := b.file.Write(encoded); err != nil {
		return err
	}

	crc := crc32.ChecksumIEEE(b.buf)
	nowPos := b.dataWritePos + 4 + int64(len(encoded))
	b.index = append(b.index, IndexEntry{
		RawOff:  b.header.TotalSize,
		CompOff: b.dataWritePos,
		CompLen: nowPos - b.dataWritePos,
		Crc32:   crc,
	})
	b.header.TotalSize += int64(len(b.buf))
	b.dataWritePos = nowPos
	b.header.DataEnd = nowPos
	b.buf = b.buf[:0]
	return nil
}

// Write appends data to the file, buffering internally and auto-flushing compressed
// blocks when they reach blockSize. Writes always append to the end of the file,
// regardless of Seek position; after writing, pos points to the new end of file.
func (b *BlockFile) Write(p []byte) (int, error) {
	if b.isClosed() {
		return 0, os.ErrClosed
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	total := len(p)
	firstWrite := b.header.TxState == TxDone

	for len(p) > 0 {
		free := b.blockSize - len(b.buf)
		if free <= 0 {
			if firstWrite {
				b.header.TxState = TxActive
			}
			if err := b.flushBlock(); err != nil {
				return total - len(p), err
			}
			// After the first flush, update the on-disk header so DataEnd is current.
			if firstWrite {
				firstWrite = false
				if err := b.writeHeader(); err != nil {
					return total - len(p), err
				}
			}
			continue
		}
		n := free
		if len(p) < n {
			n = len(p)
		}
		b.buf = append(b.buf, p[:n]...)
		p = p[n:]
	}

	b.totalSize += int64(total)
	b.pos = b.totalSize
	return total, nil
}

// Read reads data sequentially, automatically decompressing across block boundaries.
func (b *BlockFile) Read(p []byte) (int, error) {
	if b.isClosed() {
		return 0, os.ErrClosed
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.pos >= b.totalSize {
		return 0, io.EOF
	}
	n, err := b.readAt(p, b.pos)
	b.pos += int64(n)
	return n, err
}

// Seek sets the offset for the next Read or Write.
func (b *BlockFile) Seek(offset int64, whence int) (int64, error) {
	if b.isClosed() {
		return 0, os.ErrClosed
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = b.pos + offset
	case io.SeekEnd:
		newPos = b.totalSize + offset
	default:
		return 0, ErrorInvalidSeekMode
	}
	if newPos < 0 {
		return 0, ErrorNegativePosition
	}
	b.pos = newPos
	return newPos, nil
}

// Sync flushes the current buffer to disk without closing the file or writing the index.
func (b *BlockFile) Sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		return os.ErrClosed
	}
	if b.header.TxState == TxDone && len(b.buf) > 0 {
		b.header.TxState = TxActive
		if err := b.flushBlock(); err != nil {
			return err
		}
		err := b.writeHeader()
		if err != nil {
			return err
		}
		return b.file.Sync()
	}
	err := b.flushBlock()
	if err != nil {
		return err
	}
	return b.file.Sync()
}

// writeIndexLocked writes the block index at the current DataEnd position and
// updates the file header to TxDone. Must be called with mu held.
func (b *BlockFile) writeIndexLocked() error {
	var idxBuf bytes.Buffer
	idxBuf.Grow(4 + len(b.index)*32)
	if err := binary.Write(&idxBuf, binary.LittleEndian, uint32(len(b.index))); err != nil {
		return err
	}
	for _, ent := range b.index {
		if err := binary.Write(&idxBuf, binary.LittleEndian, ent); err != nil {
			return err
		}
	}
	idxOffset, err := b.file.Seek(b.header.DataEnd, io.SeekStart)
	if err != nil {
		return err
	}
	if _, err := b.file.Write(idxBuf.Bytes()); err != nil {
		return err
	}
	b.header.IndexOffset = idxOffset
	b.header.TxState = TxDone
	return b.writeHeader()
}

// Commit flushes the buffer, writes the index, marks TxDone, but keeps
// the file open for further writes. Used when the same segment will receive
// more data in a future flush transaction.
func (b *BlockFile) Commit() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		return os.ErrClosed
	}
	if err := b.flushBlock(); err != nil {
		return err
	}
	err := b.writeIndexLocked()
	if err != nil {
		return err
	}
	return b.file.Sync()
}

// Close flushes the buffer, writes the index, marks TxDone, and closes the file.
func (b *BlockFile) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		return nil
	}
	b.closed.Store(true)

	if err := b.flushBlock(); err != nil {
		return err
	}
	if err := b.writeIndexLocked(); err != nil {
		return err
	}
	err := b.file.Sync()
	if err != nil {
		return err
	}
	return b.file.Close()
}

// Drop closes the file immediately without flushing the buffer or writing the index.
// Used for cleanup/rollback scenarios.
func (b *BlockFile) Drop() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		return nil
	}
	b.closed.Store(true)
	return b.file.Close()
}

// Size returns the logical file size (uncompressed byte count).
func (b *BlockFile) Size() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.totalSize
}

// PhysicalSize returns the on-disk data size in bytes (compressed blocks + index, excluding file header).
func (b *BlockFile) PhysicalSize() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.header.DataEnd - HeaderSize
}

// DataEnd returns the physical end position of valid data.
func (b *BlockFile) DataEnd() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.header.DataEnd
}

// Truncate truncates the file to the specified logical size (uncompressed bytes),
// updating the index and header accordingly.
func (b *BlockFile) Truncate(logicalSize int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Buffered data only (not yet flushed): truncate directly in the buffer.
	if len(b.index) == 0 {
		if logicalSize == 0 {
			b.buf = b.buf[:0]
			b.totalSize = 0
			b.pos = 0
			b.header.TotalSize = 0
			return b.writeHeader()
		}
		if logicalSize >= int64(len(b.buf)) {
			return nil
		}
		b.buf = b.buf[:logicalSize]
		b.totalSize = logicalSize
		b.pos = logicalSize
		b.header.TotalSize = logicalSize
		return b.writeHeader()
	}

	if logicalSize == 0 {
		if err := b.file.Truncate(HeaderSize); err != nil {
			return err
		}
		b.index = b.index[:0]
		b.dataWritePos = HeaderSize
		b.header.TotalSize = 0
		b.header.DataEnd = HeaderSize
		b.header.IndexOffset = 0
		b.totalSize = 0
		b.pos = 0
		b.buf = b.buf[:0]
		b.lastPhysicalPos = -1
		b.header.TxState = TxDone
		return b.writeHeader()
	}

	if logicalSize >= b.totalSize {
		return nil
	}

	idx := b.findBlock(logicalSize - 1)
	if idx < 0 || idx >= len(b.index) {
		return ErrorInvalidTruncationPoint
	}

	ent := b.index[idx]
	offsetInBlock := logicalSize - ent.RawOff

	// Read the block containing the truncation point to decide if intra-block truncation is needed.
	if _, err := b.file.Seek(ent.CompOff+4, io.SeekStart); err != nil {
		return err
	}
	compressed := make([]byte, ent.CompLen-4)
	if _, err := io.ReadFull(b.file, compressed); err != nil {
		return err
	}
	data, err := b.blockCompressor.Decode(compressed)
	if err != nil {
		return err
	}

	if offsetInBlock < int64(len(data)) {
		// Intra-block truncation: truncate the file to the block start, then re-compress and write the trimmed data.
		data = data[:offsetInBlock]
		reEncoded := b.blockCompressor.Encode(data)

		if err := b.file.Truncate(ent.CompOff); err != nil {
			return err
		}
		b.lastPhysicalPos = -1

		var prefix [4]byte
		binary.LittleEndian.PutUint32(prefix[:], uint32(len(reEncoded)))
		_, err = b.file.Seek(ent.CompOff, io.SeekStart)
		if err != nil {
			return err
		}
		_, err = b.file.Write(prefix[:])
		if err != nil {
			return err
		}
		_, err = b.file.Write(reEncoded)
		if err != nil {
			return err
		}
		newDataEnd := ent.CompOff + 4 + int64(len(reEncoded))
		b.index[idx] = IndexEntry{
			RawOff:  ent.RawOff,
			CompOff: ent.CompOff,
			CompLen: 4 + int64(len(reEncoded)),
			Crc32:   crc32.ChecksumIEEE(data),
		}
		b.index = b.index[:idx+1]
		b.dataWritePos = newDataEnd
		b.header.DataEnd = newDataEnd
	} else {
		// Block-boundary truncation: keep the full block, only update index and header.
		newDataEnd := ent.CompOff + ent.CompLen
		b.index = b.index[:idx+1]
		b.dataWritePos = newDataEnd
		b.header.DataEnd = newDataEnd
	}

	b.header.TotalSize = logicalSize
	b.header.IndexOffset = 0
	b.totalSize = logicalSize
	if b.pos > logicalSize {
		b.pos = logicalSize
	}
	b.buf = b.buf[:0]

	var idxBuf bytes.Buffer
	idxBuf.Grow(4 + len(b.index)*32)
	err = binary.Write(&idxBuf, binary.LittleEndian, uint32(len(b.index)))
	if err != nil {
		return err
	}
	for _, e := range b.index {
		err = binary.Write(&idxBuf, binary.LittleEndian, e)
		if err != nil {
			return err
		}
	}
	idxOffset, err := b.file.Seek(b.header.DataEnd, io.SeekStart)
	if err != nil {
		return err
	}
	if _, err := b.file.Write(idxBuf.Bytes()); err != nil {
		return err
	}

	b.header.IndexOffset = idxOffset
	b.header.TxState = TxDone
	return b.writeHeader()
}

func (b *BlockFile) isClosed() bool {
	return b.closed.Load()
}

// rebuildIndex scans compressed blocks on disk to reconstruct the index.
// Used during crash recovery or when the index file is lost.
func (b *BlockFile) rebuildIndex() error {
	b.index = nil
	var rawOff int64
	pos := int64(HeaderSize)

	for pos < b.header.DataEnd {
		if _, err := b.file.Seek(pos, io.SeekStart); err != nil {
			return err
		}
		var blockLen uint32
		if err := binary.Read(b.file, binary.LittleEndian, &blockLen); err != nil {
			return err
		}
		if blockLen == 0 || pos+4+int64(blockLen) > b.header.DataEnd {
			break
		}

		compressed := make([]byte, blockLen)
		if _, err := io.ReadFull(b.file, compressed); err != nil {
			return err
		}
		data, err := b.blockCompressor.Decode(compressed)
		if err != nil {
			return err
		}

		b.index = append(b.index, IndexEntry{
			RawOff:  rawOff,
			CompOff: pos,
			CompLen: 4 + int64(blockLen),
			Crc32:   crc32.ChecksumIEEE(data),
		})
		rawOff += int64(len(data))
		pos += 4 + int64(blockLen)
	}

	b.header.TotalSize = rawOff
	b.header.DataEnd = pos
	return nil
}

// findBlock performs a binary search for the block index containing logical offset off.
func (b *BlockFile) findBlock(off int64) int {
	lo, hi := 0, len(b.index)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		if b.index[mid].RawOff <= off {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return hi
}

// readAt reads data from the specified logical offset, automatically decompressing across block boundaries.
func (b *BlockFile) readAt(p []byte, off int64) (int, error) {
	if len(b.index) == 0 && len(b.buf) == 0 {
		return 0, io.EOF
	}

	var totalRead int
	bufStart := b.totalSize - int64(len(b.buf))

	for totalRead < len(p) && off < b.totalSize {
		if len(b.buf) > 0 && off >= bufStart {
			start := off - bufStart
			n := copy(p[totalRead:], b.buf[start:])
			totalRead += n
			off += int64(n)
			if totalRead >= len(p) {
				break
			}
			if off >= b.totalSize {
				break
			}
		}

		data, rawOff, err := b.blockData(off)
		if err != nil {
			return totalRead, err
		}

		start := off - rawOff
		if start >= int64(len(data)) {
			off = rawOff + int64(len(data))
			continue
		}
		n := copy(p[totalRead:], data[start:])
		totalRead += n
		off += int64(n)
	}

	if totalRead == 0 && off >= b.totalSize {
		return 0, io.EOF
	}
	return totalRead, nil
}

// blockData decompresses the compressed block containing logical offset off
// into the reusable decode buffer and returns (data, rawOff, err). The
// returned slice aliases b.decodeBuf and stays valid until the next read on
// this BlockFile — callers must consume it before issuing another read.
func (b *BlockFile) blockData(off int64) ([]byte, int64, error) {
	blockIdx := b.findBlock(off)
	if blockIdx < 0 || blockIdx >= len(b.index) {
		return nil, 0, io.EOF
	}
	ent := b.index[blockIdx]

	target := ent.CompOff + 4
	if b.lastPhysicalPos != target {
		if _, err := b.file.Seek(target, io.SeekStart); err != nil {
			return nil, 0, err
		}
	}
	need := int(ent.CompLen - 4)
	if cap(b.readBuf) < need {
		b.readBuf = make([]byte, need)
	}
	rBuf := b.readBuf[:need]
	if _, err := io.ReadFull(b.file, rBuf); err != nil {
		return nil, 0, err
	}
	b.lastPhysicalPos = ent.CompOff + ent.CompLen

	// Decompress into the reusable buffer (one allocation per compressed block
	// lifetime instead of per read); NoCompressor is identity so the compressed
	// bytes themselves are returned without a copy.
	var decodeBuf []byte
	var err error
	storeDecode := true
	switch b.blockCompressor.(type) {
	case ZstdCompressor:
		decodeBuf, err = blockZstdDecoder().DecodeAll(rBuf, b.decodeBuf[:0])
	case SnappyCompressor:
		decodeBuf, err = snappy.Decode(b.decodeBuf[:0], rBuf)
	case NoCompressor:
		// 恒等：直接返回压缩字节。不要把它存入 b.decodeBuf——那会使
		// decodeBuf 与 readBuf 别名同一数组，后续 zstd/snappy 以
		// b.decodeBuf[:0] 为 dst 时与 src（rBuf）底层重叠，解压会覆盖
		// 尚未读取的源数据。
		decodeBuf = rBuf
		storeDecode = false
	default:
		decodeBuf, err = b.blockCompressor.Decode(rBuf)
	}
	if err != nil {
		return nil, 0, err
	}
	if storeDecode {
		b.decodeBuf = decodeBuf
	}
	if ent.Crc32 != 0 && crc32.ChecksumIEEE(decodeBuf) != ent.Crc32 {
		return nil, 0, ErrorBlockCRCMismatch
	}
	return decodeBuf, ent.RawOff, nil
}

// ReadBlockFrom returns a reference to the decompressed bytes starting at
// logical offset off (the returned slice covers [off, end of its compressed
// block)). The reference aliases the BlockFile's internal decode buffer and
// stays valid only until the next read on this BlockFile. Ranges that span
// compressed-block boundaries must be assembled by the caller.
func (b *BlockFile) ReadBlockFrom(off int64) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, rawOff, err := b.blockData(off)
	if err != nil {
		return nil, err
	}
	return data[off-rawOff:], nil
}

// Path returns the file path this BlockFile is bound to.
func (b *BlockFile) Path() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.filePath
}

// Rebind re-points this BlockFile at another file, reusing the instance (and
// its buffers) for a different segment within one query. The caller must not
// hold references to decode buffers returned before the rebind.
func (b *BlockFile) Rebind(path string, compressor BlockCompressor) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.file.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	b.file = f
	b.filePath = path
	b.blockCompressor = compressor
	b.buf = b.buf[:0]
	b.index = nil
	b.lastRawOff = 0
	b.lastPhysicalPos = -1
	b.decodeBuf = b.decodeBuf[:0]
	if err := b.loadAndRecover(); err != nil {
		if err == io.EOF {
			return b.initNew()
		}
		return err
	}
	return nil
}
