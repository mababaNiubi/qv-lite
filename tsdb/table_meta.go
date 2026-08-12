package tsdb

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/mababaNiubi/qv-lite/container"
)

// Meta file format (append-only binary, crash-safe via per-entry CRC32):
//
//   Header (4 bytes):
//     [4 bytes: magic "META" (0x4D455441)]
//
//   Entry (variable):
//     [2 bytes: tagLen uint16, big-endian]
//     [tagLen bytes: UTF-8 tag string]
//     [4 bytes: code uint32, big-endian]
//     [4 bytes: CRC32-IEEE over all preceding entry bytes]
//
// MaxPointDict is not stored in the file; it is recovered during NewMeta
// by scanning all entries and taking the maximum code value.
//
// Crash recovery: if the process dies mid-write, the last entry may be
// partial or have a mismatched CRC. NewMeta detects this and truncates
// the file to the last complete entry automatically.

const (
	metaMagic   = 0x4D455441 // "META"
	metaHeadLen = 4
)

type Meta struct {
	MaxPointDict tagCode
	Path         string
	container.SyncMap[string, tagCode]

	mu      sync.Mutex
	file    *os.File
	// pending buffers tag entries written by addTag. They are flushed to the
	// file (and fsynced) by flushPendingLocked, which the WAL calls before any
	// WAL bytes reach the OS. This turns per-tag fsync (~3ms/tag) into one
	// fsync per WAL batch, which is what makes high-cardinality writes viable.
	pending []byte
}

func (s *Meta) addTag(tag string) (tagCode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.MaxPointDict++

	tagBytes := []byte(tag)
	// entry layout: [2b tagLen][tag bytes][4b code][4b CRC32]
	dataLen := 2 + len(tagBytes) + 4
	entry := make([]byte, dataLen+4) // +4 for trailing CRC32
	binary.BigEndian.PutUint16(entry[0:2], uint16(len(tagBytes)))
	copy(entry[2:], tagBytes)
	binary.BigEndian.PutUint32(entry[2+len(tagBytes):], uint32(s.MaxPointDict))

	// CRC32-IEEE over all bytes except the CRC field itself.
	crc := crc32.ChecksumIEEE(entry[:dataLen])
	binary.BigEndian.PutUint32(entry[dataLen:], crc)

	// Buffer the entry; it reaches the file only when the WAL asks to persist
	// (flushPendingLocked), so creating a tag costs no fsync. The in-memory
	// map is updated immediately so the code is usable right away.
	s.pending = append(s.pending, entry...)
	s.Store(tag, s.MaxPointDict)
	return s.MaxPointDict, nil
}

// FlushPending writes and fsyncs any buffered tag entries. The WAL calls it
// before writing WAL bytes to the OS, guaranteeing tag codes are durable
// before the points that reference them can be. Idempotent and safe to call
// when nothing is pending.
func (s *Meta) FlushPending() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushPendingLocked()
}

func (s *Meta) flushPendingLocked() error {
	if len(s.pending) == 0 {
		return nil
	}
	if _, err := s.file.Write(s.pending); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}
	s.pending = s.pending[:0]
	return nil
}

// Close flushes any buffered tag entries, fsyncs, and closes the file.
// Call on table shutdown.
func (s *Meta) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		err := s.flushPendingLocked()
		if syncErr := s.file.Sync(); syncErr != nil && err == nil {
			err = syncErr
		}
		if closeErr := s.file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		s.file = nil
		return err
	}
	return nil
}

func NewMeta(path string) (*Meta, error) {
	m := &Meta{
		Path:    filepath.Join(path, metaFile),
		pending: make([]byte, 0, 1024),
	}

	f, err := os.OpenFile(m.Path, os.O_RDONLY, 0644)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		// File does not exist — create a fresh file.
		return m, m.initFile()
	}

	// Read header.
	var header [metaHeadLen]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		f.Close()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// File exists but header is incomplete (crash during initFile).
			// Re-initialise.
			return m, m.initFile()
		}
		return nil, err
	}

	if magic := binary.BigEndian.Uint32(header[:]); magic != metaMagic {
		f.Close()
		return nil, ErrorMetaFileFormat
	}

	// Read entries sequentially. MaxPointDict is recovered from the max code.
	maxCode, err := m.readEntries(f)
	f.Close()
	if err != nil {
		return nil, err
	}
	m.MaxPointDict = maxCode

	// Re-open for append writes.
	m.file, err = os.OpenFile(m.Path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// readEntries replays all complete entries from the file into the in-memory
// SyncMap and returns the maximum tag code seen (0 if no entries).
// If a partial or corrupt entry is detected at the end of the file it is
// silently truncated so that future writes stay consistent.
func (m *Meta) readEntries(f *os.File) (tagCode, error) {
	var (
		entryHdr [2]byte
		codeBuf  [4]byte
		crcBuf   [4]byte
		maxCode  tagCode
	)

	for {
		entryStart, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}

		// ── tagLen ──
		if _, err := io.ReadFull(f, entryHdr[:]); err != nil {
			if err == io.EOF {
				break // clean end of file
			}
			// Partial tagLen (or other I/O error) — truncate.
			_ = truncateFile(m.Path, entryStart)
			break
		}
		tagLen := int(binary.BigEndian.Uint16(entryHdr[:]))

		// Sanity check: a tag should not be unreasonably large.
		const maxTagLen = 65535
		if tagLen < 0 || tagLen > maxTagLen {
			_ = truncateFile(m.Path, entryStart)
			break
		}

		// ── tag bytes ──
		tagBytes := make([]byte, tagLen)
		if _, err := io.ReadFull(f, tagBytes); err != nil {
			_ = truncateFile(m.Path, entryStart)
			break
		}

		// ── code ──
		if _, err := io.ReadFull(f, codeBuf[:]); err != nil {
			_ = truncateFile(m.Path, entryStart)
			break
		}

		// ── CRC ──
		if _, err := io.ReadFull(f, crcBuf[:]); err != nil {
			_ = truncateFile(m.Path, entryStart)
			break
		}

		// Verify CRC over [tagLen][tag][code].
		expected := computeEntryCRC(entryHdr[:], tagBytes, codeBuf[:])
		actual := binary.BigEndian.Uint32(crcBuf[:])
		if expected != actual {
			_ = truncateFile(m.Path, entryStart)
			break
		}

		code := tagCode(binary.BigEndian.Uint32(codeBuf[:]))
		if code > maxCode {
			maxCode = code
		}
		m.Store(string(tagBytes), code)
	}

	return maxCode, nil
}

// computeEntryCRC returns the CRC32-IEEE checksum of the concatenation of
// tagLen (2 bytes), tag, and code — i.e. everything before the CRC field.
func computeEntryCRC(tagLen, tag, code []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(tagLen)
	h.Write(tag)
	h.Write(code)
	return h.Sum32()
}

// truncateFile truncates the file at the given offset, removing any
// incomplete or corrupt trailing data.
func truncateFile(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(size)
}

// initFile creates a new meta file with a header only (no entries).
// It ensures the parent directory exists first.
func (s *Meta) initFile() error {
	if err := os.MkdirAll(filepath.Dir(s.Path), 0755); err != nil {
		return err
	}
	var err error
	s.file, err = os.Create(s.Path)
	if err != nil {
		return err
	}
	var header [metaHeadLen]byte
	binary.BigEndian.PutUint32(header[0:4], metaMagic)
	if _, err = s.file.Write(header[:]); err != nil {
		s.file.Close()
		return err
	}
	return nil
}
