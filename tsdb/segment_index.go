package tsdb

import (
	"bufio"
	"encoding/binary"
	"os"
	"sort"
	"strings"
)

const (
	indexHeaderSize = 4 + 4 + 8 + 8
	indexEntrySize  = 4 + 8 + 8 + 8 + 8
)

type BlockIndexEntry struct {
	Attribute tagCode
	MinTime   int64
	MaxTime   int64
	Offset    int64
	DataSize  int64
}

// FileIndex is the block-level index for a data segment (binary persisted).
type FileIndex struct {
	MinTime int64
	MaxTime int64
	Blocks  []BlockIndexEntry

	// tagBlocks maps a tag to positions in Blocks. Positions preserve the
	// on-disk block order, so the common monotonic time-series case can use
	// binary search without duplicating BlockIndexEntry values in memory.
	// These fields are derived in memory and intentionally are not persisted;
	// the existing .idx wire format remains unchanged.
	tagBlocks    map[tagCode][]int
	unsortedTags map[tagCode]struct{}
}

func newFileIndex(blockCap int) *FileIndex {
	if blockCap < 0 {
		blockCap = 0
	}
	return &FileIndex{
		Blocks:    make([]BlockIndexEntry, 0, blockCap),
		tagBlocks: make(map[tagCode][]int),
	}
}

// appendBlock adds a block to both the persisted flat index and the derived
// per-tag index. A tag is marked unsorted only when recovery/imported data
// violates the normal per-tag monotonic-time invariant.
func (idx *FileIndex) appendBlock(block BlockIndexEntry) {
	if idx.tagBlocks == nil {
		idx.rebuildTagIndex()
	}
	positions := idx.tagBlocks[block.Attribute]
	if len(positions) > 0 {
		previous := &idx.Blocks[positions[len(positions)-1]]
		if block.MinTime < previous.MinTime || block.MaxTime < previous.MaxTime {
			if idx.unsortedTags == nil {
				idx.unsortedTags = make(map[tagCode]struct{})
			}
			idx.unsortedTags[block.Attribute] = struct{}{}
		}
	}
	position := len(idx.Blocks)
	idx.Blocks = append(idx.Blocks, block)
	idx.tagBlocks[block.Attribute] = append(positions, position)
}

func (idx *FileIndex) rebuildTagIndex() {
	idx.tagBlocks = make(map[tagCode][]int)
	idx.unsortedTags = nil
	for position := range idx.Blocks {
		block := &idx.Blocks[position]
		positions := idx.tagBlocks[block.Attribute]
		if len(positions) > 0 {
			previous := &idx.Blocks[positions[len(positions)-1]]
			if block.MinTime < previous.MinTime || block.MaxTime < previous.MaxTime {
				if idx.unsortedTags == nil {
					idx.unsortedTags = make(map[tagCode]struct{})
				}
				idx.unsortedTags[block.Attribute] = struct{}{}
			}
		}
		idx.tagBlocks[block.Attribute] = append(positions, position)
	}
}

// matchingBlockPositions returns flat Blocks positions for one tag and time
// range. Monotonic tag groups use two binary searches and return a zero-copy
// subslice; malformed/out-of-order groups safely fall back to a grouped scan.
func (idx *FileIndex) matchingBlockPositions(tag tagCode, startTime, endTime int64) []int {
	if idx == nil || len(idx.Blocks) == 0 {
		return nil
	}
	if idx.tagBlocks == nil {
		idx.rebuildTagIndex()
	}
	positions := idx.tagBlocks[tag]
	if len(positions) == 0 {
		return nil
	}
	if _, unsorted := idx.unsortedTags[tag]; unsorted {
		matching := make([]int, 0, min(len(positions), 5))
		for _, position := range positions {
			block := &idx.Blocks[position]
			if startTime <= block.MaxTime && endTime >= block.MinTime {
				matching = append(matching, position)
			}
		}
		return matching
	}

	first := sort.Search(len(positions), func(i int) bool {
		return idx.Blocks[positions[i]].MaxTime >= startTime
	})
	positions = positions[first:]
	last := sort.Search(len(positions), func(i int) bool {
		return idx.Blocks[positions[i]].MinTime > endTime
	})
	return positions[:last]
}

func indexFilePath(tsbPath string) string {
	return strings.TrimSuffix(tsbPath, dataSuffix) + indexFileSuffix
}

// ─── Index file I/O ─────────────────────────────────────────────────

func writeIndexFile(path string, idx *FileIndex) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	var header [indexHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], uint32(indexMagic))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(idx.Blocks)))
	binary.BigEndian.PutUint64(header[8:16], uint64(idx.MinTime))
	binary.BigEndian.PutUint64(header[16:24], uint64(idx.MaxTime))
	if _, err = bw.Write(header[:]); err != nil {
		return err
	}

	var entryBuf [indexEntrySize]byte
	for i := range idx.Blocks {
		entry := &idx.Blocks[i]
		binary.BigEndian.PutUint32(entryBuf[0:4], uint32(entry.Attribute))
		binary.BigEndian.PutUint64(entryBuf[4:12], uint64(entry.MinTime))
		binary.BigEndian.PutUint64(entryBuf[12:20], uint64(entry.MaxTime))
		binary.BigEndian.PutUint64(entryBuf[20:28], uint64(entry.Offset))
		binary.BigEndian.PutUint64(entryBuf[28:36], uint64(entry.DataSize))
		if _, err = bw.Write(entryBuf[:]); err != nil {
			return err
		}
	}
	if err = bw.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

func readIndexFile(path string) *FileIndex {
	data, err := os.ReadFile(path)
	if err != nil || len(data) < indexHeaderSize {
		return nil
	}
	if binary.BigEndian.Uint32(data[0:4]) != uint32(indexMagic) {
		return nil
	}
	blockCount := binary.BigEndian.Uint32(data[4:8])
	if uint64(blockCount) > uint64((len(data)-indexHeaderSize)/indexEntrySize) {
		return nil
	}
	idx := newFileIndex(int(blockCount))
	idx.MinTime = int64(binary.BigEndian.Uint64(data[8:16]))
	idx.MaxTime = int64(binary.BigEndian.Uint64(data[16:24]))
	position := indexHeaderSize
	for i := uint32(0); i < blockCount; i++ {
		entry := BlockIndexEntry{
			Attribute: tagCode(binary.BigEndian.Uint32(data[position : position+4])),
			MinTime:   int64(binary.BigEndian.Uint64(data[position+4 : position+12])),
			MaxTime:   int64(binary.BigEndian.Uint64(data[position+12 : position+20])),
			Offset:    int64(binary.BigEndian.Uint64(data[position+20 : position+28])),
			DataSize:  int64(binary.BigEndian.Uint64(data[position+28 : position+36])),
		}
		idx.appendBlock(entry)
		position += indexEntrySize
	}
	return idx
}
