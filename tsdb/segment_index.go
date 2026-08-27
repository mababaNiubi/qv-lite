package tsdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
	"sort"
	"strings"
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
	err = binary.Write(bw, binary.BigEndian, uint32(indexMagic))
	if err != nil {
		return err
	}
	err = binary.Write(bw, binary.BigEndian, uint32(len(idx.Blocks)))
	if err != nil {
		return err
	}
	err = binary.Write(bw, binary.BigEndian, idx.MinTime)
	if err != nil {
		return err
	}
	err = binary.Write(bw, binary.BigEndian, idx.MaxTime)
	if err != nil {
		return err
	}
	for i := range idx.Blocks {
		err = binary.Write(bw, binary.BigEndian, idx.Blocks[i])
	}
	err = bw.Flush()
	if err != nil {
		return err
	}
	return f.Sync()
}

func readIndexFile(path string) *FileIndex {
	data, err := os.ReadFile(path)
	if err != nil || len(data) < 24 {
		return nil
	}
	br := bytes.NewReader(data)
	var magic, blockCount uint32
	var minTime, maxTime int64
	if binary.Read(br, binary.BigEndian, &magic) != nil || magic != uint32(indexMagic) {
		return nil
	}
	binary.Read(br, binary.BigEndian, &blockCount)
	binary.Read(br, binary.BigEndian, &minTime)
	binary.Read(br, binary.BigEndian, &maxTime)
	idx := newFileIndex(int(blockCount))
	idx.MinTime = minTime
	idx.MaxTime = maxTime
	for i := uint32(0); i < blockCount; i++ {
		var b BlockIndexEntry
		err = binary.Read(br, binary.BigEndian, &b)
		if err != nil {
			return nil
		}
		idx.appendBlock(b)
	}
	return idx
}
