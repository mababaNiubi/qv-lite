package tsdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
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
	idx := &FileIndex{MinTime: minTime, MaxTime: maxTime, Blocks: make([]BlockIndexEntry, 0, blockCount)}
	for i := uint32(0); i < blockCount; i++ {
		var b BlockIndexEntry
		err = binary.Read(br, binary.BigEndian, &b)
		if err != nil {
			return nil
		}
		idx.Blocks = append(idx.Blocks, b)
	}
	return idx
}
