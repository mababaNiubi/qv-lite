package tsdb

import (
	"fmt"
	"io"
)

type FileWriter interface {
	OpenWriter() error
	Write(p []byte) (n int, err error)
	FlushBlock() error
	Commit() error
	Cleanup() error
	Size() int64
	PhysicalSize() int64
	RollbackLastCommit() error
}

type fileWriter struct {
	filePath          string
	bf                *BlockFile
	closed            bool
	writeOffset       int64
	lastOpenWrtOffset int64
	compressor        BlockCompressor
}

func NewFileWriter(filePath string, compressor BlockCompressor) FileWriter {
	return &fileWriter{
		filePath:   filePath,
		closed:     true,
		compressor: compressor,
	}
}

func (w *fileWriter) OpenWriter() error {
	if !w.closed {
		// Already open (soft-commit kept it open). Snapshot the current size
		// as the rollback point for this transaction.
		if w.bf != nil {
			w.lastOpenWrtOffset = w.bf.Size()
		}
		return nil
	}
	bf, err := OpenBlockFile(w.filePath, w.compressor, BlockSizeDef)
	if err != nil {
		return fmt.Errorf("unable to open file: %v", err)
	}
	w.bf = bf
	w.bf.Seek(0, io.SeekEnd)
	w.writeOffset = bf.Size()
	w.lastOpenWrtOffset = bf.Size()
	w.closed = false
	return nil
}

func (w *fileWriter) Write(p []byte) (n int, err error) {
	if w.bf == nil || w.closed {
		return 0, ErrorWriterNotOpen
	}
	n, err = w.bf.Write(p)
	if err != nil {
		return n, err
	}
	w.writeOffset += int64(n)
	return n, nil
}

func (w *fileWriter) FlushBlock() error {
	if w.bf == nil {
		return nil
	}
	return w.bf.Sync()
}

func (w *fileWriter) Commit() error {
	if w.bf == nil || w.closed {
		return nil
	}
	// Soft commit: write index + header but keep the file open for next flush.
	if err := w.bf.Commit(); err != nil {
		return fmt.Errorf("failed to commit file: %v", err)
	}
	return nil
}

// Cleanup truly closes the underlying BlockFile. Called when a new segment
// is added or when the table is shutting down.
func (w *fileWriter) Cleanup() error {
	if w.bf == nil || w.closed {
		return nil
	}
	w.closed = true
	if err := w.bf.Close(); err != nil {
		return fmt.Errorf("failed to close file: %v", err)
	}
	w.bf = nil
	return nil
}

func (w *fileWriter) Size() int64 { return w.writeOffset }

func (w *fileWriter) PhysicalSize() int64 {
	if w.bf == nil {
		return 0
	}
	return w.bf.PhysicalSize()
}

func (w *fileWriter) RollbackLastCommit() error {
	// Close the current writer's handle if open (from a soft-commit).
	if w.bf != nil && !w.closed {
		_ = w.bf.Drop()
		w.bf = nil
		w.closed = true
	}
	bf, err := OpenBlockFile(w.filePath, w.compressor, BlockSizeDef)
	if err != nil {
		return err
	}
	defer bf.Close()
	return bf.Truncate(w.lastOpenWrtOffset)
}
