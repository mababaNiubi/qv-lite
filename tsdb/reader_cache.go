package tsdb

import (
	"sync"
)

const defaultMaxOpenReaders = 64

type readerCache struct {
	mu      sync.Mutex
	maxSize int
	readers map[string]*BlockFile
}

func newReaderCache(maxSize int) *readerCache {
	if maxSize <= 0 {
		maxSize = defaultMaxOpenReaders
	}
	return &readerCache{
		maxSize: maxSize,
		readers: make(map[string]*BlockFile),
	}
}

// acquire returns an idle BlockFile for the given path, or nil if none is cached.
func (c *readerCache) acquire(filePath string) *BlockFile {
	c.mu.Lock()
	defer c.mu.Unlock()
	bf, ok := c.readers[filePath]
	if !ok {
		return nil
	}
	delete(c.readers, filePath)
	return bf
}

// release returns a BlockFile to the cache for reuse. If the cache is full,
// the BlockFile is dropped (closed) instead.
func (c *readerCache) release(filePath string, bf *BlockFile) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.readers) >= c.maxSize {
		c.evictLocked()
	}
	c.readers[filePath] = bf
}

// evictLocked removes and closes one reader from the cache. Must be called with mu held.
func (c *readerCache) evictLocked() {
	for path, bf := range c.readers {
		bf.Drop()
		delete(c.readers, path)
		return
	}
}

// evict closes and removes the cached reader for the given path.
func (c *readerCache) evict(filePath string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if bf, ok := c.readers[filePath]; ok {
		bf.Drop()
		delete(c.readers, filePath)
	}
}

// closeAll closes all cached readers.
func (c *readerCache) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for path, bf := range c.readers {
		bf.Drop()
		delete(c.readers, path)
	}
}
