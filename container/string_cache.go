package container

import (
	"sync/atomic"
)

// stringKeyEntry is one slot entry in StringKeyCache. The key string shares the
// caller's immutable bytes (no copy), and the tag→code / table→table mapping is
// permanent once established, so entries never need invalidation.
type stringKeyEntry[V any] struct {
	key string
	val V
}

// StringKeyCache is a small direct-mapped cache keyed by string, backed by
// per-slot atomic pointers so reads are lock-free and safe under concurrency.
// A miss simply falls through to the authoritative lookup (the caller), so the
// cache is correct for any cardinality: hot/skewed keys hit; uniformly rotating
// keys only pay a hash + atomic load + one small store per miss.
type StringKeyCache[V any] struct {
	slots []atomic.Pointer[stringKeyEntry[V]]
}

func NewStringKeyCache[V any](slots int) *StringKeyCache[V] {
	return &StringKeyCache[V]{slots: make([]atomic.Pointer[stringKeyEntry[V]], slots)}
}

func (c *StringKeyCache[V]) Lookup(key string) (V, bool) {
	return c.LookupHash(key, HashString(key))
}

// LookupHash is Lookup for callers that already hashed key for routing. The
// exact string comparison remains the collision guard.
func (c *StringKeyCache[V]) LookupHash(key string, hash uint64) (V, bool) {
	var zero V
	if c == nil {
		return zero, false
	}
	e := c.slots[hash&uint64(len(c.slots)-1)].Load()
	if e != nil && e.key == key {
		return e.val, true
	}
	return zero, false
}

func (c *StringKeyCache[V]) Store(key string, val V) {
	c.StoreHash(key, HashString(key), val)
}

// StoreHash is Store for callers that already hashed key.
func (c *StringKeyCache[V]) StoreHash(key string, hash uint64, val V) {
	if c == nil {
		return
	}
	c.slots[hash&uint64(len(c.slots)-1)].Store(&stringKeyEntry[V]{key: key, val: val})
}

// HashString is FNV-1a (64-bit). Self-contained (no maphash seed) and fast for
// the short tags/table names that dominate. Callers use the low bits as a slot
// index, so the size must be a power of two.
func HashString(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}
