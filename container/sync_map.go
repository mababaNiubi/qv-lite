package container

import (
	"sync"
)

type SyncMap[K any, T any] struct {
	sync.Map
}

func (c *SyncMap[K, T]) Load(key K) (T, bool) {
	value, b := c.Map.Load(key)
	var into T
	if !b {
		return into, b
	}
	return value.(T), b
}

func (c *SyncMap[K, T]) Store(key K, value T) {
	c.Map.Store(key, value)
}

func (c *SyncMap[K, T]) Range(f func(k K, v T) bool) {
	c.Map.Range(func(key, value any) bool {
		return f(key.(K), value.(T))
	})
}

func (c *SyncMap[K, T]) Delete(key K) {
	c.Map.Delete(key)
}

type SyncSet[T any] SyncMap[T, bool]

func (c *SyncSet[T]) Load(key T) bool {
	value, b := c.Map.Load(key)
	if !b {
		return false
	}
	return value.(bool)
}

func (c *SyncSet[T]) Store(key T) {
	c.Map.Store(key, true)
}

func (c *SyncSet[T]) Delete(key T) {
	c.Map.Store(key, false)
}

func (c *SyncSet[T]) Clear(key T) {
	c.Map.Delete(key)
}

func (c *SyncSet[T]) Range(f func(k T) bool) {
	c.Map.Range(func(key, value any) bool {
		if value.(bool) {
			return f(key.(T))
		}
		return true
	})
}
