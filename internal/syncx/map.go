package syncx

import (
	"sync"
)

type Map[K comparable, V any] struct {
	Raw map[K]V
	sync.RWMutex
}

func NewMap[K comparable, V any]() Map[K, V] {
	return Map[K, V]{Raw: make(map[K]V)}
}
