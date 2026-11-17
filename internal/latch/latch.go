// Package latch remembers which failures were logged, so each is logged once when it starts and once
// when it recovers.
package latch

import "sync"

// Latch is a set of keys. The zero value is ready to use and safe for concurrent use.
type Latch[K comparable] struct {
	mu   sync.Mutex // guards keys
	keys map[K]struct{}
}

// Set adds k and reports whether it was not there.
func (l *Latch[K]) Set(k K) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.keys[k]; ok {
		return false
	}
	if l.keys == nil {
		l.keys = map[K]struct{}{}
	}
	l.keys[k] = struct{}{}
	return true
}

// Clear removes k and reports whether it was there.
func (l *Latch[K]) Clear(k K) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.keys[k]
	delete(l.keys, k)
	return ok
}

// ClearAll removes every key and reports whether there was one.
func (l *Latch[K]) ClearAll() bool {
	return l.Retain(func(K) bool { return false })
}

// Retain removes the keys keep rejects and reports whether it removed one.
func (l *Latch[K]) Retain(keep func(K) bool) (removed bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k := range l.keys {
		if !keep(k) {
			delete(l.keys, k)
			removed = true
		}
	}
	return removed
}
