package main

import (
	"container/list"
	"sync"
)

// lruResults is a small bounded LRU cache for completed-job result entries.
// The orchestrator keeps the most recent N results in memory so GetJobStatus
// is fast; older results spill out and fall back to the BoltDB read path on
// retrieval. Without this bound, `Server.results` grew unboundedly over the
// orchestrator's lifetime.
type lruResults struct {
	mu    sync.Mutex
	cap   int
	items map[string]*list.Element
	order *list.List // front = most recently used; back = next to evict
}

type lruEntry struct {
	key   string
	value resultEntry
}

func newLRUResults(capacity int) *lruResults {
	if capacity < 1 {
		capacity = 1
	}
	return &lruResults{
		cap:   capacity,
		items: make(map[string]*list.Element, capacity),
		order: list.New(),
	}
}

func (l *lruResults) Put(key string, value resultEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.items[key]; ok {
		el.Value.(*lruEntry).value = value
		l.order.MoveToFront(el)
		return
	}
	el := l.order.PushFront(&lruEntry{key: key, value: value})
	l.items[key] = el
	if l.order.Len() > l.cap {
		oldest := l.order.Back()
		if oldest != nil {
			l.order.Remove(oldest)
			delete(l.items, oldest.Value.(*lruEntry).key)
		}
	}
}

func (l *lruResults) Get(key string) (resultEntry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	el, ok := l.items[key]
	if !ok {
		return resultEntry{}, false
	}
	l.order.MoveToFront(el)
	return el.Value.(*lruEntry).value, true
}

func (l *lruResults) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.order.Len()
}
