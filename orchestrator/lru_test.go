package main

import (
	"strconv"
	"sync"
	"testing"
)

func TestLRU_PutAndGet(t *testing.T) {
	l := newLRUResults(3)
	l.Put("a", resultEntry{resultBytes: []byte("A")})
	got, ok := l.Get("a")
	if !ok {
		t.Fatal("expected hit on freshly-put key")
	}
	if string(got.resultBytes) != "A" {
		t.Errorf("wrong value: %s", got.resultBytes)
	}
}

func TestLRU_MissReturnsFalse(t *testing.T) {
	l := newLRUResults(3)
	if _, ok := l.Get("missing"); ok {
		t.Error("expected miss on empty cache")
	}
}

func TestLRU_EvictsOldestOnOverflow(t *testing.T) {
	l := newLRUResults(3)
	l.Put("a", resultEntry{})
	l.Put("b", resultEntry{})
	l.Put("c", resultEntry{})
	l.Put("d", resultEntry{}) // evicts "a"

	if _, ok := l.Get("a"); ok {
		t.Error("a should have been evicted")
	}
	for _, k := range []string{"b", "c", "d"} {
		if _, ok := l.Get(k); !ok {
			t.Errorf("expected %s still present", k)
		}
	}
	if l.Len() != 3 {
		t.Errorf("len: want 3, got %d", l.Len())
	}
}

func TestLRU_GetRefreshesRecency(t *testing.T) {
	l := newLRUResults(3)
	l.Put("a", resultEntry{})
	l.Put("b", resultEntry{})
	l.Put("c", resultEntry{})

	// Touch "a" — now "b" is least-recently-used.
	if _, ok := l.Get("a"); !ok {
		t.Fatal("a missing before refresh")
	}
	l.Put("d", resultEntry{}) // should evict "b", not "a"

	if _, ok := l.Get("b"); ok {
		t.Error("b should have been evicted (oldest after a got refreshed)")
	}
	if _, ok := l.Get("a"); !ok {
		t.Error("a should still be present (refreshed before overflow)")
	}
}

func TestLRU_PutSameKeyUpdatesValue(t *testing.T) {
	l := newLRUResults(3)
	l.Put("a", resultEntry{resultBytes: []byte("v1")})
	l.Put("a", resultEntry{resultBytes: []byte("v2")})

	got, _ := l.Get("a")
	if string(got.resultBytes) != "v2" {
		t.Errorf("expected v2 after overwrite, got %s", got.resultBytes)
	}
	if l.Len() != 1 {
		t.Errorf("overwrite must not grow size: len=%d", l.Len())
	}
}

func TestLRU_CapacityFloorIsOne(t *testing.T) {
	l := newLRUResults(0)
	l.Put("a", resultEntry{})
	l.Put("b", resultEntry{})
	if l.Len() != 1 {
		t.Errorf("zero/negative capacity should clamp to 1, got len %d", l.Len())
	}
}

// Concurrent Put + Get with race detector enabled must not race.
func TestLRU_RaceFree(t *testing.T) {
	l := newLRUResults(64)
	const goroutines = 16
	const iters = 200

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				l.Put(strconv.Itoa(g*iters+i), resultEntry{})
			}
		}(g)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_, _ = l.Get(strconv.Itoa(g*iters + i))
			}
		}(g)
	}
	wg.Wait()

	if got := l.Len(); got > 64 {
		t.Errorf("len exceeded cap: %d > 64", got)
	}
}
