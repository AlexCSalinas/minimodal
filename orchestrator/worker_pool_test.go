package main

import (
	"sync"
	"testing"
	"time"
)

func TestWorkerPool_RegisterAndAliveCount(t *testing.T) {
	p := NewWorkerPool(time.Second)
	if got := p.AliveCount(); got != 0 {
		t.Errorf("empty pool: want 0, got %d", got)
	}
	p.Register("w1", "127.0.0.1:50100", 4)
	p.Register("w2", "127.0.0.1:50101", 4)
	if got := p.AliveCount(); got != 2 {
		t.Errorf("after 2 registers: want 2, got %d", got)
	}
}

func TestWorkerPool_RecordHeartbeat_UnknownReturnsFalse(t *testing.T) {
	p := NewWorkerPool(time.Second)
	if p.RecordHeartbeat("ghost", 0) {
		t.Error("heartbeat from unknown worker should return false (drain signal)")
	}
}

func TestWorkerPool_RecordHeartbeat_ResurrectsDeadWorker(t *testing.T) {
	p := NewWorkerPool(50 * time.Millisecond)
	p.Register("w", "127.0.0.1:1", 1)

	// Force the worker dead by rewinding its heartbeat past the timeout.
	p.mu.Lock()
	p.workers["w"].lastHeartbeat = time.Now().Add(-1 * time.Second)
	p.mu.Unlock()

	dead := p.sweep()
	if len(dead) != 1 || dead[0] != "w" {
		t.Fatalf("expected sweep to mark w dead, got %v", dead)
	}

	// Subsequent heartbeat should bring it back.
	if !p.RecordHeartbeat("w", 0) {
		t.Error("heartbeat on dead-but-known worker should return true")
	}
	if p.AliveCount() != 1 {
		t.Errorf("expected resurrected worker to count as alive")
	}
}

func TestWorkerPool_Sweep_PreservesLiveWorkers(t *testing.T) {
	p := NewWorkerPool(50 * time.Millisecond)
	p.Register("alive", "a", 1)
	p.Register("stale", "s", 1)

	p.mu.Lock()
	p.workers["stale"].lastHeartbeat = time.Now().Add(-1 * time.Second)
	p.mu.Unlock()

	dead := p.sweep()
	if len(dead) != 1 || dead[0] != "stale" {
		t.Errorf("expected only stale to be swept, got %v", dead)
	}
	if p.AliveCount() != 1 {
		t.Errorf("alive worker should remain alive after sweep")
	}
}

func TestWorkerPool_LeastLoaded_PicksLowestActiveTasks(t *testing.T) {
	p := NewWorkerPool(time.Second)
	p.Register("busy", "127.0.0.1:1", 4)
	p.Register("idle", "127.0.0.1:2", 4)
	p.Register("mid", "127.0.0.1:3", 4)
	p.RecordHeartbeat("busy", 3)
	p.RecordHeartbeat("idle", 0)
	p.RecordHeartbeat("mid", 1)

	id, addr, ok := p.LeastLoadedAlive()
	if !ok {
		t.Fatal("LeastLoadedAlive: expected ok")
	}
	if id != "idle" || addr != "127.0.0.1:2" {
		t.Errorf("expected idle worker, got id=%s addr=%s", id, addr)
	}
}

func TestWorkerPool_LeastLoaded_RespectsExclusion(t *testing.T) {
	p := NewWorkerPool(time.Second)
	p.Register("a", "127.0.0.1:1", 4)
	p.Register("b", "127.0.0.1:2", 4)
	// Both at zero load; exclude "a" → must pick "b".
	id, _, ok := p.LeastLoadedAliveExcluding(map[string]bool{"a": true})
	if !ok || id != "b" {
		t.Errorf("expected b (a excluded), got id=%s ok=%v", id, ok)
	}
}

func TestWorkerPool_LeastLoaded_SkipsDead(t *testing.T) {
	p := NewWorkerPool(50 * time.Millisecond)
	p.Register("dead", "d", 4)
	p.Register("live", "l", 4)
	p.mu.Lock()
	p.workers["dead"].lastHeartbeat = time.Now().Add(-1 * time.Second)
	p.mu.Unlock()
	_ = p.sweep()

	id, _, ok := p.LeastLoadedAlive()
	if !ok || id != "live" {
		t.Errorf("dead worker selected: id=%s ok=%v", id, ok)
	}
}

func TestWorkerPool_LeastLoaded_NoWorkers(t *testing.T) {
	p := NewWorkerPool(time.Second)
	if _, _, ok := p.LeastLoadedAlive(); ok {
		t.Error("empty pool: expected ok=false")
	}
}

func TestWorkerPool_Snapshot_ReportsUtilization(t *testing.T) {
	p := NewWorkerPool(time.Second)
	p.Register("w", "addr", 4)
	p.RecordHeartbeat("w", 1)

	snap := p.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("want 1 entry, got %d", len(snap))
	}
	if snap[0].Utilization != 0.25 {
		t.Errorf("utilization: want 0.25, got %f", snap[0].Utilization)
	}
	if !snap[0].Alive {
		t.Error("expected alive=true")
	}
}

// Sweep + reaper callback: when a worker dies, the registered reaper must be
// called exactly once per death event. Verifies the contract used by Server.
func TestWorkerPool_Sweep_CallsReaperExternally(t *testing.T) {
	p := NewWorkerPool(50 * time.Millisecond)
	p.Register("doomed", "d", 1)
	p.mu.Lock()
	p.workers["doomed"].lastHeartbeat = time.Now().Add(-1 * time.Second)
	p.mu.Unlock()

	var (
		mu     sync.Mutex
		called []string
	)
	p.SetReaper(reaperFunc(func(id string) {
		mu.Lock()
		defer mu.Unlock()
		called = append(called, id)
	}))

	dead := p.sweep()
	for _, id := range dead {
		if r := p.getReaper(); r != nil {
			r.ReassignJobsOf(id)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(called) != 1 || called[0] != "doomed" {
		t.Errorf("reaper called wrong: %v", called)
	}
}

type reaperFunc func(string)

func (f reaperFunc) ReassignJobsOf(id string) { f(id) }
