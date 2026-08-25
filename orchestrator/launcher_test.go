package main

import (
	"strings"
	"testing"
	"time"
)

// newTestLauncher launches long-running `sleep` processes instead of real
// workers — enough to exercise process lifecycle, ports, and signals.
func newTestLauncher(t *testing.T) *ProcessLauncher {
	t.Helper()
	l := NewProcessLauncher([]string{"sleep", "300"}, t.TempDir(), "127.0.0.1:0", 51000)
	l.stopGrace = 500 * time.Millisecond
	t.Cleanup(l.StopAll)
	return l
}

func (l *ProcessLauncher) proc(id string) *workerProc {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.procs[id]
}

func TestProcessLauncher_LaunchStartsProcessWithWorkerEnv(t *testing.T) {
	l := newTestLauncher(t)
	if err := l.LaunchWorker("w1"); err != nil {
		t.Fatalf("LaunchWorker: %v", err)
	}
	p := l.proc("w1")
	if p == nil {
		t.Fatal("no process entry for w1")
	}
	var haveID, havePort bool
	for _, e := range p.cmd.Env {
		if e == "WORKER_ID=w1" {
			haveID = true
		}
		if strings.HasPrefix(e, "WORKER_PORT=") {
			havePort = true
		}
	}
	if !haveID || !havePort {
		t.Errorf("worker env incomplete: WORKER_ID=%v WORKER_PORT=%v", haveID, havePort)
	}
	// Double-launch under the same ID must be rejected.
	if err := l.LaunchWorker("w1"); err == nil {
		t.Error("duplicate LaunchWorker(w1) should fail")
	}
}

func TestProcessLauncher_StopTerminatesAndIsIdempotent(t *testing.T) {
	l := newTestLauncher(t)
	if err := l.LaunchWorker("w1"); err != nil {
		t.Fatalf("LaunchWorker: %v", err)
	}
	p := l.proc("w1")
	if err := l.StopWorker("w1"); err != nil {
		t.Fatalf("StopWorker: %v", err)
	}
	select {
	case <-p.done:
	case <-time.After(3 * time.Second):
		t.Fatal("process did not exit after StopWorker")
	}
	// Stopping again (or stopping something unknown) must be a no-op.
	if err := l.StopWorker("w1"); err != nil {
		t.Errorf("second StopWorker should be a no-op, got %v", err)
	}
	if err := l.StopWorker("ghost"); err != nil {
		t.Errorf("StopWorker(unknown) should be a no-op, got %v", err)
	}
}

func TestProcessLauncher_PortsAreDistinctAndRecycled(t *testing.T) {
	l := newTestLauncher(t)
	if err := l.LaunchWorker("w1"); err != nil {
		t.Fatalf("launch w1: %v", err)
	}
	if err := l.LaunchWorker("w2"); err != nil {
		t.Fatalf("launch w2: %v", err)
	}
	p1, p2 := l.proc("w1"), l.proc("w2")
	if p1.port == p2.port {
		t.Fatalf("concurrent workers share port %d", p1.port)
	}
	if p1.port != 51000 || p2.port != 51001 {
		t.Errorf("want ports 51000/51001, got %d/%d", p1.port, p2.port)
	}

	// Stop w1 and wait for its reaper to release the lease.
	_ = l.StopWorker("w1")
	<-p1.done
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		l.mu.Lock()
		free := !l.ports[p1.port]
		l.mu.Unlock()
		if free {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := l.LaunchWorker("w3"); err != nil {
		t.Fatalf("launch w3: %v", err)
	}
	if got := l.proc("w3").port; got != p1.port {
		t.Errorf("want w3 to recycle freed port %d, got %d", p1.port, got)
	}
}

func TestProcessLauncher_StopAllTerminatesEverything(t *testing.T) {
	l := newTestLauncher(t)
	for _, id := range []string{"w1", "w2", "w3"} {
		if err := l.LaunchWorker(id); err != nil {
			t.Fatalf("launch %s: %v", id, err)
		}
	}
	l.StopAll()
	// StopAll waits for exits; the reaper goroutines then drop the entries.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		l.mu.Lock()
		n := len(l.procs)
		l.mu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("StopAll left live process entries behind")
}
