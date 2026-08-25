package main

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "minimodal/orchestrator/pb"
)

// fakeLauncher records launch/stop calls. onLaunch (optional) lets a test
// simulate the worker side — e.g. boot a fake gRPC worker and register it.
type fakeLauncher struct {
	mu       sync.Mutex
	launched []string
	stopped  []string
	onLaunch func(id string)
}

func (f *fakeLauncher) LaunchWorker(id string) error {
	f.mu.Lock()
	f.launched = append(f.launched, id)
	hook := f.onLaunch
	f.mu.Unlock()
	if hook != nil {
		hook(id)
	}
	return nil
}

func (f *fakeLauncher) StopWorker(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, id)
	return nil
}

func (f *fakeLauncher) StopAll() {}

func (f *fakeLauncher) Launched() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.launched...)
}

func (f *fakeLauncher) Stopped() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.stopped...)
}

func testASConfig() AutoscalerConfig {
	return AutoscalerConfig{
		Min:             0,
		Max:             4,
		QueueThreshold:  1,
		IdleTimeout:     10 * time.Second,
		Cooldown:        5 * time.Second,
		RegisterTimeout: 15 * time.Second,
		Tick:            time.Hour, // tests drive tick() by hand
	}
}

func newTestAutoscaler(cfg AutoscalerConfig, queue *int) (*Autoscaler, *WorkerPool, *fakeLauncher) {
	pool := NewWorkerPool(time.Minute)
	fl := &fakeLauncher{}
	as := NewAutoscaler(cfg, pool, func() int { return *queue }, fl)
	return as, pool, fl
}

// Jobs queueing with zero workers must trigger a launch; a second launch must
// wait out the cooldown.
func TestAutoscaler_ScaleUpWhenQueuedAndNoWorkers(t *testing.T) {
	queue := 5
	as, _, fl := newTestAutoscaler(testASConfig(), &queue)
	t0 := time.Now()

	as.tick(t0)
	if got := len(fl.Launched()); got != 1 {
		t.Fatalf("want 1 launch, got %d", got)
	}
	as.tick(t0.Add(time.Second)) // inside cooldown
	if got := len(fl.Launched()); got != 1 {
		t.Errorf("launch inside cooldown: want still 1, got %d", got)
	}
	as.tick(t0.Add(6 * time.Second)) // cooldown elapsed, queue still deep
	if got := len(fl.Launched()); got != 2 {
		t.Errorf("after cooldown with queue still deep: want 2 launches, got %d", got)
	}
}

// A launched-but-not-yet-registered worker counts toward Max — otherwise a
// deep queue would over-spawn during worker startup.
func TestAutoscaler_PendingLaunchCountsTowardMax(t *testing.T) {
	cfg := testASConfig()
	cfg.Max = 1
	cfg.Cooldown = 0
	queue := 5
	as, _, fl := newTestAutoscaler(cfg, &queue)
	t0 := time.Now()

	for i := 0; i < 5; i++ {
		as.tick(t0.Add(time.Duration(i) * time.Second))
	}
	if got := len(fl.Launched()); got != 1 {
		t.Errorf("Max=1: want exactly 1 launch, got %d", got)
	}
}

// A pool with spare capacity and an empty queue must not scale up.
func TestAutoscaler_NoLaunchWithSpareCapacity(t *testing.T) {
	queue := 0
	as, pool, fl := newTestAutoscaler(testASConfig(), &queue)
	pool.Register("ext", "127.0.0.1:1", 4)
	pool.RecordHeartbeat("ext", 1) // 1 of 4 slots busy

	as.tick(time.Now())
	if got := len(fl.Launched()); got != 0 {
		t.Errorf("spare capacity: want 0 launches, got %d", got)
	}
}

// Every alive worker at declared capacity is the load signal for a nonzero
// pool (the queue stays empty because workers ack ExecuteTask immediately).
func TestAutoscaler_ScaleUpWhenSaturated(t *testing.T) {
	queue := 0
	as, pool, fl := newTestAutoscaler(testASConfig(), &queue)
	pool.Register("ext", "127.0.0.1:1", 2)
	pool.RecordHeartbeat("ext", 2) // at capacity

	as.tick(time.Now())
	if got := len(fl.Launched()); got != 1 {
		t.Errorf("saturated pool: want 1 launch, got %d", got)
	}
}

// With Min set, the pool must come up to the floor without waiting for demand
// or cooldown.
func TestAutoscaler_ScalesToMin(t *testing.T) {
	cfg := testASConfig()
	cfg.Min = 2
	queue := 0
	as, _, fl := newTestAutoscaler(cfg, &queue)
	t0 := time.Now()

	as.tick(t0)
	as.tick(t0.Add(time.Millisecond)) // no cooldown below min
	as.tick(t0.Add(2 * time.Millisecond))
	if got := len(fl.Launched()); got != 2 {
		t.Errorf("Min=2: want exactly 2 launches, got %d", got)
	}
}

// A managed worker idle past IdleTimeout with an empty queue must be stopped;
// with Min raised to cover it, it must survive.
func TestAutoscaler_ScaleDownIdleManagedWorker(t *testing.T) {
	for _, min := range []int{0, 1} {
		queue := 5
		cfg := testASConfig()
		cfg.Min = min
		as, pool, fl := newTestAutoscaler(cfg, &queue)
		t0 := time.Now()

		as.tick(t0) // launch
		id := fl.Launched()[0]
		pool.Register(id, "127.0.0.1:1", 4)
		queue = 0

		as.tick(t0.Add(time.Second))     // observes registration
		as.tick(t0.Add(2 * time.Second)) // idleSince starts here
		as.tick(t0.Add(2*time.Second + cfg.IdleTimeout + time.Second))

		stopped := fl.Stopped()
		if min == 0 {
			if len(stopped) != 1 || stopped[0] != id {
				t.Errorf("Min=0: want %s stopped, got %v", id, stopped)
			}
			if as.ManagedCount() != 0 {
				t.Errorf("Min=0: want empty managed set, got %d", as.ManagedCount())
			}
		} else {
			if len(stopped) != 0 {
				t.Errorf("Min=1: idle worker at the floor must survive, got stops %v", stopped)
			}
		}
	}
}

// An idle worker must not be reclaimed while jobs are still queued.
func TestAutoscaler_NoScaleDownWhileQueueNonEmpty(t *testing.T) {
	queue := 5
	cfg := testASConfig()
	cfg.Max = 1 // block further scale-up so the scale-down branch actually runs
	as, pool, fl := newTestAutoscaler(cfg, &queue)
	t0 := time.Now()

	as.tick(t0)
	id := fl.Launched()[0]
	pool.Register(id, "127.0.0.1:1", 4)
	as.tick(t0.Add(time.Second))
	as.tick(t0.Add(2 * time.Second))
	// Queue stays deep; way past idle timeout — still no stop allowed.
	as.tick(t0.Add(2*time.Second + time.Minute))
	if got := fl.Stopped(); len(got) != 0 {
		t.Errorf("queue nonempty: want no stops, got %v", got)
	}
}

// Workers the autoscaler didn't launch are never stopped, no matter how idle.
func TestAutoscaler_NeverStopsExternalWorkers(t *testing.T) {
	queue := 0
	as, pool, fl := newTestAutoscaler(testASConfig(), &queue)
	pool.Register("ext", "127.0.0.1:1", 4)
	t0 := time.Now()

	as.tick(t0)
	as.tick(t0.Add(time.Minute))
	as.tick(t0.Add(time.Hour))
	if got := fl.Stopped(); len(got) != 0 {
		t.Errorf("external worker must never be stopped, got %v", got)
	}
}

// A launch that never registers must be given up on after RegisterTimeout,
// freeing the slot for a replacement.
func TestAutoscaler_ExpiresLaunchThatNeverRegisters(t *testing.T) {
	cfg := testASConfig()
	cfg.Max = 1
	queue := 5
	as, _, fl := newTestAutoscaler(cfg, &queue)
	t0 := time.Now()

	as.tick(t0)
	first := fl.Launched()[0]
	as.tick(t0.Add(cfg.RegisterTimeout + time.Second))

	stopped := fl.Stopped()
	if len(stopped) != 1 || stopped[0] != first {
		t.Errorf("want expired launch %s stopped, got %v", first, stopped)
	}
	// Slot freed → the same tick's scale-up pass launches a replacement.
	launched := fl.Launched()
	if len(launched) != 2 {
		t.Fatalf("want a replacement launch after expiry, got %v", launched)
	}
	if launched[1] == first {
		t.Errorf("replacement must get a fresh worker ID, got %s twice", first)
	}
}

// A managed worker the fault detector declares dead must be dropped from the
// managed set (and its process reaped) so a replacement can launch.
func TestAutoscaler_ReplacesDeadManagedWorker(t *testing.T) {
	cfg := testASConfig()
	cfg.Max = 1
	cfg.Cooldown = 0
	queue := 5
	pool := NewWorkerPool(10 * time.Millisecond)
	fl := &fakeLauncher{}
	as := NewAutoscaler(cfg, pool, func() int { return queue }, fl)
	t0 := time.Now()

	as.tick(t0)
	id := fl.Launched()[0]
	pool.Register(id, "127.0.0.1:1", 4)
	as.tick(t0.Add(time.Second)) // observes registration

	// Let the heartbeat lapse, then run the fault detector's sweep directly.
	time.Sleep(20 * time.Millisecond)
	pool.sweep()

	as.tick(t0.Add(2 * time.Second))
	if got := fl.Stopped(); len(got) != 1 || got[0] != id {
		t.Errorf("dead managed worker: want %s reaped, got %v", id, got)
	}
	launched := fl.Launched()
	if len(launched) != 2 {
		t.Errorf("want replacement launch after death, got %v", launched)
	}
}

// End-to-end: a job invoked with zero workers must drive the full loop —
// queue depth → autoscaler launch → worker registration → dispatch → RUNNING.
func TestAutoscaler_EndToEnd_QueueDrivesLaunchAndDispatch(t *testing.T) {
	srv := newTestServer(t, Config{MaxDispatchAttempts: 100})

	fl := &fakeLauncher{}
	stops := make(chan func(), 4)
	fl.onLaunch = func(id string) {
		// Simulate the worker side: boot a fake gRPC worker that accepts
		// everything, and register it with the orchestrator under this ID.
		addr, _, stop := startFakeWorker(t, true, "")
		stops <- stop
		srv.workerPool.Register(id, addr, 4)
	}

	as := NewAutoscaler(AutoscalerConfig{
		Min:             0,
		Max:             1,
		QueueThreshold:  1,
		IdleTimeout:     time.Hour,
		Cooldown:        0,
		RegisterTimeout: time.Hour,
		Tick:            10 * time.Millisecond,
	}, srv.workerPool, srv.QueueDepth, fl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go as.Run(ctx)
	go srv.RunPendingQueueLoop(ctx)
	defer func() {
		cancel()
		close(stops)
		for stop := range stops {
			stop()
		}
	}()

	resp, err := srv.InvokeFunction(context.Background(), &pb.InvokeFunctionRequest{
		FunctionBytes: []byte("payload"),
		FunctionName:  "fn",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rec, err := srv.jobStore.GetJob(resp.JobId)
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if rec.Status == StatusRunning {
			if got := len(fl.Launched()); got != 1 {
				t.Errorf("want exactly 1 autoscale launch, got %d", got)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job never reached RUNNING — autoscale → register → dispatch loop did not close")
}
