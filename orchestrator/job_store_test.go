package main

import (
	"errors"
	"path/filepath"
	"testing"
)

func newTestStore(t *testing.T) *BoltStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestJobStore_Ping_OnOpenStoreReturnsNil(t *testing.T) {
	store := newTestStore(t)
	if err := store.Ping(); err != nil {
		t.Errorf("Ping on healthy store: want nil, got %v", err)
	}
}

func TestJobStore_Ping_OnClosedStoreReturnsErr(t *testing.T) {
	store := newTestStore(t)
	_ = store.Close()
	if err := store.Ping(); err == nil {
		t.Error("Ping on closed store should return error")
	}
}

func TestJobStore_PutGet(t *testing.T) {
	store := newTestStore(t)

	rec := JobRecord{
		ID:           "job-1",
		Status:       StatusPending,
		FunctionName: "greet",
	}
	if err := store.PutJob(rec); err != nil {
		t.Fatalf("PutJob: %v", err)
	}

	got, err := store.GetJob("job-1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got == nil {
		t.Fatal("GetJob returned nil for existing record")
	}
	if got.FunctionName != "greet" || got.Status != StatusPending {
		t.Errorf("roundtrip mismatch: %+v", got)
	}
	if got.UpdatedAt.IsZero() || got.CreatedAt.IsZero() {
		t.Error("timestamps not stamped on PutJob")
	}
}

func TestJobStore_GetMissing(t *testing.T) {
	store := newTestStore(t)
	got, err := store.GetJob("does-not-exist")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for missing job, got %+v", got)
	}
}

func TestJobStore_Transition_HappyPath(t *testing.T) {
	store := newTestStore(t)
	if err := store.PutJob(JobRecord{ID: "j", Status: StatusPending}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	committed, err := store.Transition("j", func(r *JobRecord) error {
		r.Status = StatusRunning
		r.WorkerID = "w1"
		return nil
	})
	if err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if !committed {
		t.Error("expected committed=true on clean mutation")
	}

	got, _ := store.GetJob("j")
	if got.Status != StatusRunning || got.WorkerID != "w1" {
		t.Errorf("mutation did not persist: %+v", got)
	}
}

func TestJobStore_Transition_SkipsWriteOnSentinel(t *testing.T) {
	store := newTestStore(t)
	if err := store.PutJob(JobRecord{ID: "j", Status: StatusDone}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	committed, err := store.Transition("j", func(r *JobRecord) error {
		if r.Status == StatusDone {
			return ErrSkipTransition
		}
		r.Status = StatusFailed
		return nil
	})
	if err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if committed {
		t.Error("expected committed=false when mutator returns ErrSkipTransition")
	}

	got, _ := store.GetJob("j")
	if got.Status != StatusDone {
		t.Errorf("ErrSkipTransition should leave record untouched, got %s", got.Status)
	}
}

func TestJobStore_Transition_MissingJob(t *testing.T) {
	store := newTestStore(t)
	_, err := store.Transition("nope", func(r *JobRecord) error { return nil })
	if err == nil {
		t.Fatal("expected error transitioning missing job")
	}
}

func TestJobStore_SubmitWithIdempotency_NewKey(t *testing.T) {
	store := newTestStore(t)

	id, created, err := store.SubmitWithIdempotency(JobRecord{ID: "j1", Status: StatusPending}, "k1")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !created {
		t.Error("expected created=true for new key")
	}
	if id != "j1" {
		t.Errorf("expected id=j1, got %q", id)
	}
}

func TestJobStore_SubmitWithIdempotency_DuplicateKey(t *testing.T) {
	store := newTestStore(t)

	id1, created1, err := store.SubmitWithIdempotency(JobRecord{ID: "j1", Status: StatusPending}, "k1")
	if err != nil || !created1 || id1 != "j1" {
		t.Fatalf("first submit: id=%q created=%v err=%v", id1, created1, err)
	}

	id2, created2, err := store.SubmitWithIdempotency(JobRecord{ID: "j2", Status: StatusPending}, "k1")
	if err != nil {
		t.Fatalf("second submit: %v", err)
	}
	if created2 {
		t.Error("expected created=false for duplicate key")
	}
	if id2 != "j1" {
		t.Errorf("expected duplicate-key submit to return original id j1, got %q", id2)
	}

	// j2 must not have been inserted.
	if got, _ := store.GetJob("j2"); got != nil {
		t.Errorf("duplicate-key submit must not insert j2, got %+v", got)
	}
}

func TestJobStore_SubmitWithIdempotency_EmptyKeyAlwaysInserts(t *testing.T) {
	store := newTestStore(t)

	if _, created, _ := store.SubmitWithIdempotency(JobRecord{ID: "a"}, ""); !created {
		t.Error("empty key first submit should create")
	}
	if _, created, _ := store.SubmitWithIdempotency(JobRecord{ID: "b"}, ""); !created {
		t.Error("empty key second submit should also create (no dedup)")
	}
}

func TestJobStore_ListUnfinished(t *testing.T) {
	store := newTestStore(t)
	for _, rec := range []JobRecord{
		{ID: "p1", Status: StatusPending},
		{ID: "r1", Status: StatusRunning, WorkerID: "w1"},
		{ID: "d1", Status: StatusDone},
		{ID: "f1", Status: StatusFailed},
	} {
		if err := store.PutJob(rec); err != nil {
			t.Fatalf("seed %s: %v", rec.ID, err)
		}
	}

	got, err := store.ListUnfinished()
	if err != nil {
		t.Fatalf("ListUnfinished: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 unfinished, got %d: %+v", len(got), got)
	}
	seen := map[string]bool{}
	for _, j := range got {
		seen[j.ID] = true
	}
	if !seen["p1"] || !seen["r1"] {
		t.Errorf("expected {p1,r1}, got %v", seen)
	}
}

func TestJobStore_ListRunningOnWorker(t *testing.T) {
	store := newTestStore(t)
	for _, rec := range []JobRecord{
		{ID: "r1", Status: StatusRunning, WorkerID: "w1"},
		{ID: "r2", Status: StatusRunning, WorkerID: "w1"},
		{ID: "r3", Status: StatusRunning, WorkerID: "w2"},
		{ID: "d1", Status: StatusDone, WorkerID: "w1"},
		{ID: "p1", Status: StatusPending},
	} {
		if err := store.PutJob(rec); err != nil {
			t.Fatalf("seed %s: %v", rec.ID, err)
		}
	}

	got, err := store.ListRunningOnWorker("w1")
	if err != nil {
		t.Fatalf("ListRunningOnWorker: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 RUNNING jobs on w1, got %d", len(got))
	}
	for _, j := range got {
		if j.WorkerID != "w1" || j.Status != StatusRunning {
			t.Errorf("unexpected job in result: %+v", j)
		}
	}
}

// Concurrent transitions on the same job must serialize cleanly; only one
// can move PENDING→RUNNING, the other must see RUNNING and skip via the
// ErrSkipTransition sentinel.
func TestJobStore_Transition_ConcurrentSerializes(t *testing.T) {
	store := newTestStore(t)
	if err := store.PutJob(JobRecord{ID: "race", Status: StatusPending}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const goroutines = 8
	results := make(chan bool, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			committed, err := store.Transition("race", func(r *JobRecord) error {
				if r.Status != StatusPending {
					return ErrSkipTransition
				}
				r.Status = StatusRunning
				return nil
			})
			if err != nil && !errors.Is(err, ErrSkipTransition) {
				t.Errorf("unexpected err: %v", err)
			}
			results <- committed
		}()
	}

	wins := 0
	for i := 0; i < goroutines; i++ {
		if <-results {
			wins++
		}
	}
	if wins != 1 {
		t.Errorf("expected exactly one winner, got %d", wins)
	}
}
