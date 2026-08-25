package main

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

// memSink is an in-memory raft.SnapshotSink for exercising Persist.
type memSink struct {
	bytes.Buffer
	canceled bool
}

func (m *memSink) Close() error  { return nil }
func (m *memSink) Cancel() error { m.canceled = true; return nil }
func (m *memSink) ID() string    { return "mem" }

func applyCmd(t *testing.T, fsm *jobFSM, cmd fsmCommand) fsmResponse {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp := fsm.Apply(&raft.Log{Data: data}).(fsmResponse)
	return resp
}

// Snapshot → Persist → Restore into a fresh FSM must reproduce the exact
// state: jobs, versions, and the idempotency index.
func TestJobFSM_SnapshotRestoreRoundTrip(t *testing.T) {
	src := &jobFSM{store: newTestStore(t)}

	if resp := applyCmd(t, src, fsmCommand{Op: cmdSubmit, Record: JobRecord{
		ID: "a", Status: StatusDone, ResultBytes: []byte("res"), Version: 1,
	}, IdempotencyKey: "key-a"}); resp.Err != nil {
		t.Fatalf("submit: %v", resp.Err)
	}
	if resp := applyCmd(t, src, fsmCommand{Op: cmdPut, Record: JobRecord{
		ID: "b", Status: StatusRunning, WorkerID: "w1", Version: 3,
	}}); resp.Err != nil {
		t.Fatalf("put: %v", resp.Err)
	}

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if sink.canceled {
		t.Fatal("sink should not be canceled on success")
	}

	dst := &jobFSM{store: newTestStore(t)}
	// Pre-existing state on the restoring node must be wiped, not merged.
	if resp := applyCmd(t, dst, fsmCommand{Op: cmdPut, Record: JobRecord{ID: "stale"}}); resp.Err != nil {
		t.Fatalf("seed stale: %v", resp.Err)
	}
	if err := dst.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if rec, _ := dst.store.GetJob("stale"); rec != nil {
		t.Error("restore must replace state, not merge — stale job survived")
	}
	a, _ := dst.store.GetJob("a")
	if a == nil || a.Status != StatusDone || string(a.ResultBytes) != "res" || a.Version != 1 {
		t.Errorf("job a not restored faithfully: %+v", a)
	}
	b, _ := dst.store.GetJob("b")
	if b == nil || b.WorkerID != "w1" || b.Version != 3 {
		t.Errorf("job b not restored faithfully: %+v", b)
	}
	// Idempotency index restored: same key maps to job a.
	id, created, err := dst.store.submitRaw(JobRecord{ID: "new"}, "key-a")
	if err != nil || created || id != "a" {
		t.Errorf("idempotency index lost in restore: id=%s created=%v err=%v", id, created, err)
	}
}

// The CAS command is the concurrency backbone — verify both arms directly.
func TestJobFSM_CASRespectsVersions(t *testing.T) {
	fsm := &jobFSM{store: newTestStore(t)}
	applyCmd(t, fsm, fsmCommand{Op: cmdPut, Record: JobRecord{ID: "j", Status: StatusPending, Version: 1}})

	// Matching version applies.
	resp := applyCmd(t, fsm, fsmCommand{Op: cmdCAS, ExpectedVersion: 1, Record: JobRecord{
		ID: "j", Status: StatusRunning, Version: 2,
	}})
	if resp.Err != nil || resp.Conflict {
		t.Fatalf("matching CAS: conflict=%v err=%v", resp.Conflict, resp.Err)
	}

	// Stale version conflicts and writes nothing.
	resp = applyCmd(t, fsm, fsmCommand{Op: cmdCAS, ExpectedVersion: 1, Record: JobRecord{
		ID: "j", Status: StatusFailed, Version: 2,
	}})
	if resp.Err != nil || !resp.Conflict {
		t.Fatalf("stale CAS must conflict: conflict=%v err=%v", resp.Conflict, resp.Err)
	}
	rec, _ := fsm.store.GetJob("j")
	if rec.Status != StatusRunning || rec.Version != 2 {
		t.Errorf("stale CAS must not write: %+v", rec)
	}
}

func TestJobFSM_UnknownOpIsAnErrorNotAPanic(t *testing.T) {
	fsm := &jobFSM{store: newTestStore(t)}
	if resp := applyCmd(t, fsm, fsmCommand{Op: "explode"}); resp.Err == nil {
		t.Fatal("unknown op must return an error response")
	}
}
