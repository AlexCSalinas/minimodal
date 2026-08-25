package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
)

// jobFSM is the replicated state machine: every node applies the same
// command log to its own embedded BoltStore, so all replicas converge on
// identical job state. Commands are fully materialized by the leader before
// proposing (timestamps stamped, mutators already run) — the FSM itself is
// deterministic and does no clock reads or decision-making beyond the CAS
// version check.
type jobFSM struct {
	store *BoltStore
}

const (
	cmdPut    = "put"    // unconditional write (tests, recovery tooling)
	cmdCAS    = "cas"    // versioned compare-and-swap (Transition)
	cmdSubmit = "submit" // idempotency-checked insert (SubmitWithIdempotency)
)

// fsmCommand is the wire format of one Raft log entry.
type fsmCommand struct {
	Op string `json:"op"`

	// put + cas + submit: the fully-stamped record to write.
	Record JobRecord `json:"record"`

	// cas only: the version the leader read before running the mutator. The
	// write applies iff the stored version still matches.
	ExpectedVersion int64 `json:"expected_version,omitempty"`

	// submit only.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// fsmResponse is what Apply returns to the proposing leader.
type fsmResponse struct {
	// cas: true when the version check failed and nothing was written.
	Conflict bool
	// submit: the authoritative job ID and whether a new record was created.
	JobID   string
	Created bool
	// Any store error. Held here rather than returned as a panic so a bad
	// command can't take down the whole node.
	Err error
}

func (f *jobFSM) Apply(entry *raft.Log) any {
	var cmd fsmCommand
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return fsmResponse{Err: fmt.Errorf("decode command: %w", err)}
	}
	switch cmd.Op {
	case cmdPut:
		return fsmResponse{Err: f.store.putJobRaw(cmd.Record)}
	case cmdCAS:
		conflict, err := f.store.casPut(cmd.Record.ID, cmd.ExpectedVersion, cmd.Record)
		return fsmResponse{Conflict: conflict, Err: err}
	case cmdSubmit:
		jobID, created, err := f.store.submitRaw(cmd.Record, cmd.IdempotencyKey)
		return fsmResponse{JobID: jobID, Created: created, Err: err}
	default:
		return fsmResponse{Err: fmt.Errorf("unknown fsm op %q", cmd.Op)}
	}
}

// fsmSnapshot captures the full store state at a point in time. Persist
// writes it to the snapshot sink; Raft then truncates the log behind it.
type fsmSnapshot struct {
	Jobs []JobRecord       `json:"jobs"`
	Idem map[string]string `json:"idempotency"`
}

func (f *jobFSM) Snapshot() (raft.FSMSnapshot, error) {
	jobs, idem, err := f.store.exportState()
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{Jobs: jobs, Idem: idem}, nil
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

func (f *jobFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var snap fsmSnapshot
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		return err
	}
	return f.store.importState(snap.Jobs, snap.Idem)
}
