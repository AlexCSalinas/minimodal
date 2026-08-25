package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// ErrNotLeader is returned for writes on a non-leader node. The wrapped
// message carries the current leader's address so RPC handlers can tell the
// client where to go.
var ErrNotLeader = errors.New("not the raft leader")

const (
	raftApplyTimeout = 5 * time.Second
	maxCASRetries    = 16
)

// RaftStore is the multi-node Store: every write is a command replicated
// through a hashicorp/raft log and applied by each node's jobFSM to its own
// embedded BoltStore. A majority of nodes must acknowledge a write before it
// commits, so job state survives the loss of any minority — including the
// leader.
//
// Reads are served from the local FSM store. On the leader (where the
// scheduler, reaper, and RPC handlers run) local reads reflect every command
// that node has proposed; on followers they can trail the log by a beat,
// which is fine for the dashboard/metrics surface they serve.
//
// Transition keeps its closure-based interface via optimistic concurrency:
// the leader runs the mutator against its local copy, then replicates a
// compare-and-swap keyed on JobRecord.Version. A concurrent writer makes the
// CAS conflict at apply time; the leader re-reads and re-runs the mutator.
type RaftStore struct {
	raft  *raft.Raft
	inner *BoltStore
}

// Compile-time assertion that RaftStore satisfies Store.
var _ Store = (*RaftStore)(nil)

// RaftDeps are the raft plumbing pieces. Production wiring comes from
// NewRaftStore; tests inject in-memory transports and stores.
type RaftDeps struct {
	Config    *raft.Config
	FSMStore  *BoltStore
	Logs      raft.LogStore
	Stable    raft.StableStore
	Snapshots raft.SnapshotStore
	Transport raft.Transport
}

func newRaftStoreWithDeps(deps RaftDeps) (*RaftStore, error) {
	fsm := &jobFSM{store: deps.FSMStore}
	r, err := raft.NewRaft(deps.Config, fsm, deps.Logs, deps.Stable, deps.Snapshots, deps.Transport)
	if err != nil {
		return nil, fmt.Errorf("start raft: %w", err)
	}
	return &RaftStore{raft: r, inner: deps.FSMStore}, nil
}

// NewRaftStore builds the production RaftStore: BoltDB-backed raft log +
// stable store, file snapshots, and a TCP transport, all under cfg.RaftDir.
// When cfg.RaftJoin is empty the node bootstraps itself as a single-node
// cluster (idempotent across restarts); otherwise it starts blank and waits
// to be added by an existing leader via the /raft/join HTTP endpoint.
func NewRaftStore(cfg Config) (*RaftStore, error) {
	if err := os.MkdirAll(cfg.RaftDir, 0o755); err != nil {
		return nil, err
	}
	fsmStore, err := NewBoltStore(filepath.Join(cfg.RaftDir, "fsm.db"))
	if err != nil {
		return nil, err
	}
	logs, err := raftboltdb.NewBoltStore(filepath.Join(cfg.RaftDir, "raft.db"))
	if err != nil {
		return nil, err
	}
	snaps, err := raft.NewFileSnapshotStore(cfg.RaftDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftBind)
	if err != nil {
		return nil, fmt.Errorf("resolve raft bind %s: %w", cfg.RaftBind, err)
	}
	trans, err := raft.NewTCPTransport(cfg.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft transport on %s: %w", cfg.RaftBind, err)
	}

	rcfg := raft.DefaultConfig()
	rcfg.LocalID = raft.ServerID(cfg.RaftID)

	store, err := newRaftStoreWithDeps(RaftDeps{
		Config: rcfg, FSMStore: fsmStore,
		Logs: logs, Stable: logs, Snapshots: snaps, Transport: trans,
	})
	if err != nil {
		return nil, err
	}

	if cfg.RaftJoin == "" {
		// Self-bootstrap. ErrCantBootstrap means a previous lifetime already
		// did this — normal on restart.
		f := store.raft.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
			{ID: rcfg.LocalID, Address: raft.ServerAddress(cfg.RaftBind)},
		}})
		if err := f.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}
	return store, nil
}

// =============================================================================
// Write path
// =============================================================================

func (s *RaftStore) notLeaderErr() error {
	leaderAddr, leaderID := s.raft.LeaderWithID()
	return fmt.Errorf("%w (leader: %s at %s)", ErrNotLeader, leaderID, leaderAddr)
}

func (s *RaftStore) apply(cmd fsmCommand) (fsmResponse, error) {
	if s.raft.State() != raft.Leader {
		return fsmResponse{}, s.notLeaderErr()
	}
	buf, err := json.Marshal(cmd)
	if err != nil {
		return fsmResponse{}, err
	}
	f := s.raft.Apply(buf, raftApplyTimeout)
	if err := f.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return fsmResponse{}, s.notLeaderErr()
		}
		return fsmResponse{}, fmt.Errorf("raft apply: %w", err)
	}
	resp := f.Response().(fsmResponse)
	return resp, resp.Err
}

// PutJob replicates an unconditional (last-writer-wins) write. Production
// state changes go through Transition/SubmitWithIdempotency; this exists for
// tests and tooling parity with BoltStore.
func (s *RaftStore) PutJob(rec JobRecord) error {
	rec.UpdatedAt = time.Now()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = rec.UpdatedAt
	}
	if cur, err := s.inner.GetJob(rec.ID); err == nil && cur != nil {
		rec.Version = cur.Version + 1
	} else {
		rec.Version = 1
	}
	_, err := s.apply(fsmCommand{Op: cmdPut, Record: rec})
	return err
}

func (s *RaftStore) SubmitWithIdempotency(rec JobRecord, idempotencyKey string) (string, bool, error) {
	rec.IdempotencyKey = idempotencyKey
	rec.UpdatedAt = time.Now()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = rec.UpdatedAt
	}
	rec.Version = 1
	resp, err := s.apply(fsmCommand{Op: cmdSubmit, Record: rec, IdempotencyKey: idempotencyKey})
	if err != nil {
		return "", false, err
	}
	return resp.JobID, resp.Created, nil
}

// Transition mirrors BoltStore.Transition's contract (including
// ErrSkipTransition) via a read–mutate–CAS loop. On conflict the mutator
// re-runs against the fresh record, exactly as if its transaction had
// serialized later.
func (s *RaftStore) Transition(jobID string, mutate func(*JobRecord) error) (bool, error) {
	for attempt := 0; attempt < maxCASRetries; attempt++ {
		rec, err := s.inner.GetJob(jobID)
		if err != nil {
			return false, err
		}
		if rec == nil {
			return false, fmt.Errorf("job %s not found", jobID)
		}
		cur := *rec
		if err := mutate(&cur); err != nil {
			if err == ErrSkipTransition {
				return false, nil
			}
			return false, err
		}
		expected := rec.Version
		cur.UpdatedAt = time.Now()
		if cur.CreatedAt.IsZero() {
			cur.CreatedAt = cur.UpdatedAt
		}
		cur.Version = expected + 1
		resp, err := s.apply(fsmCommand{Op: cmdCAS, Record: cur, ExpectedVersion: expected})
		if err != nil {
			return false, err
		}
		if resp.Conflict {
			continue
		}
		return true, nil
	}
	return false, fmt.Errorf("transition %s: gave up after %d CAS conflicts", jobID, maxCASRetries)
}

// =============================================================================
// Read path — local FSM state
// =============================================================================

func (s *RaftStore) GetJob(id string) (*JobRecord, error) { return s.inner.GetJob(id) }
func (s *RaftStore) ListUnfinished() ([]JobRecord, error) { return s.inner.ListUnfinished() }
func (s *RaftStore) ListRunningOnWorker(workerID string) ([]JobRecord, error) {
	return s.inner.ListRunningOnWorker(workerID)
}

func (s *RaftStore) Ping() error {
	if s.raft.State() == raft.Shutdown {
		return errors.New("raft node is shut down")
	}
	return s.inner.Ping()
}

func (s *RaftStore) Close() error {
	shutdownErr := s.raft.Shutdown().Error()
	closeErr := s.inner.Close()
	if shutdownErr != nil {
		return shutdownErr
	}
	return closeErr
}

// =============================================================================
// Cluster surface — used by main.go and the HTTP join/status endpoints
// =============================================================================

func (s *RaftStore) IsLeader() bool { return s.raft.State() == raft.Leader }

// LeaderCh signals leadership acquisition (true) and loss (false).
// Deliveries can coalesce under churn; treat it as edge-triggered advice.
func (s *RaftStore) LeaderCh() <-chan bool { return s.raft.LeaderCh() }

// Join adds a node as a voter. Leader only.
func (s *RaftStore) Join(id, addr string) error {
	if !s.IsLeader() {
		return s.notLeaderErr()
	}
	f := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, raftApplyTimeout)
	return f.Error()
}

// RaftStatus is the /raft/status payload.
type RaftStatus struct {
	ID         string           `json:"id"`
	State      string           `json:"state"`
	LeaderID   string           `json:"leader_id"`
	LeaderAddr string           `json:"leader_addr"`
	Servers    []RaftServerInfo `json:"servers"`
	LastIndex  uint64           `json:"last_index"`
	AppliedIdx uint64           `json:"applied_index"`
}

type RaftServerInfo struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Suffrage string `json:"suffrage"`
}

func (s *RaftStore) Status(localID string) RaftStatus {
	leaderAddr, leaderID := s.raft.LeaderWithID()
	st := RaftStatus{
		ID:         localID,
		State:      s.raft.State().String(),
		LeaderID:   string(leaderID),
		LeaderAddr: string(leaderAddr),
		LastIndex:  s.raft.LastIndex(),
		AppliedIdx: s.raft.AppliedIndex(),
	}
	if cf := s.raft.GetConfiguration(); cf.Error() == nil {
		for _, srv := range cf.Configuration().Servers {
			st.Servers = append(st.Servers, RaftServerInfo{
				ID:       string(srv.ID),
				Address:  string(srv.Address),
				Suffrage: srv.Suffrage.String(),
			})
		}
	}
	return st
}
