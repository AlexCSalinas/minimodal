package main

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// =============================================================================
// In-process cluster harness: real raft consensus over in-memory transports.
// =============================================================================

type testCluster struct {
	t      *testing.T
	stores []*RaftStore
	ids    []string
	trans  []*raft.InmemTransport
	addrs  []raft.ServerAddress
}

func fastRaftConfig(id string) *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.Logger = hclog.NewNullLogger()
	return cfg
}

// newTestCluster boots n raft nodes over connected in-memory transports and
// bootstraps them with an identical configuration.
func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{t: t}

	var servers []raft.Server
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node-%d", i)
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(id))
		c.ids = append(c.ids, id)
		c.addrs = append(c.addrs, addr)
		c.trans = append(c.trans, trans)
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: addr})
	}
	for i := range c.trans {
		for j := range c.trans {
			if i != j {
				c.trans[i].Connect(c.addrs[j], c.trans[j])
			}
		}
	}
	for i := 0; i < n; i++ {
		rs := c.startNode(i)
		f := rs.raft.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := f.Error(); err != nil {
			t.Fatalf("bootstrap node %d: %v", i, err)
		}
	}
	return c
}

func (c *testCluster) startNode(i int) *RaftStore {
	c.t.Helper()
	rs, err := newRaftStoreWithDeps(RaftDeps{
		Config:    fastRaftConfig(c.ids[i]),
		FSMStore:  newTestStore(c.t),
		Logs:      raft.NewInmemStore(),
		Stable:    raft.NewInmemStore(),
		Snapshots: raft.NewInmemSnapshotStore(),
		Transport: c.trans[i],
	})
	if err != nil {
		c.t.Fatalf("start raft node %d: %v", i, err)
	}
	c.t.Cleanup(func() { _ = rs.raft.Shutdown().Error() })
	c.stores = append(c.stores, rs)
	return rs
}

func (c *testCluster) waitLeader() *RaftStore {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, rs := range c.stores {
			if rs.raft.State() == raft.Leader {
				return rs
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader elected within 5s")
	return nil
}

func (c *testCluster) followers() []*RaftStore {
	var out []*RaftStore
	for _, rs := range c.stores {
		if rs.raft.State() != raft.Leader && rs.raft.State() != raft.Shutdown {
			out = append(out, rs)
		}
	}
	return out
}

// waitFor polls until check passes — replication to followers is
// asynchronous, so assertions on follower state need a grace window.
func waitFor(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// =============================================================================
// Store conformance on a single-node cluster: RaftStore must behave exactly
// like BoltStore for every Store operation the orchestrator uses.
// =============================================================================

func TestRaftStore_StoreConformance(t *testing.T) {
	c := newTestCluster(t, 1)
	s := c.waitLeader()

	// Put + Get round-trip.
	if err := s.PutJob(JobRecord{ID: "j1", Status: StatusPending, FunctionBytes: []byte("p")}); err != nil {
		t.Fatalf("PutJob: %v", err)
	}
	got, err := s.GetJob("j1")
	if err != nil || got == nil || got.Status != StatusPending || string(got.FunctionBytes) != "p" {
		t.Fatalf("GetJob after Put: %+v err=%v", got, err)
	}

	// Transition commits, bumps state, and honors ErrSkipTransition.
	committed, err := s.Transition("j1", func(r *JobRecord) error {
		r.Status = StatusRunning
		r.WorkerID = "w1"
		return nil
	})
	if err != nil || !committed {
		t.Fatalf("Transition: committed=%v err=%v", committed, err)
	}
	committed, err = s.Transition("j1", func(r *JobRecord) error { return ErrSkipTransition })
	if err != nil || committed {
		t.Fatalf("skip transition: committed=%v err=%v", committed, err)
	}
	if _, err := s.Transition("ghost", func(r *JobRecord) error { return nil }); err == nil {
		t.Fatal("Transition on missing job must error")
	}

	// Idempotent submit dedupes on key.
	id1, created1, err := s.SubmitWithIdempotency(JobRecord{ID: "j2", Status: StatusPending}, "key-a")
	if err != nil || !created1 || id1 != "j2" {
		t.Fatalf("first submit: id=%s created=%v err=%v", id1, created1, err)
	}
	id2, created2, err := s.SubmitWithIdempotency(JobRecord{ID: "j3", Status: StatusPending}, "key-a")
	if err != nil || created2 || id2 != "j2" {
		t.Fatalf("duplicate submit must return existing job: id=%s created=%v err=%v", id2, created2, err)
	}

	// Listings see raft-applied state.
	unfinished, err := s.ListUnfinished()
	if err != nil || len(unfinished) != 2 {
		t.Fatalf("ListUnfinished: %d err=%v", len(unfinished), err)
	}
	onW1, err := s.ListRunningOnWorker("w1")
	if err != nil || len(onW1) != 1 || onW1[0].ID != "j1" {
		t.Fatalf("ListRunningOnWorker: %+v err=%v", onW1, err)
	}

	if err := s.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// =============================================================================
// Replication + failover
// =============================================================================

func TestRaftStore_WritesReplicateToFollowers(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitLeader()

	if err := leader.PutJob(JobRecord{ID: "rep", Status: StatusPending, FunctionName: "fn"}); err != nil {
		t.Fatalf("PutJob on leader: %v", err)
	}

	for i, f := range c.followers() {
		waitFor(t, fmt.Sprintf("follower %d to see the job", i), func() bool {
			rec, err := f.GetJob("rep")
			return err == nil && rec != nil && rec.Status == StatusPending && rec.FunctionName == "fn"
		})
	}
}

func TestRaftStore_LeaderFailoverPreservesJobs(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitLeader()

	// Commit a job, make sure a majority has it.
	if _, _, err := leader.SubmitWithIdempotency(JobRecord{
		ID: "survivor", Status: StatusPending, FunctionBytes: []byte("payload"),
	}, "key-1"); err != nil {
		t.Fatalf("submit: %v", err)
	}
	for _, f := range c.followers() {
		waitFor(t, "replication before failover", func() bool {
			rec, _ := f.GetJob("survivor")
			return rec != nil
		})
	}

	// Kill the leader — the moment this project exists for.
	if err := leader.raft.Shutdown().Error(); err != nil {
		t.Fatalf("shutdown leader: %v", err)
	}

	newLeader := c.waitLeader()
	if newLeader == leader {
		t.Fatal("dead node cannot be leader")
	}

	// State intact on the new leader...
	rec, err := newLeader.GetJob("survivor")
	if err != nil || rec == nil || string(rec.FunctionBytes) != "payload" {
		t.Fatalf("job lost across failover: %+v err=%v", rec, err)
	}
	// ...idempotency index too: a client retry with the same key must not
	// double-run the job on the new leader.
	id, created, err := newLeader.SubmitWithIdempotency(JobRecord{ID: "dup", Status: StatusPending}, "key-1")
	if err != nil || created || id != "survivor" {
		t.Fatalf("idempotency lost across failover: id=%s created=%v err=%v", id, created, err)
	}
	// ...and the new leader accepts writes.
	committed, err := newLeader.Transition("survivor", func(r *JobRecord) error {
		r.Status = StatusRunning
		r.WorkerID = "w-after-failover"
		return nil
	})
	if err != nil || !committed {
		t.Fatalf("write on new leader: committed=%v err=%v", committed, err)
	}
}

func TestRaftStore_FollowerRejectsWritesWithLeaderHint(t *testing.T) {
	c := newTestCluster(t, 3)
	c.waitLeader()
	followers := c.followers()
	if len(followers) == 0 {
		t.Fatal("expected followers")
	}

	err := followers[0].PutJob(JobRecord{ID: "nope", Status: StatusPending})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("want ErrNotLeader, got %v", err)
	}
	_, err = followers[0].Transition("nope", func(r *JobRecord) error { return nil })
	if err == nil {
		t.Fatal("Transition on follower must fail")
	}
}

// Concurrent Transitions on one record must all land exactly once — this is
// the CAS retry loop under real contention.
func TestRaftStore_ConcurrentTransitionsAllApply(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitLeader()

	if err := leader.PutJob(JobRecord{ID: "hot", Status: StatusPending}); err != nil {
		t.Fatalf("PutJob: %v", err)
	}

	const writers = 10
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			committed, err := leader.Transition("hot", func(r *JobRecord) error {
				r.RetryCount++
				return nil
			})
			if err != nil {
				errs <- err
			} else if !committed {
				errs <- errors.New("transition reported not committed")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent transition: %v", err)
	}

	rec, err := leader.GetJob("hot")
	if err != nil || rec == nil {
		t.Fatalf("GetJob: %v", err)
	}
	if rec.RetryCount != writers {
		t.Errorf("lost updates: want RetryCount=%d, got %d", writers, rec.RetryCount)
	}
	if rec.Version != writers+1 { // 1 from PutJob + one per transition
		t.Errorf("want version %d, got %d", writers+1, rec.Version)
	}
}

// A node added to a running cluster must catch up to the full state.
func TestRaftStore_NewNodeCatchesUp(t *testing.T) {
	c := newTestCluster(t, 1)
	leader := c.waitLeader()

	for i := 0; i < 5; i++ {
		if err := leader.PutJob(JobRecord{ID: fmt.Sprintf("pre-%d", i), Status: StatusPending}); err != nil {
			t.Fatalf("PutJob: %v", err)
		}
	}

	// Wire up a brand-new node and add it as a voter.
	id := "node-late"
	addr, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	c.trans[0].Connect(addr, trans)
	trans.Connect(c.addrs[0], c.trans[0])
	c.ids = append(c.ids, id)
	c.addrs = append(c.addrs, addr)
	c.trans = append(c.trans, trans)
	late := c.startNode(len(c.ids) - 1)

	if err := leader.Join(id, string(addr)); err != nil {
		t.Fatalf("Join: %v", err)
	}
	waitFor(t, "late node to replicate pre-join writes", func() bool {
		for i := 0; i < 5; i++ {
			if rec, _ := late.GetJob(fmt.Sprintf("pre-%d", i)); rec == nil {
				return false
			}
		}
		return true
	})

	// And it stays current for post-join writes.
	if err := leader.PutJob(JobRecord{ID: "post", Status: StatusPending}); err != nil {
		t.Fatalf("PutJob: %v", err)
	}
	waitFor(t, "late node to see post-join write", func() bool {
		rec, _ := late.GetJob("post")
		return rec != nil
	})
}
