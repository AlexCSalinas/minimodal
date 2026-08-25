package main

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "minimodal/orchestrator/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func raftServerConfig() Config {
	return Config{
		WorkerTimeout:       time.Second,
		MaxRetries:          3,
		MaxDispatchAttempts: 50,
	}
}

// The whole orchestrator stack — invoke, dispatch, report, status, watch —
// must run unmodified over the raft-replicated store.
func TestServer_FullJobLifecycleOverRaft(t *testing.T) {
	c := newTestCluster(t, 1)
	leader := c.waitLeader()

	srv, err := NewServerWithStore(raftServerConfig(), leader)
	if err != nil {
		t.Fatalf("NewServerWithStore: %v", err)
	}
	t.Cleanup(srv.scheduler.Close)

	addr, calls, stop := startFakeWorker(t, true, "")
	defer stop()
	srv.workerPool.Register("w1", addr, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.RunPendingQueueLoop(ctx)

	resp, err := srv.InvokeFunction(context.Background(), &pb.InvokeFunctionRequest{
		FunctionBytes:  []byte("payload"),
		FunctionName:   "fn",
		IdempotencyKey: "raft-key",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	select {
	case <-calls:
	case <-time.After(5 * time.Second):
		t.Fatal("worker never received the dispatched task")
	}

	if _, err := srv.ReportTaskResult(context.Background(), &pb.ReportTaskResultRequest{
		JobId: resp.JobId, WorkerId: "w1", Success: true, ResultBytes: []byte("done!"),
	}); err != nil {
		t.Fatalf("ReportTaskResult: %v", err)
	}

	st, err := srv.GetJobStatus(context.Background(), &pb.GetJobStatusRequest{JobId: resp.JobId})
	if err != nil {
		t.Fatalf("GetJobStatus: %v", err)
	}
	if st.Status != pb.JobStatus_JOB_STATUS_DONE || string(st.ResultBytes) != "done!" {
		t.Errorf("bad terminal status over raft: %v %q", st.Status, st.ResultBytes)
	}

	// Idempotent retry resolves through the replicated index.
	again, err := srv.InvokeFunction(context.Background(), &pb.InvokeFunctionRequest{
		FunctionBytes:  []byte("payload"),
		FunctionName:   "fn",
		IdempotencyKey: "raft-key",
	})
	if err != nil {
		t.Fatalf("idempotent re-invoke: %v", err)
	}
	if again.JobId != resp.JobId {
		t.Errorf("idempotency broken over raft: %s vs %s", again.JobId, resp.JobId)
	}
}

// A client that hits a follower must get FailedPrecondition with the
// leader's address in the message — not an opaque internal error.
func TestServer_InvokeOnFollowerRedirectsToLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	c.waitLeader()
	followers := c.followers()
	if len(followers) == 0 {
		t.Fatal("expected followers")
	}

	srv, err := NewServerWithStore(raftServerConfig(), followers[0])
	if err != nil {
		t.Fatalf("NewServerWithStore: %v", err)
	}
	t.Cleanup(srv.scheduler.Close)

	_, err = srv.InvokeFunction(context.Background(), &pb.InvokeFunctionRequest{
		FunctionBytes: []byte("p"),
		FunctionName:  "fn",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition from follower, got %v", err)
	}
	// The hint is formatted by RaftStore.notLeaderErr: "... (leader: <id> at <addr>)".
	if s := status.Convert(err).Message(); !strings.Contains(s, "leader:") {
		t.Errorf("error should carry the leader hint, got %q", s)
	}
}
