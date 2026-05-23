package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	pb "minimodal/orchestrator/pb"
)

func newTestServer(t *testing.T, cfg Config) *Server {
	t.Helper()
	if cfg.DBPath == "" {
		cfg.DBPath = filepath.Join(t.TempDir(), "test.db")
	}
	if cfg.WorkerTimeout == 0 {
		cfg.WorkerTimeout = time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(srv.Close)
	return srv
}

// attemptDispatch must mark a job FAILED after exhausting MaxDispatchAttempts
// when no workers are available — otherwise it would spin forever, burning
// CPU and never surfacing the error to the client.
func TestServer_AttemptDispatch_FailsAfterMaxAttempts(t *testing.T) {
	srv := newTestServer(t, Config{MaxDispatchAttempts: 3})

	resp, err := srv.InvokeFunction(context.Background(), &pb.InvokeFunctionRequest{
		FunctionBytes: []byte("payload"),
		FunctionName:  "fn",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	// Drain the job that InvokeFunction enqueued so it doesn't double-run
	// against our direct attemptDispatch call.
	<-srv.pendingQueue

	start := time.Now()
	srv.attemptDispatch(context.Background(), resp.JobId)
	elapsed := time.Since(start)

	got, err := srv.jobStore.GetJob(resp.JobId)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != StatusFailed {
		t.Errorf("expected FAILED after exhausting dispatch attempts, got %s", got.Status)
	}
	if got.Error == "" {
		t.Error("expected error message on failed dispatch")
	}
	// With cap=3 and doubling backoff (100→200ms), we wait ~300ms between
	// attempts. Anything wildly over 2s suggests the cap didn't take effect.
	if elapsed > 2*time.Second {
		t.Errorf("dispatch took too long (%v); cap may not be applied", elapsed)
	}
	if srv.metrics.totalFailed.Load() != 1 {
		t.Errorf("totalFailed metric: want 1, got %d", srv.metrics.totalFailed.Load())
	}
}

// InvokeFunction should reject empty payload bytes — protects against an
// SDK bug that would silently enqueue an undispatchable job.
func TestServer_InvokeFunction_RejectsEmptyPayload(t *testing.T) {
	srv := newTestServer(t, Config{MaxDispatchAttempts: 3})
	_, err := srv.InvokeFunction(context.Background(), &pb.InvokeFunctionRequest{
		FunctionBytes: nil,
		FunctionName:  "fn",
	})
	if err == nil {
		t.Fatal("expected InvalidArgument for empty payload")
	}
}

// GetJobStatus on an unknown job ID must return NotFound, not panic / not
// silently return an empty response.
func TestServer_GetJobStatus_NotFound(t *testing.T) {
	srv := newTestServer(t, Config{MaxDispatchAttempts: 3})
	_, err := srv.GetJobStatus(context.Background(), &pb.GetJobStatusRequest{JobId: "ghost"})
	if err == nil {
		t.Fatal("expected NotFound error for unknown job")
	}
}

// Heartbeat from a worker the orchestrator never registered must signal the
// worker to drain — otherwise a half-restarted worker would silently
// continue heartbeating into the void.
func TestServer_Heartbeat_UnknownWorkerSignalsDrain(t *testing.T) {
	srv := newTestServer(t, Config{})
	resp, err := srv.Heartbeat(context.Background(), &pb.HeartbeatRequest{WorkerId: "ghost"})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !resp.ShouldDrain {
		t.Error("unknown worker heartbeat should signal drain")
	}
}

// enqueue must not block once shutdown has begun — otherwise an InvokeFunction
// RPC landing during graceful shutdown could deadlock the gRPC server. The
// job is already in BoltDB; recovery will pick it up on next startup.
func TestServer_Enqueue_NonBlockingDuringShutdown(t *testing.T) {
	srv := newTestServer(t, Config{})
	// Saturate the queue so enqueue would normally block on send.
	for i := 0; i < pendingQueueSize; i++ {
		srv.pendingQueue <- "filler"
	}
	srv.BeginShutdown()

	done := make(chan struct{})
	go func() {
		srv.enqueue("post-shutdown-job")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("enqueue blocked after BeginShutdown — would deadlock graceful shutdown")
	}
}

// InvokeFunction must reject payloads above MaxPayloadBytes — protects the
// orchestrator from a buggy/malicious client allocating arbitrary memory.
func TestServer_InvokeFunction_RejectsOversizedPayload(t *testing.T) {
	srv := newTestServer(t, Config{MaxPayloadBytes: 1024})
	_, err := srv.InvokeFunction(context.Background(), &pb.InvokeFunctionRequest{
		FunctionBytes: make([]byte, 2048),
		FunctionName:  "big",
	})
	if err == nil {
		t.Fatal("expected payload-too-large error")
	}
}

// Idempotency: two InvokeFunction calls with the same key return the same
// job_id; only one job is enqueued.
func TestServer_InvokeFunction_IdempotentKey(t *testing.T) {
	srv := newTestServer(t, Config{})

	r1, err := srv.InvokeFunction(context.Background(), &pb.InvokeFunctionRequest{
		FunctionBytes:  []byte("p"),
		FunctionName:   "fn",
		IdempotencyKey: "dup",
	})
	if err != nil {
		t.Fatalf("first Invoke: %v", err)
	}
	r2, err := srv.InvokeFunction(context.Background(), &pb.InvokeFunctionRequest{
		FunctionBytes:  []byte("p"),
		FunctionName:   "fn",
		IdempotencyKey: "dup",
	})
	if err != nil {
		t.Fatalf("second Invoke: %v", err)
	}
	if r1.JobId != r2.JobId {
		t.Errorf("duplicate-key invokes returned different job_ids: %s vs %s", r1.JobId, r2.JobId)
	}
	// Only the first enqueue should have happened; pending queue size 1.
	if got := len(srv.pendingQueue); got != 1 {
		t.Errorf("expected 1 pending job, got %d", got)
	}
}
