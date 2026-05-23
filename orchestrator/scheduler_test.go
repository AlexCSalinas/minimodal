package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pb "minimodal/orchestrator/pb"

	"google.golang.org/grpc"
)

func newTestScheduler(t *testing.T) (*Scheduler, *WorkerPool, *JobStore) {
	t.Helper()
	store := newTestStore(t)
	pool := NewWorkerPool(time.Second)
	s := NewScheduler(pool, store)
	t.Cleanup(s.Close)
	return s, pool, store
}

func TestScheduler_Dispatch_JobNotFound(t *testing.T) {
	s, _, _ := newTestScheduler(t)
	err := s.Dispatch(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error for missing job")
	}
}

func TestScheduler_Dispatch_TerminalJobReturnsSentinel(t *testing.T) {
	s, _, store := newTestScheduler(t)
	for _, status := range []JobStatus{StatusDone, StatusFailed} {
		_ = store.PutJob(JobRecord{ID: "j-" + string(status), Status: status, FunctionBytes: []byte("payload")})
		err := s.Dispatch(context.Background(), "j-"+string(status))
		if !errors.Is(err, ErrJobAlreadyTerminal) {
			t.Errorf("status=%s: want ErrJobAlreadyTerminal, got %v", status, err)
		}
	}
}

func TestScheduler_Dispatch_MissingPayloadMarksFailed(t *testing.T) {
	s, _, store := newTestScheduler(t)
	_ = store.PutJob(JobRecord{ID: "j", Status: StatusPending}) // no FunctionBytes
	err := s.Dispatch(context.Background(), "j")
	if err == nil {
		t.Fatal("expected error for missing payload")
	}
	got, _ := store.GetJob("j")
	if got.Status != StatusFailed {
		t.Errorf("expected FAILED transition, got %s", got.Status)
	}
	if got.Error == "" {
		t.Error("expected error message on FAILED job")
	}
}

func TestScheduler_Dispatch_NoWorkersAvailable(t *testing.T) {
	s, _, store := newTestScheduler(t)
	_ = store.PutJob(JobRecord{ID: "j", Status: StatusPending, FunctionBytes: []byte("payload")})
	err := s.Dispatch(context.Background(), "j")
	if !errors.Is(err, ErrNoWorkersAvailable) {
		t.Errorf("want ErrNoWorkersAvailable, got %v", err)
	}
	// Job should still be PENDING — dispatch failure does not mutate state.
	got, _ := store.GetJob("j")
	if got.Status != StatusPending {
		t.Errorf("job should remain PENDING, got %s", got.Status)
	}
}

func TestScheduler_Dispatch_HappyPath_HitsWorker(t *testing.T) {
	s, pool, store := newTestScheduler(t)
	addr, calls, stop := startFakeWorker(t, true, "")
	defer stop()

	pool.Register("w1", addr, 4)
	_ = store.PutJob(JobRecord{
		ID:            "j",
		Status:        StatusPending,
		FunctionBytes: []byte("payload"),
		ArgsBytes:     []byte("args"),
		FunctionName:  "fn",
	})

	if err := s.Dispatch(context.Background(), "j"); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	select {
	case req := <-calls:
		if req.JobId != "j" || req.FunctionName != "fn" {
			t.Errorf("worker got wrong request: %+v", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker never received ExecuteTask")
	}

	got, _ := store.GetJob("j")
	if got.Status != StatusRunning {
		t.Errorf("job should be RUNNING after dispatch, got %s", got.Status)
	}
	if got.WorkerID != "w1" {
		t.Errorf("worker_id should be w1, got %s", got.WorkerID)
	}
}

func TestScheduler_Dispatch_WorkerRejectionRevertsToPending(t *testing.T) {
	s, pool, store := newTestScheduler(t)
	addr, _, stop := startFakeWorker(t, false, "at capacity")
	defer stop()

	pool.Register("only", addr, 4)
	_ = store.PutJob(JobRecord{ID: "j", Status: StatusPending, FunctionBytes: []byte("p")})

	err := s.Dispatch(context.Background(), "j")
	if !errors.Is(err, ErrAllWorkersExhausted) {
		t.Errorf("want ErrAllWorkersExhausted, got %v", err)
	}
	got, _ := store.GetJob("j")
	if got.Status != StatusPending {
		t.Errorf("rejected dispatch must revert to PENDING, got %s", got.Status)
	}
	if got.WorkerID != "" {
		t.Errorf("worker_id should be cleared on revert, got %q", got.WorkerID)
	}
}

func TestScheduler_GetOrDial_CachesChannel(t *testing.T) {
	s, _, _ := newTestScheduler(t)
	c1, err := s.getOrDial("w1", "127.0.0.1:65535")
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	c2, err := s.getOrDial("w1", "127.0.0.1:65535")
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}
	if c1 != c2 {
		t.Error("getOrDial should reuse the cached connection for the same workerID")
	}
}

func TestScheduler_DropChannel_RemovesCachedConn(t *testing.T) {
	s, _, _ := newTestScheduler(t)
	c1, _ := s.getOrDial("w1", "127.0.0.1:65535")
	s.dropChannel("w1")
	c2, _ := s.getOrDial("w1", "127.0.0.1:65535")
	if c1 == c2 {
		t.Error("dropChannel should force a fresh dial on next getOrDial")
	}
}

// ---------------------------------------------------------------------------
// Fake worker over real gRPC
// ---------------------------------------------------------------------------

type fakeWorkerServer struct {
	pb.UnimplementedWorkerServer
	mu        sync.Mutex
	calls     chan *pb.ExecuteTaskRequest
	accept    bool
	rejectMsg string
}

func (f *fakeWorkerServer) ExecuteTask(_ context.Context, req *pb.ExecuteTaskRequest) (*pb.ExecuteTaskResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case f.calls <- req:
	default:
	}
	return &pb.ExecuteTaskResponse{Accepted: f.accept, RejectionReason: f.rejectMsg}, nil
}

// startFakeWorker boots a gRPC server on a random local port and returns the
// address, a channel that receives every ExecuteTask request, and a teardown.
func startFakeWorker(t *testing.T, accept bool, reject string) (addr string, calls chan *pb.ExecuteTaskRequest, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	calls = make(chan *pb.ExecuteTaskRequest, 8)
	srv := grpc.NewServer()
	pb.RegisterWorkerServer(srv, &fakeWorkerServer{calls: calls, accept: accept, rejectMsg: reject})
	go func() { _ = srv.Serve(lis) }()
	return lis.Addr().String(), calls, func() { srv.GracefulStop() }
}
