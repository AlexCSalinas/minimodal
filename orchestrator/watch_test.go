package main

import (
	"context"
	"net"
	"testing"
	"time"

	pb "minimodal/orchestrator/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// startOrchGRPC boots the real gRPC server around a Server so streaming RPCs
// are exercised over an actual connection, not handler mocks.
func startOrchGRPC(t *testing.T, srv *Server) pb.OrchestratorClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterOrchestratorServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewOrchestratorClient(conn)
}

func invokeJob(t *testing.T, srv *Server) string {
	t.Helper()
	resp, err := srv.InvokeFunction(context.Background(), &pb.InvokeFunctionRequest{
		FunctionBytes: []byte("payload"),
		FunctionName:  "fn",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	return resp.JobId
}

func recvEvent(t *testing.T, w pb.Orchestrator_WatchJobClient) *pb.JobEvent {
	t.Helper()
	ev, err := w.Recv()
	if err != nil {
		t.Fatalf("watch Recv: %v", err)
	}
	return ev
}

func shortenWatchPoll(t *testing.T, d time.Duration) {
	t.Helper()
	old := watchStatusPollInterval
	watchStatusPollInterval = d
	t.Cleanup(func() { watchStatusPollInterval = old })
}

// The marquee flow: watcher sees initial status, live-streamed lines, the
// completion-time tail lines, and finally the pushed result — in order, and
// the stream ends.
func TestWatchJob_FullFlow_LiveLogsTailAndResult(t *testing.T) {
	srv := newTestServer(t, Config{})
	client := startOrchGRPC(t, srv)
	jobID := invokeJob(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	watch, err := client.WatchJob(ctx, &pb.WatchJobRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("WatchJob: %v", err)
	}

	if ev := recvEvent(t, watch); ev.GetStatus() != pb.JobStatus_JOB_STATUS_PENDING {
		t.Fatalf("first event should be PENDING status, got %v", ev)
	}

	// Worker side: stream two live lines.
	logs, err := client.StreamTaskLogs(ctx)
	if err != nil {
		t.Fatalf("StreamTaskLogs: %v", err)
	}
	err = logs.Send(&pb.TaskLogChunk{JobId: jobID, Lines: []*pb.LogLine{
		{Line: "hello"}, {Line: "world", IsStderr: true},
	}})
	if err != nil {
		t.Fatalf("log send: %v", err)
	}

	if got := recvEvent(t, watch).GetLog(); got.GetLine() != "hello" || got.GetIsStderr() {
		t.Errorf("want live stdout line hello, got %v", got)
	}
	if got := recvEvent(t, watch).GetLog(); got.GetLine() != "world" || !got.GetIsStderr() {
		t.Errorf("want live stderr line world, got %v", got)
	}

	if _, err := logs.CloseAndRecv(); err != nil {
		t.Fatalf("log stream close: %v", err)
	}

	// Worker reports completion with a tail line (buffered-only output).
	_, err = srv.ReportTaskResult(context.Background(), &pb.ReportTaskResultRequest{
		JobId:       jobID,
		WorkerId:    "w1",
		Success:     true,
		ResultBytes: []byte("result!"),
		ColdStartMs: 3,
		ExecutionMs: 7,
		TailLines:   []*pb.LogLine{{Line: "tail-line"}},
	})
	if err != nil {
		t.Fatalf("ReportTaskResult: %v", err)
	}

	if got := recvEvent(t, watch).GetLog().GetLine(); got != "tail-line" {
		t.Errorf("want tail line before result, got %q", got)
	}
	res := recvEvent(t, watch).GetResult()
	if res == nil {
		t.Fatal("want result event")
	}
	if res.Status != pb.JobStatus_JOB_STATUS_DONE || string(res.ResultBytes) != "result!" {
		t.Errorf("bad result event: %v", res)
	}
	if res.ColdStartMs != 3 || res.TotalDurationMs != 10 {
		t.Errorf("bad timing on result event: cold=%d total=%d", res.ColdStartMs, res.TotalDurationMs)
	}
	if _, err := watch.Recv(); err == nil {
		t.Error("stream must end after the result event")
	}
}

// A watcher joining mid-run must get everything printed so far (replay).
func TestWatchJob_MidRunSubscriberGetsReplay(t *testing.T) {
	srv := newTestServer(t, Config{})
	client := startOrchGRPC(t, srv)
	jobID := invokeJob(t, srv)

	srv.logHub.Append(jobID, []*pb.LogLine{{Line: "before-attach"}})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watch, _ := client.WatchJob(ctx, &pb.WatchJobRequest{JobId: jobID})
	recvEvent(t, watch) // status
	if got := recvEvent(t, watch).GetLog().GetLine(); got != "before-attach" {
		t.Errorf("want replayed line, got %q", got)
	}
}

// A watcher attaching after the job completed gets the result immediately
// from BoltDB — no logs (live-only), no hang.
func TestWatchJob_LateWatcherGetsResultImmediately(t *testing.T) {
	srv := newTestServer(t, Config{})
	client := startOrchGRPC(t, srv)
	jobID := invokeJob(t, srv)
	_, err := srv.ReportTaskResult(context.Background(), &pb.ReportTaskResultRequest{
		JobId: jobID, Success: true, ResultBytes: []byte("r"),
	})
	if err != nil {
		t.Fatalf("ReportTaskResult: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watch, _ := client.WatchJob(ctx, &pb.WatchJobRequest{JobId: jobID})
	for {
		ev := recvEvent(t, watch)
		if res := ev.GetResult(); res != nil {
			if string(res.ResultBytes) != "r" {
				t.Errorf("bad result bytes: %q", res.ResultBytes)
			}
			return
		}
		if ev.GetLog() != nil {
			t.Errorf("late watcher must not receive logs, got %v", ev)
		}
	}
}

func TestWatchJob_UnknownJobIsNotFound(t *testing.T) {
	srv := newTestServer(t, Config{})
	client := startOrchGRPC(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watch, _ := client.WatchJob(ctx, &pb.WatchJobRequest{JobId: "ghost"})
	_, err := watch.Recv()
	if status.Code(err) != codes.NotFound {
		t.Errorf("want NotFound, got %v", err)
	}
}

// Terminal transitions that bypass notifyTerminal (e.g. the scheduler's
// missing-payload path writes FAILED directly) must still end the stream via
// the store safety-net poll.
func TestWatchJob_SafetyNetCatchesUnnotifiedTerminal(t *testing.T) {
	shortenWatchPoll(t, 20*time.Millisecond)
	srv := newTestServer(t, Config{})
	client := startOrchGRPC(t, srv)
	jobID := invokeJob(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watch, _ := client.WatchJob(ctx, &pb.WatchJobRequest{JobId: jobID})
	recvEvent(t, watch) // PENDING

	_, _ = srv.jobStore.Transition(jobID, func(r *JobRecord) error {
		r.Status = StatusFailed
		r.Error = "terminal without notify"
		return nil
	})

	res := recvEvent(t, watch).GetResult()
	if res == nil || res.Status != pb.JobStatus_JOB_STATUS_FAILED {
		t.Fatalf("want FAILED result via safety net, got %v", res)
	}
	if res.Error != "terminal without notify" {
		t.Errorf("bad error: %q", res.Error)
	}
}

// The poll loop also surfaces intermediate status transitions (PENDING →
// RUNNING) that have no push hook.
func TestWatchJob_SamplesStatusTransitions(t *testing.T) {
	shortenWatchPoll(t, 20*time.Millisecond)
	srv := newTestServer(t, Config{})
	client := startOrchGRPC(t, srv)
	jobID := invokeJob(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watch, _ := client.WatchJob(ctx, &pb.WatchJobRequest{JobId: jobID})
	recvEvent(t, watch) // PENDING

	_, _ = srv.jobStore.Transition(jobID, func(r *JobRecord) error {
		r.Status = StatusRunning
		r.WorkerID = "w1"
		return nil
	})
	if ev := recvEvent(t, watch); ev.GetStatus() != pb.JobStatus_JOB_STATUS_RUNNING {
		t.Fatalf("want RUNNING status event, got %v", ev)
	}
}

// Chunks for unknown or already-terminal jobs must not leak LogHub entries.
func TestStreamTaskLogs_DropsUnknownAndTerminalJobs(t *testing.T) {
	srv := newTestServer(t, Config{})
	client := startOrchGRPC(t, srv)

	doneJob := invokeJob(t, srv)
	_, _ = srv.ReportTaskResult(context.Background(), &pb.ReportTaskResultRequest{
		JobId: doneJob, Success: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	logs, err := client.StreamTaskLogs(ctx)
	if err != nil {
		t.Fatalf("StreamTaskLogs: %v", err)
	}
	_ = logs.Send(&pb.TaskLogChunk{JobId: "ghost", Lines: []*pb.LogLine{{Line: "x"}}})
	_ = logs.Send(&pb.TaskLogChunk{JobId: doneJob, Lines: []*pb.LogLine{{Line: "y"}}})
	resp, err := logs.CloseAndRecv()
	if err != nil || !resp.Acknowledged {
		t.Fatalf("close: %v %v", resp, err)
	}
	if got := srv.logHub.JobCount(); got != 0 {
		t.Errorf("garbage chunks must not create hub entries, got %d", got)
	}
}
