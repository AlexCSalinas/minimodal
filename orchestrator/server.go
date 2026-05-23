package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	pb "minimodal/orchestrator/pb"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type resultEntry struct {
	resultBytes []byte
	errorMsg    string
	coldStartMs int64
	executionMs int64
}

// Server implements pb.OrchestratorServer plus the Reaper interface (called
// by WorkerPool when a worker dies).
type Server struct {
	pb.UnimplementedOrchestratorServer
	cfg        Config
	workerPool *WorkerPool
	scheduler  *Scheduler
	jobStore   *JobStore
	metrics    *Metrics

	// Async dispatch: InvokeFunction + WAL replay + reaper all push job_ids
	// here. RunPendingQueueLoop drains it. A large buffered channel + a
	// single drainer goroutine is fine for v1 throughput; could promote to
	// per-worker deques with stealing for higher throughput.
	pendingQueue chan string

	// Bounded LRU of completed-job results; cache miss falls back to BoltDB
	// (which is the source of truth — important for idempotency after
	// orchestrator restart).
	results *lruResults
}

const (
	pendingQueueSize = 10000
	resultsCacheSize = 10000
)

func NewServer(cfg Config) (*Server, error) {
	store, err := NewJobStore(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	pool := NewWorkerPool(cfg.WorkerTimeout)
	sched := NewScheduler(pool, store)
	sched.SetRPCTimeout(cfg.ExecuteTaskTimeout)
	srv := &Server{
		cfg:          cfg,
		workerPool:   pool,
		scheduler:    sched,
		jobStore:     store,
		metrics:      NewMetrics(),
		pendingQueue: make(chan string, pendingQueueSize),
		results:      newLRUResults(resultsCacheSize),
	}
	// WorkerPool calls back into Server when workers die.
	pool.SetReaper(srv)
	return srv, nil
}

func (s *Server) Close() {
	if s.scheduler != nil {
		s.scheduler.Close()
	}
	if s.jobStore != nil {
		_ = s.jobStore.Close()
	}
}

// =============================================================================
// Startup recovery
// =============================================================================

// RecoverUnfinishedJobs is called once at startup before the gRPC server
// accepts requests. It scans the WAL for any non-terminal jobs (PENDING or
// RUNNING from a previous orchestrator lifetime), demotes RUNNING to PENDING
// (their workers are necessarily gone), and pushes them into the pending
// queue. The drainer goroutine will dispatch them as workers register.
func (s *Server) RecoverUnfinishedJobs() error {
	jobs, err := s.jobStore.ListUnfinished()
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return nil
	}
	for _, j := range jobs {
		if _, err := s.jobStore.Transition(j.ID, func(r *JobRecord) error {
			if r.Status == StatusRunning {
				r.Status = StatusPending
				r.WorkerID = ""
			}
			return nil
		}); err != nil {
			log.Printf("warn: recover transition %s: %v", j.ID, err)
			continue
		}
		s.enqueue(j.ID)
	}
	log.Printf("WAL replay: recovered %d unfinished job(s)", len(jobs))
	return nil
}

// =============================================================================
// Pending queue drainer
// =============================================================================
func (s *Server) RunPendingQueueLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case jobID := <-s.pendingQueue:
			s.attemptDispatch(ctx, jobID)
		}
	}
}

func (s *Server) attemptDispatch(ctx context.Context, jobID string) {
	backoff := 100 * time.Millisecond
	const maxBackoff = 2 * time.Second
	maxAttempts := s.cfg.MaxDispatchAttempts
	if maxAttempts < 1 {
		maxAttempts = 30
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := s.scheduler.Dispatch(ctx, jobID)
		if err == nil {
			return
		}
		if errors.Is(err, ErrJobAlreadyTerminal) {
			return
		}
		if errors.Is(err, ErrNoWorkersAvailable) || errors.Is(err, ErrAllWorkersExhausted) {
			// Workers are absent or busy — wait and retry, up to maxAttempts.
			if attempt == maxAttempts {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}
		// Unexpected — mark FAILED and move on.
		log.Printf("dispatch job=%s permanent error: %v", jobID, err)
		_, _ = s.jobStore.Transition(jobID, func(r *JobRecord) error {
			if r.Status == StatusDone || r.Status == StatusFailed {
				return ErrSkipTransition
			}
			r.Status = StatusFailed
			r.Error = "dispatch failed: " + err.Error()
			return nil
		})
		return
	}

	// Exhausted all dispatch attempts (no workers ever became available).
	log.Printf("dispatch job=%s exhausted %d attempts — marking FAILED", jobID, maxAttempts)
	_, _ = s.jobStore.Transition(jobID, func(r *JobRecord) error {
		if r.Status == StatusDone || r.Status == StatusFailed {
			return ErrSkipTransition
		}
		r.Status = StatusFailed
		r.Error = fmt.Sprintf("dispatch failed: no worker accepted after %d attempts", maxAttempts)
		return nil
	})
	s.metrics.RecordFailed()
}

func (s *Server) enqueue(jobID string) {
	select {
	case s.pendingQueue <- jobID:
	default:
		// Queue full — block. Better to apply backpressure than drop jobs.
		s.pendingQueue <- jobID
	}
}

// =============================================================================
// Reaper interface (called by WorkerPool when a worker dies)
// =============================================================================
func (s *Server) ReassignJobsOf(workerID string) {
	jobs, err := s.jobStore.ListRunningOnWorker(workerID)
	if err != nil {
		log.Printf("reaper: list running for %s: %v", workerID, err)
		return
	}
	for _, j := range jobs {
		// Track action via closure-captured locals. Only act on them when
		// the BoltDB write actually committed — otherwise our local view
		// could disagree with on-disk reality after a transaction error.
		var (
			markedPending bool
			markedFailed  bool
		)
		committed, err := s.jobStore.Transition(j.ID, func(r *JobRecord) error {
			if r.Status != StatusRunning {
				// Already moved by ReportTaskResult or another reaper sweep.
				return ErrSkipTransition
			}
			r.RetryCount++
			if r.RetryCount > s.cfg.MaxRetries {
				r.Status = StatusFailed
				r.Error = "exceeded max retries after worker death(s)"
				markedFailed = true
				return nil
			}
			r.Status = StatusPending
			r.WorkerID = ""
			markedPending = true
			return nil
		})
		if err != nil {
			log.Printf("reaper: transition %s: %v", j.ID, err)
			continue
		}
		if !committed {
			// Lost the race to ReportTaskResult — job is already terminal.
			continue
		}
		if markedPending {
			log.Printf("reaper: requeue job=%s (retry %d/%d) after worker=%s death",
				j.ID, j.RetryCount+1, s.cfg.MaxRetries, workerID)
			s.enqueue(j.ID)
		} else if markedFailed {
			log.Printf("reaper: job=%s exceeded max retries — marked FAILED", j.ID)
		}
	}
}

// =============================================================================
// Worker-facing RPCs
// =============================================================================
func (s *Server) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	if req.WorkerId == "" || req.Address == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id and address required")
	}
	if req.Capacity <= 0 {
		req.Capacity = 1
	}
	s.workerPool.Register(req.WorkerId, req.Address, int(req.Capacity))
	log.Printf("registered worker id=%s addr=%s capacity=%d", req.WorkerId, req.Address, req.Capacity)
	return &pb.RegisterWorkerResponse{
		WorkerId:            req.WorkerId,
		HeartbeatIntervalMs: int32(s.cfg.HeartbeatInterval.Milliseconds()),
	}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id required")
	}
	known := s.workerPool.RecordHeartbeat(req.WorkerId, int(req.ActiveTasks))
	if !known {
		return &pb.HeartbeatResponse{ShouldDrain: true}, nil
	}
	return &pb.HeartbeatResponse{ShouldDrain: false}, nil
}

func (s *Server) ReportTaskResult(ctx context.Context, req *pb.ReportTaskResultRequest) (*pb.ReportTaskResultResponse, error) {
	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id required")
	}

	s.results.Put(req.JobId, resultEntry{
		resultBytes: req.ResultBytes,
		errorMsg:    req.Error,
		coldStartMs: req.ColdStartMs,
		executionMs: req.ExecutionMs,
	})

	_, err := s.jobStore.Transition(req.JobId, func(rec *JobRecord) error {
		if rec.Status == StatusDone || rec.Status == StatusFailed {
			return ErrSkipTransition
		}
		if req.Success {
			rec.Status = StatusDone
			rec.Error = ""
		} else {
			rec.Status = StatusFailed
			rec.Error = req.Error
		}
		// Free payload bytes once terminal — bounds WAL growth — but
		// persist result_bytes so GetJobStatus survives orchestrator restart
		// (important for idempotency: a retry with the same key after
		// restart should return the original result, not None).
		rec.FunctionBytes = nil
		rec.ArgsBytes = nil
		rec.ResultBytes = req.ResultBytes
		rec.ColdStartMs = req.ColdStartMs
		rec.ExecutionMs = req.ExecutionMs
		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "transition job: %v", err)
	}

	s.metrics.RecordColdStart(req.ColdStartMs)
	if req.Success {
		s.metrics.RecordCompleted()
	} else {
		s.metrics.RecordFailed()
	}

	log.Printf("task done job=%s worker=%s success=%v cold_start_ms=%d exec_ms=%d",
		req.JobId, req.WorkerId, req.Success, req.ColdStartMs, req.ExecutionMs)
	return &pb.ReportTaskResultResponse{Acknowledged: true}, nil
}

// =============================================================================
// SDK-facing RPCs
// =============================================================================
func (s *Server) InvokeFunction(ctx context.Context, req *pb.InvokeFunctionRequest) (*pb.InvokeFunctionResponse, error) {
	if len(req.FunctionBytes) == 0 {
		return nil, status.Error(codes.InvalidArgument, "function_bytes required")
	}

	rec := JobRecord{
		ID:            uuid.NewString(),
		Status:        StatusPending,
		FunctionName:  req.FunctionName,
		FunctionBytes: req.FunctionBytes,
		ArgsBytes:     req.ArgsBytes,
		CreatedAt:     time.Now(),
	}
	jobID, created, err := s.jobStore.SubmitWithIdempotency(rec, req.IdempotencyKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "persist job: %v", err)
	}

	if created {
		s.enqueue(jobID)
		s.metrics.RecordInvocation()
		log.Printf("invoked job=%s function=%s bytes=%d key=%q",
			jobID, req.FunctionName, len(req.FunctionBytes), req.IdempotencyKey)
	} else {
		log.Printf("idempotent: returning existing job=%s for key=%q", jobID, req.IdempotencyKey)
	}
	return &pb.InvokeFunctionResponse{JobId: jobID}, nil
}

// MetricsSnapshot composes the public /metrics payload from Metrics +
// WorkerPool + queue length. Called from http.go's handleMetrics.
func (s *Server) MetricsSnapshot() MetricsSnapshot {
	p50, p95, p99, mean, samples := s.metrics.snapshotPercentiles()
	workers := s.workerPool.Snapshot()
	alive := 0
	for _, w := range workers {
		if w.Alive {
			alive++
		}
	}
	total := s.metrics.totalInvocations.Load()
	failed := s.metrics.totalFailed.Load()
	completed := s.metrics.totalCompleted.Load()
	var errRate float64
	if completed+failed > 0 {
		errRate = float64(failed) / float64(completed+failed)
	}
	return MetricsSnapshot{
		UptimeS:          int64(time.Since(s.metrics.startedAt).Seconds()),
		ActiveWorkers:    alive,
		TotalWorkers:     len(workers),
		QueuedJobs:       len(s.pendingQueue),
		TotalInvocations: total,
		TotalCompleted:   completed,
		TotalFailed:      failed,
		InFlightJobs:     total - completed - failed,
		ErrorRate:        errRate,
		ColdStartSamples: samples,
		P50ColdStartMs:   p50,
		P95ColdStartMs:   p95,
		P99ColdStartMs:   p99,
		MeanColdStartMs:  mean,
		Workers:          workers,
	}
}

func (s *Server) GetJobStatus(ctx context.Context, req *pb.GetJobStatusRequest) (*pb.JobStatusResponse, error) {
	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id required")
	}
	rec, err := s.jobStore.GetJob(req.JobId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load job: %v", err)
	}
	if rec == nil {
		return nil, status.Errorf(codes.NotFound, "job %s not found", req.JobId)
	}

	resp := &pb.JobStatusResponse{
		JobId:           rec.ID,
		Status:          mapStatus(rec.Status),
		Error:           rec.Error,
		RetryCount:      int32(rec.RetryCount),
		WorkerId:        rec.WorkerID,
		TotalDurationMs: int64(time.Since(rec.CreatedAt) / time.Millisecond),
	}

	if rec.Status == StatusDone || rec.Status == StatusFailed {
		// Prefer the in-memory cache (most-recent result for currently-live
		// orchestrator). Fall back to BoltDB (survives restart).
		entry, hit := s.results.Get(rec.ID)
		if hit {
			resp.ResultBytes = entry.resultBytes
			resp.ColdStartMs = entry.coldStartMs
			if entry.errorMsg != "" && resp.Error == "" {
				resp.Error = entry.errorMsg
			}
			resp.TotalDurationMs = entry.executionMs + entry.coldStartMs
		} else if len(rec.ResultBytes) > 0 || rec.ColdStartMs > 0 || rec.ExecutionMs > 0 {
			resp.ResultBytes = rec.ResultBytes
			resp.ColdStartMs = rec.ColdStartMs
			resp.TotalDurationMs = rec.ColdStartMs + rec.ExecutionMs
		}
	}
	return resp, nil
}

func mapStatus(s JobStatus) pb.JobStatus {
	switch s {
	case StatusPending:
		return pb.JobStatus_JOB_STATUS_PENDING
	case StatusRunning:
		return pb.JobStatus_JOB_STATUS_RUNNING
	case StatusDone:
		return pb.JobStatus_JOB_STATUS_DONE
	case StatusFailed:
		return pb.JobStatus_JOB_STATUS_FAILED
	default:
		return pb.JobStatus_JOB_STATUS_UNKNOWN
	}
}
