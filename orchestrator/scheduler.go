package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "minimodal/orchestrator/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Keepalive parameters for worker connections. Without these, a worker
// connection silently broken by a network partition or NAT timeout would
// hang the next ExecuteTask until the per-call gRPC timeout fired. With
// these, gRPC pings idle connections and proactively tears down half-open
// ones, so the cached *grpc.ClientConn never returns a "live" channel that
// is actually dead.
var workerKeepalive = keepalive.ClientParameters{
	Time:                10 * time.Second,
	Timeout:             3 * time.Second,
	PermitWithoutStream: true,
}

// Scheduler routes jobs to workers.
//
// Phase 4: Dispatch takes only a job_id, loads the payload from BoltDB. This
// lets recovery + reaper paths share the same dispatch logic with no
// special-casing — they all just push job_ids into the pending queue and
// the drainer calls Dispatch.

var (
	ErrNoWorkersAvailable  = errors.New("no alive workers available")
	ErrAllWorkersExhausted = errors.New("all alive workers refused or unreachable")
	ErrJobAlreadyTerminal  = errors.New("job already in terminal state")
)

type Scheduler struct {
	pool       *WorkerPool
	jobStore   Store
	rpcTimeout time.Duration

	mu       sync.Mutex
	channels map[string]*grpc.ClientConn // keyed by worker_id
}

func NewScheduler(pool *WorkerPool, jobStore Store) *Scheduler {
	return &Scheduler{
		pool:       pool,
		jobStore:   jobStore,
		rpcTimeout: 5 * time.Second,
		channels:   make(map[string]*grpc.ClientConn),
	}
}

// SetRPCTimeout overrides the default 5s ExecuteTask deadline.
func (s *Scheduler) SetRPCTimeout(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d > 0 {
		s.rpcTimeout = d
	}
}

// Dispatch sends a job to a worker. Loads the JobRecord from BoltDB, picks
// an alive worker, calls ExecuteTask. Transitions the job to RUNNING *before*
// the RPC so a fast worker that has already ReportTaskResult'd by the time
// we return won't get clobbered.
//
// Tries each alive worker at most once per call. Returns:
//   - nil:                       worker accepted, job is RUNNING
//   - ErrNoWorkersAvailable:     no alive workers — caller should requeue
//   - ErrAllWorkersExhausted:    every alive worker NACKed or was unreachable
//   - ErrJobAlreadyTerminal:     job is already DONE/FAILED — caller skips
//   - any other error:           wraps a BoltDB or unexpected error
func (s *Scheduler) Dispatch(ctx context.Context, jobID string) error {
	rec, err := s.jobStore.GetJob(jobID)
	if err != nil {
		return fmt.Errorf("load job %s: %w", jobID, err)
	}
	if rec == nil {
		return fmt.Errorf("job %s not found", jobID)
	}
	if rec.Status == StatusDone || rec.Status == StatusFailed {
		return ErrJobAlreadyTerminal
	}
	if len(rec.FunctionBytes) == 0 {
		// Recovery edge case: an older WAL entry with no payload. Mark FAILED
		// so the client's Future surfaces a clear error instead of timing out.
		_, _ = s.jobStore.Transition(jobID, func(r *JobRecord) error {
			r.Status = StatusFailed
			r.Error = "missing payload (pre-Phase-4 WAL entry)"
			return nil
		})
		return fmt.Errorf("job %s has no payload", jobID)
	}

	tried := map[string]bool{}
	for {
		workerID, address, ok := s.pool.LeastLoadedAliveExcluding(tried)
		if !ok {
			if len(tried) == 0 {
				return ErrNoWorkersAvailable
			}
			return ErrAllWorkersExhausted
		}
		tried[workerID] = true

		// Mark RUNNING up front (atomic CAS via Transition). If the mutator
		// returns ErrSkipTransition (job became terminal), `committed` is
		// false and we bail.
		committed, err := s.jobStore.Transition(jobID, func(r *JobRecord) error {
			if r.Status == StatusDone || r.Status == StatusFailed {
				return ErrSkipTransition
			}
			r.Status = StatusRunning
			r.WorkerID = workerID
			return nil
		})
		if err != nil {
			return fmt.Errorf("mark RUNNING for job %s: %w", jobID, err)
		}
		if !committed {
			return ErrJobAlreadyTerminal
		}

		if err := s.sendExecuteTask(ctx, workerID, address, rec); err == nil {
			return nil
		} else {
			slog.Warn("dispatch failed; trying another worker",
				"job", jobID, "worker", workerID, "err", err)
		}

		// Worker NACK'd or unreachable — revert to PENDING for the next attempt
		// only if the job hasn't gone terminal in the meantime. This counts as
		// a retry: a worker that dies between "mark RUNNING" and ExecuteTask
		// returning is the same failure the reaper handles a moment later, and
		// RetryCount is the only record a client has of it.
		_, _ = s.jobStore.Transition(jobID, func(r *JobRecord) error {
			if r.Status == StatusDone || r.Status == StatusFailed {
				return ErrSkipTransition
			}
			r.Status = StatusPending
			r.WorkerID = ""
			r.RetryCount++
			return nil
		})
	}
}

func (s *Scheduler) sendExecuteTask(ctx context.Context, workerID, address string, rec *JobRecord) error {
	conn, err := s.getOrDial(workerID, address)
	if err != nil {
		return fmt.Errorf("dial %s: %w", address, err)
	}
	client := pb.NewWorkerClient(conn)
	rpcCtx, cancel := context.WithTimeout(ctx, s.rpcTimeout)
	defer cancel()
	resp, err := client.ExecuteTask(rpcCtx, &pb.ExecuteTaskRequest{
		JobId:         rec.ID,
		FunctionBytes: rec.FunctionBytes,
		ArgsBytes:     rec.ArgsBytes,
		FunctionName:  rec.FunctionName,
	})
	if err != nil {
		s.dropChannel(workerID)
		return err
	}
	if !resp.Accepted {
		return fmt.Errorf("worker rejected: %s", resp.RejectionReason)
	}
	return nil
}

func (s *Scheduler) getOrDial(workerID, address string) (*grpc.ClientConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channels == nil {
		// Close() ran while a dispatch was in flight (shutdown, or test
		// teardown). Fail the dial instead of panicking on the nil map —
		// the caller treats it like an unreachable worker.
		return nil, errors.New("scheduler is closed")
	}
	if conn, ok := s.channels[workerID]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(workerKeepalive),
	)
	if err != nil {
		return nil, err
	}
	s.channels[workerID] = conn
	return conn, nil
}

func (s *Scheduler) dropChannel(workerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if conn, ok := s.channels[workerID]; ok {
		_ = conn.Close()
		delete(s.channels, workerID)
	}
}

func (s *Scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.channels {
		_ = conn.Close()
	}
	s.channels = nil
}
