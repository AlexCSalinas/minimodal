package main

import (
	"io"
	"time"

	pb "minimodal/orchestrator/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// How often WatchJob samples the job store. This is (a) the status-transition
// sampler — PENDING→RUNNING has no push hook because the scheduler doesn't
// know about the LogHub — and (b) a safety net that terminates the stream for
// terminal transitions that happen outside server.go (e.g. the scheduler's
// missing-payload path). A var so tests can shorten it.
var watchStatusPollInterval = 500 * time.Millisecond

// WatchJob streams a job's lifecycle to an SDK client: an initial status
// snapshot, a replay of any log lines already captured, then live logs and
// status transitions, and finally the terminal result event, which always
// ends the stream.
func (s *Server) WatchJob(req *pb.WatchJobRequest, stream pb.Orchestrator_WatchJobServer) error {
	if req.JobId == "" {
		return status.Error(codes.InvalidArgument, "job_id required")
	}
	rec, err := s.jobStore.GetJob(req.JobId)
	if err != nil {
		return status.Errorf(codes.Internal, "load job: %v", err)
	}
	if rec == nil {
		return status.Errorf(codes.NotFound, "job %s not found", req.JobId)
	}

	sub, replay, terminal := s.logHub.Subscribe(req.JobId)
	defer s.logHub.Unsubscribe(req.JobId, sub)

	lastStatus := rec.Status
	if err := stream.Send(statusEvent(req.JobId, lastStatus)); err != nil {
		return err
	}
	for _, ev := range replay {
		if err := stream.Send(ev); err != nil {
			return err
		}
	}
	if terminal != nil {
		return stream.Send(terminal)
	}

	// Re-check the store before settling into the event loop: the job may
	// have gone terminal between GetJob and Subscribe (Finish with no
	// subscribers deletes the hub entry, so our subscription would wait
	// forever).
	ticker := time.NewTicker(watchStatusPollInterval)
	defer ticker.Stop()
	for {
		rec, err := s.jobStore.GetJob(req.JobId)
		if err == nil && rec != nil {
			if rec.Status == StatusDone || rec.Status == StatusFailed {
				// Deliver any mailbox backlog first so logs precede the
				// result even on this fallback path.
				for drained := false; !drained; {
					select {
					case ev := <-sub.ch:
						if err := stream.Send(ev); err != nil {
							return err
						}
						if ev.GetResult() != nil {
							return nil
						}
					default:
						drained = true
					}
				}
				return stream.Send(s.jobResultEvent(rec))
			}
			if rec.Status != lastStatus {
				lastStatus = rec.Status
				if err := stream.Send(statusEvent(req.JobId, lastStatus)); err != nil {
					return err
				}
			}
		}

		select {
		case <-stream.Context().Done():
			return nil
		case ev := <-sub.ch:
			if err := stream.Send(ev); err != nil {
				return err
			}
			if ev.GetResult() != nil {
				return nil
			}
		case <-ticker.C:
		}
	}
}

// StreamTaskLogs ingests live user-function output from a worker and fans it
// out to watchers. Chunks for unknown or already-terminal jobs are dropped —
// this keeps garbage or straggler chunks from creating LogHub entries that
// nothing would ever clean up.
func (s *Server) StreamTaskLogs(stream pb.Orchestrator_StreamTaskLogsServer) error {
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.StreamTaskLogsResponse{Acknowledged: true})
		}
		if err != nil {
			return err
		}
		if chunk.JobId == "" || len(chunk.Lines) == 0 {
			continue
		}
		rec, err := s.jobStore.GetJob(chunk.JobId)
		if err != nil || rec == nil || rec.Status == StatusDone || rec.Status == StatusFailed {
			continue
		}
		s.logHub.Append(chunk.JobId, chunk.Lines)
	}
}

// notifyTerminal pushes the terminal event for a job to any watchers. Called
// after every transition in server.go that may have made the job terminal.
func (s *Server) notifyTerminal(jobID string) {
	rec, err := s.jobStore.GetJob(jobID)
	if err != nil || rec == nil {
		return
	}
	if rec.Status != StatusDone && rec.Status != StatusFailed {
		return
	}
	s.logHub.Finish(jobID, s.jobResultEvent(rec))
}

func statusEvent(jobID string, st JobStatus) *pb.JobEvent {
	return &pb.JobEvent{
		JobId: jobID,
		Event: &pb.JobEvent_Status{Status: mapStatus(st)},
	}
}

func (s *Server) jobResultEvent(rec *JobRecord) *pb.JobEvent {
	return &pb.JobEvent{
		JobId: rec.ID,
		Event: &pb.JobEvent_Result{Result: &pb.JobResult{
			Status:          mapStatus(rec.Status),
			ResultBytes:     rec.ResultBytes,
			Error:           rec.Error,
			ColdStartMs:     rec.ColdStartMs,
			TotalDurationMs: rec.ColdStartMs + rec.ExecutionMs,
			RetryCount:      int32(rec.RetryCount),
			WorkerId:        rec.WorkerID,
		}},
	}
}
