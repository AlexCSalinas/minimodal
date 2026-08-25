package main

import (
	"log/slog"
	"sync"

	pb "minimodal/orchestrator/pb"
)

// LogHub routes live job output from workers to watching SDK clients.
//
// Lifecycle of a job entry: created on first Append or Subscribe; log lines
// are buffered (bounded) so a watcher joining mid-run gets everything so far;
// Finish delivers the terminal event and the entry is deleted once the last
// subscriber detaches (or immediately if nobody is watching). Logs are
// live-only by design — they are never persisted to the WAL, so a watcher
// subscribing after the job is terminal gets the result (from BoltDB, in
// server.go) but no log replay.
type LogHub struct {
	mu       sync.Mutex
	jobs     map[string]*jobLogState
	maxLines int // per-job replay buffer cap; excess lines are counted, not kept
}

type jobLogState struct {
	lines    []*pb.LogLine
	dropped  int // lines discarded once over maxLines
	subs     map[*logSub]struct{}
	terminal *pb.JobEvent // non-nil once Finish was called
}

// logSub is one WatchJob stream's mailbox. The channel is buffered; a
// consumer that can't keep up loses lines (counted) rather than blocking the
// worker's log ingestion path.
type logSub struct {
	ch      chan *pb.JobEvent
	dropped int
}

const (
	defaultMaxBufferedLines = 10000
	subChannelBuffer        = 1024
	maxLineBytes            = 8192
)

func NewLogHub() *LogHub {
	return &LogHub{
		jobs:     make(map[string]*jobLogState),
		maxLines: defaultMaxBufferedLines,
	}
}

func (h *LogHub) get(jobID string) *jobLogState {
	st, ok := h.jobs[jobID]
	if !ok {
		st = &jobLogState{subs: make(map[*logSub]struct{})}
		h.jobs[jobID] = st
	}
	return st
}

// Append buffers lines for replay and fans them out to live subscribers.
// Lines arriving after Finish are ignored (the watcher stream has already
// been closed with the result event).
func (h *LogHub) Append(jobID string, lines []*pb.LogLine) {
	if len(lines) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.get(jobID)
	if st.terminal != nil {
		return
	}
	for _, line := range lines {
		if len(line.Line) > maxLineBytes {
			line.Line = line.Line[:maxLineBytes] + "…[truncated]"
		}
		if len(st.lines) >= h.maxLines {
			st.dropped++
		} else {
			st.lines = append(st.lines, line)
		}
		ev := &pb.JobEvent{JobId: jobID, Event: &pb.JobEvent_Log{Log: line}}
		for sub := range st.subs {
			select {
			case sub.ch <- ev:
			default:
				sub.dropped++
			}
		}
	}
	if st.dropped > 0 && st.dropped%1000 == 1 {
		slog.Warn("log buffer full; dropping lines", "job", jobID, "dropped", st.dropped)
	}
}

// Subscribe attaches a watcher. Returns the live mailbox, a replay of lines
// buffered so far, and the terminal event if the job already finished while
// its entry was still alive. Callers must Unsubscribe when done.
func (h *LogHub) Subscribe(jobID string) (sub *logSub, replay []*pb.JobEvent, terminal *pb.JobEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.get(jobID)
	sub = &logSub{ch: make(chan *pb.JobEvent, subChannelBuffer)}
	st.subs[sub] = struct{}{}
	replay = make([]*pb.JobEvent, 0, len(st.lines))
	for _, line := range st.lines {
		replay = append(replay, &pb.JobEvent{JobId: jobID, Event: &pb.JobEvent_Log{Log: line}})
	}
	return sub, replay, st.terminal
}

func (h *LogHub) Unsubscribe(jobID string, sub *logSub) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st, ok := h.jobs[jobID]
	if !ok {
		return
	}
	delete(st.subs, sub)
	if sub.dropped > 0 {
		slog.Warn("watcher fell behind; lines dropped from its stream",
			"job", jobID, "dropped", sub.dropped)
	}
	if st.terminal != nil && len(st.subs) == 0 {
		delete(h.jobs, jobID)
	}
}

// Finish records the terminal event and pushes it to every subscriber.
// Idempotent — only the first call per job wins. The entry is deleted
// immediately if nobody is watching; otherwise the last Unsubscribe cleans up.
func (h *LogHub) Finish(jobID string, result *pb.JobEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st, ok := h.jobs[jobID]
	if !ok {
		// Nobody watching and no logs — nothing to deliver, nothing to keep.
		return
	}
	if st.terminal != nil {
		return
	}
	st.terminal = result
	for sub := range st.subs {
		select {
		case sub.ch <- result:
		default:
			// Mailbox full: the WatchJob handler's safety-net store check
			// will still terminate the stream with the result.
			sub.dropped++
		}
	}
	if len(st.subs) == 0 {
		delete(h.jobs, jobID)
	}
}

// JobCount reports tracked job entries (for tests / metrics).
func (h *LogHub) JobCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.jobs)
}
