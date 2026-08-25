package main

import (
	"fmt"
	"testing"

	pb "minimodal/orchestrator/pb"
)

func line(s string) *pb.LogLine { return &pb.LogLine{Line: s} }

func resultEvent(jobID string) *pb.JobEvent {
	return &pb.JobEvent{JobId: jobID, Event: &pb.JobEvent_Result{Result: &pb.JobResult{}}}
}

func TestLogHub_SubscriberReceivesLiveLines(t *testing.T) {
	h := NewLogHub()
	sub, replay, terminal := h.Subscribe("j")
	if len(replay) != 0 || terminal != nil {
		t.Fatalf("fresh job: want empty replay and no terminal, got %d/%v", len(replay), terminal)
	}
	h.Append("j", []*pb.LogLine{line("a"), line("b")})
	for _, want := range []string{"a", "b"} {
		ev := <-sub.ch
		if got := ev.GetLog().GetLine(); got != want {
			t.Errorf("want line %q, got %q", want, got)
		}
	}
}

func TestLogHub_LateSubscriberGetsReplay(t *testing.T) {
	h := NewLogHub()
	h.Append("j", []*pb.LogLine{line("early-1"), line("early-2")})
	_, replay, _ := h.Subscribe("j")
	if len(replay) != 2 || replay[0].GetLog().GetLine() != "early-1" {
		t.Errorf("want 2 replayed lines starting with early-1, got %v", replay)
	}
}

func TestLogHub_FinishDeliversTerminalAndCleansUp(t *testing.T) {
	h := NewLogHub()
	sub, _, _ := h.Subscribe("j")
	h.Append("j", []*pb.LogLine{line("a")})
	h.Finish("j", resultEvent("j"))

	<-sub.ch // the log line
	ev := <-sub.ch
	if ev.GetResult() == nil {
		t.Fatal("want terminal result event after Finish")
	}
	// Lines after Finish are ignored.
	h.Append("j", []*pb.LogLine{line("straggler")})
	select {
	case ev := <-sub.ch:
		t.Errorf("no events expected after terminal, got %v", ev)
	default:
	}
	// Last unsubscribe deletes the entry.
	h.Unsubscribe("j", sub)
	if h.JobCount() != 0 {
		t.Errorf("want 0 tracked jobs after last unsubscribe, got %d", h.JobCount())
	}
}

func TestLogHub_FinishIsIdempotent(t *testing.T) {
	h := NewLogHub()
	sub, _, _ := h.Subscribe("j")
	h.Finish("j", resultEvent("j"))
	h.Finish("j", resultEvent("j"))
	<-sub.ch
	select {
	case <-sub.ch:
		t.Error("second Finish must not deliver a second terminal event")
	default:
	}
}

func TestLogHub_FinishWithoutWatchersDeletesEntry(t *testing.T) {
	h := NewLogHub()
	h.Append("j", []*pb.LogLine{line("a")})
	h.Finish("j", resultEvent("j"))
	if h.JobCount() != 0 {
		t.Errorf("unwatched finished job must be dropped, got %d entries", h.JobCount())
	}
	// Finish for a job the hub never saw is a no-op, not an entry leak.
	h.Finish("ghost", resultEvent("ghost"))
	if h.JobCount() != 0 {
		t.Errorf("Finish(unknown) must not create entries, got %d", h.JobCount())
	}
}

func TestLogHub_ReplayBufferIsBounded(t *testing.T) {
	h := NewLogHub()
	h.maxLines = 10
	lines := make([]*pb.LogLine, 25)
	for i := range lines {
		lines[i] = line(fmt.Sprintf("l%d", i))
	}
	h.Append("j", lines)
	_, replay, _ := h.Subscribe("j")
	if len(replay) != 10 {
		t.Errorf("want replay capped at 10, got %d", len(replay))
	}
}

func TestLogHub_SlowSubscriberDropsInsteadOfBlocking(t *testing.T) {
	h := NewLogHub()
	sub, _, _ := h.Subscribe("j")
	// Overfill the mailbox; Append must not block.
	for i := 0; i < subChannelBuffer+100; i++ {
		h.Append("j", []*pb.LogLine{line("x")})
	}
	if sub.dropped == 0 {
		t.Error("overflow must be counted as dropped")
	}
	if len(sub.ch) != subChannelBuffer {
		t.Errorf("mailbox should be exactly full, got %d", len(sub.ch))
	}
}

func TestLogHub_OverlongLinesAreTruncated(t *testing.T) {
	h := NewLogHub()
	huge := make([]byte, maxLineBytes*2)
	for i := range huge {
		huge[i] = 'x'
	}
	h.Append("j", []*pb.LogLine{line(string(huge))})
	_, replay, _ := h.Subscribe("j")
	if got := len(replay[0].GetLog().GetLine()); got > maxLineBytes+64 {
		t.Errorf("line not truncated: %d bytes", got)
	}
}
