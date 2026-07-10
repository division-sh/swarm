package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
)

func TestSchedulerKeysSchedulesByRunID(t *testing.T) {
	runA := "11111111-1111-1111-1111-111111111111"
	runB := "22222222-2222-2222-2222-222222222222"
	base := Schedule{
		AgentID:      "agent-a",
		EventType:    "timer.fire",
		Mode:         "once",
		At:           time.Now().Add(time.Hour),
		EntityID:     "33333333-3333-3333-3333-333333333333",
		FlowInstance: "flow-a/1",
		TaskID:       "task-a",
	}
	scA := base
	scA.RunID = runA
	scB := base
	scB.RunID = runB

	if scheduleKey(scA) == scheduleKey(scB) {
		t.Fatalf("schedule keys matched across run_id: %q", scheduleKey(scA))
	}

	s := NewScheduler()
	if err := s.Register(scA); err != nil {
		t.Fatalf("Register(run A): %v", err)
	}
	if err := s.Register(scB); err != nil {
		t.Fatalf("Register(run B): %v", err)
	}
	if len(s.tasks) != 2 {
		t.Fatalf("registered tasks = %d, want 2", len(s.tasks))
	}
	if err := s.CancelExact(scA); err != nil {
		t.Fatalf("CancelExact(run A): %v", err)
	}
	if len(s.tasks) != 1 {
		t.Fatalf("registered tasks after exact cancel = %d, want 1", len(s.tasks))
	}
	if _, ok := s.tasks[scheduleKey(scB)]; !ok {
		t.Fatalf("run B schedule was cancelled by run A exact cancel")
	}
	s.Stop()
}

func TestSchedulerOneShotPreservesReplyContextToFire(t *testing.T) {
	fired := make(chan Schedule, 1)
	scheduler := NewScheduler(func(schedule Schedule) {
		fired <- schedule
	})
	t.Cleanup(scheduler.Stop)
	want := "reply-v1:one-shot-fire"
	if err := scheduler.Register(Schedule{
		Context:   events.DeliveryContext{Reply: &events.ReplyContextRef{ID: want}},
		AgentID:   "provider-agent",
		EventType: "provider.resume",
		Mode:      "once",
		At:        time.Now().Add(10 * time.Millisecond),
		TaskID:    "reply-resume",
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	select {
	case got := <-fired:
		if got.Context.ReplyContextID() != want {
			t.Fatalf("fired reply context = %q, want %q", got.Context.ReplyContextID(), want)
		}
	case <-time.After(time.Second):
		t.Fatal("one-shot schedule did not fire")
	}
}
