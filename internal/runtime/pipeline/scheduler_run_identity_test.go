package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/google/uuid"
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

	s := NewSchedulerWithWorkOwner(pipelineTestWorkOwner(t))
	if err := s.Register(context.Background(), scA); err != nil {
		t.Fatalf("Register(run A): %v", err)
	}
	if err := s.Register(context.Background(), scB); err != nil {
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

func TestSchedulerBindsTaskToContextualStandingOccurrence(t *testing.T) {
	process := worklifetime.NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{RuntimeInstanceID: "scheduler-runtime", BundleHash: "scheduler-bundle"})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	standing, err := runtimeOwner.NewStanding(context.Background(), worklifetime.StandingIdentity{ServiceID: "timer-service", RunID: uuid.NewString(), Generation: 1})
	if err != nil {
		t.Fatalf("new standing: %v", err)
	}
	scheduler := NewSchedulerWithWorkOwner(runtimeOwner)
	requestCtx, cancelRequest := context.WithCancel(worklifetime.WithOccurrence(context.Background(), standing))
	schedule := Schedule{AgentID: "agent-a", EventType: "timer.fire", Mode: "once", At: time.Now().Add(time.Hour), TaskID: "standing-owner"}
	if err := scheduler.Register(requestCtx, schedule); err != nil {
		t.Fatalf("Register: %v", err)
	}
	cancelRequest()
	task := scheduler.tasks[scheduleKey(schedule)]
	select {
	case <-task.lease.Context().Done():
		t.Fatal("scheduled task inherited request cancellation")
	default:
	}
	standing.Retire()
	select {
	case <-task.lease.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("standing retirement did not cancel scheduled task")
	}
	if err := standing.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire standing: %v", err)
	}
	if owner, ok := worklifetime.OccurrenceFromContext(task.lease.Context()); !ok || owner != standing {
		t.Fatalf("task context owner = %T, want exact standing occurrence", owner)
	}
	if err := scheduler.Wait(context.Background()); err != nil {
		t.Fatalf("scheduler wait: %v", err)
	}
	if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire runtime: %v", err)
	}
	process.Retire()
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatalf("join process: %v", err)
	}
}

func TestSchedulerPrunesCompletedOneShotHistory(t *testing.T) {
	scheduler := NewSchedulerWithWorkOwner(pipelineTestWorkOwner(t))
	for i := 0; i < 100; i++ {
		if err := scheduler.Register(context.Background(), Schedule{
			AgentID: "agent-a", EventType: "timer.fire", Mode: "once", At: time.Now(), TaskID: uuid.NewString(),
		}); err != nil {
			t.Fatalf("Register(%d): %v", i, err)
		}
	}
	if err := scheduler.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if len(scheduler.tasks) != 0 || len(scheduler.draining) != 0 {
		t.Fatalf("retained scheduler state = active:%d draining:%d, want 0/0", len(scheduler.tasks), len(scheduler.draining))
	}
}

func TestSchedulerOneShotPreservesReplyContextToFire(t *testing.T) {
	fired := make(chan Schedule, 1)
	scheduler := NewSchedulerWithWorkOwner(pipelineTestWorkOwner(t), func(_ context.Context, schedule Schedule) {
		fired <- schedule
	})
	t.Cleanup(scheduler.Stop)
	want := "reply-v1:one-shot-fire"
	if err := scheduler.Register(context.Background(), Schedule{
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
