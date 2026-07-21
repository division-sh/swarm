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

func TestSchedulerParksDirectAndManagerComposedStandingSchedules(t *testing.T) {
	for _, composed := range []bool{false, true} {
		name := "direct"
		if composed {
			name = "manager_composed"
		}
		t.Run(name, func(t *testing.T) {
			process := worklifetime.NewProcess()
			runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{RuntimeInstanceID: "scheduler-" + name, BundleHash: "bundle-" + name})
			if err != nil {
				t.Fatalf("new runtime: %v", err)
			}
			predecessor, err := runtimeOwner.NewStanding(context.Background(), worklifetime.StandingIdentity{ServiceID: "timer-service", RunID: uuid.NewString(), Generation: 1})
			if err != nil {
				t.Fatalf("new predecessor standing: %v", err)
			}
			successor, err := runtimeOwner.NewStanding(context.Background(), worklifetime.StandingIdentity{ServiceID: "timer-service", RunID: uuid.NewString(), Generation: 2})
			if err != nil {
				t.Fatalf("new successor standing: %v", err)
			}
			var manager *worklifetime.ManagerRunOccurrence
			owner := worklifetime.Occurrence(predecessor)
			var producer *worklifetime.Lease
			if composed {
				manager, err = worklifetime.NewManagerRunOccurrence(context.Background(), runtimeOwner, worklifetime.ManagerRunIdentity{Generation: 1})
				if err != nil {
					t.Fatalf("new manager run occurrence: %v", err)
				}
				producer, err = manager.Begin(context.Background(), predecessor)
				if err != nil {
					t.Fatalf("begin manager standing work: %v", err)
				}
				var ok bool
				owner, ok = worklifetime.OccurrenceFromContext(producer.Context())
				if !ok {
					t.Fatal("manager work context has no occurrence")
				}
			}
			scheduler := NewSchedulerWithWorkOwner(runtimeOwner)
			schedules := []Schedule{
				{RunID: uuid.NewString(), AgentID: "agent-a", EventType: "timer.once", Mode: "once", At: time.Now().Add(time.Hour), TaskID: uuid.NewString()},
				{RunID: uuid.NewString(), AgentID: "agent-a", EventType: "timer.cron", Mode: "cron", Cron: "@every 1h", TaskID: uuid.NewString()},
			}
			ctx := worklifetime.WithOccurrence(context.Background(), owner)
			registered := make([]*scheduledTask, 0, len(schedules))
			for _, schedule := range schedules {
				if err := scheduler.Register(ctx, schedule); err != nil {
					t.Fatalf("register %s: %v", schedule.Mode, err)
				}
				task := scheduler.tasks[scheduleKey(schedule)]
				if task == nil || task.owner != owner || task.standingOwner != predecessor {
					t.Fatalf("%s task owners = execution:%T %p standing:%p, want %T %p/%p", schedule.Mode, task.owner, task.owner, task.standingOwner, owner, owner, predecessor)
				}
				registered = append(registered, task)
			}
			if producer != nil {
				if err := producer.Done(); err != nil {
					t.Fatalf("settle manager producer: %v", err)
				}
			}
			parked, err := scheduler.ParkOccurrence(context.Background(), predecessor)
			if err != nil {
				t.Fatalf("park predecessor: %v", err)
			}
			if len(parked) != len(schedules) {
				t.Fatalf("parked schedules = %#v, want once and cron", parked)
			}
			for _, task := range registered {
				select {
				case <-task.done:
				default:
					t.Fatal("park returned before scheduled task completion")
				}
				select {
				case <-task.lease.Context().Done():
				default:
					t.Fatal("park did not cancel scheduled task lease")
				}
			}
			if err := predecessor.RetireAndWait(context.Background()); err != nil {
				t.Fatalf("retire predecessor: %v", err)
			}
			if err := scheduler.RestoreOccurrence(context.Background(), successor, parked); err != nil {
				t.Fatalf("restore successor schedules: %v", err)
			}
			rehydrated, err := scheduler.ParkOccurrence(context.Background(), successor)
			if err != nil {
				t.Fatalf("park successor: %v", err)
			}
			if len(rehydrated) != len(schedules) {
				t.Fatalf("successor schedules = %#v, want once and cron", rehydrated)
			}
			if manager != nil {
				if err := manager.RetireAndWait(context.Background()); err != nil {
					t.Fatalf("retire manager: %v", err)
				}
			}
			if err := successor.RetireAndWait(context.Background()); err != nil {
				t.Fatalf("retire successor: %v", err)
			}
			if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
				t.Fatalf("retire runtime: %v", err)
			}
			process.Retire()
			if _, err := process.Join(context.Background()); err != nil {
				t.Fatalf("join process: %v", err)
			}
		})
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
