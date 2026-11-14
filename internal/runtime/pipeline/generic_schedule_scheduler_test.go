package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/google/uuid"
)

func newGenericSchedulerProof(t *testing.T, callback func(context.Context, runtimegenericschedule.Wakeup)) *Scheduler {
	t.Helper()
	scheduler := NewSchedulerWithWorkOwner(pipelineTestWorkOwner(t))
	if err := scheduler.BindGenericScheduleLifecycle(callback); err != nil {
		t.Fatalf("bind generic schedule lifecycle: %v", err)
	}
	t.Cleanup(func() {
		scheduler.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := scheduler.Wait(ctx); err != nil {
			t.Errorf("wait for generic schedule test scheduler: %v", err)
		}
	})
	return scheduler
}

func genericSchedulerProofWakeup(t *testing.T, dueAt time.Time) runtimegenericschedule.Wakeup {
	t.Helper()
	wakeup, err := runtimegenericschedule.NewWakeup(uuid.NewString(), dueAt)
	if err != nil {
		t.Fatalf("create generic schedule proof wakeup: %v", err)
	}
	return wakeup
}

func TestSchedulerGenericWakeupIsOneInertOneShotClock(t *testing.T) {
	fired := make(chan runtimegenericschedule.Wakeup, 2)
	scheduler := newGenericSchedulerProof(t, func(_ context.Context, wakeup runtimegenericschedule.Wakeup) {
		fired <- wakeup
	})
	wakeup := genericSchedulerProofWakeup(t, time.Now().Add(20*time.Millisecond))
	if err := scheduler.RegisterGenericScheduleWakeup(context.Background(), wakeup, runtimegenericschedule.OwnerSystem); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-fired:
		if got.ActivationID() != wakeup.ActivationID() || !got.DueAt().Equal(wakeup.DueAt()) {
			t.Fatalf("generic scheduler callback = %#v, want %#v", got, wakeup)
		}
	case <-time.After(time.Second):
		t.Fatal("generic schedule wakeup did not fire")
	}
	select {
	case duplicate := <-fired:
		t.Fatalf("generic scheduler repeated an inert wakeup: %#v", duplicate)
	case <-time.After(80 * time.Millisecond):
	}
}

func TestStopGenericScheduleWakeupsJoinsAndPreventsPostStopCallback(t *testing.T) {
	fired := make(chan runtimegenericschedule.Wakeup, 1)
	scheduler := newGenericSchedulerProof(t, func(_ context.Context, wakeup runtimegenericschedule.Wakeup) {
		fired <- wakeup
	})
	wakeup := genericSchedulerProofWakeup(t, time.Now().Add(100*time.Millisecond))
	if err := scheduler.RegisterGenericScheduleWakeup(context.Background(), wakeup, runtimegenericschedule.OwnerSystem); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := scheduler.StopGenericScheduleWakeups(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-fired:
		t.Fatalf("generic schedule fired after joined stop: %#v", got)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestIdleInstanceClockDoesNotHoldRunTransition(t *testing.T) {
	owner := pipelineTestWorkOwner(t)
	scheduler := NewSchedulerWithWorkOwner(owner)
	if err := scheduler.BindGenericScheduleLifecycle(func(context.Context, runtimegenericschedule.Wakeup) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		scheduler.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := scheduler.Wait(ctx); err != nil {
			t.Error(err)
		}
	})
	runID := uuid.NewString()
	ctx := correlation.WithRunID(t.Context(), runID)
	wakeup := genericSchedulerProofWakeup(t, time.Now().Add(time.Hour))
	if err := scheduler.RegisterGenericScheduleWakeup(ctx, wakeup, runtimegenericschedule.OwnerInstance); err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	release, err := owner.BeginRunTransition(wait, runID)
	if err != nil {
		t.Fatalf("idle instance clock held run transition: %v", err)
	}
	release()
}
