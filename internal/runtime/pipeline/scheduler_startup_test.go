package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

func TestSchedulerStartupRetainsExactWakeupAndSettlesWithheldWork(t *testing.T) {
	for _, exit := range []string{"release", "cancel_release", "retire", "stop"} {
		t.Run(exit, func(t *testing.T) {
			owner := pipelineTestWorkOwner(t)
			scheduler := NewSchedulerWithWorkOwner(owner)
			fired := make(chan runtimegenericschedule.Wakeup, 2)
			if err := scheduler.BindGenericScheduleLifecycle(func(_ context.Context, wakeup runtimegenericschedule.Wakeup) { fired <- wakeup }); err != nil {
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
			if err := scheduler.PrepareStartup(); err != nil {
				t.Fatal(err)
			}
			if err := scheduler.PrepareStartup(); err == nil {
				t.Fatal("duplicate preparation succeeded")
			}
			wakeup := genericSchedulerProofWakeup(t, time.Now().Add(-time.Second))
			if err := scheduler.RegisterGenericScheduleWakeup(context.Background(), wakeup); err != nil {
				t.Fatal(err)
			}
			if owner.ActiveCount() != 1 {
				t.Fatalf("withheld task leases=%d, want one", owner.ActiveCount())
			}
			select {
			case got := <-fired:
				t.Fatalf("callback fired before recovery handoff: %v", got)
			case <-time.After(20 * time.Millisecond):
			}
			switch exit {
			case "release":
				if err := scheduler.ReleaseStartup(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := scheduler.ReleaseStartup(context.Background()); err == nil {
					t.Fatal("duplicate release succeeded")
				}
				select {
				case got := <-fired:
					if got.ActivationID() != wakeup.ActivationID() || !got.DueAt().Equal(wakeup.DueAt()) {
						t.Fatalf("late callback changed exact wakeup: got=%v want=%v", got, wakeup)
					}
				case <-time.After(time.Second):
					t.Fatal("late callback was lost")
				}
			case "cancel_release":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := scheduler.ReleaseStartup(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled release=%v", err)
				}
				scheduler.Stop()
			case "retire":
				if err := scheduler.RetireGenericScheduleWakeup(wakeup); err != nil {
					t.Fatal(err)
				}
				if err := scheduler.ReleaseStartup(context.Background()); err != nil {
					t.Fatal(err)
				}
			case "stop":
				scheduler.Stop()
				if err := scheduler.ReleaseStartup(context.Background()); err == nil {
					t.Fatal("stopped scheduler released callbacks")
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := scheduler.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if owner.ActiveCount() != 0 {
				t.Fatalf("settled task retains %d leases", owner.ActiveCount())
			}
			select {
			case got := <-fired:
				t.Fatalf("unexpected callback after settlement: %v", got)
			default:
			}
		})
	}
}

func TestSchedulerStartupRejectsAlreadyRegisteredWork(t *testing.T) {
	scheduler := newGenericSchedulerProof(t, func(context.Context, runtimegenericschedule.Wakeup) {})
	if err := scheduler.RegisterGenericScheduleWakeup(context.Background(), genericSchedulerProofWakeup(t, time.Now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.PrepareStartup(); err == nil {
		t.Fatal("startup silently paused an already active scheduler")
	}
}

func TestSchedulerStartupReleasesCronAndEverySuccessorsThroughSameTaskPath(t *testing.T) {
	for _, basis := range []runtimegenericschedule.DueBasis{
		runtimegenericschedule.CronDue("* * * * *"),
		runtimegenericschedule.EveryDue(time.Minute),
	} {
		t.Run(string(basis.Kind), func(t *testing.T) {
			firstDue, err := basis.FirstDue(time.Now().UTC().Add(-3 * time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			nextDue, err := basis.Next(firstDue)
			if err != nil {
				t.Fatal(err)
			}
			first := genericSchedulerProofWakeup(t, firstDue)
			next, err := runtimegenericschedule.NewWakeup(first.ActivationID(), nextDue)
			if err != nil {
				t.Fatal(err)
			}
			fired := make(chan runtimegenericschedule.Wakeup, 3)
			reconciled := make(chan error, 1)
			var scheduler *Scheduler
			scheduler = newGenericSchedulerProof(t, func(ctx context.Context, wakeup runtimegenericschedule.Wakeup) {
				fired <- wakeup
				if wakeup.DueAt().Equal(firstDue) {
					// The lifecycle re-registers its committed successor, not a
					// scheduler-owned recurring clock. Replacement joins this task.
					reconciled <- scheduler.RegisterGenericScheduleWakeup(context.WithoutCancel(ctx), next)
				}
			})
			if err := scheduler.PrepareStartup(); err != nil {
				t.Fatal(err)
			}
			if err := scheduler.RegisterGenericScheduleWakeup(context.Background(), first); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-fired:
				t.Fatalf("recurring callback escaped startup: %v", got)
			case <-time.After(20 * time.Millisecond):
			}
			if err := scheduler.ReleaseStartup(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, want := range []runtimegenericschedule.Wakeup{first, next} {
				select {
				case got := <-fired:
					if got.ActivationID() != want.ActivationID() || !got.DueAt().Equal(want.DueAt()) {
						t.Fatalf("callback=%v want=%v", got, want)
					}
				case <-time.After(time.Second):
					t.Fatal("recurring callback was stranded behind startup release")
				}
			}
			if err := <-reconciled; err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := scheduler.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-fired:
				t.Fatalf("scheduler invented a recurring callback: %v", got)
			default:
			}
		})
	}
}
