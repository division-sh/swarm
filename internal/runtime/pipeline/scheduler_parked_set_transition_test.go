package pipeline

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/google/uuid"
)

func TestParkedSetTransitionLinearizesExactTargetIncumbents(t *testing.T) {
	for _, mode := range []string{"once", "cron"} {
		for _, targetState := range []string{"armed", "firing"} {
			for _, targetOwnerKind := range []string{"runtime", "standing"} {
				name := fmt.Sprintf("%s_%s_%s_owned", mode, targetState, targetOwnerKind)
				t.Run(name, func(t *testing.T) {
					runtimeOwner := pipelineTestWorkOwner(t)
					predecessor := newSchedulerTestStanding(t, runtimeOwner, "source-"+name, 1)
					successor := newSchedulerTestStanding(t, runtimeOwner, "target-"+name, 2)
					source := NewSchedulerWithWorkOwner(runtimeOwner)
					targetStarted := make(chan struct{}, 1)
					reboundStarted := make(chan struct{}, 1)
					releaseTarget := make(chan struct{})
					var active atomic.Int32
					var overlap atomic.Bool
					var activated atomic.Bool
					target := NewSchedulerWithWorkOwner(runtimeOwner, func(context.Context, Schedule) {
						if active.Add(1) > 1 {
							overlap.Store(true)
						}
						defer active.Add(-1)
						if activated.Load() {
							select {
							case reboundStarted <- struct{}{}:
							default:
							}
							return
						}
						select {
						case targetStarted <- struct{}{}:
						default:
						}
						<-releaseTarget
					})
					t.Cleanup(func() {
						source.Stop()
						target.Stop()
						_ = source.Wait(context.Background())
						_ = target.Wait(context.Background())
					})

					schedule := Schedule{
						RunID: uuid.NewString(), AgentID: "timer-agent", EventType: "timer." + mode,
						Mode: mode, TaskID: "exact-target-" + name,
					}
					if mode == "once" {
						schedule.At = time.Now().Add(80 * time.Millisecond)
					} else {
						schedule.Cron = "@every 5ms"
					}
					if err := source.Register(worklifetime.WithOccurrence(context.Background(), predecessor), schedule); err != nil {
						t.Fatalf("register source schedule: %v", err)
					}
					parked, err := source.ParkOccurrence(context.Background(), predecessor)
					if err != nil {
						t.Fatalf("park source schedule: %v", err)
					}

					targetSchedule := schedule
					if targetState == "firing" {
						if mode == "once" {
							targetSchedule.At = time.Now()
						} else {
							targetSchedule.Cron = "@every 1ms"
						}
					} else if mode == "once" {
						targetSchedule.At = time.Now().Add(time.Hour)
					} else {
						targetSchedule.Cron = "@every 1h"
					}
					targetCtx := context.Background()
					if targetOwnerKind == "standing" {
						targetCtx = worklifetime.WithOccurrence(targetCtx, successor)
					}
					if err := target.Register(targetCtx, targetSchedule); err != nil {
						t.Fatalf("register target incumbent: %v", err)
					}
					target.mu.Lock()
					incumbent := target.tasks[scheduleKey(targetSchedule)]
					target.mu.Unlock()
					if incumbent == nil {
						t.Fatal("target incumbent was not installed")
					}

					if targetState == "firing" {
						select {
						case <-targetStarted:
						case <-time.After(time.Second):
							t.Fatal("target incumbent did not start")
						}
					}
					preparedResult := make(chan struct {
						prepared *PreparedParkedSetRebind
						err      error
					}, 1)
					go func() {
						prepared, prepareErr := PrepareParkedSetRebind(context.Background(), target, []ParkedRebind{{Parked: parked, Owner: successor}})
						preparedResult <- struct {
							prepared *PreparedParkedSetRebind
							err      error
						}{prepared: prepared, err: prepareErr}
					}()
					if targetState == "firing" {
						select {
						case result := <-preparedResult:
							t.Fatalf("prepare returned before firing incumbent settled: %v", result.err)
						default:
						}
						close(releaseTarget)
					} else {
						close(releaseTarget)
					}
					result := <-preparedResult
					if result.err != nil {
						t.Fatalf("prepare parked set: %v", result.err)
					}
					select {
					case <-incumbent.done:
					default:
						t.Fatal("aggregate preparation returned before exact target incumbent joined")
					}
					if err := result.prepared.CommitDormant(); err != nil {
						t.Fatalf("commit dormant parked set: %v", err)
					}
					select {
					case <-reboundStarted:
						t.Fatal("rebound callback started before aggregate activation")
					default:
					}
					activated.Store(true)
					if err := result.prepared.Activate(); err != nil {
						t.Fatalf("activate parked set: %v", err)
					}
					select {
					case <-reboundStarted:
					case <-time.After(time.Second):
						t.Fatal("rebound callback did not start after activation")
					}
					if overlap.Load() {
						t.Fatal("rebound callback overlapped exact target incumbent")
					}
				})
			}
		}
	}
}

func TestParkedSetTransitionAdmissionFailureIsAtomicAndRetryable(t *testing.T) {
	for _, failureIndex := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("failure_at_%d", failureIndex), func(t *testing.T) {
			runtimeOwner := pipelineTestWorkOwner(t)
			source := NewSchedulerWithWorkOwner(runtimeOwner)
			failedTarget := NewSchedulerWithWorkOwner(runtimeOwner)
			retryTarget := NewSchedulerWithWorkOwner(runtimeOwner)
			t.Cleanup(func() {
				source.Stop()
				failedTarget.Stop()
				retryTarget.Stop()
				_ = source.Wait(context.Background())
				_ = failedTarget.Wait(context.Background())
				_ = retryTarget.Wait(context.Background())
			})

			parked := make([]*ParkedOccurrence, 3)
			failedBindings := make([]ParkedRebind, 3)
			for i := range parked {
				predecessor := newSchedulerTestStanding(t, runtimeOwner, fmt.Sprintf("source-%d", i), 1)
				successor := newSchedulerTestStanding(t, runtimeOwner, fmt.Sprintf("failed-%d", i), 2)
				schedule := Schedule{
					RunID: uuid.NewString(), AgentID: "timer-agent", EventType: "timer.once", Mode: "once",
					At: time.Now().Add(time.Hour), TaskID: fmt.Sprintf("service-%d", i),
				}
				if err := source.Register(worklifetime.WithOccurrence(context.Background(), predecessor), schedule); err != nil {
					t.Fatalf("register source %d: %v", i, err)
				}
				var err error
				parked[i], err = source.ParkOccurrence(context.Background(), predecessor)
				if err != nil {
					t.Fatalf("park source %d: %v", i, err)
				}
				failedBindings[i] = ParkedRebind{Parked: parked[i], Owner: successor}
			}
			if err := failedBindings[failureIndex].Owner.RetireAndWait(context.Background()); err != nil {
				t.Fatalf("retire rejected owner: %v", err)
			}
			if _, err := PrepareParkedSetRebind(context.Background(), failedTarget, failedBindings); err == nil {
				t.Fatal("aggregate preparation succeeded with a retired successor owner")
			}
			failedTarget.mu.Lock()
			if len(failedTarget.tasks) != 0 || len(failedTarget.reservations) != 0 || len(failedTarget.transitions) != 0 {
				failedTarget.mu.Unlock()
				t.Fatal("failed aggregate left candidate tasks or reservations live")
			}
			failedTarget.mu.Unlock()

			retryBindings := make([]ParkedRebind, len(parked))
			for i := range parked {
				retryBindings[i] = ParkedRebind{
					Parked: parked[i],
					Owner:  newSchedulerTestStanding(t, runtimeOwner, fmt.Sprintf("retry-%d", i), 3),
				}
			}
			prepared, err := PrepareParkedSetRebind(context.Background(), retryTarget, retryBindings)
			if err != nil {
				t.Fatalf("prepare fresh-candidate retry: %v", err)
			}
			if err := prepared.Publish(); err != nil {
				t.Fatalf("publish fresh-candidate retry: %v", err)
			}
			retryTarget.mu.Lock()
			gotTasks := len(retryTarget.tasks)
			retryTarget.mu.Unlock()
			if gotTasks != len(parked) {
				t.Fatalf("retry tasks = %d, want %d", gotTasks, len(parked))
			}
		})
	}
}

func TestParkedSetReservationRejectsConcurrentTargetMutation(t *testing.T) {
	runtimeOwner := pipelineTestWorkOwner(t)
	predecessor := newSchedulerTestStanding(t, runtimeOwner, "reservation-source", 1)
	successor := newSchedulerTestStanding(t, runtimeOwner, "reservation-target", 2)
	retryOwner := newSchedulerTestStanding(t, runtimeOwner, "reservation-retry", 3)
	source := NewSchedulerWithWorkOwner(runtimeOwner)
	targetStarted := make(chan struct{})
	releaseTarget := make(chan struct{})
	target := NewSchedulerWithWorkOwner(runtimeOwner, func(context.Context, Schedule) {
		close(targetStarted)
		<-releaseTarget
	})
	retryTarget := NewSchedulerWithWorkOwner(runtimeOwner)
	t.Cleanup(func() {
		source.Stop()
		target.Stop()
		retryTarget.Stop()
		_ = source.Wait(context.Background())
		_ = target.Wait(context.Background())
		_ = retryTarget.Wait(context.Background())
	})
	schedule := Schedule{
		RunID: uuid.NewString(), AgentID: "timer-agent", EventType: "timer.cron", Mode: "cron",
		Cron: "@every 1ms", TaskID: "reserved-key",
	}
	if err := source.Register(worklifetime.WithOccurrence(context.Background(), predecessor), schedule); err != nil {
		t.Fatalf("register source: %v", err)
	}
	parked, err := source.ParkOccurrence(context.Background(), predecessor)
	if err != nil {
		t.Fatalf("park source: %v", err)
	}
	if err := target.Register(worklifetime.WithOccurrence(context.Background(), successor), schedule); err != nil {
		t.Fatalf("register target incumbent: %v", err)
	}
	select {
	case <-targetStarted:
	case <-time.After(time.Second):
		t.Fatal("target callback did not start")
	}
	preparedResult := make(chan *PreparedParkedSetRebind, 1)
	prepareErr := make(chan error, 1)
	go func() {
		prepared, prepareError := PrepareParkedSetRebind(context.Background(), target, []ParkedRebind{{Parked: parked, Owner: successor}})
		if prepareError != nil {
			prepareErr <- prepareError
			return
		}
		preparedResult <- prepared
	}()
	waitForScheduleReservation(t, target, schedule)
	if err := target.Register(worklifetime.WithOccurrence(context.Background(), successor), schedule); err == nil {
		t.Fatal("same-key Register bypassed aggregate reservation")
	}
	if err := target.CancelExact(schedule); err == nil {
		t.Fatal("same-key CancelExact bypassed aggregate reservation")
	}
	if err := target.Cancel(schedule.AgentID, schedule.EventType); err == nil {
		t.Fatal("matching Cancel bypassed aggregate reservation")
	}
	if _, err := target.ParkOccurrence(context.Background(), successor); err == nil {
		t.Fatal("target-owner ParkOccurrence bypassed aggregate reservation")
	}
	if err := parked.RestoreOriginal(context.Background()); err == nil {
		t.Fatal("source RestoreOriginal bypassed aggregate reservation")
	}
	if err := parked.Rebind(context.Background(), retryTarget, retryOwner); err == nil {
		t.Fatal("source Rebind bypassed aggregate reservation")
	}
	if _, err := PrepareParkedSetRebind(context.Background(), retryTarget, []ParkedRebind{{Parked: parked, Owner: retryOwner}}); err == nil {
		t.Fatal("second aggregate transition consumed a reserved source authority")
	}
	stopDone := make(chan struct{})
	go func() {
		target.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("scheduler stop crossed an active aggregate reservation")
	default:
	}
	close(releaseTarget)
	var prepared *PreparedParkedSetRebind
	select {
	case err := <-prepareErr:
		t.Fatalf("prepare reserved transition: %v", err)
	case prepared = <-preparedResult:
	case <-time.After(time.Second):
		t.Fatal("reserved transition did not finish after incumbent release")
	}
	if err := prepared.Abort(); err != nil {
		t.Fatalf("abort prepared transition: %v", err)
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("scheduler stop did not resume after transition abort")
	}
	prepared, err = PrepareParkedSetRebind(context.Background(), retryTarget, []ParkedRebind{{Parked: parked, Owner: retryOwner}})
	if err != nil {
		t.Fatalf("source authority was not retryable after abort: %v", err)
	}
	if err := prepared.Publish(); err != nil {
		t.Fatalf("publish retry after abort: %v", err)
	}
}

func newSchedulerTestStanding(t *testing.T, owner *worklifetime.RuntimeOccurrence, serviceID string, generation uint64) *worklifetime.StandingOccurrence {
	t.Helper()
	standing, err := owner.NewStanding(context.Background(), worklifetime.StandingIdentity{
		ServiceID: serviceID, RunID: uuid.NewString(), Generation: generation,
	})
	if err != nil {
		t.Fatalf("new standing occurrence: %v", err)
	}
	t.Cleanup(func() {
		if err := standing.RetireAndWait(context.Background()); err != nil {
			t.Errorf("retire standing occurrence %s: %v", serviceID, err)
		}
	})
	return standing
}

func waitForScheduleReservation(t *testing.T, scheduler *Scheduler, schedule Schedule) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		scheduler.mu.Lock()
		_, reserved := scheduler.reservations[scheduleKey(schedule)]
		scheduler.mu.Unlock()
		if reserved {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("schedule key was not reserved")
}
