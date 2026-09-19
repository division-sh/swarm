package bus_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/google/uuid"
)

type stopClaimInterceptor struct {
	eventID string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	after   func(context.Context) error
}

func (i *stopClaimInterceptor) Intercept(ctx context.Context, event events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if event.ID() != i.eventID {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	i.once.Do(func() { close(i.entered) })
	select {
	case <-i.release:
	case <-ctx.Done():
		return false, nil, runtimepipelineobligation.Continue(), ctx.Err()
	}
	if i.after != nil {
		if err := i.after(ctx); err != nil {
			return false, nil, runtimepipelineobligation.Continue(), err
		}
	}
	return true, nil, runtimepipelineobligation.Continue(), nil
}

func newStopClaimInterceptor(t *testing.T, eventID string) (*stopClaimInterceptor, func()) {
	t.Helper()
	i := &stopClaimInterceptor{eventID: eventID, entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(i.release) }) }
	t.Cleanup(release)
	return i, release
}

type stopClaimQueue struct {
	*runtimebus.EventBus
	entered chan struct{}
}

func (q stopClaimQueue) BeginRunStop(ctx context.Context, runID string) (runtimeruncontrol.StopTransition, error) {
	close(q.entered)
	return q.EventBus.BeginRunStop(ctx, runID)
}

func TestRunStopPreservesForegroundClaimFenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCompleteEventDispatchFixture(t, backend, false)
			event := newRetryReleaseTestEvent(f, time.Now().UTC())
			i, release := newStopClaimInterceptor(t, event.ID())
			f.bus.SetInterceptors(i)
			if err := f.bus.PublishAcknowledged(f.ctx, event); err != nil {
				t.Fatal(err)
			}
			requireSignalBefore(t, i.entered, 5*time.Second, "held foreground publication")
			before := readStopClaimRun(t, f)
			ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
			defer cancel()
			controller := runtimeruncontrol.NewController(f.store.(runtimeruncontrol.Store), f.bus, runtimeruncontrol.Options{})
			_, err := controller.Stop(ctx, runtimeruncontrol.TransitionRequest{RunID: event.RunID()})
			if !errors.Is(err, runtimepipelineobligation.ErrBusy) {
				t.Fatalf("foreground stop=%v, want retained claim refusal, not a quiescence wait", err)
			}
			failure, typed := runtimefailures.EnvelopeFromError(err)
			if !typed || failure.Class != runtimefailures.ClassLifecycleConflict || failure.Detail.Code != "pipeline_parent_claim_busy" || failure.Detail.Attributes["purpose"] != string(runtimepipelineobligation.PurposePublication) || failure.Detail.Attributes["stage"] != "pipeline_claim" {
				t.Fatalf("foreground claim evidence=%+v typed=%v", failure, typed)
			}
			if got := readStopClaimRun(t, f); got != before {
				t.Fatalf("busy stop changed durable state: before=%+v after=%+v", before, got)
			}
			if got := retryReleasePipelineReceiptCount(t, f, event.ID()); got != 0 {
				t.Fatalf("busy stop settled foreground claim: receipts=%d", got)
			}
			release()
			if err := f.bus.WaitForQuiescence(ctx); err != nil {
				t.Fatal(err)
			}
			var outcome string
			if err := f.db.QueryRow(`SELECT outcome FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, event.ID()).Scan(&outcome); err != nil || outcome != "success" {
				t.Fatalf("original foreground claimant could not settle after stop rollback: outcome=%s err=%v", outcome, err)
			}
		})
	}
}

func TestRunStopDrainsRecoveryBeforeMutationAndRequiredPublicationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, separateBus := range []bool{false, true} {
			name := backend + "/same_bus"
			if separateBus {
				name = backend + "/other_bus"
			}
			t.Run(name, func(t *testing.T) {
				f := newCompleteEventDispatchFixture(t, backend, false)
				stopBus := f.bus
				if separateBus {
					var err error
					stopBus, err = newScopedTestEventBus(f.store, runtimebus.EventBusOptions{})
					if err != nil {
						t.Fatal(err)
					}
				}
				f.subscribe(t, f.event.Type())
				i, release := newStopClaimInterceptor(t, f.event.ID())
				child := eventtest.InExecutionMode(eventtest.PersistedChildForProducer(
					uuid.NewString(), f.event.Type(), eventtest.Producer(events.EventProducerPlatform, "runtime"),
					"", []byte(`{}`), f.event.ChainDepth()+1, f.event.RunID(), f.event.ID(), events.EventEnvelope{}, time.Now().UTC(),
				), f.event.ExecutionMode())
				// The admitted recovery cannot finish before this real publication's
				// selected-store mutation and synchronous dispatch finish. Stop must
				// not hold a run/database mutation while waiting for that work.
				published := make(chan error, 1)
				i.after = func(ctx context.Context) error {
					err := f.bus.Publish(ctx, child)
					published <- err
					return err
				}
				f.bus.SetInterceptors(i)
				sweepDone := make(chan error, 1)
				go func() {
					_, err := f.bus.SweepPipelineObligations(f.ctx, 1)
					sweepDone <- err
				}()
				requireSignalBefore(t, i.entered, 5*time.Second, "held recovery sweep")
				before := readStopClaimRun(t, f)
				store := f.store.(runtimeruncontrol.Store)
				// A different run must not wait for this held recovery, including
				// when both run controllers use the very same scanning bus.
				unrelatedRun := uuid.NewString()
				seedCompleteEventDispatchRun(t, f.ctx, f.db, backend, unrelatedRun, time.Now().UTC())
				unrelatedCtx, unrelatedCancel := context.WithTimeout(f.ctx, time.Second)
				defer unrelatedCancel()
				if _, err := runtimeruncontrol.NewController(store, stopBus, runtimeruncontrol.Options{}).Stop(unrelatedCtx, runtimeruncontrol.TransitionRequest{RunID: unrelatedRun}); err != nil {
					t.Fatalf("unrelated stop waited for another run's recovery: %v", err)
				}
				// Cancellation proves the exclusion wait precedes mutation and is
				// bounded by the request, without stealing the sweep's claim.
				canceledCtx, cancel := context.WithCancel(f.ctx)
				queue := stopClaimQueue{EventBus: stopBus, entered: make(chan struct{})}
				canceled := make(chan error, 1)
				go func() {
					_, err := runtimeruncontrol.NewController(store, queue, runtimeruncontrol.Options{}).Stop(canceledCtx, runtimeruncontrol.TransitionRequest{RunID: f.event.RunID()})
					canceled <- err
				}()
				requireSignalBefore(t, queue.entered, 5*time.Second, "stop waiting for recovery")
				cancel()
				if err := requireErrorBefore(t, canceled, 5*time.Second, "canceled stop exclusion"); !errors.Is(err, context.Canceled) {
					t.Fatalf("stop exclusion cancellation=%v", err)
				}
				if got := readStopClaimRun(t, f); got != before {
					t.Fatalf("canceled wait changed durable state: before=%+v after=%+v", before, got)
				}
				queue.entered = make(chan struct{})
				stopped := make(chan error, 1)
				ctx, finish := context.WithTimeout(f.ctx, 5*time.Second)
				defer finish()
				go func() {
					_, err := runtimeruncontrol.NewController(store, queue, runtimeruncontrol.Options{}).Stop(ctx, runtimeruncontrol.TransitionRequest{RunID: f.event.RunID()})
					stopped <- err
				}()
				requireSignalBefore(t, queue.entered, 5*time.Second, "second stop waiting for recovery")
				select {
				case err := <-stopped:
					t.Fatalf("stop returned while recovery claim was held: %v", err)
				default:
				}
				release()
				if err := requireErrorBefore(t, published, 5*time.Second, "required child publication before recovery release"); err != nil {
					t.Fatal(err)
				}
				if err := requireErrorBefore(t, sweepDone, 5*time.Second, "recovery settlement"); err != nil {
					t.Fatal(err)
				}
				if err := requireErrorBefore(t, stopped, 5*time.Second, "stop after finite recovery"); err != nil {
					t.Fatal(err)
				}
				after := readStopClaimRun(t, f)
				if after.status != "cancelled" || after.control != "stopped" || after.pending != 0 {
					t.Fatalf("stop did not settle run after recovery: %+v", after)
				}
				for _, eventID := range []string{f.event.ID(), child.ID()} {
					var outcome string
					if err := f.db.QueryRow(`SELECT outcome FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, eventID).Scan(&outcome); err != nil || outcome != "success" {
						t.Fatalf("stop overwrote completed recovery/publication: event=%s outcome=%s err=%v", eventID, outcome, err)
					}
				}
			})
		}
	}
}

type stopClaimRunSnapshot struct {
	status, control    string
	pending, revisions int
}

func readStopClaimRun(t *testing.T, f completeEventDispatchFixture) stopClaimRunSnapshot {
	t.Helper()
	var result stopClaimRunSnapshot
	if err := f.db.QueryRow(`SELECT r.status,COALESCE(c.control_status,'') FROM runs r LEFT JOIN run_control_state c ON c.run_id=r.run_id WHERE r.run_id=$1`, f.event.RunID()).Scan(&result.status, &result.control); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','processing','retrying')`, f.event.RunID()).Scan(&result.pending); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRow(`SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1`, f.event.RunID()).Scan(&result.revisions); err != nil {
		t.Fatal(err)
	}
	return result
}
