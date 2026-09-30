package bus_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestPausedHandedAgentParksUntilContinueBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, state := range []string{"pending", "failed", "stale_in_progress"} {
			t.Run(backend+"/"+state, func(t *testing.T) {
				f := newCompleteEventDispatchFixture(t, backend, false)
				owner := f.store.PipelineObligations()
				work, err := owner.ClaimEvent(f.ctx, f.event.ID(), runtimepipelineobligation.PurposeRecovery)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := owner.Settle(f.ctx, work.Claim, runtimepipelineobligation.Acknowledged("audit_handed")); err != nil {
					t.Fatal(err)
				}
				route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(f.agentID), AgentIdentity: f.identity}
				id, err := runtimedelivery.DeliveryID(f.event.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				if state != "pending" {
					claim, err := storetest.ClaimDelivery(f.ctx, f.store, f.event, route)
					if err != nil {
						t.Fatal(err)
					}
					if state == "failed" {
						failure := runtimefailures.FromError(errors.New("audit retry"), "audit", "retry")
						if _, err := f.store.SettleFailure(f.ctx, claim.Claim, runtimedelivery.Settlement{
							Disposition: runtimedelivery.FailureRetry, Failure: &failure.Failure,
							RetryBase: time.Nanosecond, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation(),
						}); err != nil {
							t.Fatal(err)
						}
					} else {
						expirePausedDeliveryClaim(t, f, id)
					}
				}
				controller := runtimeruncontrol.NewController(f.store.(runtimeruncontrol.Store), f.bus, runtimeruncontrol.Options{})
				f.bus.SetRunDispatchGate(controller)
				if _, err := controller.Pause(f.ctx, runtimeruncontrol.TransitionRequest{RunID: f.event.RunID(), Reason: "audit"}); err != nil {
					t.Fatal(err)
				}
				before, err := f.store.Snapshot(f.ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				page, err := f.store.ScanDeliveryContinuations(f.ctx, before.Authority, runtimedelivery.ContinuationCursor{}, 100)
				if err != nil {
					t.Fatal(err)
				}
				if len(page.Items) != 1 {
					t.Fatalf("paused scan items = %d", len(page.Items))
				}
				intent, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "global/agents.yaml#agents."+f.agentID+".intent", "Audit pause eligibility.")
				if err != nil {
					t.Fatal(err)
				}
				f.source = completeEventAgentSource(f.agentID, string(f.event.Type()), intent)
				seen := make(chan events.Event, 4)
				generation := f.newRecordingManager(t, seen)
				managerCtx := f.managedContext(t)
				if _, err := generation.manager.HydrateForStartup(managerCtx); err != nil {
					t.Fatal(err)
				}
				if err := generation.manager.Run(managerCtx); err != nil {
					t.Fatal(err)
				}
				f.startDeliveryContinuations(t, managerCtx, generation)
				if err := generation.coordinator.Synchronize(context.Background()); err != nil {
					t.Fatal(err)
				}
				select {
				case event := <-seen:
					t.Fatalf("paused OnEvent: %s", event.ID())
				default:
				}
				before, err = f.store.Snapshot(f.ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				page, err = f.store.ScanDeliveryContinuations(f.ctx, before.Authority, runtimedelivery.ContinuationCursor{}, 100)
				if err != nil || len(page.Items) != 1 || page.Items[0].Disposition != runtimedelivery.ClaimParked {
					t.Fatalf("parked page: %+v %v", page, err)
				}
				if _, present := page.Items[0].Wake.After(); present {
					t.Fatal("parked work acquired a timed wake")
				}
				if blocked, err := controller.QueueableRunDispatchBlocked(f.ctx, f.event.RunID()); err != nil || !blocked {
					t.Fatalf("pause state = %t, %v", blocked, err)
				}
				result, err := controller.Continue(f.ctx, runtimeruncontrol.TransitionRequest{RunID: f.event.RunID(), Reason: "release"})
				if err != nil {
					t.Fatal(err)
				}
				assertCompleteEventDelivery(t, seen, f.event)
				deadline := time.Now().Add(3 * time.Second)
				var after runtimedelivery.Snapshot
				for {
					after, err = f.store.Snapshot(f.ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if after.Status == "delivered" {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("not settled: %+v", after)
					}
					time.Sleep(time.Millisecond)
				}
				if result.Recovery.Sweep.Examined != 0 {
					t.Fatalf("already handed event selected: %+v", result.Recovery)
				}
				if err := generation.coordinator.Synchronize(context.Background()); err != nil {
					t.Fatal(err)
				}
				select {
				case event := <-seen:
					t.Fatalf("duplicate after continue: %s", event.ID())
				default:
				}
			})
		}
	}
}

type pauseSignalCounter struct {
	*deliverycontinuation.Coordinator
	signals atomic.Int64
}

func (c *pauseSignalCounter) Signal() { c.signals.Add(1); c.Coordinator.Signal() }

func TestHandedOnlyContinueSignalsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCompleteEventDispatchFixture(t, backend, false)
			work, err := f.store.PipelineObligations().ClaimEvent(f.ctx, f.event.ID(), runtimepipelineobligation.PurposeRecovery)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.PipelineObligations().Settle(f.ctx, work.Claim, runtimepipelineobligation.Acknowledged("audit_handed")); err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(f.agentID), AgentIdentity: f.identity}
			proof, err := f.store.ProveHandoff(f.ctx, f.event.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			before, err := f.store.Snapshot(f.ctx, proof.DeliveryID())
			if err != nil {
				t.Fatal(err)
			}
			process := worklifetime.NewProcess()
			owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{RuntimeInstanceID: before.Authority.ExecutionID(), BundleHash: before.Authority.SourceArtifact().BundleHash()})
			if err != nil {
				t.Fatal(err)
			}
			coordinator, err := deliverycontinuation.New(f.store, f.store, before.Authority, owner, f.bus, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := coordinator.Retire(context.Background()); err != nil {
					t.Error(err)
				}
				if _, err := owner.RetireAndWait(context.Background()); err != nil {
					t.Error(err)
				}
				process.Retire()
				if _, err := process.Join(context.Background()); err != nil {
					t.Error(err)
				}
			})
			counter := &pauseSignalCounter{Coordinator: coordinator}
			if err := f.bus.SetDeliveryContinuationOwner(counter); err != nil {
				t.Fatal(err)
			}
			controller := runtimeruncontrol.NewController(f.store.(runtimeruncontrol.Store), f.bus, runtimeruncontrol.Options{})
			if _, err := controller.Pause(f.ctx, runtimeruncontrol.TransitionRequest{RunID: f.event.RunID(), Reason: "audit"}); err != nil {
				t.Fatal(err)
			}
			result, err := controller.Continue(f.ctx, runtimeruncontrol.TransitionRequest{RunID: f.event.RunID(), Reason: "audit wake"})
			if err != nil {
				t.Fatal(err)
			}
			after, err := f.store.Snapshot(f.ctx, proof.DeliveryID())
			if err != nil {
				t.Fatal(err)
			}
			if result.Recovery.Sweep.Examined != 0 || counter.signals.Load() == 0 || !reflect.DeepEqual(before, after) {
				t.Fatalf("continue=%+v signals=%d changed=%t", result.Recovery, counter.signals.Load(), !reflect.DeepEqual(before, after))
			}
		})
	}
}

func TestUnownedPausedContinueRefusesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCompleteEventDispatchFixture(t, backend, false)
			if _, err := f.store.TransitionActiveRun(f.ctx, runtimerunlifecycle.ActiveTransitionRequest{RunID: f.event.RunID(), State: runtimerunlifecycle.StatePaused}); err != nil {
				t.Fatal(err)
			}
			result, err := f.store.(runtimeruncontrol.Store).ContinueRunControlOutcome(f.ctx, runtimeruncontrol.TransitionRequest{RunID: f.event.RunID()})
			if !errors.Is(err, runtimeruncontrol.ErrNotPaused) || result.Acknowledged {
				t.Fatalf("unowned continue = %+v, %v", result, err)
			}
		})
	}
}

func TestMixedPausedRunningRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCompleteEventDispatchFixture(t, backend, false)
			runningID := uuid.NewString()
			seedCompleteEventDispatchRun(t, f.ctx, f.db, backend, runningID, f.event.CreatedAt())
			running := newRetryReleaseRunRoot(runningID, f.event.CreatedAt().Add(time.Microsecond))
			storetest.CommitSemanticEventWithRoutes(t, f.ctx, f.store, running, nil, runtimepipelineobligation.ScopeSubscribed)
			controller := runtimeruncontrol.NewController(f.store.(runtimeruncontrol.Store), f.bus, runtimeruncontrol.Options{})
			f.bus.SetRunDispatchGate(controller)
			if _, err := controller.Pause(f.ctx, runtimeruncontrol.TransitionRequest{RunID: f.event.RunID(), Reason: "mixed audit"}); err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(f.agentID), AgentIdentity: f.identity}
			id, err := runtimedelivery.DeliveryID(f.event.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			before, err := f.store.Snapshot(f.ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if err := runtimepipeline.NewRecoveryManagerWith(f.bus).RecoverToExhaustion(f.ctx); err != nil {
				t.Fatal(err)
			}
			after, err := f.store.Snapshot(f.ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("paused delivery changed during pipeline sweep")
			}
			if retryReleasePipelineReceiptCount(t, f, f.event.ID()) != 0 || retryReleasePipelineReceiptCount(t, f, running.ID()) != 1 {
				t.Fatal("wrong mixed receipts")
			}
		})
	}
}

func expirePausedDeliveryClaim(t *testing.T, f completeEventDispatchFixture, id string) {
	t.Helper()
	tx, err := f.db.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	start, end := time.Now().UTC().Add(-2*time.Hour), time.Now().UTC().Add(-time.Hour)
	deliverySQL := "UPDATE event_deliveries SET created_at = ?, started_at = ?, updated_at = ? WHERE delivery_id = ? AND status = 'in_progress'"
	attemptSQL := "UPDATE event_delivery_attempts SET started_at = ?, lease_expires_at = ? WHERE delivery_id = ? AND open_marker = TRUE"
	deliveryArgs := []any{start, start, end, id}
	if f.dialect == "postgres" {
		deliverySQL = "UPDATE event_deliveries SET created_at = $1, started_at = $1, updated_at = $2 WHERE delivery_id = $3::uuid AND status = 'in_progress'"
		attemptSQL = "UPDATE event_delivery_attempts SET started_at = $1, lease_expires_at = $2 WHERE delivery_id = $3::uuid AND open_marker = TRUE"
		deliveryArgs = []any{start, end, id}
	}
	for _, query := range []struct {
		sql  string
		args []any
	}{{deliverySQL, deliveryArgs}, {attemptSQL, []any{start, end, id}}} {
		result, err := tx.ExecContext(f.ctx, query.sql, query.args...)
		if err != nil {
			t.Fatal(err)
		}
		if count, err := result.RowsAffected(); err != nil || count != 1 {
			t.Fatalf("expiry affected=%d error=%v", count, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestPausedUnhandedRecoveryParksThenContinueReleasesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCompleteEventDispatchFixture(t, backend, false)
			ch := f.subscribe(t, f.event.Type())
			controller := runtimeruncontrol.NewController(f.store.(runtimeruncontrol.Store), f.bus, runtimeruncontrol.Options{})
			f.bus.SetRunDispatchGate(controller)
			if _, err := controller.Pause(f.ctx, runtimeruncontrol.TransitionRequest{RunID: f.event.RunID(), Reason: "audit"}); err != nil {
				t.Fatal(err)
			}
			err := runtimepipeline.NewRecoveryManagerWith(f.bus).RecoverToExhaustion(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case delivery := <-ch:
				_ = delivery.Complete()
				t.Fatal("unhanded paused execution")
			default:
			}
			if n := retryReleasePipelineReceiptCount(t, f, f.event.ID()); n != 0 {
				t.Fatalf("paused receipts = %d", n)
			}
			result, err := controller.Continue(f.ctx, runtimeruncontrol.TransitionRequest{RunID: f.event.RunID(), Reason: "audit release"})
			if err != nil {
				t.Fatal(err)
			}
			assertCompleteLocalDelivery(t, ch, f.event)
			if result.Recovery.Sweep.Settled != 1 || retryReleasePipelineReceiptCount(t, f, f.event.ID()) != 1 {
				t.Fatalf("release = %+v", result.Recovery)
			}
		})
	}
}

func TestPausedFutureRetryAndLiveClaimRemainOwnedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, posture := range []string{"future_retry", "live_claim"} {
			t.Run(backend+"/"+posture, func(t *testing.T) {
				f := newCompleteEventDispatchFixture(t, backend, false)
				owner := f.store.PipelineObligations()
				work, err := owner.ClaimEvent(f.ctx, f.event.ID(), runtimepipelineobligation.PurposeRecovery)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := owner.Settle(f.ctx, work.Claim, runtimepipelineobligation.Acknowledged("handed")); err != nil {
					t.Fatal(err)
				}
				route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(f.agentID), AgentIdentity: f.identity}
				claim, err := storetest.ClaimDelivery(f.ctx, f.store, f.event, route)
				if err != nil {
					t.Fatal(err)
				}
				want := runtimedelivery.ClaimBusy
				if posture == "future_retry" {
					want = runtimedelivery.ClaimDeferred
					failure := runtimefailures.FromError(errors.New("future retry"), "pause-proof", "retry")
					if _, err := f.store.SettleFailure(f.ctx, claim.Claim, runtimedelivery.Settlement{Disposition: runtimedelivery.FailureRetry, Failure: &failure.Failure, RetryBase: time.Hour, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation()}); err != nil {
						t.Fatal(err)
					}
				}
				controller := runtimeruncontrol.NewController(f.store.(runtimeruncontrol.Store), f.bus, runtimeruncontrol.Options{})
				if _, err := controller.Pause(f.ctx, runtimeruncontrol.TransitionRequest{RunID: f.event.RunID(), Reason: "custom-reason"}); err != nil {
					t.Fatal(err)
				}
				before, err := f.store.Snapshot(f.ctx, claim.Claim.DeliveryID())
				if err != nil {
					t.Fatal(err)
				}
				page, err := f.store.ScanDeliveryContinuations(f.ctx, before.Authority, runtimedelivery.ContinuationCursor{}, 100)
				if err != nil || len(page.Items) != 1 || page.Items[0].Disposition != runtimedelivery.ClaimParked {
					t.Fatalf("parked continuation: %+v %v", page, err)
				}
				if _, present := page.Items[0].Wake.After(); present {
					t.Fatal("paused continuation acquired a polling wake")
				}
				fresh, err := f.store.ClaimDelivery(f.ctx, before.Authority, f.event, route)
				if err != nil || fresh.Disposition != want {
					t.Fatalf("existing timing/claim lost: %+v %v", fresh, err)
				}
				after, err := f.store.Snapshot(f.ctx, claim.Claim.DeliveryID())
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("paused read/claim changed facts: before=%+v after=%+v err=%v", before, after, err)
				}
				if posture == "live_claim" {
					settled, err := f.store.SettleSuccess(f.ctx, claim.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection())
					if err != nil || settled.Status != "delivered" || settled.ClaimVersion != before.ClaimVersion {
						t.Fatalf("pause revoked in-flight settlement: %+v %v", settled, err)
					}
				} else {
					if _, err := controller.Continue(f.ctx, runtimeruncontrol.TransitionRequest{RunID: f.event.RunID()}); err != nil {
						t.Fatal(err)
					}
					fresh, err = f.store.ClaimDelivery(f.ctx, before.Authority, f.event, route)
					if err != nil || fresh.Disposition != runtimedelivery.ClaimDeferred || !fresh.Snapshot.NextEligibleAt.Equal(before.NextEligibleAt) || fresh.Snapshot.RetryCount != before.RetryCount {
						t.Fatalf("continue accelerated or reset retry: %+v %v", fresh, err)
					}
				}
			})
		}
	}
}

func TestPauseAfterCarrierElectionFencesClaimBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCompleteEventDispatchFixture(t, backend, false)
			ctx, cancel := context.WithTimeout(f.managedContext(t), 10*time.Second)
			defer cancel()
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(f.agentID), AgentIdentity: f.identity}
			work, err := f.store.PipelineObligations().ClaimEvent(ctx, f.event.ID(), runtimepipelineobligation.PurposeRecovery)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.PipelineObligations().Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("audit_handed")); err != nil {
				t.Fatal(err)
			}
			intent, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "global/agents.yaml#agents."+f.agentID+".intent", "Pause claim proof.")
			if err != nil {
				t.Fatal(err)
			}
			f.source = completeEventAgentSource(f.agentID, string(f.event.Type()), intent)
			seen := make(chan events.Event, 4)
			generation := f.newRecordingManager(t, seen)
			if _, err := generation.manager.HydrateForStartup(ctx); err != nil {
				t.Fatal(err)
			}
			if err := generation.manager.Run(ctx); err != nil {
				t.Fatal(err)
			}
			f.startDeliveryContinuations(t, ctx, generation)
			// Restore the exact run-bound recipient through real delivery before
			// testing a new publication; a static receiver is not that identity.
			assertCompleteEventDelivery(t, seen, f.event)
			if err := generation.coordinator.Synchronize(ctx); err != nil {
				t.Fatal(err)
			}
			event := eventtest.InExecutionMode(eventtest.PersistedChildForProducer(uuid.NewString(), f.event.Type(), f.event.Producer(), f.event.TaskID(), f.event.Payload(), f.event.ChainDepth()+1, f.event.RunID(), f.event.ID(), f.event.Envelope(), time.Now().UTC()), executionmode.Mock)
			id, err := runtimedelivery.DeliveryID(event.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			gate := &gatedPublicationElection{Coordinator: generation.coordinator, after: true, id: id, entered: make(chan struct{}), release: make(chan struct{})}
			var release sync.Once
			defer release.Do(func() { close(gate.release) })
			if err := f.bus.SetDeliveryContinuationOwner(gate); err != nil {
				t.Fatal(err)
			}
			controller := runtimeruncontrol.NewController(f.store.(runtimeruncontrol.Store), f.bus, runtimeruncontrol.Options{})
			f.bus.SetRunDispatchGate(controller)
			published := make(chan error, 1)
			go func() { published <- f.bus.PublishDirectRoutes(ctx, event, []events.DeliveryRoute{route}) }()
			select {
			case <-gate.entered:
			case err := <-published:
				t.Fatalf("did not reach elected carrier: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if _, err := controller.Pause(ctx, runtimeruncontrol.TransitionRequest{RunID: event.RunID(), Reason: "pause-after-election"}); err != nil {
				t.Fatal(err)
			}
			before, err := f.store.Snapshot(ctx, id)
			if err != nil || before.ClaimVersion != 0 || before.Status != "pending" {
				t.Fatalf("preclaim checkpoint: %+v %v", before, err)
			}
			release.Do(func() { close(gate.release) })
			select {
			case err := <-published:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := generation.coordinator.Synchronize(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-seen:
				t.Fatalf("pause admitted OnEvent: %s", got.ID())
			default:
			}
			after, err := f.store.Snapshot(ctx, id)
			if err != nil || after.ClaimVersion != 0 || after.Status != "pending" || after.RetryCount != before.RetryCount {
				t.Fatalf("pause admitted attempt: %+v %v", after, err)
			}
			if _, err := controller.Continue(ctx, runtimeruncontrol.TransitionRequest{RunID: event.RunID()}); err != nil {
				t.Fatal(err)
			}
			assertCompleteEventDelivery(t, seen, event)
			if err := generation.coordinator.Synchronize(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-seen:
				t.Fatalf("duplicate OnEvent: %s", got.ID())
			default:
			}
		})
	}
}
