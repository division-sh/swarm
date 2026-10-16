package bus_test

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type observedAgentTransitionDispatch struct {
	bus     *runtimebus.EventBus
	results chan deliverycontinuation.DispatchResult
}

func (d observedAgentTransitionDispatch) DispatchDeliveryContinuation(ctx context.Context, event events.Event, route events.DeliveryRoute) deliverycontinuation.DispatchResult {
	result := d.bus.DispatchDeliveryContinuation(ctx, event, route)
	select {
	case d.results <- result:
	case <-ctx.Done():
	}
	return result
}

func TestCommittedAgentTransitionNativePublicationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, disposition := range []string{"live", "retired", "wrong_generation"} {
			t.Run(backend+"/"+disposition, func(t *testing.T) {
				f := newExecutableCompleteEventDispatchFixture(t, backend, nil, nil)
				if err := f.generation.manager.Run(f.ctx); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
				defer cancel()
				route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(f.agentID), AgentIdentity: f.identity}
				old, err := storetest.ClaimDelivery(ctx, f.store, f.event, route)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.SettleSuccess(ctx, old.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
					t.Fatal(err)
				}
				original, err := f.store.Snapshot(ctx, old.Claim.DeliveryID())
				if err != nil {
					t.Fatal(err)
				}
				if err := f.bus.SetDeliveryAuthority(original.Authority); err != nil {
					t.Fatal(err)
				}
				if err := f.store.ActivateDeliveryAuthority(ctx, original.Authority); err != nil {
					t.Fatal(err)
				}
				process := worklifetime.NewProcess()
				owner, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: original.Authority.ExecutionID(), BundleHash: original.Authority.SourceArtifact().BundleHash()})
				if err != nil {
					t.Fatal(err)
				}
				results := make(chan deliverycontinuation.DispatchResult, 8)
				coordinatorAuthority := original.Authority
				if disposition == "wrong_generation" {
					coordinatorAuthority, err = deliverylifecycle.NewNormalExecutionAuthority(original.Authority.SourceArtifact(), uuid.NewString(), 2)
					if err != nil {
						t.Fatal(err)
					}
				}
				coordinator, err := deliverycontinuation.New(f.store, f.store, coordinatorAuthority, owner, observedAgentTransitionDispatch{bus: f.bus, results: results}, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := coordinator.Retire(context.Background()); err != nil {
						t.Error(err)
					}
					_, _ = owner.RetireAndWait(context.Background())
					process.Retire()
					_, _ = process.Join(context.Background())
				})
				if err := f.bus.SetDeliveryContinuationOwner(coordinator); err != nil {
					t.Fatal(err)
				}
				if disposition == "retired" {
					if err := coordinator.Retire(ctx); err != nil {
						t.Fatal(err)
					}
				}
				var held atomic.Bool
				held.Store(true)
				f.bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(func(context.Context, events.Event, []events.DeliveryRoute) error {
					if held.Load() {
						return runtimebus.ErrCommittedAgentRouteTransition
					}
					return nil
				}))
				deliveries := f.subscribe(t, f.event.Type())
				defer bustest.UnsubscribeIdentity(f.bus, f.identity)
				event := eventtest.InExecutionMode(eventtest.PersistedChildForProducer(uuid.NewString(), f.event.Type(), f.event.Producer(), f.event.TaskID(), f.event.Payload(),
					f.event.ChainDepth()+1, f.event.RunID(), f.event.ID(), f.event.Envelope(), time.Now().UTC()), executionmode.Mock)
				// This invokes native COMMIT, shared consequence finalization and its
				// existing publication settlement, not a pre-settled delivery fixture.
				publishErr := f.bus.PublishDirectRoutes(ctx, event, []events.DeliveryRoute{route})
				id, err := deliverylifecycle.DeliveryID(event.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				before, err := f.store.Snapshot(ctx, id)
				if err != nil || before.Status != deliverylifecycle.StatusPending || before.ClaimVersion != 0 {
					t.Fatalf("native ACK directly claimed a held actor: %+v err=%v", before, err)
				}
				if disposition != "live" {
					if publishErr == nil {
						t.Fatal("unavailable coordinator silently accepted the transition")
					}
					work, err := f.store.PipelineObligations().ClaimEvent(ctx, event.ID(), pipelineobligation.PurposeRecovery)
					if err != nil {
						t.Fatalf("refused transfer lost durable recovery: %v", err)
					}
					if err := f.store.PipelineObligations().Release(ctx, work.Claim); err != nil {
						t.Fatal(err)
					}
					current, err := f.store.Snapshot(ctx, id)
					if err != nil || !reflect.DeepEqual(before, current) {
						t.Fatalf("refused coordinator changed exact delivery: %+v err=%v", current, err)
					}
					return
				}
				if publishErr != nil {
					t.Fatalf("acknowledged native transition publication: %v", publishErr)
				}
				if err := coordinator.Start(ctx); err != nil {
					t.Fatal(err)
				}
				select {
				case result := <-results:
					if result.Disposition() != deliverycontinuation.DispatchDeferred || result.WakeAuthority() != deliverycontinuation.DispatchWakeAgentRouteLifecycle {
						t.Fatalf("native transfer lost its exact topology wake: %+v", result)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				after, err := f.store.Snapshot(ctx, id)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("deferred native handoff mutated its claim: %+v err=%v", after, err)
				}
				select {
				case delivery := <-deliveries:
					_ = delivery.Complete()
					t.Fatal("held actor received ordinary direct dispatch")
				default:
				}
				held.Store(false)
				coordinator.Signal()
				select {
				case delivery := <-deliveries:
					if delivery.Event().ID() != event.ID() {
						t.Fatal("topology wake borrowed another event")
					}
					claim, err := f.store.ClaimDelivery(ctx, original.Authority, delivery.Event(), route)
					if err != nil {
						t.Fatal(err)
					}
					acquired, ok := claim.Acquired()
					if !ok || acquired.Claim.Version() != 1 {
						t.Fatalf("topology wake did not claim once: %+v", claim)
					}
					guard, err := worklifetime.NewEventDeliveryCarrierGuard(delivery)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := guard.Consume(nil); err != nil {
						t.Fatal(err)
					}
					if _, err := f.store.SettleSuccess(ctx, acquired.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
					if err := coordinator.Release(id); err != nil {
						t.Fatal(err)
					}
					if _, err := guard.Complete(nil); err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if err := coordinator.Synchronize(ctx); err != nil {
					t.Fatal(err)
				}
				final, err := f.store.Snapshot(ctx, id)
				if err != nil || final.Status != deliverylifecycle.StatusDelivered || final.ClaimVersion != 1 {
					t.Fatalf("native final handoff: %+v err=%v", final, err)
				}
				outcomes, err := f.store.Outcomes(ctx, id)
				if err != nil || len(outcomes) != 1 {
					t.Fatalf("native settlement was repeated: %+v err=%v", outcomes, err)
				}
			})
		}
	}
}
