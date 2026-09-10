package bus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type gatedPublicationElection struct {
	*deliverycontinuation.Coordinator
	mu      sync.Mutex
	first   bool
	after   bool
	id      string
	entered chan struct{}
	release chan struct{}
}

func (g *gatedPublicationElection) Acquire(id string) (worklifetime.DeliveryAcquisition, error) {
	g.mu.Lock()
	first := id == g.id && !g.first
	if first {
		g.first = true
	}
	g.mu.Unlock()
	if first && !g.after {
		close(g.entered)
		<-g.release
	}
	result, err := g.Coordinator.Acquire(id)
	if first && g.after {
		close(g.entered)
		<-g.release
	}
	return result, err
}

func TestPublicationAndStartupElectOneExactCarrierBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, publicationWins := range []bool{false, true} {
			name := "scan_wins"
			if publicationWins {
				name = "publication_wins"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				f := newCompleteEventDispatchFixture(t, backend, false)
				ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
				defer cancel()
				route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(f.agentID), AgentIdentity: f.identity}
				// Finish the fixture's historical delivery through the real owner.
				old, err := storetest.ClaimDelivery(ctx, f.store, f.event, route)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.SettleSuccess(ctx, old.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
					t.Fatal(err)
				}
				snapshot, err := f.store.Snapshot(ctx, old.Claim.DeliveryID())
				if err != nil {
					t.Fatal(err)
				}
				if err := f.bus.SetDeliveryAuthority(snapshot.Authority); err != nil {
					t.Fatal(err)
				}
				if err := f.store.ActivateDeliveryAuthority(ctx, snapshot.Authority); err != nil {
					t.Fatal(err)
				}
				process := worklifetime.NewProcess()
				owner, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: snapshot.Authority.ExecutionID(), BundleHash: "publication-election"})
				if err != nil {
					t.Fatal(err)
				}
				coordinator, err := deliverycontinuation.New(f.store, f.store, snapshot.Authority, owner, f.bus, nil)
				if err != nil {
					t.Fatal(err)
				}
				evt := eventtest.InExecutionMode(eventtest.PersistedChildForProducer(
					uuid.NewString(), f.event.Type(), f.event.Producer(), f.event.TaskID(), f.event.Payload(),
					f.event.ChainDepth()+1, f.event.RunID(), f.event.ID(), f.event.Envelope(), time.Now().UTC(),
				), executionmode.Mock)
				id, err := runtimedelivery.DeliveryID(evt.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				gate := &gatedPublicationElection{Coordinator: coordinator, after: publicationWins, id: id, entered: make(chan struct{}), release: make(chan struct{})}
				var release sync.Once
				t.Cleanup(func() {
					release.Do(func() { close(gate.release) })
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
				if err := f.bus.SetDeliveryContinuationOwner(gate); err != nil {
					t.Fatal(err)
				}
				deliveries := f.subscribe(t, evt.Type())
				defer runtimebustest.UnsubscribeIdentity(f.bus, f.identity)
				published := make(chan error, 1)
				go func() { published <- f.bus.PublishDirectRoutes(ctx, evt, []events.DeliveryRoute{route}) }()
				select {
				case <-gate.entered:
				case err := <-published:
					t.Fatalf("publication did not reach election: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				// The publication is paused precisely before or after the same
				// atomic Acquire that the real startup dispatcher now consumes.
				if err := coordinator.Start(ctx); err != nil {
					t.Fatal(err)
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
				select {
				case delivery := <-deliveries:
					if delivery.Event().ID() != evt.ID() {
						t.Fatal("wrong carrier")
					}
					claim, err := f.store.ClaimDelivery(ctx, snapshot.Authority, delivery.Event(), route)
					if err != nil {
						t.Fatal(err)
					}
					acquired, ok := claim.Acquired()
					if !ok {
						t.Fatalf("winner has no canonical claim: %+v", claim)
					}
					guard, err := worklifetime.NewEventDeliveryCarrierGuard(delivery)
					if err != nil {
						t.Fatal(err)
					}
					if resolution, err := guard.Consume(nil); err != nil || resolution != worklifetime.DeliveryContinuationConsumed {
						t.Fatalf("consume: %v %v", resolution, err)
					}
					if _, err := f.store.SettleSuccess(ctx, acquired.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
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
				select {
				case duplicate := <-deliveries:
					_ = duplicate.Complete()
					t.Fatal("duplicate business carrier")
				default:
				}
				final, err := f.store.Snapshot(ctx, id)
				if err != nil || final.Status != runtimedelivery.StatusDelivered {
					t.Fatalf("terminal readback: %+v %v", final, err)
				}
				outcomes, err := f.store.Outcomes(ctx, id)
				if err != nil || len(outcomes) != 1 {
					t.Fatalf("outcomes: %+v %v", outcomes, err)
				}
			})
		}
	}
}
