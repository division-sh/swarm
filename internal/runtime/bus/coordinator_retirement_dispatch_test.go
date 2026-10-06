package bus

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

type retirementBusCarrier struct {
	worklifetime.DeliveryContinuation
	returns atomic.Int32
}

func (c *retirementBusCarrier) Resolve(ctx context.Context, intent worklifetime.DeliveryContinuationIntent) (worklifetime.DeliveryContinuationResolution, error) {
	if intent == worklifetime.DeliveryContinuationReturn || intent == worklifetime.DeliveryContinuationReturnUnqueued {
		c.returns.Add(1)
	}
	return c.DeliveryContinuation.Resolve(ctx, intent)
}

type retirementBusElection struct {
	*deliverycontinuation.Coordinator
	after   bool
	entered chan struct{}
	release chan struct{}
	carrier *retirementBusCarrier
	refusal error
}

func (g *retirementBusElection) Acquire(id string) (worklifetime.DeliveryAcquisition, error) {
	if !g.after {
		close(g.entered)
		<-g.release
	}
	acquisition, err := g.Coordinator.Acquire(id)
	g.refusal = err
	if err == nil && g.after {
		if capability, acquired := acquisition.Acquired(); acquired {
			g.carrier = &retirementBusCarrier{DeliveryContinuation: capability}
			acquisition = worklifetime.AcquiredDelivery(g.carrier)
		}
		close(g.entered)
		<-g.release
	}
	return acquisition, err
}

type retirementBusDispatcher struct {
	bus    *EventBus
	result chan deliverycontinuation.DispatchResult
}

func (d retirementBusDispatcher) DispatchDeliveryContinuation(ctx context.Context, event events.Event, route events.DeliveryRoute) deliverycontinuation.DispatchResult {
	result := d.bus.DispatchDeliveryContinuation(ctx, event, route)
	d.result <- result
	return result
}

func TestCoordinatorRetirementRealEventBusDispatch(t *testing.T) {
	for _, selected := range []bool{false, true} {
		name := "normal"
		if selected {
			name = "selected"
		}
		for _, after := range []bool{false, true} {
			cut := "before_acquire"
			if after {
				cut = "after_acquire"
			}
			t.Run(name+"/"+cut, func(t *testing.T) {
				process := worklifetime.NewProcess()
				var owner worklifetime.Occurrence
				var retireOwner func(context.Context) error
				runID := uuid.NewString()
				opts := EventBusOptions{}
				if selected {
					executionID := uuid.NewString()
					occurrence, err := process.NewSelectedFork(context.Background(), worklifetime.SelectedForkIdentity{ExecutionID: executionID, RunID: runID, Generation: 1})
					if err != nil {
						t.Fatal(err)
					}
					owner, retireOwner = occurrence, occurrence.RetireAndWait
					opts.ReceiverExecution = newSelectedReceiverProjectionExecution(t, executionID, runID)
					opts.DeliveryAuthority, err = runtimedelivery.NewSelectedExecutionAuthority(authorActivityTestSourceArtifactFact, executionID, runID, 1)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					occurrence := newReceiverProjectionRuntimeOwner(t, process, "coordinator-retirement")
					owner = occurrence
					retireOwner = func(ctx context.Context) error { _, err := occurrence.RetireAndWait(ctx); return err }
				}
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := retireOwner(ctx); err != nil {
						t.Error(err)
					}
					process.Retire()
					if _, err := process.Join(ctx); err != nil {
						t.Error(err)
					}
				})
				opts.WorkOwner = owner
				eb, err := newScopedTestEventBus(InMemoryEventStore{}, opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := eb.ResetInMemoryState(); err != nil {
						t.Error(err)
					}
				})
				eb.durable.RunOrigins = &recoveryOriginStore{}
				event := eventtest.RunCreatingRootIngress(uuid.NewString(), "custom.retirement", "", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
				route := nodeOnlyDeliveryPlan(t, event, "retirement-node").DeliveryRoutes()[0]
				id, err := runtimedelivery.DeliveryID(event.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				authority, err := eb.DeliveryAuthority()
				if err != nil {
					t.Fatal(err)
				}
				scan := recoveryReadScan{item: runtimedelivery.ContinuationItem{DeliveryID: id, Event: event, Disposition: runtimedelivery.ClaimAcquired, Snapshot: runtimedelivery.Snapshot{DeliveryID: id, Route: route, Authority: authority, Status: runtimedelivery.StatusPending}}}
				reported := make(chan error, 2)
				result := make(chan deliverycontinuation.DispatchResult, 1)
				dispatcher := retirementBusDispatcher{bus: eb, result: result}
				var c *deliverycontinuation.Coordinator
				if selected {
					c, err = deliverycontinuation.NewSelected(scan, authority, owner, dispatcher, func(_ context.Context, err error) { reported <- err })
				} else {
					c, err = deliverycontinuation.New(scan, &recoveryOriginStore{}, authority, owner, dispatcher, func(_ context.Context, err error) { reported <- err })
				}
				if err != nil {
					t.Fatal(err)
				}
				gate := &retirementBusElection{Coordinator: c, after: after, entered: make(chan struct{}), release: make(chan struct{})}
				t.Cleanup(func() {
					select {
					case <-gate.release:
					default:
						close(gate.release)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := c.Retire(ctx); err != nil {
						t.Error(err)
					}
				})
				if err := eb.SetDeliveryContinuationOwner(gate); err != nil {
					t.Fatal(err)
				}
				var business atomic.Int32
				eb.SetInterceptors(electionNodeInterceptor{resolve: func(context.Context, events.Event, events.DeliveryRoute) error {
					business.Add(1)
					return errors.New("retirement allowed receiver execution")
				}})
				if err := c.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				select {
				case <-gate.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("real EventBus did not reach exact acquisition barrier")
				}
				c.BeginRetirement()
				waitCtx, cancelWait := context.WithCancel(context.Background())
				cancelWait()
				if err := c.Retire(waitCtx); !errors.Is(err, context.Canceled) {
					t.Fatalf("retirement waiter lost cancellation: %v", err)
				}
				joined := make(chan error, 1)
				go func() { joined <- c.Retire(context.Background()) }()
				select {
				case err := <-joined:
					t.Fatalf("retirement joined before held carrier cleanup: %v", err)
				default:
				}
				close(gate.release)
				select {
				case err := <-joined:
					if err != nil {
						t.Fatalf("real EventBus retirement was fatal: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("real EventBus carrier did not join")
				}
				actual := <-result
				if actual.Validate() != nil || actual.Disposition() != deliverycontinuation.DispatchFatal {
					t.Fatalf("EventBus lost its typed refusal: %+v", actual)
				}
				if after {
					if !errors.Is(actual.Failure(), context.Canceled) || gate.carrier == nil || gate.carrier.returns.Load() != 1 {
						t.Fatalf("accepted carrier was not returned exactly once: result=%+v carrier=%+v", actual, gate.carrier)
					}
					if _, err := gate.carrier.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err == nil {
						t.Fatal("retired carrier returned twice")
					}
				} else if gate.refusal == nil || !errors.Is(actual.Failure(), gate.refusal) {
					t.Fatalf("exact retirement refusal lost: %+v", actual)
				}
				if business.Load() != 0 {
					t.Fatal("receiver executed after retirement")
				}
				select {
				case err := <-reported:
					t.Fatalf("owned retirement reported failure: %v", err)
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := owner.Wait(ctx); err != nil {
					t.Fatalf("retirement left accepted work: %v", err)
				}
				if _, err := c.Acquire(id); err == nil {
					t.Fatal("retired normal/selected owner admitted another carrier")
				}
			})
		}
	}
}
