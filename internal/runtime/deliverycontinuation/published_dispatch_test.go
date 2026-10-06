package deliverycontinuation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type publishedDispatchTestFixture struct {
	coordinator *Coordinator
	store       *coordinatorTestStore
	authority   runtimedelivery.ExecutionAuthority
	owner       *worklifetime.RuntimeOccurrence
}

func newPublishedDispatchTestFixture(t *testing.T, dispatcher Dispatcher, report ErrorReporter) *publishedDispatchTestFixture {
	t.Helper()
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	t.Cleanup(cleanup)
	store := &coordinatorTestStore{observations: make(map[string]runtimedelivery.ContinuationObservation)}
	c, err := New(store, coordinatorTestRestarts{}, authority, owner, dispatcher, report)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Retire(ctx); errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("published dispatch cleanup did not drain: %v", err)
		}
	})
	return &publishedDispatchTestFixture{coordinator: c, store: store, authority: authority, owner: owner}
}

func publishedDispatchTestNodeRoute(t *testing.T, name string) events.DeliveryRoute {
	t.Helper()
	return events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(identitytest.RootNode(t, name)),
		Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowInstance: "published/" + name}),
	}
}

func publishedDispatchTestProof(t *testing.T, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute) runtimedelivery.DurableHandoffProof {
	t.Helper()
	id, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := route.Identity()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := runtimedelivery.AdmitDurableHandoffProof(id, event.ID(), events.EncodeDeliveryRouteIdentity(identity), authority)
	if err != nil {
		t.Fatal(err)
	}
	return proof
}

func (f *publishedDispatchTestFixture) accept(t *testing.T, event events.Event, routes ...events.DeliveryRoute) {
	t.Helper()
	proofs := make([]runtimedelivery.DurableHandoffProof, 0, len(routes))
	for _, route := range routes {
		proof := publishedDispatchTestProof(t, f.authority, event, route)
		proofs = append(proofs, proof)
		// Empty scans must not reclaim an attempt or terminalize the fake's proof.
		f.store.mu.Lock()
		f.store.observations[proof.DeliveryID()] = runtimedelivery.ContinuationObservation{
			DeliveryID: proof.DeliveryID(), Disposition: runtimedelivery.ClaimDeferred,
		}
		f.store.mu.Unlock()
	}
	if err := f.coordinator.AcceptCommitted(proofs); err != nil {
		t.Fatal(err)
	}
}

func (f *publishedDispatchTestFixture) enqueueScan(t *testing.T, event events.Event, route events.DeliveryRoute) {
	t.Helper()
	proof := publishedDispatchTestProof(t, f.authority, event, route)
	item := runtimedelivery.ContinuationItem{
		DeliveryID: proof.DeliveryID(), Event: event, Disposition: runtimedelivery.ClaimAcquired,
		Snapshot: runtimedelivery.Snapshot{
			DeliveryID: proof.DeliveryID(), RunID: event.RunID(), Route: route,
			Authority: f.authority, Status: runtimedelivery.StatusPending,
		},
	}
	f.store.mu.Lock()
	f.store.pages = append(f.store.pages, runtimedelivery.ContinuationPage{
		Items: []runtimedelivery.ContinuationItem{item}, Exhausted: true,
	})
	f.store.mu.Unlock()
}

func publishedDispatchTestSynchronize(t *testing.T, c *Coordinator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Synchronize(ctx); err != nil {
		t.Fatal(err)
	}
}

func publishedDispatchTestQuiesce(t *testing.T, owner *worklifetime.RuntimeOccurrence) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := owner.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("published work did not settle: %v", err)
	}
}

func publishedDispatchTestRelease(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	return release, unblock
}

type publishedDispatchTestCall struct {
	ctx   context.Context
	event events.Event
	route events.DeliveryRoute
}

type publishedDispatchTestGate struct {
	dispatcher Dispatcher
	entered    chan publishedDispatchTestCall
	release    <-chan struct{}
}

func (g *publishedDispatchTestGate) DispatchDeliveryContinuation(ctx context.Context, event events.Event, route events.DeliveryRoute) DispatchResult {
	select {
	case g.entered <- publishedDispatchTestCall{ctx: ctx, event: event, route: route}:
	case <-ctx.Done():
		return Fatal(ctx.Err())
	}
	select {
	case <-g.release:
		return g.dispatcher.DispatchDeliveryContinuation(ctx, event, route)
	case <-ctx.Done():
		return Fatal(ctx.Err())
	}
}

func TestCoordinatorDispatchPublishedUsesConfiguredDispatcherAndGate(t *testing.T) {
	downstream := &coordinatorTestDispatcher{result: TerminallySettled()}
	gate := &publishedDispatchTestGate{dispatcher: downstream, entered: make(chan publishedDispatchTestCall, 2)}
	f := newPublishedDispatchTestFixture(t, gate, nil)
	release, unblock := publishedDispatchTestRelease(t)
	gate.release = release
	if err := f.coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	published, recovered := coordinatorTestEvent("published-gate"), coordinatorTestEvent("recovery-gate")
	publicationRoute, recoveryRoute := publishedDispatchTestNodeRoute(t, "published"), publishedDispatchTestNodeRoute(t, "recovered")
	f.accept(t, published, publicationRoute)
	f.accept(t, recovered, recoveryRoute)
	if err := f.coordinator.DispatchPublished(published, []events.DeliveryRoute{publicationRoute}); err != nil {
		t.Fatal(err)
	}
	f.enqueueScan(t, recovered, recoveryRoute)
	publishedDispatchTestSynchronize(t, f.coordinator)
	want := map[string]publishedDispatchTestCall{
		published.ID(): {event: published, route: publicationRoute},
		recovered.ID(): {event: recovered, route: recoveryRoute},
	}
	for range 2 {
		select {
		case call := <-gate.entered:
			expected, ok := want[call.event.ID()]
			if !ok || !reflect.DeepEqual(call.event, expected.event) || !reflect.DeepEqual(call.route, expected.route) {
				t.Fatalf("configured gate received a changed or duplicate route: %+v", call)
			}
			if owner, ok := worklifetime.RuntimeOccurrenceFromContext(call.ctx); !ok || owner != f.owner {
				t.Fatal("configured dispatcher did not receive the admitted runtime context")
			}
			delete(want, call.event.ID())
		case <-time.After(5 * time.Second):
			t.Fatal("publication or scan bypassed the configured gate")
		}
	}
	if downstream.callCount() != 0 || f.owner.ActiveCount() != 3 {
		t.Fatalf("gate bypass or unowned dispatch: calls=%d leases=%d", downstream.callCount(), f.owner.ActiveCount())
	}
	unblock()
	publishedDispatchTestQuiesce(t, f.owner)
	if err := f.coordinator.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if downstream.callCount() != 2 {
		t.Fatalf("configured downstream calls=%d, want one publication and one scan", downstream.callCount())
	}
}

func TestCoordinatorDispatchPublishedRequiresExactCommittedOwnership(t *testing.T) {
	for _, phase := range []string{"before_start", "started"} {
		for _, scenario := range []string{"missing", "different_event", "different_node", "different_target", "foreign_generation"} {
			t.Run(phase+"/"+scenario, func(t *testing.T) {
				dispatcher := &coordinatorTestDispatcher{}
				f := newPublishedDispatchTestFixture(t, dispatcher, nil)
				var baseline uint64
				if phase == "started" {
					if err := f.coordinator.Start(context.Background()); err != nil {
						t.Fatal(err)
					}
					baseline = 1
				}
				event, route := coordinatorTestEvent("exact-proof"), publishedDispatchTestNodeRoute(t, "exact-proof")
				proofEvent, proofRoute := event, route
				switch scenario {
				case "different_event":
					proofEvent = coordinatorTestEvent("other-proof-event")
				case "different_node":
					proofRoute.Recipient = events.MustNodeDeliveryRecipient(identitytest.RootNode(t, "other-node"))
				case "different_target":
					proofRoute.Target = events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowInstance: "published/other-instance"})
				case "foreign_generation":
					foreign, err := runtimedelivery.NewNormalExecutionAuthority(f.authority.SourceArtifact(), f.authority.ExecutionID(), f.authority.Generation()+1)
					if err != nil {
						t.Fatal(err)
					}
					proof := publishedDispatchTestProof(t, foreign, event, route)
					if err := f.coordinator.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err == nil {
						t.Fatal("foreign proof was accepted by this generation")
					}
				}
				if scenario != "missing" && scenario != "foreign_generation" {
					f.accept(t, proofEvent, proofRoute)
				}
				if err := f.coordinator.DispatchPublished(event, []events.DeliveryRoute{route}); err == nil || !strings.Contains(err.Error(), "no committed continuation evidence") {
					t.Fatalf("non-exact publication was admitted: %v", err)
				}
				if f.owner.ActiveCount() != baseline {
					t.Fatalf("refused publication admitted work: %d leases", f.owner.ActiveCount())
				}
				if err := f.coordinator.Retire(context.Background()); err != nil {
					t.Fatal(err)
				}
				if dispatcher.callCount() != 0 {
					t.Fatal("dispatcher executed without exact committed ownership")
				}
			})
		}
	}
}

func TestCoordinatorDispatchPublishedRejectsInvalidRoutesAtomically(t *testing.T) {
	for _, phase := range []string{"before_start", "started"} {
		for _, scenario := range []string{"empty", "duplicate", "non_node", "missing_target", "valid_then_missing", "valid_then_non_node", "valid_then_duplicate"} {
			t.Run(phase+"/"+scenario, func(t *testing.T) {
				dispatcher := &coordinatorTestDispatcher{}
				f := newPublishedDispatchTestFixture(t, dispatcher, nil)
				var baseline uint64
				if phase == "started" {
					if err := f.coordinator.Start(context.Background()); err != nil {
						t.Fatal(err)
					}
					baseline = 1
				}
				event, route := coordinatorTestEvent("invalid-routes"), publishedDispatchTestNodeRoute(t, "valid")
				agentRoute := coordinatorTestAgentRoute(t, "not-a-node")
				f.accept(t, event, route, agentRoute)
				var routes []events.DeliveryRoute
				switch scenario {
				case "duplicate":
					routes = []events.DeliveryRoute{route, route}
				case "non_node":
					routes = []events.DeliveryRoute{agentRoute}
				case "missing_target":
					routes = []events.DeliveryRoute{{Recipient: route.Recipient}}
				case "valid_then_missing":
					routes = []events.DeliveryRoute{route, publishedDispatchTestNodeRoute(t, "uncommitted")}
				case "valid_then_non_node":
					routes = []events.DeliveryRoute{route, agentRoute}
				case "valid_then_duplicate":
					second := publishedDispatchTestNodeRoute(t, "second-valid")
					f.accept(t, event, second)
					routes = []events.DeliveryRoute{route, second, route}
				}
				if err := f.coordinator.DispatchPublished(event, routes); err == nil {
					t.Fatal("invalid publication route batch was accepted")
				}
				if f.owner.ActiveCount() != baseline {
					t.Fatalf("invalid suffix admitted a valid prefix: %d leases", f.owner.ActiveCount())
				}
				if err := f.coordinator.Retire(context.Background()); err != nil {
					t.Fatal(err)
				}
				if dispatcher.callCount() != 0 {
					t.Fatal("invalid batch partially dispatched")
				}
			})
		}
	}
}

func TestCoordinatorDispatchPublishedCompetesWithScanWithoutDuplicateExecution(t *testing.T) {
	for _, first := range []string{"publication", "scan"} {
		t.Run(first+"_wins", func(t *testing.T) {
			winnerEntered, loserEntered, winnerSettled := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var winners, executions, calls atomic.Int32
			var loserOnce sync.Once
			var f *publishedDispatchTestFixture
			var release <-chan struct{}
			dispatcher := electionDispatcher(func(ctx context.Context, event events.Event, route events.DeliveryRoute) DispatchResult {
				calls.Add(1)
				id, err := runtimedelivery.DeliveryID(event.ID(), route)
				if err != nil {
					return Fatal(err)
				}
				acquisition, err := f.coordinator.Acquire(id)
				if err != nil {
					return Fatal(err)
				}
				if err := acquisition.Validate(id); err != nil {
					return Fatal(err)
				}
				carrier, acquired := acquisition.Acquired()
				if !acquired {
					if acquisition.Disposition() != worklifetime.DeliveryAlreadyOwned {
						return Fatal(errors.New("competing dispatch lost without an exact owner"))
					}
					loserOnce.Do(func() { close(loserEntered) })
					return AlreadyOwned()
				}
				if winners.Add(1) != 1 {
					return Fatal(errors.New("scan and publication both acquired the delivery"))
				}
				close(winnerEntered)
				<-release
				if resolution, err := carrier.Resolve(ctx, worklifetime.DeliveryContinuationConsume); err != nil || resolution != worklifetime.DeliveryContinuationConsumed {
					return Fatal(fmt.Errorf("winning carrier consume: resolution=%d err=%v", resolution, err))
				}
				executions.Add(1)
				if err := f.coordinator.Release(id); err != nil {
					return Fatal(err)
				}
				close(winnerSettled)
				return Transferred()
			})
			f = newPublishedDispatchTestFixture(t, dispatcher, nil)
			var unblock func()
			release, unblock = publishedDispatchTestRelease(t)
			if err := f.coordinator.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			event, route := coordinatorTestEvent("competing-publication"), publishedDispatchTestNodeRoute(t, "competing")
			f.accept(t, event, route)
			publish := func() {
				if err := f.coordinator.DispatchPublished(event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
			}
			scan := func() {
				f.enqueueScan(t, event, route)
				publishedDispatchTestSynchronize(t, f.coordinator)
			}
			if first == "publication" {
				publish()
			} else {
				scan()
			}
			awaitRetirementProof(t, winnerEntered)
			if first == "publication" {
				scan()
			} else {
				publish()
			}
			awaitRetirementProof(t, loserEntered)
			if winners.Load() != 1 || executions.Load() != 0 {
				t.Fatalf("competing winners=%d executions=%d, want one held carrier", winners.Load(), executions.Load())
			}
			unblock()
			awaitRetirementProof(t, winnerSettled)
			if err := f.coordinator.Retire(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 || executions.Load() != 1 {
				t.Fatalf("calls=%d executions=%d, want two elections and one execution", calls.Load(), executions.Load())
			}
		})
	}
}

func TestCoordinatorDispatchPublishedDrainsAdmittedWork(t *testing.T) {
	for _, stop := range []string{"retire", "parent_cancel", "retire_joined_failure", "parent_cancel_joined_failure"} {
		t.Run(stop, func(t *testing.T) {
			parent, cancelParent := context.WithCancel(context.Background())
			entered, canceled := make(chan struct{}), make(chan struct{})
			reported := make(chan error, 2)
			var want error
			if strings.HasSuffix(stop, "joined_failure") {
				want = errors.New("independent published dispatch failure after cancellation")
			}
			var f *publishedDispatchTestFixture
			var release <-chan struct{}
			dispatcher := electionDispatcher(func(ctx context.Context, event events.Event, route events.DeliveryRoute) DispatchResult {
				id, err := runtimedelivery.DeliveryID(event.ID(), route)
				if err != nil {
					return Fatal(err)
				}
				carrier, err := acquireCoordinatorTestCapability(f.coordinator, id)
				if err != nil {
					return Fatal(err)
				}
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-release
				if resolution, err := carrier.Resolve(ctx, worklifetime.DeliveryContinuationReturnUnqueued); err != nil || resolution != worklifetime.DeliveryContinuationReturned {
					return Fatal(fmt.Errorf("retiring carrier return: resolution=%d err=%v", resolution, err))
				}
				if _, err := carrier.Resolve(ctx, worklifetime.DeliveryContinuationReturnUnqueued); err == nil {
					return Fatal(errors.New("retiring carrier returned twice"))
				}
				return Fatal(errors.Join(ctx.Err(), want))
			})
			f = newPublishedDispatchTestFixture(t, dispatcher, func(_ context.Context, err error) { reported <- err })
			t.Cleanup(cancelParent)
			var unblock func()
			release, unblock = publishedDispatchTestRelease(t)
			if err := f.coordinator.Start(parent); err != nil {
				t.Fatal(err)
			}
			event, route := coordinatorTestEvent("draining-publication"), publishedDispatchTestNodeRoute(t, "draining")
			f.accept(t, event, route)
			if err := f.coordinator.DispatchPublished(event, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			awaitRetirementProof(t, entered)
			retired := make(chan error, 1)
			if strings.HasPrefix(stop, "retire") {
				go func() { retired <- f.coordinator.Retire(context.Background()) }()
			} else {
				cancelParent()
			}
			awaitRetirementProof(t, canceled)
			select {
			case <-f.coordinator.done:
				t.Fatal("coordinator retired before admitted publication work settled")
			default:
			}
			if f.owner.ActiveCount() != 2 {
				t.Fatalf("canceled publication lost its standing/dispatch leases: %d", f.owner.ActiveCount())
			}
			if err := f.coordinator.DispatchPublished(event, []events.DeliveryRoute{route}); !errors.Is(err, errCoordinatorRetired) && !errors.Is(err, context.Canceled) {
				t.Fatalf("stopped coordinator admitted another publication: %v", err)
			}
			unblock()
			assertRetirementResult(t, f.coordinator, f.owner, reported, want, true)
			if strings.HasPrefix(stop, "retire") {
				select {
				case err := <-retired:
					if !errors.Is(err, want) || want == nil && err != nil {
						t.Fatalf("retirement result=%v, want %v", err, want)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("retirement did not join the admitted publication")
				}
			}
		})
	}
}

func TestCoordinatorDispatchPublishedPreservesDispatchResults(t *testing.T) {
	fault := errors.New("published dispatcher failure")
	type resultCase struct {
		name   string
		result DispatchResult
		state  ownershipState
		fatal  bool
		cause  error
	}
	cases := []resultCase{
		{name: "transferred", result: Transferred(), state: ownershipAttempt},
		{name: "already_owned", result: AlreadyOwned(), state: ownershipCarrier},
		{name: "terminal", result: TerminallySettled(), state: ownershipTerminal},
		{name: "fatal", result: Fatal(fault), fatal: true, cause: fault},
		{name: "joined_independent_failure", result: Fatal(errors.Join(context.Canceled, fault)), fatal: true, cause: fault},
		{name: "unowned_cancellation", result: Fatal(context.Canceled), fatal: true, cause: context.Canceled},
		{name: "invalid_result", result: DispatchResult{}, fatal: true},
		{name: "deferred_without_wake", result: Deferred(0), fatal: true},
	}
	for _, wake := range []DispatchWakeAuthority{
		DispatchWakeAgentRouteLifecycle, DispatchWakeInternalSubscriptionLifecycle, DispatchWakeCarrierReturn,
		DispatchWakeDeliveryLifecycle, DispatchWakeRunContinue, DispatchWakeIngressContinue,
	} {
		cases = append(cases, resultCase{name: "deferred_" + wake.String(), result: Deferred(wake), state: ownershipCoordinator})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reported := make(chan error, 2)
			var calls atomic.Int32
			var f *publishedDispatchTestFixture
			dispatcher := electionDispatcher(func(ctx context.Context, event events.Event, route events.DeliveryRoute) DispatchResult {
				calls.Add(1)
				id, err := runtimedelivery.DeliveryID(event.ID(), route)
				if err != nil {
					return Fatal(err)
				}
				switch tc.result.Disposition() {
				case DispatchTransferred:
					carrier, err := acquireCoordinatorTestCapability(f.coordinator, id)
					if err != nil {
						return Fatal(err)
					}
					if _, err := carrier.Resolve(ctx, worklifetime.DeliveryContinuationConsume); err != nil {
						return Fatal(err)
					}
				case DispatchAlreadyOwned:
					result, err := f.coordinator.Acquire(id)
					if err != nil || result.Disposition() != worklifetime.DeliveryAlreadyOwned {
						return Fatal(fmt.Errorf("existing carrier was replaced: %+v %v", result, err))
					}
				}
				return tc.result
			})
			f = newPublishedDispatchTestFixture(t, dispatcher, func(_ context.Context, err error) { reported <- err })
			if err := f.coordinator.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			event, route := coordinatorTestEvent("result-"+tc.name), publishedDispatchTestNodeRoute(t, "result")
			f.accept(t, event, route)
			id := publishedDispatchTestProof(t, f.authority, event, route).DeliveryID()
			var existing worklifetime.DeliveryContinuation
			if tc.result.Disposition() == DispatchAlreadyOwned {
				var err error
				existing, err = acquireCoordinatorTestCapability(f.coordinator, id)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := f.coordinator.DispatchPublished(event, []events.DeliveryRoute{route}); err != nil {
				t.Fatalf("asynchronous result changed publication admission: %v", err)
			}
			if tc.fatal {
				awaitRetirementProof(t, f.coordinator.done)
				err := f.coordinator.Retire(context.Background())
				if err == nil || tc.cause != nil && !errors.Is(err, tc.cause) || tc.cause == nil && !strings.Contains(err.Error(), "invalid result") {
					t.Fatalf("dispatch failure semantics changed: %v", err)
				}
				assertRetirementResult(t, f.coordinator, f.owner, reported, err, true)
				if err := f.coordinator.DispatchPublished(event, []events.DeliveryRoute{route}); err == nil {
					t.Fatal("fatal dispatcher admitted successor publication work")
				}
			} else {
				publishedDispatchTestQuiesce(t, f.owner)
				publishedDispatchTestSynchronize(t, f.coordinator)
				if state, ok := coordinatorOwnershipState(f.coordinator, id); !ok || state != tc.state {
					t.Fatalf("dispatch ownership=%d present=%t, want %d", state, ok, tc.state)
				}
				if err := f.coordinator.Retire(context.Background()); err != nil {
					t.Fatal(err)
				}
				assertRetirementResult(t, f.coordinator, f.owner, reported, nil, true)
				if existing != nil {
					if _, err := existing.Resolve(context.Background(), worklifetime.DeliveryContinuationReturnUnqueued); err != nil {
						t.Fatal(err)
					}
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("result manufactured retry execution: %d calls", calls.Load())
			}
		})
	}
}

func TestCoordinatorDispatchPublishedDeferredWorkResumesOnSignal(t *testing.T) {
	entered := make(chan struct{}, 2)
	var calls atomic.Int32
	dispatcher := electionDispatcher(func(context.Context, events.Event, events.DeliveryRoute) DispatchResult {
		defer func() { entered <- struct{}{} }()
		if calls.Add(1) == 1 {
			return Deferred(DispatchWakeIngressContinue)
		}
		return TerminallySettled()
	})
	f := newPublishedDispatchTestFixture(t, dispatcher, nil)
	if err := f.coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	event, route := coordinatorTestEvent("published-wake"), publishedDispatchTestNodeRoute(t, "published-wake")
	f.accept(t, event, route)
	if err := f.coordinator.DispatchPublished(event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	awaitRetirementProof(t, entered)
	publishedDispatchTestQuiesce(t, f.owner)
	publishedDispatchTestSynchronize(t, f.coordinator)
	if calls.Load() != 1 {
		t.Fatal("deferral scheduled its own execution without lifecycle progress")
	}
	f.enqueueScan(t, event, route)
	f.coordinator.Signal()
	awaitRetirementProof(t, entered)
	publishedDispatchTestQuiesce(t, f.owner)
	if err := f.coordinator.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := publishedDispatchTestProof(t, f.authority, event, route).DeliveryID()
	if state, ok := coordinatorOwnershipState(f.coordinator, id); !ok || state != ownershipTerminal || calls.Load() != 2 {
		t.Fatalf("named wake did not terminalize one retry: state=%d present=%t calls=%d", state, ok, calls.Load())
	}
}

type publishedDispatchFailingOccurrence struct {
	worklifetime.Occurrence
	calls    int
	admitted []*worklifetime.Lease
	failure  error
}

func (o *publishedDispatchFailingOccurrence) Begin(ctx context.Context) (*worklifetime.Lease, error) {
	o.calls++
	if o.calls == 2 {
		return nil, o.failure
	}
	lease, err := o.Occurrence.Begin(ctx)
	if err == nil {
		o.admitted = append(o.admitted, lease)
	}
	return lease, err
}

func TestCoordinatorDispatchPublishedRollsBackPartialAdmission(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	t.Cleanup(cleanup)
	fault := errors.New("second published lease admission failed")
	occurrence := &publishedDispatchFailingOccurrence{Occurrence: owner, failure: fault}
	dispatcher := &coordinatorTestDispatcher{}
	store := &coordinatorTestStore{observations: make(map[string]runtimedelivery.ContinuationObservation)}
	c, err := New(store, coordinatorTestRestarts{}, authority, occurrence, dispatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Retire(context.Background()) })
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	f := &publishedDispatchTestFixture{coordinator: c, store: store, authority: authority, owner: owner}
	event := coordinatorTestEvent("partial-published-admission")
	routes := []events.DeliveryRoute{publishedDispatchTestNodeRoute(t, "first"), publishedDispatchTestNodeRoute(t, "second")}
	f.accept(t, event, routes...)
	if err := c.DispatchPublished(event, routes); !errors.Is(err, fault) {
		t.Fatalf("partial admission error=%v, want exact failure", err)
	}
	if occurrence.calls != 2 || len(occurrence.admitted) != 1 || owner.ActiveCount() != 1 {
		t.Fatalf("partial publication retained work: attempts=%d admitted=%d active=%d", occurrence.calls, len(occurrence.admitted), owner.ActiveCount())
	}
	if err := occurrence.admitted[0].Done(); !errors.Is(err, worklifetime.ErrAlreadySettled) {
		t.Fatalf("admitted prefix lease was not settled exactly once: %v", err)
	}
	if err := c.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if dispatcher.callCount() != 0 {
		t.Fatal("partial admission dispatched a valid prefix")
	}
}

func TestCoordinatorDispatchPublishedBeforeStartUsesStartupScan(t *testing.T) {
	dispatcher := &coordinatorTestDispatcher{result: TerminallySettled(), dispatched: make(chan struct{}, 2)}
	f := newPublishedDispatchTestFixture(t, dispatcher, nil)
	event, route := coordinatorTestEvent("before-start"), publishedDispatchTestNodeRoute(t, "before-start")
	f.accept(t, event, route)
	if err := f.coordinator.DispatchPublished(event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	if dispatcher.callCount() != 0 || f.owner.ActiveCount() != 0 {
		t.Fatal("publication dispatched without a running coordinator lease")
	}
	f.enqueueScan(t, event, route)
	if err := f.coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	awaitRetirementProof(t, dispatcher.dispatched)
	if err := f.coordinator.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if dispatcher.callCount() != 1 {
		t.Fatalf("startup fallback dispatches=%d, want one", dispatcher.callCount())
	}
}

func TestCoordinatorDispatchPublishedRejectsSelectedAuthority(t *testing.T) {
	normal, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	t.Cleanup(cleanup)
	event, route := coordinatorTestEvent("selected-published"), publishedDispatchTestNodeRoute(t, "selected")
	authority, err := runtimedelivery.NewSelectedExecutionAuthority(normal.SourceArtifact(), event.ID(), event.RunID(), 1)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &coordinatorTestDispatcher{}
	store := &coordinatorTestStore{observations: make(map[string]runtimedelivery.ContinuationObservation)}
	c, err := NewSelected(store, authority, owner, dispatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Retire(context.Background()) })
	f := &publishedDispatchTestFixture{coordinator: c, store: store, authority: authority, owner: owner}
	f.accept(t, event, route)
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.DispatchPublished(event, []events.DeliveryRoute{route}); err == nil || !strings.Contains(err.Error(), "normal authority") {
		t.Fatalf("selected execution admitted ordinary publication dispatch: %v", err)
	}
	if err := c.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if dispatcher.callCount() != 0 {
		t.Fatal("selected authority escaped into publication dispatch")
	}
}
