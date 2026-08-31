package deliverycontinuation

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

const coordinatorTestBundleHash = "bundle-v2:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

func coordinatorTestAgentRoute(t testing.TB, agentID string) events.DeliveryRoute {
	t.Helper()
	return events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: agentidentitytest.RootRuntime(t, agentID, "delivery-continuation-test")}
}

type coordinatorTestStore struct {
	runtimedelivery.Store

	mu       sync.Mutex
	pages    []runtimedelivery.ContinuationPage
	scanErr  error
	scanErrs []error
	scanCall int
	scanned  chan struct{}

	observations map[string]runtimedelivery.ContinuationObservation
	observeErr   error
}

type coordinatorTestRestarts map[string]runtimepipeline.StandingRestartDispositionKind

func (s coordinatorTestRestarts) StandingRunRestartDisposition(_ context.Context, runID string) (runtimepipeline.StandingRestartDisposition, error) {
	if kind := s[strings.TrimSpace(runID)]; kind != "" {
		return runtimepipeline.StandingRestartDisposition{Kind: kind}, nil
	}
	return runtimepipeline.ClassifyStandingRestart(runtimepipeline.StandingRestartFact{})
}

func (s *coordinatorTestStore) ScanDeliveryContinuations(
	context.Context,
	runtimedelivery.ExecutionAuthority,
	runtimedelivery.ContinuationCursor,
	int,
) (runtimedelivery.ContinuationPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scanCall++
	if s.scanned != nil {
		select {
		case s.scanned <- struct{}{}:
		default:
		}
	}
	if len(s.scanErrs) > 0 {
		err := s.scanErrs[0]
		s.scanErrs = s.scanErrs[1:]
		if err != nil {
			return runtimedelivery.ContinuationPage{}, err
		}
	}
	if s.scanErr != nil {
		return runtimedelivery.ContinuationPage{}, s.scanErr
	}
	if len(s.pages) == 0 {
		return runtimedelivery.ContinuationPage{Exhausted: true}, nil
	}
	page := s.pages[0]
	s.pages = s.pages[1:]
	return page, nil
}

func (s *coordinatorTestStore) scanCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanCall
}

func (s *coordinatorTestStore) ObserveDeliveryContinuation(
	_ context.Context,
	_ runtimedelivery.ExecutionAuthority,
	deliveryID string,
) (runtimedelivery.ContinuationObservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.observeErr != nil {
		return runtimedelivery.ContinuationObservation{}, s.observeErr
	}
	if observation, ok := s.observations[deliveryID]; ok {
		return observation, nil
	}
	return runtimedelivery.ContinuationObservation{
		DeliveryID: deliveryID, Disposition: runtimedelivery.ClaimAcquired,
	}, nil
}

type coordinatorTestDispatcher struct {
	mu         sync.Mutex
	err        error
	errs       []error
	result     DispatchResult
	results    []DispatchResult
	calls      []string
	dispatched chan struct{}
}

type coordinatorCancellationDispatcher struct {
	started chan struct{}
}

func (d *coordinatorCancellationDispatcher) DispatchDeliveryContinuation(
	ctx context.Context,
	_ events.Event,
	_ events.DeliveryRoute,
) DispatchResult {
	close(d.started)
	<-ctx.Done()
	return Fatal(ctx.Err())
}

func (d *coordinatorTestDispatcher) DispatchDeliveryContinuation(
	_ context.Context,
	event events.Event,
	route events.DeliveryRoute,
) DispatchResult {
	d.mu.Lock()
	d.calls = append(d.calls, event.ID()+"\x00"+route.Recipient.ID())
	err := d.err
	if len(d.errs) > 0 {
		err = d.errs[0]
		d.errs = d.errs[1:]
	}
	result := d.result
	if len(d.results) > 0 {
		result = d.results[0]
		d.results = d.results[1:]
	}
	d.mu.Unlock()
	select {
	case d.dispatched <- struct{}{}:
	default:
	}
	if err != nil {
		return Fatal(err)
	}
	if result.Disposition() != 0 {
		return result
	}
	return Transferred()
}

func (d *coordinatorTestDispatcher) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.calls)
}

func coordinatorOwnershipState(c *Coordinator, deliveryID string) (ownershipState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.entries[deliveryID]
	return current.state, ok
}

func TestCoordinatorStartRequiresExplicitExhaustionBeforeReadiness(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	event := coordinatorTestEvent("startup")
	route := coordinatorTestAgentRoute(t, "agent-a")
	deliveryID, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	store := &coordinatorTestStore{pages: []runtimedelivery.ContinuationPage{
		{
			Items: []runtimedelivery.ContinuationItem{{
				DeliveryID: deliveryID,
				Event:      event,
				Snapshot: runtimedelivery.Snapshot{
					DeliveryID: deliveryID,
					Route:      route,
					Status:     runtimedelivery.StatusPending,
					Authority:  authority,
				},
				Disposition: runtimedelivery.ClaimAcquired,
			}},
			Exhausted: false,
		},
		{
			Items: []runtimedelivery.ContinuationItem{{
				DeliveryID: "deferred-delivery",
				Snapshot: runtimedelivery.Snapshot{
					DeliveryID:     "deferred-delivery",
					Status:         runtimedelivery.StatusFailed,
					NextEligibleAt: time.Now().Add(time.Hour),
					Authority:      authority,
				},
				Disposition: runtimedelivery.ClaimDeferred,
			}},
			Exhausted: true,
		},
	}}
	dispatcher := &coordinatorTestDispatcher{dispatched: make(chan struct{}, 1)}
	coordinator, err := New(store, coordinatorTestRestarts{}, authority, owner, dispatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	defer func() {
		if err := coordinator.Retire(context.Background()); err != nil {
			t.Errorf("retire coordinator: %v", err)
		}
	}()
	if calls := store.scanCalls(); calls < 2 {
		t.Fatalf("startup scan calls = %d, want explicit second-page exhaustion", calls)
	}
	if calls := dispatcher.callCount(); calls != 1 {
		t.Fatalf("startup dispatch calls = %d, want one acquired continuation", calls)
	}
}

func TestCoordinatorParksNonExecutableStandingDelivery(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	event := coordinatorTestEvent("parked-standing")
	route := coordinatorTestAgentRoute(t, "agent-parked")
	deliveryID, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	store := &coordinatorTestStore{pages: []runtimedelivery.ContinuationPage{{
		Items: []runtimedelivery.ContinuationItem{{
			DeliveryID: deliveryID,
			Event:      event,
			Snapshot: runtimedelivery.Snapshot{
				DeliveryID: deliveryID, RunID: event.RunID(), Route: route,
				Status: runtimedelivery.StatusPending, Authority: authority,
			},
			Disposition: runtimedelivery.ClaimAcquired,
		}},
		Exhausted: true,
	}}}
	dispatcher := &coordinatorTestDispatcher{dispatched: make(chan struct{}, 1)}
	coordinator, err := New(store, coordinatorTestRestarts{
		event.RunID(): runtimepipeline.StandingRestartSuspended,
	}, authority, owner, dispatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	if err := coordinator.Retire(context.Background()); err != nil {
		t.Fatalf("retire coordinator: %v", err)
	}
	if calls := dispatcher.callCount(); calls != 0 {
		t.Fatalf("non-executable standing delivery dispatch calls = %d, want 0", calls)
	}
}

func TestCoordinatorCapabilityTransfersExactlyOnce(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	coordinator, err := New(
		&coordinatorTestStore{},
		coordinatorTestRestarts{},
		authority,
		owner,
		&coordinatorTestDispatcher{dispatched: make(chan struct{}, 1)},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := runtimedelivery.Snapshot{
		DeliveryID: "delivery-a",
		Status:     runtimedelivery.StatusFailed,
		Authority:  authority,
	}
	if err := coordinator.Retain(snapshot); err != nil {
		t.Fatalf("retain retry continuation: %v", err)
	}
	first, err := coordinator.Acquire(snapshot.DeliveryID)
	if err != nil {
		t.Fatalf("acquire retained continuation: %v", err)
	}
	if resolution, err := first.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationReturned {
		t.Fatalf("return carrier continuation: %v", err)
	}
	if _, err := first.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err == nil {
		t.Fatal("duplicate continuation return succeeded")
	}
	second, err := coordinator.Acquire(snapshot.DeliveryID)
	if err != nil {
		t.Fatalf("reacquire returned continuation: %v", err)
	}
	if resolution, err := second.Resolve(context.Background(), worklifetime.DeliveryContinuationConsume); err != nil || resolution != worklifetime.DeliveryContinuationConsumed {
		t.Fatalf("consume continuation into attempt: %v", err)
	}
	if _, err := second.Resolve(context.Background(), worklifetime.DeliveryContinuationConsume); err == nil {
		t.Fatal("duplicate continuation consumption succeeded")
	}
	if _, err := coordinator.Acquire(snapshot.DeliveryID); err == nil {
		t.Fatal("consumed continuation remained locally claimable")
	}
}

func TestCoordinatorCarrierReturnSignalsRedispatch(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	coordinator, err := New(
		&coordinatorTestStore{},
		coordinatorTestRestarts{},
		authority,
		owner,
		&coordinatorTestDispatcher{dispatched: make(chan struct{}, 1)},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.observe("delivery-returned"); err != nil {
		t.Fatal(err)
	}
	capability, err := coordinator.Acquire("delivery-returned")
	if err != nil {
		t.Fatal(err)
	}
	if resolution, err := capability.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationReturned {
		t.Fatalf("return carrier continuation: %v", err)
	}
	select {
	case <-coordinator.wake:
	default:
		t.Fatal("carrier return did not signal coordinator redispatch")
	}
}

func TestDispatchResultIsClosedOverDispositionAndWakeAuthority(t *testing.T) {
	tests := []struct {
		name       string
		result     DispatchResult
		wantValid  bool
		wantWake   DispatchWakeAuthority
		wantFailed bool
	}{
		{name: "transferred", result: Transferred(), wantValid: true},
		{name: "terminal", result: TerminallySettled(), wantValid: true},
		{name: "agent_route", result: Deferred(DispatchWakeAgentRouteLifecycle), wantValid: true, wantWake: DispatchWakeAgentRouteLifecycle},
		{name: "internal_subscription", result: Deferred(DispatchWakeInternalSubscriptionLifecycle), wantValid: true, wantWake: DispatchWakeInternalSubscriptionLifecycle},
		{name: "carrier_return", result: Deferred(DispatchWakeCarrierReturn), wantValid: true, wantWake: DispatchWakeCarrierReturn},
		{name: "fatal", result: Fatal(errors.New("fatal dispatch")), wantValid: true, wantFailed: true},
		{name: "zero", result: DispatchResult{}},
		{name: "deferred_without_owner", result: DispatchResult{disposition: DispatchDeferred}},
		{name: "transferred_with_owner", result: DispatchResult{disposition: DispatchTransferred, wake: DispatchWakeAgentRouteLifecycle}, wantWake: DispatchWakeAgentRouteLifecycle},
		{name: "fatal_without_failure", result: DispatchResult{disposition: DispatchFatal}},
		{name: "fatal_with_owner", result: DispatchResult{disposition: DispatchFatal, wake: DispatchWakeCarrierReturn, err: errors.New("mixed authority")}, wantWake: DispatchWakeCarrierReturn, wantFailed: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.result.Validate()
			if test.wantValid != (err == nil) {
				t.Fatalf("Validate() error = %v, want valid=%t", err, test.wantValid)
			}
			if got := test.result.WakeAuthority(); got != test.wantWake {
				t.Fatalf("wake authority = %s, want %s", got, test.wantWake)
			}
			if got := test.result.Failure() != nil; got != test.wantFailed {
				t.Fatalf("failure present = %t, want %t", got, test.wantFailed)
			}
		})
	}
}

func TestCoordinatorSynchronizationReturnsExactFatalScanResult(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	store := &coordinatorTestStore{scanned: make(chan struct{}, 8)}
	reported := make(chan error, 1)
	coordinator, err := New(
		store,
		coordinatorTestRestarts{},
		authority,
		owner,
		&coordinatorTestDispatcher{dispatched: make(chan struct{}, 1)},
		func(_ context.Context, err error) { reported <- err },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	defer func() {
		if err := coordinator.Retire(context.Background()); err != nil {
			t.Errorf("retire coordinator: %v", err)
		}
	}()
	for store.scanCalls() < 2 {
		select {
		case <-store.scanned:
		case <-time.After(time.Second):
			t.Fatal("coordinator did not complete initial run-loop scan")
		}
	}
	store.mu.Lock()
	store.scanErr = errors.New("synchronized selected-store failure")
	store.mu.Unlock()
	err = coordinator.Synchronize(context.Background())
	if err == nil || !strings.Contains(err.Error(), "synchronized selected-store failure") {
		t.Fatalf("Synchronize() error = %v, want exact scan failure", err)
	}
	select {
	case reportedErr := <-reported:
		if !strings.Contains(reportedErr.Error(), "synchronized selected-store failure") {
			t.Fatalf("reported error = %v", reportedErr)
		}
	case <-time.After(time.Second):
		t.Fatal("fatal synchronized scan was not reported")
	}
	if repeated := coordinator.Synchronize(context.Background()); repeated == nil || !strings.Contains(repeated.Error(), "synchronized selected-store failure") {
		t.Fatalf("repeated Synchronize() error = %v, want retained fatal result", repeated)
	}
	requireCoordinatorRejectsPostFailureAdmission(t, coordinator, authority)
}

func TestCoordinatorStopsAfterUnownedStoreFailure(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	store := &coordinatorTestStore{scanned: make(chan struct{}, 8)}
	dispatcher := &coordinatorTestDispatcher{dispatched: make(chan struct{}, 1)}
	reported := make(chan error, 1)
	coordinator, err := New(store, coordinatorTestRestarts{}, authority, owner, dispatcher, func(_ context.Context, err error) {
		reported <- err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	defer func() {
		if err := coordinator.Retire(context.Background()); err != nil {
			t.Errorf("retire coordinator: %v", err)
		}
	}()
	for store.scanCalls() < 2 {
		select {
		case <-store.scanned:
		case <-time.After(time.Second):
			t.Fatal("coordinator did not complete initial run-loop scan")
		}
	}

	store.mu.Lock()
	store.scanErrs = []error{errors.New("selected-store failure without wake authority")}
	store.mu.Unlock()
	coordinator.Signal()

	select {
	case err := <-reported:
		if !strings.Contains(err.Error(), "selected-store failure without wake authority") {
			t.Fatalf("reported error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("selected-store failure was not reported")
	}
	for {
		select {
		case <-store.scanned:
		default:
			goto drained
		}
	}
drained:
	scansAfterFailure := store.scanCalls()
	coordinator.Signal()
	select {
	case <-store.scanned:
		t.Fatal("fatal selected-store failure was retried")
	case <-time.After(100 * time.Millisecond):
	}
	if calls := store.scanCalls(); calls != scansAfterFailure {
		t.Fatalf("scan calls after fatal failure = %d, want %d", calls, scansAfterFailure)
	}
}

func TestCoordinatorStopsAfterFatalDispatchWithoutPolling(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	event := coordinatorTestEvent("dispatch-retry")
	route := coordinatorTestAgentRoute(t, "agent-a")
	deliveryID, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	item := runtimedelivery.ContinuationItem{
		DeliveryID: deliveryID,
		Event:      event,
		Snapshot: runtimedelivery.Snapshot{
			DeliveryID: deliveryID,
			Route:      route,
			Status:     runtimedelivery.StatusPending,
			Authority:  authority,
		},
		Disposition: runtimedelivery.ClaimAcquired,
	}
	store := &coordinatorTestStore{
		pages: []runtimedelivery.ContinuationPage{
			{Exhausted: true},
			{Items: []runtimedelivery.ContinuationItem{item}, Exhausted: true},
		},
		scanned: make(chan struct{}, 8),
	}
	dispatcher := &coordinatorTestDispatcher{
		result:     Fatal(errors.New("dispatch failure without wake authority")),
		dispatched: make(chan struct{}, 2),
	}
	reported := make(chan error, 1)
	coordinator, err := New(store, coordinatorTestRestarts{}, authority, owner, dispatcher, func(_ context.Context, err error) {
		reported <- err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	defer func() {
		if err := coordinator.Retire(context.Background()); err != nil {
			t.Errorf("retire coordinator: %v", err)
		}
	}()

	select {
	case err := <-reported:
		if !strings.Contains(err.Error(), "dispatch failure without wake authority") {
			t.Fatalf("reported error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fatal dispatch failure was not reported")
	}
	select {
	case <-dispatcher.dispatched:
	default:
		t.Fatal("fatal dispatch did not record its exact attempt")
	}
	coordinator.Signal()
	select {
	case <-dispatcher.dispatched:
		t.Fatal("fatal dispatch was retried")
	case <-time.After(100 * time.Millisecond):
	}
	if calls := dispatcher.callCount(); calls != 1 {
		t.Fatalf("fatal dispatch calls = %d, want 1", calls)
	}
	requireCoordinatorRejectsPostFailureAdmission(t, coordinator, authority)
}

func requireCoordinatorRejectsPostFailureAdmission(t *testing.T, coordinator *Coordinator, authority runtimedelivery.ExecutionAuthority) {
	t.Helper()
	proof, err := runtimedelivery.AdmitDurableHandoffProof("later-delivery", "later-event", "later-route", authority)
	if err != nil {
		t.Fatalf("build later committed handoff: %v", err)
	}
	if err := coordinator.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err == nil || !strings.Contains(err.Error(), "retired") {
		t.Fatalf("post-failure committed handoff error = %v, want retired", err)
	}
	if err := coordinator.Retain(runtimedelivery.Snapshot{DeliveryID: "later-retry", Status: runtimedelivery.StatusFailed, Authority: authority}); err == nil || !strings.Contains(err.Error(), "retired") {
		t.Fatalf("post-failure retry retention error = %v, want retired", err)
	}
	if _, err := coordinator.Acquire("later-delivery"); err == nil || !strings.Contains(err.Error(), "retired") {
		t.Fatalf("post-failure carrier acquisition error = %v, want retired", err)
	}
}

func TestCoordinatorAttemptOwnershipReconcilesFromExactStoreState(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	store := &coordinatorTestStore{
		observations: map[string]runtimedelivery.ContinuationObservation{},
	}
	coordinator, err := New(
		store,
		coordinatorTestRestarts{},
		authority,
		owner,
		&coordinatorTestDispatcher{dispatched: make(chan struct{}, 1)},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	const deliveryID = "attempt-owned"
	if err := coordinator.observe(deliveryID); err != nil {
		t.Fatal(err)
	}
	carrier, err := coordinator.Acquire(deliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if resolution, err := carrier.Resolve(context.Background(), worklifetime.DeliveryContinuationConsume); err != nil || resolution != worklifetime.DeliveryContinuationConsumed {
		t.Fatalf("consume carrier into attempt: %v", err)
	}
	if state, ok := coordinatorOwnershipState(coordinator, deliveryID); !ok || state != ownershipAttempt {
		t.Fatalf("ownership after consume = %d, %v; want attempt", state, ok)
	}

	store.observations[deliveryID] = runtimedelivery.ContinuationObservation{
		DeliveryID: deliveryID, Disposition: runtimedelivery.ClaimBusy,
	}
	if _, _, err := coordinator.reconcileHeld(context.Background()); err != nil {
		t.Fatalf("reconcile busy attempt: %v", err)
	}
	if state, ok := coordinatorOwnershipState(coordinator, deliveryID); !ok || state != ownershipAttempt {
		t.Fatalf("busy ownership = %d, %v; want attempt", state, ok)
	}

	store.observations[deliveryID] = runtimedelivery.ContinuationObservation{
		DeliveryID: deliveryID, Disposition: runtimedelivery.ClaimReclaimable,
	}
	if _, wake, err := coordinator.reconcileHeld(context.Background()); err != nil {
		t.Fatalf("reconcile reclaimable attempt: %v", err)
	} else if !wake {
		t.Fatal("reclaimable attempt did not request immediate continuation scan")
	}
	if state, ok := coordinatorOwnershipState(coordinator, deliveryID); !ok || state != ownershipCoordinator {
		t.Fatalf("reclaimable ownership = %d, %v; want coordinator", state, ok)
	}

	store.observations[deliveryID] = runtimedelivery.ContinuationObservation{
		DeliveryID: deliveryID, Disposition: runtimedelivery.ClaimTerminal,
	}
	if _, _, err := coordinator.reconcileHeld(context.Background()); err != nil {
		t.Fatalf("reconcile terminal delivery: %v", err)
	}
	if _, ok := coordinatorOwnershipState(coordinator, deliveryID); ok {
		t.Fatal("terminal delivery retained a process-local continuation owner")
	}
}

func TestCoordinatorTerminalReleaseFencesUnclaimedAndCarrierOwnedWork(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	store := &coordinatorTestStore{observations: make(map[string]runtimedelivery.ContinuationObservation)}
	coordinator, err := New(
		store,
		coordinatorTestRestarts{},
		authority,
		owner,
		&coordinatorTestDispatcher{dispatched: make(chan struct{}, 1)},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := coordinator.observe("terminal-before-claim"); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Release("terminal-before-claim"); err != nil {
		t.Fatalf("release unclaimed terminal continuation: %v", err)
	}
	if _, err := coordinator.Acquire("terminal-before-claim"); err == nil {
		t.Fatal("terminal continuation remained claimable")
	}

	if err := coordinator.observe("terminal-with-carrier"); err != nil {
		t.Fatal(err)
	}
	carrier, err := coordinator.Acquire("terminal-with-carrier")
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Release("terminal-with-carrier"); err != nil {
		t.Fatalf("release carrier-owned terminal continuation: %v", err)
	}
	if err := coordinator.Release("terminal-with-carrier"); err != nil {
		t.Fatalf("repeat release before terminal carrier resolution: %v", err)
	}
	if state, ok := coordinatorOwnershipState(coordinator, "terminal-with-carrier"); !ok || state != ownershipTerminalCarrier {
		t.Fatalf("terminal carrier ownership = %d, %v; want terminal carrier fence", state, ok)
	}
	if resolution, err := carrier.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationTerminal {
		t.Fatalf("return late terminal carrier: %v", err)
	}
	if _, ok := coordinatorOwnershipState(coordinator, "terminal-with-carrier"); ok {
		t.Fatal("late terminal carrier return retained a process-local owner")
	}
	if err := coordinator.Release("terminal-with-carrier"); err != nil {
		t.Fatalf("repeat exact terminal release: %v", err)
	}

	if err := coordinator.observe("terminal-during-consume"); err != nil {
		t.Fatal(err)
	}
	consumeCarrier, err := coordinator.Acquire("terminal-during-consume")
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Release("terminal-during-consume"); err != nil {
		t.Fatalf("terminalize carrier before consume: %v", err)
	}
	if resolution, err := consumeCarrier.Resolve(context.Background(), worklifetime.DeliveryContinuationConsume); err != nil || resolution != worklifetime.DeliveryContinuationTerminal {
		t.Fatalf("terminal carrier consume resolution = %d, %v; want terminal", resolution, err)
	}
	if _, ok := coordinatorOwnershipState(coordinator, "terminal-during-consume"); ok {
		t.Fatal("terminal consume retained a process-local owner")
	}

	if err := coordinator.observe("terminal-during-reconcile"); err != nil {
		t.Fatal(err)
	}
	reconciledCarrier, err := coordinator.Acquire("terminal-during-reconcile")
	if err != nil {
		t.Fatal(err)
	}
	store.observations["terminal-during-reconcile"] = runtimedelivery.ContinuationObservation{
		DeliveryID:  "terminal-during-reconcile",
		Disposition: runtimedelivery.ClaimTerminal,
	}
	for range 2 {
		if _, _, err := coordinator.reconcileHeld(context.Background()); err != nil {
			t.Fatalf("reconcile repeated terminal carrier observation: %v", err)
		}
	}
	if state, ok := coordinatorOwnershipState(coordinator, "terminal-during-reconcile"); !ok || state != ownershipTerminalCarrier {
		t.Fatalf("repeated reconciliation ownership = %d, %v; want terminal carrier fence", state, ok)
	}
	if resolution, err := reconciledCarrier.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationTerminal {
		t.Fatalf("reconciled terminal carrier resolution = %d, %v; want terminal", resolution, err)
	}
	if _, ok := coordinatorOwnershipState(coordinator, "terminal-during-reconcile"); ok {
		t.Fatal("reconciled terminal carrier retained a process-local owner")
	}
}

func TestCoordinatorRetirementCancellationIsNotReportedAsExecutionFailure(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	store := &coordinatorTestStore{}
	dispatcher := &coordinatorCancellationDispatcher{started: make(chan struct{})}
	reported := make(chan error, 1)
	coordinator, err := New(store, coordinatorTestRestarts{}, authority, owner, dispatcher, func(_ context.Context, err error) {
		reported <- err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	event := coordinatorTestEvent("retirement-cancellation")
	route := coordinatorTestAgentRoute(t, "agent-a")
	deliveryID, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.pages = []runtimedelivery.ContinuationPage{{
		Items: []runtimedelivery.ContinuationItem{{
			DeliveryID: deliveryID,
			Event:      event,
			Snapshot: runtimedelivery.Snapshot{
				DeliveryID: deliveryID,
				Route:      route,
				Status:     runtimedelivery.StatusPending,
				Authority:  authority,
			},
			Disposition: runtimedelivery.ClaimAcquired,
		}},
		Exhausted: true,
	}}
	store.mu.Unlock()
	coordinator.Signal()
	select {
	case <-dispatcher.started:
	case <-time.After(time.Second):
		t.Fatal("delivery continuation dispatch did not start")
	}
	if err := coordinator.Retire(context.Background()); err != nil {
		t.Fatalf("retire coordinator: %v", err)
	}
	select {
	case err := <-reported:
		t.Fatalf("retirement cancellation was reported as execution failure: %v", err)
	default:
	}
}

func TestCoordinatorNamedRouteDeferralRetainsWorkUntilSignal(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	event := coordinatorTestEvent("topology")
	route := coordinatorTestAgentRoute(t, "agent-missing")
	deliveryID, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	store := &coordinatorTestStore{
		pages: []runtimedelivery.ContinuationPage{
			{Exhausted: true},
			{
				Items: []runtimedelivery.ContinuationItem{{
					DeliveryID: deliveryID,
					Event:      event,
					Snapshot: runtimedelivery.Snapshot{
						DeliveryID: deliveryID,
						Route:      route,
						Status:     runtimedelivery.StatusPending,
						Authority:  authority,
					},
					Disposition: runtimedelivery.ClaimAcquired,
				}},
				Exhausted: true,
			},
		},
		scanned: make(chan struct{}, 4),
	}
	dispatcher := &coordinatorTestDispatcher{
		result:     Deferred(DispatchWakeAgentRouteLifecycle),
		dispatched: make(chan struct{}, 2),
	}
	reported := make(chan error, 1)
	coordinator, err := New(store, coordinatorTestRestarts{}, authority, owner, dispatcher, func(_ context.Context, err error) {
		reported <- err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	defer func() {
		if err := coordinator.Retire(context.Background()); err != nil {
			t.Errorf("retire coordinator: %v", err)
		}
	}()
	select {
	case <-dispatcher.dispatched:
	case <-time.After(time.Second):
		t.Fatal("route-deferred continuation was not dispatched")
	}
	for {
		select {
		case <-store.scanned:
		default:
			goto scansDrained
		}
	}
scansDrained:
	scansAfterBlock := store.scanCalls()
	select {
	case <-store.scanned:
		t.Fatal("named route deferral armed an implicit retry")
	case <-time.After(100 * time.Millisecond):
	}
	if calls := store.scanCalls(); calls != scansAfterBlock {
		t.Fatalf("named route deferral armed a retry: scan calls %d -> %d", scansAfterBlock, calls)
	}
	select {
	case err := <-reported:
		t.Fatalf("topology block was reported as an execution failure: %v", err)
	default:
	}
	capability, err := coordinator.Acquire(deliveryID)
	if err != nil {
		t.Fatalf("route-deferred continuation was not retained: %v", err)
	}
	if resolution, err := capability.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationReturned {
		t.Fatalf("return route-deferred continuation: %v", err)
	}
}

func TestCoordinatorStartFailsClosedOnInvalidContinuation(t *testing.T) {
	for _, test := range []struct {
		name        string
		disposition runtimedelivery.ClaimDisposition
		invariant   error
		want        string
	}{
		{name: "wrong_authority", disposition: runtimedelivery.ClaimWrongAuthority, want: "crossed execution authority"},
		{name: "absent", disposition: runtimedelivery.ClaimAbsent, want: "has no durable delivery"},
		{name: "invariant_invalid", disposition: runtimedelivery.ClaimInvariantInvalid, invariant: errors.New("corrupt row"), want: "violates durable invariant"},
		{name: "unknown", disposition: "future_state", want: "unknown disposition"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
			defer cleanup()
			store := &coordinatorTestStore{pages: []runtimedelivery.ContinuationPage{{
				Items: []runtimedelivery.ContinuationItem{{
					DeliveryID:  "invalid-delivery",
					Disposition: test.disposition,
					Invariant:   test.invariant,
				}},
				Exhausted: true,
			}}}
			coordinator, err := New(
				store,
				coordinatorTestRestarts{},
				authority,
				owner,
				&coordinatorTestDispatcher{dispatched: make(chan struct{}, 1)},
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			err = coordinator.Start(context.Background())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("start error = %v, want %q", err, test.want)
			}
		})
	}
}

func coordinatorTestAuthorityAndOwner(
	t *testing.T,
) (runtimedelivery.ExecutionAuthority, *worklifetime.RuntimeOccurrence, func()) {
	t.Helper()
	source, err := runtimecorrelation.NewSourceArtifactFact(coordinatorTestBundleHash)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := runtimedelivery.NewNormalExecutionAuthority(source, "runtime-test", 1)
	if err != nil {
		t.Fatal(err)
	}
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "runtime-test",
		BundleHash:        coordinatorTestBundleHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		owner.Retire()
		if _, err := owner.RetireAndWait(context.Background()); err != nil {
			t.Errorf("retire test runtime owner: %v", err)
		}
		process.Retire()
		if _, err := process.Join(context.Background()); err != nil {
			t.Errorf("join test process owner: %v", err)
		}
	}
	return authority, owner, cleanup
}

func coordinatorTestEvent(label string) events.Event {
	return eventtest.PersistedProjection(
		eventtest.UUID("delivery-continuation-"+label),
		events.EventType("delivery.continuation"),
		"",
		"",
		[]byte(`{"ok":true}`),
		0,
		eventtest.UUID("delivery-continuation-run-"+label),
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
}
