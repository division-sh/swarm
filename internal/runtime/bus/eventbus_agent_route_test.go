package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

type ExactHandoffNativeFixtureForTest struct {
	Store         runtimedelivery.Store
	Authority     runtimedelivery.ExecutionAuthority
	Context       context.Context
	SelectedEvent events.Event
	SelectedRoute events.DeliveryRoute
	Seed          func(*testing.T, string, string, events.DeliveryRoute) events.Event
	Session       func(*testing.T, agentidentity.Identity) string
	Load          func(*testing.T, string) events.Event
}

type ExactHandoffNativeOpenerForTest func(*testing.T, bool) ExactHandoffNativeFixtureForTest

// The spy records the exact source and injects only the named handoff fault.
// Native owners retain every storage operation and acknowledgment.
type exactHandoffProofStore struct {
	InMemoryEventStore
	runtimedelivery.Store
	fixture               ExactHandoffNativeFixtureForTest
	authority             runtimedelivery.ExecutionAuthority
	mu                    sync.Mutex
	attempts, binds       int
	failOnce              bool
	handoffFact, bindFact runtimecorrelation.SourceArtifactFact
}

func (s *exactHandoffProofStore) ProveHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) (runtimedelivery.DurableHandoffProof, error) {
	s.mu.Lock()
	s.attempts++
	s.handoffFact, _ = runtimecorrelation.SourceArtifactFactFromContext(ctx)
	fail := s.failOnce && s.attempts == 1
	s.mu.Unlock()
	if fail {
		return runtimedelivery.DurableHandoffProof{}, errors.New("injected handoff proof failure")
	}
	return s.Store.ProveHandoff(ctx, eventID, route)
}

func (s *exactHandoffProofStore) BindAgentSession(ctx context.Context, claim runtimedelivery.Claim, sessionID string) (runtimedelivery.ClaimCommit, error) {
	s.mu.Lock()
	s.binds++
	s.bindFact, _ = runtimecorrelation.SourceArtifactFactFromContext(ctx)
	s.mu.Unlock()
	return s.Store.BindAgentSession(ctx, claim, sessionID)
}

func newExactHandoffProofStore(fixture ExactHandoffNativeFixtureForTest, failOnce bool) *exactHandoffProofStore {
	return &exactHandoffProofStore{Store: fixture.Store, fixture: fixture, authority: fixture.Authority, failOnce: failOnce}
}

func (s *exactHandoffProofStore) seed(t *testing.T, eventID, runID string, route events.DeliveryRoute) events.Event {
	t.Helper()
	return s.fixture.Seed(t, eventID, runID, route)
}

func (s *exactHandoffProofStore) claim(t *testing.T, eventID, runID string, route events.DeliveryRoute) runtimedelivery.Claim {
	t.Helper()
	event := s.fixture.Load(t, eventID)
	if event.RunID() != runID {
		t.Fatal("native handoff claim lost exact run identity")
	}
	result, err := s.Store.ClaimDelivery(s.fixture.Context, s.authority, event, route)
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok := result.Acquired()
	if !ok {
		t.Fatalf("native handoff claim disposition=%s", result.Disposition)
	}
	return claimed.Claim
}

func deliverToTestAgent(ctx context.Context, eb *EventBus, evt events.Event, identity agentidentity.Identity) error {
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
	_, err := eb.deliverToRecipientsWithRoutes(ctx, evt, []string{identity.AgentID()}, []events.DeliveryRoute{route})
	return err
}

func VerifySelectedDeliveryTransfersAcceptCommittedIsAtomicForTest(t *testing.T, open ExactHandoffNativeOpenerForTest) {
	fixture := open(t, true)
	store := newExactHandoffProofStore(fixture, false)
	authority := fixture.Authority
	eventID, forkRunID, route := fixture.SelectedEvent.ID(), fixture.SelectedEvent.RunID(), fixture.SelectedRoute
	store.seed(t, eventID, forkRunID, route)
	proof, err := store.ProveHandoff(fixture.Context, eventID, route)
	if err != nil {
		t.Fatalf("prove selected committed handoff: %v", err)
	}
	owner, err := newSelectedDeliveryTransfers(authority)
	if err != nil {
		t.Fatalf("construct selected delivery transfers: %v", err)
	}

	if err := owner.AcceptCommitted([]runtimedelivery.DurableHandoffProof{
		proof,
		runtimedelivery.DurableHandoffProof{},
	}); err == nil {
		t.Fatal("partially invalid selected handoff batch succeeded")
	}
	if _, err := owner.Acquire(proof.DeliveryID()); err == nil {
		t.Fatal("selected handoff valid prefix transferred before whole-batch validation")
	}
	if err := owner.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err != nil {
		t.Fatalf("accept selected committed handoff: %v", err)
	}
	capability, err := acquireTestDeliveryCapability(owner, proof.DeliveryID())
	if err != nil {
		t.Fatalf("acquire selected delivery carrier: %v", err)
	}
	if resolution, err := capability.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationReturned {
		t.Fatalf("return selected delivery carrier: %v", err)
	}
	reacquired, err := acquireTestDeliveryCapability(owner, proof.DeliveryID())
	if err != nil {
		t.Fatalf("reacquire returned selected delivery: %v", err)
	}
	if resolution, err := reacquired.Resolve(context.Background(), worklifetime.DeliveryContinuationConsume); err != nil || resolution != worklifetime.DeliveryContinuationConsumed {
		t.Fatalf("consume selected delivery into attempt: %v", err)
	}
	if err := owner.Release(proof.DeliveryID()); err != nil {
		t.Fatalf("release selected delivery attempt: %v", err)
	}
	if _, err := owner.Acquire(proof.DeliveryID()); err == nil {
		t.Fatal("released selected delivery retained a process-local owner")
	}
	if err := owner.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err != nil {
		t.Fatalf("reaccept selected delivery for terminal race: %v", err)
	}
	terminalCarrier, err := acquireTestDeliveryCapability(owner, proof.DeliveryID())
	if err != nil {
		t.Fatalf("acquire selected terminal-race carrier: %v", err)
	}
	if err := owner.Release(proof.DeliveryID()); err != nil {
		t.Fatalf("terminalize selected carrier: %v", err)
	}
	if err := owner.Release(proof.DeliveryID()); err != nil {
		t.Fatalf("repeat selected terminal release before carrier resolution: %v", err)
	}
	if held := owner.held[proof.DeliveryID()]; held != selectedDeliveryTerminalCarrier {
		t.Fatalf("selected terminal carrier ownership = %d; want terminal carrier fence", held)
	}
	if resolution, err := terminalCarrier.Resolve(context.Background(), worklifetime.DeliveryContinuationConsume); err != nil || resolution != worklifetime.DeliveryContinuationTerminal {
		t.Fatalf("selected terminal-race resolution = %d, %v; want terminal", resolution, err)
	}
	if _, exists := owner.held[proof.DeliveryID()]; exists {
		t.Fatal("selected terminal carrier retained a process-local owner")
	}
	if err := owner.Release(proof.DeliveryID()); err != nil {
		t.Fatalf("repeat selected terminal release after carrier resolution: %v", err)
	}
}

func TestEventBusAgentRouteReplacementWaitsForExactDequeuedPredecessor(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldToken := testAgentLifecycleToken(t, "agent-a", "", 7, 1)
	newToken := testAgentLifecycleToken(t, "agent-a", "", 7, 2)
	oldCh := eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.work")))
	if oldCh == nil {
		t.Fatal("predecessor route was not installed")
	}
	oldEventID := uuid.NewString()
	evt := eventtest.RuntimeControl(oldEventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now())
	if err := deliverToTestAgent(context.Background(), eb, evt, oldToken.Identity); err != nil {
		t.Fatalf("deliver predecessor event: %v", err)
	}
	delivery := <-oldCh

	replaced := make(chan (<-chan *LocalDelivery), 1)
	go func() {
		replaced <- eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.work")))
	}()
	select {
	case <-replaced:
		t.Fatal("replacement published before dequeued predecessor completed")
	case <-time.After(25 * time.Millisecond):
	}
	if err := delivery.Complete(); err != nil {
		t.Fatalf("complete predecessor delivery: %v", err)
	}
	var newCh <-chan *LocalDelivery
	select {
	case newCh = <-replaced:
	case <-time.After(time.Second):
		t.Fatal("replacement did not finish after predecessor completion")
	}
	if newCh == nil || newCh == oldCh {
		t.Fatal("replacement did not publish an exact fresh route")
	}

	newEventID := uuid.NewString()
	newEvent := eventtest.RuntimeControl(newEventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now())
	if err := deliverToTestAgent(context.Background(), eb, newEvent, newToken.Identity); err != nil {
		t.Fatalf("deliver successor event: %v", err)
	}
	newDelivery := <-newCh
	if newDelivery.ID() != newEventID {
		t.Fatalf("successor event id = %q", newDelivery.ID())
	}
	if err := newDelivery.Complete(); err != nil {
		t.Fatalf("complete successor delivery: %v", err)
	}
}

func TestEventBusAgentRouteRemovalWaitsForDequeuedWork(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	token := testAgentLifecycleToken(t, "agent-a", "", 7, 1)
	ch := eb.ReplaceAgentRoute(token, testAgentSubscriptionAdmission(t, token.AgentID, events.EventType("test.work")))
	evt := eventtest.RuntimeControl(uuid.NewString(), events.EventType("test.work"), "test", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now())
	if err := deliverToTestAgent(context.Background(), eb, evt, token.Identity); err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	delivery := <-ch
	done := make(chan struct{})
	go func() { eb.RemoveAgentRoute(token); close(done) }()
	select {
	case <-done:
		t.Fatal("route removal returned before dequeued work completed")
	case <-time.After(25 * time.Millisecond):
	}
	if err := delivery.Complete(); err != nil {
		t.Fatalf("complete route delivery: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("route removal did not join completed work")
	}
}

func VerifyEventBusSnapshottedAgentRouteSendLinearizesWithRemovalForTest(t *testing.T, open ExactHandoffNativeOpenerForTest) {
	store := newExactHandoffProofStore(open(t, false), false)
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	for generation := uint64(1); generation <= 64; generation++ {
		token := testAgentLifecycleToken(t, "agent-race", "", 7, generation)
		if ch := eb.ReplaceAgentRoute(token, testAgentSubscriptionAdmission(t, token.AgentID, events.EventType("test.work"))); ch == nil {
			t.Fatalf("generation %d route was not installed", generation)
		}
		recipients := eb.snapshotRoutePlanRecipientChans(
			[]string{token.AgentID},
			[]RoutePlanLiveRecipient{{Recipient: events.MustAgentDeliveryRecipient(token.AgentID), AgentIdentity: token.Identity,

				PersistAsDelivery: true,
				liveAuthority:     liveRecipientAuthorityIdentity,
			}},
		)
		if len(recipients) != 1 || recipients[0].route == nil {
			t.Fatalf("generation %d snapshot = %#v, want exact route handle", generation, recipients)
		}
		eventID, runID := uuid.NewString(), busInternalTestRunID
		route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(token.AgentID), AgentIdentity: token.Identity}
		evt := eventtest.RuntimeControl(
			eventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0,
			runID, "", events.EventEnvelope{}, time.Now(),
		)
		store.seed(t, eventID, runID, route)
		start := make(chan struct{})
		sendResult := make(chan agentRouteSendResult, 1)
		removed := make(chan struct{})
		go func(recipient agentRecipient) {
			<-start
			result, err := recipient.send(context.Background(), evt, route, nil)
			if err != nil {
				t.Errorf("generation %d send: %v", generation, err)
			}
			sendResult <- result
		}(recipients[0])
		go func() {
			<-start
			eb.RemoveAgentRoute(token)
			close(removed)
		}()
		close(start)
		if result := <-sendResult; result != agentRouteSendDelivered && result != agentRouteSendInactive {
			t.Fatalf("generation %d send result = %v, want delivered-before-retirement or inactive-after-retirement", generation, result)
		}
		select {
		case <-removed:
		case <-time.After(time.Second):
			t.Fatalf("generation %d route removal did not join linearized send", generation)
		}
	}
	joinCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := eb.WaitForQuiescence(joinCtx); err != nil {
		t.Fatalf("WaitForQuiescence after route races: %v", err)
	}
}

func TestAgentRecipientWithoutExactLifecycleHandleFailsClosed(t *testing.T) {
	recipient := agentRecipient{identity: testAgentRouteIdentity(t, "orphan", ""), kind: inMemorySubscriberAgent}
	evt := eventtest.RuntimeControl("orphan-send", events.EventType("test.work"), "test", "", []byte(`{}`), 0,
		"run-1", "", events.EventEnvelope{}, time.Now())
	result, err := recipient.send(context.Background(), evt, events.DeliveryRoute{}, nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if result != agentRouteSendInactive {
		t.Fatalf("send result = %v, want inactive without exact lifecycle handle", result)
	}
}

func TestInternalSubscriptionInactiveSendReturnsExactContinuation(t *testing.T) {
	evt := eventtest.RuntimeControl(
		uuid.NewString(),
		events.EventType("test.work"),
		"test",
		"",
		[]byte(`{}`),
		0,
		uuid.NewString(),
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "node-a"))}
	for _, test := range []struct {
		name   string
		handle *internalSubscriptionHandle
	}{
		{name: "nil handle"},
		{name: "missing bus", handle: &internalSubscriptionHandle{}},
		{name: "inactive", handle: &internalSubscriptionHandle{bus: &EventBus{}, ch: make(chan *LocalDelivery, 1)}},
		{name: "missing channel", handle: &internalSubscriptionHandle{bus: &EventBus{}, active: true}},
		{name: "missing work owner", handle: &internalSubscriptionHandle{bus: &EventBus{}, active: true, ch: make(chan *LocalDelivery, 1)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := &controlledTestDeliveryOwner{}
			continuation, err := acquireTestDeliveryCapability(owner, "delivery-"+strings.ReplaceAll(test.name, " ", "-"))
			if err != nil {
				t.Fatal(err)
			}
			result, err := test.handle.send(context.Background(), evt, route, continuation)
			if err != nil {
				t.Fatalf("send: %v", err)
			}
			if result != agentRouteSendInactive {
				t.Fatalf("send result = %v, want inactive", result)
			}
			owner.mu.Lock()
			defer owner.mu.Unlock()
			if owner.returnAttempts != 1 || len(owner.returnedIDs) != 1 ||
				owner.returnedIDs[0] != continuation.DeliveryID() {
				t.Fatalf(
					"continuation returns = attempts:%d ids:%v, want one exact return",
					owner.returnAttempts,
					owner.returnedIDs,
				)
			}
		})
	}
}

func TestEventBusAgentRouteBufferedRemovalFailsClosedWithoutDurableHandoff(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	continuations := &controlledTestDeliveryOwner{failAcquire: true}
	if err := eb.SetDeliveryContinuationOwner(continuations); err != nil {
		t.Fatalf("install rejecting delivery continuation owner: %v", err)
	}
	oldToken := testAgentLifecycleToken(t, "agent-a", "", 7, 1)
	newToken := testAgentLifecycleToken(t, "agent-a", "", 7, 2)
	eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.work")))
	evt := eventtest.RuntimeControl(uuid.NewString(), events.EventType("test.work"), "test", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now())
	if err := deliverToTestAgent(context.Background(), eb, evt, oldToken.Identity); err == nil || !strings.Contains(err.Error(), "admission failure") {
		t.Fatalf("delivery admission error = %v, want fail-closed continuation admission", err)
	}
	if got := eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.work"))); got == nil {
		t.Fatal("rejected delivery was enqueued and blocked route retirement")
	}
}

func TestEventBusAgentRouteFailedHandoffRetainsCarrierForExactRetry(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	continuations := &controlledTestDeliveryOwner{returnFailures: 1}
	if err := eb.SetDeliveryContinuationOwner(continuations); err != nil {
		t.Fatalf("install fail-once delivery continuation owner: %v", err)
	}
	oldToken := testAgentLifecycleToken(t, "agent-a", "", 7, 1)
	newToken := testAgentLifecycleToken(t, "agent-a", "", 7, 2)
	eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.work")))
	eventID, runID := uuid.NewString(), uuid.NewString()
	evt := eventtest.RuntimeControl(eventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now())
	if err := deliverToTestAgent(context.Background(), eb, evt, oldToken.Identity); err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	if got := eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.work"))); got != nil {
		t.Fatal("successor route published after first handoff proof failed")
	}
	if err := eb.WaitForQuiescence(context.Background()); err != nil {
		t.Fatalf("retry retained handoff and join: %v", err)
	}
	continuations.mu.Lock()
	attempts := continuations.returnAttempts
	continuations.mu.Unlock()
	if attempts != 2 {
		t.Fatalf("continuation return attempts = %d, want exact fail-once retry", attempts)
	}
}

func TestNoContextRouteCleanupReturnsExactDeliveryContinuation(t *testing.T) {
	owned := sourceMutationFact(t, "9")
	for _, tc := range []struct {
		name           string
		subscriberType string
		cleanup        func(*EventBus, string, runtimeeffects.LifecycleToken, worklifetime.InternalSubscription) error
	}{
		{
			name:           "agent route removal",
			subscriberType: "agent",
			cleanup: func(bus *EventBus, _ string, token runtimeeffects.LifecycleToken, _ worklifetime.InternalSubscription) error {
				bus.RemoveAgentRoute(token)
				return nil
			},
		},
		{
			name:           "internal natural completion",
			subscriberType: "node",
			cleanup: func(_ *EventBus, _ string, _ runtimeeffects.LifecycleToken, subscription worklifetime.InternalSubscription) error {
				return subscription.Complete(false)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := newSourceMutationProbeBusWithStore(t, InMemoryEventStore{}, owned, newSourceMutationProbeOwner())
			continuations := &controlledTestDeliveryOwner{}
			if err := bus.SetDeliveryContinuationOwner(continuations); err != nil {
				t.Fatalf("install recording delivery continuation owner: %v", err)
			}
			subscriberID := "cleanup-" + strings.ReplaceAll(tc.name, " ", "-")
			token := testAgentLifecycleToken(t, subscriberID, "", 7, 1)
			var subscription worklifetime.InternalSubscription
			if tc.subscriberType == "agent" {
				if ch := bus.ReplaceAgentRoute(
					token,
					testAgentSubscriptionAdmission(t, subscriberID, events.EventType("test.work")),
				); ch == nil {
					t.Fatal("install agent route")
				}
			} else {
				var err error
				subscription, err = bus.SubscribeInternal(context.Background(), workflowRuntimeInternalCarrierID, events.EventType("test.work"))
				if err != nil {
					t.Fatalf("subscribe internal route: %v", err)
				}
			}

			eventID, runID := uuid.NewString(), uuid.NewString()
			recipient := events.MustNodeDeliveryRecipient(testRootNode(t, subscriberID))
			if tc.subscriberType == "agent" {
				recipient = events.MustAgentDeliveryRecipient(subscriberID)
			}
			route := events.DeliveryRoute{Recipient: recipient}
			if recipient.IsAgent() {
				route.AgentIdentity = token.Identity
			} else {
				route.Target = events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowInstance: "root"})
			}
			evt := eventtest.RuntimeControl(
				eventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0,
				runID, "", events.EventEnvelope{}, time.Now().UTC(),
			)
			if tc.subscriberType == "agent" {
				if err := deliverToTestAgent(context.Background(), bus, evt, token.Identity); err != nil {
					t.Fatalf("buffer agent delivery: %v", err)
				}
			} else {
				if err := bus.deliverLiveRecipientsWithRoutes(
					context.Background(),
					evt,
					[]RoutePlanLiveRecipient{{InternalID: workflowRuntimeInternalCarrierID, PersistAsDelivery: false,
						liveAuthority: liveRecipientAuthorityIdentity,
					}},
					[]events.DeliveryRoute{route},
				); err != nil {
					t.Fatalf("buffer internal delivery: %v", err)
				}
			}
			if err := tc.cleanup(bus, subscriberID, token, subscription); err != nil {
				t.Fatalf("cleanup retained route: %v", err)
			}
			wantDeliveryID, err := runtimedelivery.DeliveryID(eventID, route)
			if err != nil {
				t.Fatalf("derive expected delivery id: %v", err)
			}
			continuations.mu.Lock()
			attempts := continuations.returnAttempts
			returned := append([]string(nil), continuations.returnedIDs...)
			continuations.mu.Unlock()
			if attempts != 1 || len(returned) != 1 || returned[0] != wantDeliveryID {
				t.Fatalf("continuation returns = attempts:%d ids:%v, want exact %s", attempts, returned, wantDeliveryID)
			}
		})
	}
}

func TestEventBusResetRetiresAndRestartsInternalSubscriptionGeneration(t *testing.T) {
	eb, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan int, 2)
	received := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for generation := 1; ; generation++ {
			subscription, err := eb.SubscribeInternal(ctx, "reset-proof", events.EventType("test.work"))
			if err != nil {
				return
			}
			subscription.MarkReady()
			ready <- generation
			for {
				select {
				case <-ctx.Done():
					_ = subscription.Complete(false)
					return
				case <-subscription.Retiring():
					restart := ctx.Err() == nil
					_ = subscription.Complete(restart)
					if !restart {
						return
					}
					goto nextGeneration
				case delivery := <-subscription.Deliveries():
					if delivery != nil {
						received <- delivery.ID()
						_ = delivery.Complete()
					}
				}
			}
		nextGeneration:
		}
	}()
	if generation := <-ready; generation != 1 {
		t.Fatalf("initial generation = %d", generation)
	}
	if err := eb.ResetInMemoryState(); err != nil {
		t.Fatalf("ResetInMemoryState: %v", err)
	}
	if generation := <-ready; generation != 2 {
		t.Fatalf("replacement generation = %d", generation)
	}
	evt := eventtest.RuntimeControl(uuid.NewString(), events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverLiveRecipientsWithRoutes(
		context.Background(),
		evt,
		[]RoutePlanLiveRecipient{{InternalID: "reset-proof", PersistAsDelivery: false,
			liveAuthority: liveRecipientAuthorityIdentity,
		}},
		nil,
	); err != nil {
		t.Fatalf("deliver after reset: %v", err)
	}
	select {
	case eventID := <-received:
		if eventID != evt.ID() {
			t.Fatalf("event id = %q, want %q", eventID, evt.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("replacement internal subscription did not receive event")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("internal subscription receiver did not exit")
	}
}

func TestEventBusResetStopsWaitingWhenRestartingSubscriberContextIsCancelled(t *testing.T) {
	eb, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	retired := make(chan struct{})
	allowResubscribe := make(chan struct{})
	receiverDone := make(chan struct{})
	go func() {
		defer close(receiverDone)
		for {
			subscription, subscribeErr := eb.SubscribeInternal(ctx, "cancelled-restart-proof", events.EventType("test.work"))
			if subscribeErr != nil {
				return
			}
			subscription.MarkReady()
			select {
			case <-ctx.Done():
				_ = subscription.Complete(false)
				return
			case <-subscription.Retiring():
				_ = subscription.Complete(true)
				close(retired)
				<-allowResubscribe
			}
		}
	}()

	if err := eb.waitForInternalSubscriptionReady(ctx, "cancelled-restart-proof"); err != nil {
		t.Fatalf("wait for initial subscription: %v", err)
	}
	resetDone := make(chan error, 1)
	go func() { resetDone <- eb.ResetInMemoryState() }()
	<-retired
	cancel()
	close(allowResubscribe)
	select {
	case err := <-resetDone:
		if err != nil {
			t.Fatalf("ResetInMemoryState: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reset waited for a subscriber generation whose lifecycle context was cancelled")
	}
	select {
	case <-receiverDone:
	case <-time.After(time.Second):
		t.Fatal("cancelled internal subscriber did not exit")
	}
}

func TestEventBusSnapshottedInternalSendLinearizesWithReset(t *testing.T) {
	eb, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			subscription, subscribeErr := eb.SubscribeInternal(ctx, "reset-race-proof", events.EventType("test.work"))
			if subscribeErr != nil {
				return
			}
			subscription.MarkReady()
			ready <- struct{}{}
			for {
				select {
				case <-ctx.Done():
					_ = subscription.Complete(false)
					return
				case <-subscription.Retiring():
					_ = subscription.Complete(true)
					goto nextGeneration
				case delivery := <-subscription.Deliveries():
					if delivery != nil {
						_ = delivery.Complete()
					}
				}
			}
		nextGeneration:
		}
	}()
	<-ready

	for i := 0; i < 64; i++ {
		recipients := eb.snapshotRoutePlanRecipientChans(
			[]string{"reset-race-proof"},
			[]RoutePlanLiveRecipient{{Recipient: events.MustAgentDeliveryRecipient("reset-race-proof")}},
		)
		if len(recipients) != 1 || recipients[0].internal == nil {
			t.Fatalf("iteration %d snapshot = %#v, want one internal generation", i, recipients)
		}
		evt := eventtest.RuntimeControl(
			fmt.Sprintf("reset-race-%d", i), events.EventType("test.work"), "test", "", []byte(`{}`), 0,
			"run-1", "", events.EventEnvelope{}, time.Now(),
		)
		start := make(chan struct{})
		result := make(chan agentRouteSendResult, 1)
		resetErr := make(chan error, 1)
		go func(recipient agentRecipient) {
			<-start
			sendResult, err := recipient.send(context.Background(), evt, events.DeliveryRoute{}, nil)
			if err != nil {
				t.Errorf("iteration %d send: %v", i, err)
			}
			result <- sendResult
		}(recipients[0])
		go func() {
			<-start
			resetErr <- eb.ResetInMemoryState()
		}()
		close(start)
		if sendResult := <-result; sendResult != agentRouteSendDelivered && sendResult != agentRouteSendInactive {
			t.Fatalf("iteration %d send result = %v, want delivered or retirement-fenced", i, sendResult)
		}
		if err := <-resetErr; err != nil {
			t.Fatalf("iteration %d ResetInMemoryState: %v", i, err)
		}
		<-ready
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("internal subscription receiver did not exit")
	}
	joinCtx, joinCancel := context.WithTimeout(context.Background(), time.Second)
	defer joinCancel()
	if err := eb.WaitForQuiescence(joinCtx); err != nil {
		t.Fatalf("join after reset/send races: %v", err)
	}
}
