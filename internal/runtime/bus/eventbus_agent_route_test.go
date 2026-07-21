package bus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type failOnceLocalDeliveryHandoffStore struct {
	InMemoryEventStore
	mu       sync.Mutex
	attempts int
}

type provenLocalDeliveryHandoffStore struct {
	InMemoryEventStore
}

func (provenLocalDeliveryHandoffStore) ProveLocalDeliveryHandoff(context.Context, string, events.DeliveryRoute) error {
	return nil
}

func (s *failOnceLocalDeliveryHandoffStore) ProveLocalDeliveryHandoff(context.Context, string, events.DeliveryRoute) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.attempts == 1 {
		return errors.New("injected handoff proof failure")
	}
	return nil
}

func TestEventBusAgentRouteReplacementWaitsForExactDequeuedPredecessor(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 2}
	oldCh := eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.work")))
	if oldCh == nil {
		t.Fatal("predecessor route was not installed")
	}
	evt := eventtest.RuntimeControl("work-old", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"agent-a"}); err != nil {
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

	newEvent := eventtest.RuntimeControl("work-new", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), newEvent, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver successor event: %v", err)
	}
	newDelivery := <-newCh
	if newDelivery.ID() != "work-new" {
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
	token := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	ch := eb.ReplaceAgentRoute(token, testAgentSubscriptionAdmission(t, token.AgentID, events.EventType("test.work")))
	evt := eventtest.RuntimeControl("work-1", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"agent-a"}); err != nil {
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

func TestEventBusSnapshottedAgentRouteSendLinearizesWithRemoval(t *testing.T) {
	eb, err := newScopedTestEventBus(provenLocalDeliveryHandoffStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	for generation := uint64(1); generation <= 64; generation++ {
		token := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-race", Generation: generation}
		if ch := eb.ReplaceAgentRoute(token, testAgentSubscriptionAdmission(t, token.AgentID, events.EventType("test.work"))); ch == nil {
			t.Fatalf("generation %d route was not installed", generation)
		}
		recipients := eb.snapshotRecipientChans([]string{token.AgentID})
		if len(recipients) != 1 || recipients[0].route == nil {
			t.Fatalf("generation %d snapshot = %#v, want exact route handle", generation, recipients)
		}
		evt := eventtest.RuntimeControl(
			fmt.Sprintf("route-race-%d", generation), events.EventType("test.work"), "test", "", []byte(`{}`), 0,
			"run-1", "", events.EventEnvelope{}, time.Now(),
		)
		start := make(chan struct{})
		sendResult := make(chan agentRouteSendResult, 1)
		removed := make(chan struct{})
		go func(recipient agentRecipient) {
			<-start
			sendResult <- recipient.send(context.Background(), evt, events.DeliveryRoute{})
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
	recipient := agentRecipient{agentID: "orphan", kind: inMemorySubscriberAgent}
	evt := eventtest.RuntimeControl("orphan-send", events.EventType("test.work"), "test", "", []byte(`{}`), 0,
		"run-1", "", events.EventEnvelope{}, time.Now())
	if result := recipient.send(context.Background(), evt, events.DeliveryRoute{}); result != agentRouteSendInactive {
		t.Fatalf("send result = %v, want inactive without exact lifecycle handle", result)
	}
}

func TestEventBusAgentRouteBufferedRemovalFailsClosedWithoutDurableHandoff(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 2}
	eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.work")))
	evt := eventtest.RuntimeControl("work-buffered", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	if got := eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.work"))); got != nil {
		t.Fatal("successor route published after unproven buffered handoff")
	}
}

func TestEventBusAgentRouteFailedHandoffRetainsCarrierForExactRetry(t *testing.T) {
	store := &failOnceLocalDeliveryHandoffStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 2}
	eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.work")))
	evt := eventtest.RuntimeControl("work-buffered-retry", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	if got := eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.work"))); got != nil {
		t.Fatal("successor route published after first handoff proof failed")
	}
	if err := eb.WaitForQuiescence(context.Background()); err != nil {
		t.Fatalf("retry retained handoff and join: %v", err)
	}
	store.mu.Lock()
	attempts := store.attempts
	store.mu.Unlock()
	if attempts != 2 {
		t.Fatalf("handoff proof attempts = %d, want exact fail-once retry", attempts)
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
	evt := eventtest.RuntimeControl("after-reset", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"reset-proof"}); err != nil {
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
			[]RoutePlanLiveRecipient{{RecipientID: "reset-race-proof", SubscriberType: routePlanSubscriberAgent}},
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
			result <- recipient.send(context.Background(), evt, events.DeliveryRoute{})
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
