package bus

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

type captureStore struct {
	events     []events.Event
	deliveries map[string][]string
}

func (s *captureStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.events = append(s.events, evt)
	return nil
}

func (s *captureStore) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	if s.deliveries == nil {
		s.deliveries = make(map[string][]string)
	}
	s.deliveries[eventID] = append([]string(nil), agentIDs...)
	return nil
}

type failingDeliveryStore struct{}

func (failingDeliveryStore) AppendEvent(_ context.Context, _ events.Event) error { return nil }
func (failingDeliveryStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return errors.New("insert failed")
}

type selectiveFailStore struct {
	active []string
}

func (s selectiveFailStore) AppendEvent(_ context.Context, _ events.Event) error { return nil }
func (s selectiveFailStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return errors.New("insert failed")
}
func (s selectiveFailStore) ListActiveAgentIDs(_ context.Context) ([]string, error) {
	return append([]string(nil), s.active...), nil
}

func TestEventBusOpCoRoutingPersistsDeliveries(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)

	_ = bus.Subscribe("support-a", events.EventType("bug_reported"))
	_ = bus.Subscribe("cto-a", events.EventType("bug_reported"))

	if err := bus.SetRoutingTable("vertical-a", &RoutingTable{
		VerticalID: "vertical-a",
		Routes: []Route{
			{EventPattern: "bug_reported", SubscriberID: "support-a", Status: "active"},
			{EventPattern: "bug_reported", SubscriberID: "cto-a", Status: "active"},
		},
	}); err != nil {
		t.Fatalf("set routing table: %v", err)
	}

	payload, _ := json.Marshal(map[string]string{"bug": "x"})
	evt := events.Event{
		ID:          "11111111-1111-1111-1111-111111111111",
		Type:        events.EventType("bug_reported"),
		SourceAgent: "support-a",
		VerticalID:  "vertical-a",
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	d, ok := store.deliveries[evt.ID]
	if !ok {
		t.Fatalf("expected delivery manifest for event")
	}
	if len(d) != 2 {
		t.Fatalf("expected 2 deliveries, got %d", len(d))
	}
}

func TestEventBusFactoryEventWithVerticalIDUsesSubscriptions(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	ch := bus.Subscribe("validation-coordinator", events.EventType("validation.started"))

	evt := events.Event{
		ID:          "22222222-2222-2222-2222-222222222222",
		Type:        events.EventType("validation.started"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  "vertical-b",
		Payload:     []byte(`{"vertical_id":"vertical-b"}`),
		CreatedAt:   time.Now(),
	}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-ch:
		if got.ID != evt.ID {
			t.Fatalf("expected fallback event %s, got %s", evt.ID, got.ID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("expected delivery to static subscriber")
	}
	if len(store.events) != 1 {
		t.Fatalf("expected only published factory event (no contradiction), got %d", len(store.events))
	}
}

func TestEventBusPublish_ReturnsErrorWhenDeliveryPersistenceFails(t *testing.T) {
	bus := NewEventBus(failingDeliveryStore{})
	_ = bus.Subscribe("empire-coordinator", events.EventType("system.directive"))
	err := bus.Publish(context.Background(), events.Event{
		ID:          "33333333-3333-3333-3333-333333333333",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive_text":"x"}`),
		CreatedAt:   time.Now(),
	})
	if err == nil {
		t.Fatal("expected delivery persistence error")
	}
}

func TestEventBusPublish_AllowsNonAgentEphemeralSubscribersWithoutDeliveryRows(t *testing.T) {
	bus := NewEventBus(selectiveFailStore{active: []string{"empire-coordinator"}})
	ch := bus.Subscribe("telegram-delivery-loop", events.EventType("human_task.approved"))
	err := bus.Publish(context.Background(), events.Event{
		ID:          "44444444-4444-4444-4444-444444444444",
		Type:        events.EventType("human_task.approved"),
		SourceAgent: "empire-coordinator",
		Payload:     []byte(`{"task_id":"t1"}`),
		CreatedAt:   time.Now(),
	})
	if err != nil {
		t.Fatalf("expected ephemeral delivery without persistence failure, got: %v", err)
	}
	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		t.Fatal("expected event delivered to ephemeral subscriber")
	}
}

type atomicStoreStub struct {
	mu            sync.Mutex
	appendCalls   int
	insertCalls   int
	atomicCalls   int
	lastDeliverTo []string
}

func (s *atomicStoreStub) AppendEvent(_ context.Context, _ events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendCalls++
	return nil
}

func (s *atomicStoreStub) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insertCalls++
	return nil
}

func (s *atomicStoreStub) PersistEventWithDeliveries(_ context.Context, _ events.Event, agentIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.atomicCalls++
	s.lastDeliverTo = append([]string(nil), agentIDs...)
	return nil
}

func TestEventBusPublish_UsesAtomicPersistenceWhenAvailable(t *testing.T) {
	store := &atomicStoreStub{}
	bus := NewEventBus(store)
	ch := bus.Subscribe("empire-coordinator", events.EventType("system.directive"))

	evt := events.Event{
		ID:          "4f72f905-14fc-4769-bf3d-817a83953f9a",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive_text":"go"}`),
		CreatedAt:   time.Now(),
	}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		t.Fatal("expected subscribed delivery")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.atomicCalls != 1 {
		t.Fatalf("expected atomic persistence call=1, got %d", store.atomicCalls)
	}
	if store.appendCalls != 0 || store.insertCalls != 0 {
		t.Fatalf("expected non-atomic writes skipped, append=%d insert=%d", store.appendCalls, store.insertCalls)
	}
	if len(store.lastDeliverTo) != 1 || store.lastDeliverTo[0] != "empire-coordinator" {
		t.Fatalf("unexpected atomic recipients: %#v", store.lastDeliverTo)
	}
}
func TestEventBusRejectsStaleRuntimeEpoch(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})

	previous := CurrentRuntimeEpoch()
	current := BumpRuntimeEpoch()
	if current <= previous {
		t.Fatalf("expected bumped epoch > previous, got current=%d previous=%d", current, previous)
	}

	staleCtx := WithRuntimeEpoch(context.Background(), previous)
	err := bus.Publish(staleCtx, events.Event{
		ID:          "evt-stale",
		Type:        events.EventType("system.started"),
		SourceAgent: "runtime",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now(),
	})
	if !errors.Is(err, ErrStaleRuntimeEpoch) {
		t.Fatalf("expected ErrStaleRuntimeEpoch, got %v", err)
	}

	currentCtx := WithRuntimeEpoch(context.Background(), current)
	if err := bus.Publish(currentCtx, events.Event{
		ID:          "evt-current",
		Type:        events.EventType("system.started"),
		SourceAgent: "runtime",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("publish with current epoch failed: %v", err)
	}
}
func TestEventBus_DeliverByType_UsesSubscriptions(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ch := bus.Subscribe("a1", events.EventType("foo.*"))

	bus.deliverByType(events.Event{
		ID:          "e1",
		Type:        events.EventType("foo.bar"),
		SourceAgent: "x",
	})

	select {
	case evt := <-ch:
		if string(evt.Type) != "foo.bar" {
			t.Fatalf("unexpected evt: %s", evt.Type)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected delivered event")
	}
}

func TestEventBus_PublishRejectsInvalidEventType(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	err := bus.Publish(context.Background(), events.Event{
		ID:          "e-invalid",
		Type:        events.EventType("Bad Event"),
		SourceAgent: "x",
	})
	if err == nil {
		t.Fatal("expected invalid event type error")
	}
}

func TestIsValidEventTypeName(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{name: "valid", in: "scan.requested", want: true},
		{name: "valid underscore", in: "heartbeat.opco_ceo", want: true},
		{name: "empty", in: "", want: false},
		{name: "space", in: "scan requested", want: false},
		{name: "uppercase", in: "Scan.Requested", want: false},
		{name: "slash", in: "scan/requested", want: false},
		{name: "empty token", in: "scan..requested", want: false},
	}
	for _, tc := range cases {
		if got := isValidEventTypeName(tc.in); got != tc.want {
			t.Fatalf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestEventBusFactoryRoutingClassification(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	if !bus.isFactoryEvent(events.EventType("opco.spinup_requested")) {
		t.Fatal("expected opco.* to be classified as factory event")
	}
	if !bus.isFactoryEvent(events.EventType("validation.started")) {
		t.Fatal("expected validation.* to be classified as factory event")
	}
	if bus.isFactoryEvent(events.EventType("bug_reported")) {
		t.Fatal("expected short OpCo event to be non-factory")
	}
	if bus.isFactoryEvent(events.EventType("qa.validation_failed")) {
		t.Fatal("expected qa.* to be non-factory (OpCo internal)")
	}
}

type interceptStoreStub struct {
	mu         sync.Mutex
	events     []events.Event
	deliveries map[string][]string
}

func (s *interceptStoreStub) AppendEvent(_ context.Context, evt events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, evt)
	return nil
}

func (s *interceptStoreStub) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deliveries == nil {
		s.deliveries = make(map[string][]string)
	}
	s.deliveries[eventID] = append([]string(nil), agentIDs...)
	return nil
}

type interceptStub struct {
	passthrough bool
	deferred    []events.Event
}

func (s interceptStub) Intercept(_ context.Context, _ events.Event) (bool, []events.Event, error) {
	return s.passthrough, s.deferred, nil
}

func TestEventBus_Publish_InterceptorConsumeWithDeferred(t *testing.T) {
	store := &interceptStoreStub{}
	bus := NewEventBus(store)
	d := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("portfolio.digest_compiled"),
		SourceAgent: "pipeline-coordinator",
		Payload:     mustJSON(map[string]any{"ok": true}),
		CreatedAt:   time.Now(),
	}
	bus.SetInterceptors(interceptStub{
		passthrough: false,
		deferred:    []events.Event{d},
	})

	ch := bus.Subscribe("agent-a", events.EventType("portfolio.digest_compiled"))
	in := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  uuid.NewString(),
		Payload:     mustJSON(map[string]any{"vertical_id": uuid.NewString()}),
		CreatedAt:   time.Now(),
	}
	if err := bus.Publish(context.Background(), in); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case got := <-ch:
		if got.Type != d.Type {
			t.Fatalf("expected deferred type %s, got %s", d.Type, got.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("expected deferred delivery")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.events) != 2 {
		t.Fatalf("expected 2 persisted events (inbound + deferred), got %d", len(store.events))
	}
	if _, ok := store.deliveries[in.ID]; ok {
		t.Fatalf("expected consumed inbound event to have no delivery manifest")
	}
	if got := len(store.deliveries[d.ID]); got != 1 {
		t.Fatalf("expected deferred delivery manifest size=1, got %d", got)
	}
}
