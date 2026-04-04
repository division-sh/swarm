package bus_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/runtime/flowmodel"
	"swarm/internal/runtime/semanticview"
)

type waitInterceptor struct {
	started chan struct{}
	release chan struct{}
}

func (w waitInterceptor) Intercept(_ context.Context, _ events.Event) (bool, []events.Event, error) {
	select {
	case w.started <- struct{}{}:
	default:
	}
	<-w.release
	return true, nil, nil
}

type deferredChainInterceptor struct{}

func (deferredChainInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	next := ""
	switch evt.Type {
	case events.EventType("custom.root"):
		next = "custom.middle"
	case events.EventType("custom.middle"):
		next = "custom.leaf"
	case events.EventType("custom.leaf"):
		next = "custom.final"
	default:
		return true, nil, nil
	}
	return false, []events.Event{(events.Event{
		Type:      events.EventType(next),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID(evt.EntityID())}, nil
}

type recordingLoggerHook struct {
	entries []recordedLogEntry
}

type recordedLogEntry struct {
	Action string
	Detail any
}

func (h *recordingLoggerHook) Log(_ context.Context, _, _, action, _, _, _, _, _ string, _ map[string]string, detail any, _ string, _ int) {
	h.entries = append(h.entries, recordedLogEntry{Action: action, Detail: detail})
}

func TestEventBusPublish_UsesPayloadValidator(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(eventType string, payload []byte) error {
			if strings.TrimSpace(eventType) != "task.completed" {
				t.Fatalf("unexpected event type %q", eventType)
			}
			if string(payload) != `{"ok":true}` {
				t.Fatalf("unexpected payload %s", string(payload))
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), events.Event{
		Type:    "task.completed",
		Payload: []byte(`{"ok":true}`),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEventBusPublish_PayloadValidatorFailureAbortsPublish(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	err = eb.Publish(context.Background(), events.Event{
		Type:    "task.completed",
		Payload: []byte(`{}`),
	})
	if err == nil || !errors.Is(err, runtimebus.ErrPayloadValidation) {
		t.Fatalf("expected payload validator failure, got %v", err)
	}
}

func TestEventBusPublishDirect_PayloadValidatorFailureAbortsPublish(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	err = eb.PublishDirect(context.Background(), events.Event{
		Type:    "task.completed",
		Payload: []byte(`{}`),
	}, []string{"agent-a"})
	if err == nil || !errors.Is(err, runtimebus.ErrPayloadValidation) {
		t.Fatalf("expected payload validator failure, got %v", err)
	}
}

func TestEventBusWaitForQuiescenceWaitsForPublishCompletion(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{waitInterceptor{started: started, release: release}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- eb.Publish(context.Background(), events.Event{Type: "task.completed", Payload: []byte(`{}`)})
	}()

	select {
	case <-started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("interceptor did not start")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForQuiescence error = %v, want deadline exceeded while publish is blocked", err)
	}

	close(release)
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("publish did not finish")
	}

	waitCtx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after publish completion: %v", err)
	}
}

func TestEventBusPublish_InterceptsMultiHopDeferredChains(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{deferredChainInterceptor{}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), (events.Event{
		Type:      events.EventType("custom.root"),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.eventTypes(); len(got) < 4 || got[0] != "custom.root" || got[1] != "custom.middle" || got[2] != "custom.leaf" || got[3] != "custom.final" {
		t.Fatalf("persisted event types prefix = %v, want prefix [custom.root custom.middle custom.leaf custom.final]", got)
	}
}

func TestEventBusPublish_InheritsRunAndParentFromInboundContext(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := runtimecorrelation.WithInboundEvent(context.Background(), events.Event{
		ID:    "evt-parent",
		Type:  events.EventType("task.started"),
		RunID: "run-abc",
	})
	if err := eb.Publish(ctx, events.Event{
		ID:   "evt-child",
		Type: events.EventType("task.completed"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.events) != 1 {
		found := false
		for _, evt := range store.events {
			if evt.ID != "evt-child" {
				continue
			}
			found = true
			if got := evt.RunID; got != "run-abc" {
				t.Fatalf("persisted run_id = %q, want run-abc", got)
			}
			if got := evt.ParentEventID; got != "evt-parent" {
				t.Fatalf("persisted parent_event_id = %q, want evt-parent", got)
			}
		}
		if !found {
			t.Fatalf("persisted events = %#v, want child event", store.events)
		}
		return
	}
	if got := store.events[0].RunID; got != "run-abc" {
		t.Fatalf("persisted run_id = %q, want run-abc", got)
	}
	if got := store.events[0].ParentEventID; got != "evt-parent" {
		t.Fatalf("persisted parent_event_id = %q, want evt-parent", got)
	}
}

func TestEventBusPublish_ZeroRecipientsDoesNotEmitContradiction(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.Publish(context.Background(), events.Event{
		ID:   "evt-zero",
		Type: events.EventType("custom.no_subscribers"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := store.eventTypes()
	if len(got) != 1 || got[0] != "custom.no_subscribers" {
		t.Fatalf("persisted event types = %v, want [custom.no_subscribers]", got)
	}
}

func TestEventBusPublish_RuntimeLogBypassesContradictionRouting(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.Publish(context.Background(), events.Event{
		ID:   "evt-log",
		Type: events.EventType("platform.runtime_log"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := store.eventTypes()
	if len(got) != 1 || got[0] != "platform.runtime_log" {
		t.Fatalf("persisted event types = %v, want [platform.runtime_log]", got)
	}
}

func TestEventBusPublish_HumanTaskEventsRouteBySubscriptionOnly(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("requester")
	defer eb.Unsubscribe("requester")

	if err := eb.Publish(context.Background(), events.Event{
		Type:    events.EventType("human_task.approved"),
		Payload: []byte(`{"requesting_agent":"requester"}`),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case evt := <-ch:
		t.Fatalf("unexpected delivery without subscription: %#v", evt)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestEventBusPublish_LogsRoutedAndSubscribedRecipientsSeparately(t *testing.T) {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "producer",
	}
	discovery := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "discovery",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID:           "scan-orchestrator",
				SubscribesTo: []string{"scan.requested"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, discovery}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer":  &root.Children[0],
				"discovery": &root.Children[1],
			},
		},
	}
	hook := &recordingLoggerHook{}
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		Logger:         hook,
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eb.Subscribe("direct-agent", events.EventType("producer/scan.requested"))
	defer eb.Unsubscribe("direct-agent")

	if err := eb.Publish(context.Background(), events.Event{
		Type: events.EventType("producer/scan.requested"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var delivered any
	for _, entry := range hook.entries {
		if entry.Action == "delivered" {
			delivered = entry.Detail
		}
	}
	if delivered == nil {
		t.Fatal("expected delivered log entry")
	}
	detail, ok := delivered.(map[string]any)
	if !ok {
		t.Fatalf("delivered detail type = %T, want map[string]any", delivered)
	}
	routed, _ := detail["routed_recipients"].([]map[string]any)
	if len(routed) == 0 {
		// logger detail may pass through as []any after interface widening
		if raw, ok := detail["routed_recipients"].([]any); ok {
			routed = make([]map[string]any, 0, len(raw))
			for _, item := range raw {
				if cast, ok := item.(map[string]any); ok {
					routed = append(routed, cast)
				}
			}
		}
	}
	if len(routed) == 0 || routed[0]["id"] != "scan-orchestrator" {
		t.Fatalf("routed_recipients = %#v, want scan-orchestrator", detail["routed_recipients"])
	}
	if got := routed[0]["matched_pattern"]; got != "producer/scan.requested" {
		t.Fatalf("matched_pattern = %#v, want producer/scan.requested", got)
	}
	if got := routed[0]["route_source"]; got != "pin_auto_wire" {
		t.Fatalf("route_source = %#v, want pin_auto_wire", got)
	}
	if got := routed[0]["localized_event"]; got != "scan.requested" {
		t.Fatalf("localized_event = %#v, want scan.requested", got)
	}
	subs, _ := detail["subscription_recipients"].([]string)
	if len(subs) == 0 {
		if raw, ok := detail["subscription_recipients"].([]any); ok {
			subs = make([]string, 0, len(raw))
			for _, item := range raw {
				if cast, ok := item.(string); ok {
					subs = append(subs, cast)
				}
			}
		}
	}
	if len(subs) != 1 || subs[0] != "direct-agent" {
		t.Fatalf("subscription_recipients = %#v, want [direct-agent]", detail["subscription_recipients"])
	}
}

func TestEventBusPublish_RecordsPublishDiagnosticsInTurnRecorder(t *testing.T) {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "producer",
	}
	discovery := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "discovery",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID:           "scan-orchestrator",
				SubscribesTo: []string{"scan.requested"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, discovery}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer":  &root.Children[0],
				"discovery": &root.Children[1],
			},
		},
	}
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	if err := eb.Publish(ctx, (events.Event{
		Type: "producer/scan.requested",
	}).WithEntityID("ent-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	diags := recorder.SnapshotPublishes()
	if len(diags) != 1 {
		t.Fatalf("publish diagnostics = %#v", diags)
	}
	if diags[0].EventType != "producer/scan.requested" {
		t.Fatalf("event_type = %q", diags[0].EventType)
	}
	if len(diags[0].RoutedRecipients) != 1 {
		t.Fatalf("routed_recipients = %#v", diags[0].RoutedRecipients)
	}
	if diags[0].RoutedRecipients[0].RouteSource != "pin_auto_wire" {
		t.Fatalf("route_source = %q", diags[0].RoutedRecipients[0].RouteSource)
	}
	if diags[0].RoutedRecipients[0].LocalizedEvent != "scan.requested" {
		t.Fatalf("localized_event = %q", diags[0].RoutedRecipients[0].LocalizedEvent)
	}
}
