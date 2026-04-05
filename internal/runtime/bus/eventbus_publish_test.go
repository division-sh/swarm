package bus_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/runtime/flowmodel"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

type fixtureWorkflowModule struct {
	source         semanticview.Source
	workflow       *runtimepipeline.WorkflowDefinition
	workflowNodes  []runtimepipeline.WorkflowNode
	guardRegistry  runtimepipeline.GuardRegistry
	actionRegistry runtimepipeline.ActionRegistry
}

func (m *fixtureWorkflowModule) SemanticSource() semanticview.Source {
	return m.source
}

func (m *fixtureWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}

func (m *fixtureWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.workflowNodes...)
}

func (m *fixtureWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guardRegistry
}

func (m *fixtureWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actionRegistry
}

func newFixtureWorkflowModule(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) runtimepipeline.WorkflowModule {
	t.Helper()
	source := semanticview.Wrap(bundle)
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	workflowNodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	return &fixtureWorkflowModule{
		source:         source,
		workflow:       workflow,
		workflowNodes:  workflowNodes,
		guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
		actionRegistry: runtimepipeline.NewContractActionRegistry(source),
	}
}

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

type eventVisibleInTxInterceptor struct {
	t       *testing.T
	eventID string
}

func (i eventVisibleInTxInterceptor) Intercept(ctx context.Context, _ events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		i.t.Fatal("expected transactional publish context to expose sql tx")
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, i.eventID).Scan(&count); err != nil {
		i.t.Fatalf("query inbound event inside interceptor tx: %v", err)
	}
	if count != 1 {
		i.t.Fatalf("inbound event visible inside interceptor tx count=%d, want 1", count)
	}
	return true, nil, nil
}

type deferredEventVisibleInterceptor struct {
	t        *testing.T
	store    *store.PostgresStore
	eventID  string
	checkFor events.EventType
}

func (i deferredEventVisibleInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	if evt.Type == events.EventType("custom.root") {
		return false, []events.Event{{
			ID:        i.eventID,
			Type:      i.checkFor,
			CreatedAt: time.Now().UTC(),
			Payload:   []byte(`{"entity_id":"ent-1"}`),
		}}, nil
	}
	if evt.Type != i.checkFor {
		return true, nil, nil
	}
	ok, err := i.store.EventExists(ctx, i.eventID)
	if err != nil {
		i.t.Fatalf("EventExists(%s): %v", i.eventID, err)
	}
	if !ok {
		i.t.Fatalf("expected deferred event %s to be persisted before interceptors ran", i.eventID)
	}
	return true, nil, nil
}

type recordingLoggerHook struct {
	entries []recordedLogEntry
}

type recordedLogEntry struct {
	Action string
	Detail any
}

func (h *recordingLoggerHook) Log(_ context.Context, _, _, _, action, _, _, _, _, _ string, _ map[string]string, detail any, _ string, _ int) {
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

func TestEventBusPublishTransactional_PersistsInboundEventBeforeInterceptorsRun(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	eventID := "11111111-1111-1111-1111-111111111111"
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{eventVisibleInTxInterceptor{t: t, eventID: eventID}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), events.Event{
		ID:        eventID,
		Type:      events.EventType("task.completed"),
		CreatedAt: time.Now().UTC(),
		Payload:   []byte(`{}`),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEventBusPublishDeferred_PersistsInboundEventBeforeInterceptorsRun(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	eventID := "22222222-2222-2222-2222-222222222222"
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{deferredEventVisibleInterceptor{
			t:        t,
			store:    pg,
			eventID:  eventID,
			checkFor: events.EventType("custom.middle"),
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), events.Event{
		ID:        "11111111-1111-1111-1111-111111111111",
		Type:      events.EventType("custom.root"),
		CreatedAt: time.Now().UTC(),
		Payload:   []byte(`{"entity_id":"ent-1"}`),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
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

func TestEventBusPublish_NestedDescendantCompletionFlushesDeferredParentEvents(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	module := newFixtureWorkflowModule(t, bundle)
	previous := runtimepipeline.DefaultWorkflowModuleOrNil()
	runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule { return module })
	t.Cleanup(func() {
		if previous == nil {
			runtimepipeline.SetDefaultWorkflowModuleFactory(nil)
			return
		}
		runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule { return previous })
	})
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	var pc *runtimepipeline.PipelineCoordinator
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}

	const rootEntityID = "11111111-1111-1111-1111-111111111111"
	childEntityID := runtimepipeline.FlowInstanceEntityID("child/inst-1")
	grandchildEntityID := runtimepipeline.FlowInstanceEntityID("child/grandchild/inst-1")
	store := runtimepipeline.NewWorkflowInstanceStore(db)
	for _, instance := range []runtimepipeline.WorkflowInstance{
		{
			InstanceID:      rootEntityID,
			SubjectID:       rootEntityID,
			StorageRef:      rootEntityID,
			WorkflowName:    bundle.WorkflowName(),
			WorkflowVersion: bundle.WorkflowVersion(),
			CurrentState:    "idle",
			Metadata: map[string]any{
				"entity_id":  rootEntityID,
				"subject_id": rootEntityID,
			},
		},
		{
			InstanceID:      childEntityID,
			SubjectID:       rootEntityID,
			StorageRef:      "child/inst-1",
			WorkflowName:    "child",
			WorkflowVersion: bundle.WorkflowVersion(),
			CurrentState:    "waiting",
			Metadata: map[string]any{
				"entity_id":        childEntityID,
				"flow_path":        "child/inst-1",
				"subject_id":       rootEntityID,
				"parent_entity_id": rootEntityID,
			},
		},
		{
			InstanceID:      grandchildEntityID,
			SubjectID:       rootEntityID,
			StorageRef:      "child/grandchild/inst-1",
			WorkflowName:    "grandchild",
			WorkflowVersion: bundle.WorkflowVersion(),
			CurrentState:    "finished",
			Metadata: map[string]any{
				"entity_id":        grandchildEntityID,
				"flow_path":        "child/grandchild/inst-1",
				"subject_id":       rootEntityID,
				"parent_entity_id": childEntityID,
			},
		},
	} {
		if err := store.Upsert(context.Background(), instance); err != nil {
			t.Fatalf("seed workflow instance %q: %v", instance.InstanceID, err)
		}
	}

	if err := eb.Publish(context.Background(), (events.Event{
		ID:          "11111111-2222-3333-4444-555555555555",
		Type:        events.EventType("child/grandchild/micro.done"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"` + grandchildEntityID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(grandchildEntityID)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := eb.WaitForQuiescence(context.Background()); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}

	child, found, err := store.Load(context.Background(), childEntityID)
	if err != nil {
		t.Fatalf("load child instance: %v", err)
	}
	if !found {
		t.Fatal("expected child instance")
	}
	if got := strings.TrimSpace(child.CurrentState); got != "completed" {
		t.Fatalf("child current_state = %q, want completed", got)
	}

	root, found, err := store.Load(context.Background(), rootEntityID)
	if err != nil {
		t.Fatalf("load root instance: %v", err)
	}
	if !found {
		t.Fatal("expected root instance")
	}
	if got := strings.TrimSpace(root.CurrentState); got != "done" {
		t.Fatalf("root current_state = %q, want done", got)
	}

	var emitted []string
	rows, err := db.QueryContext(context.Background(), `SELECT event_name FROM events ORDER BY created_at ASC, event_id ASC`)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan event: %v", err)
		}
		emitted = append(emitted, strings.TrimSpace(name))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate events: %v", err)
	}
	if !contains(emitted, "child/step.result") {
		t.Fatalf("events = %v, want child/step.result", emitted)
	}
	if !contains(emitted, "pipeline.complete") {
		t.Fatalf("events = %v, want pipeline.complete", emitted)
	}
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if strings.TrimSpace(item) == strings.TrimSpace(want) {
			return true
		}
	}
	return false
}

func TestEventBusPublish_NestedThreeLevelChain_FromRootStartCompletesPipeline(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	module := newFixtureWorkflowModule(t, bundle)
	previous := runtimepipeline.DefaultWorkflowModuleOrNil()
	runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule { return module })
	t.Cleanup(func() {
		if previous == nil {
			runtimepipeline.SetDefaultWorkflowModuleFactory(nil)
			return
		}
		runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule { return previous })
	})
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	var pc *runtimepipeline.PipelineCoordinator
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}

	const rootEntityID = "11111111-1111-1111-1111-111111111111"
	if err := runtimepipeline.NewWorkflowInstanceStore(db).Upsert(context.Background(), runtimepipeline.WorkflowInstance{
		InstanceID:      rootEntityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "idle",
	}); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}

	if err := eb.Publish(context.Background(), (events.Event{
		ID:          "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		Type:        events.EventType("pipeline.start"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"` + rootEntityID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(rootEntityID)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := eb.WaitForQuiescence(context.Background()); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}

	root, found, err := runtimepipeline.NewWorkflowInstanceStore(db).Load(context.Background(), rootEntityID)
	if err != nil {
		t.Fatalf("load root instance: %v", err)
	}
	if !found {
		t.Fatal("expected root instance")
	}
	if got := strings.TrimSpace(root.CurrentState); got != "done" {
		rows, _ := db.QueryContext(context.Background(), `SELECT event_name, COALESCE(entity_id::text,''), COALESCE(flow_instance,'') FROM events ORDER BY created_at ASC, event_id ASC`)
		dump := make([]string, 0)
		if rows != nil {
			defer rows.Close()
			for rows.Next() {
				var name, entityID, flowInstance string
				if scanErr := rows.Scan(&name, &entityID, &flowInstance); scanErr == nil {
					dump = append(dump, name+" entity="+entityID+" flow="+flowInstance)
				}
			}
		}
		instances, _ := runtimepipeline.NewWorkflowInstanceStore(db).List(context.Background())
		t.Fatalf("root current_state = %q, want done; events=%v instances=%#v", got, dump, instances)
	}
}

func TestEventBusPublish_GatedChildFlowCompletionAdvancesRoot(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-gates-in-child-flow")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	module := newFixtureWorkflowModule(t, bundle)
	previous := runtimepipeline.DefaultWorkflowModuleOrNil()
	runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule { return module })
	t.Cleanup(func() {
		if previous == nil {
			runtimepipeline.SetDefaultWorkflowModuleFactory(nil)
			return
		}
		runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule { return previous })
	})
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	var pc *runtimepipeline.PipelineCoordinator
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}

	const rootEntityID = "11111111-1111-1111-1111-111111111111"
	if err := runtimepipeline.NewWorkflowInstanceStore(db).Upsert(context.Background(), runtimepipeline.WorkflowInstance{
		InstanceID:      rootEntityID,
		SubjectID:       rootEntityID,
		StorageRef:      rootEntityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "pending",
		Metadata: map[string]any{
			"entity_id":  rootEntityID,
			"subject_id": rootEntityID,
		},
	}); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}

	if err := eb.Publish(context.Background(), (events.Event{
		ID:          "11111111-2222-3333-4444-555555555555",
		Type:        events.EventType("validate.requested"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"` + rootEntityID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(rootEntityID)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := eb.WaitForQuiescence(context.Background()); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}

	root, found, err := runtimepipeline.NewWorkflowInstanceStore(db).Load(context.Background(), rootEntityID)
	if err != nil {
		t.Fatalf("load root instance: %v", err)
	}
	if !found {
		t.Fatal("expected root instance")
	}
	if got := strings.TrimSpace(root.CurrentState); got != "done" {
		rows, _ := db.QueryContext(context.Background(), `SELECT event_name, COALESCE(entity_id::text,''), COALESCE(flow_instance,''), COALESCE(payload::text,'') FROM events ORDER BY created_at ASC, event_id ASC`)
		dump := make([]string, 0)
		if rows != nil {
			defer rows.Close()
			for rows.Next() {
				var name, entityID, flowInstance, payload string
				if scanErr := rows.Scan(&name, &entityID, &flowInstance, &payload); scanErr == nil {
					dump = append(dump, name+" entity="+entityID+" flow="+flowInstance+" payload="+payload)
				}
			}
		}
		instances, _ := runtimepipeline.NewWorkflowInstanceStore(db).List(context.Background())
		t.Fatalf("root current_state = %q, want done; root metadata=%#v events=%v instances=%#v", got, root.Metadata, dump, instances)
	}
}

func TestEventBusPublish_RecordsNestedDescendantLocalizedEvent(t *testing.T) {
	grandchild := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"micro.done"}},
			},
		},
		Path: "child/grandchild",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.done": {},
		},
	}
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"step.result"}},
			},
		},
		Path: "child",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"child-aggregator": {
				ID:           "child-aggregator",
				SubscribesTo: []string{"grandchild/micro.done"},
			},
		},
		Children: []runtimecontracts.FlowContractView{grandchild},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child":      &root.Children[0],
				"grandchild": &root.Children[0].Children[0],
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
		Type: "child/grandchild/micro.done",
	}).WithEntityID("ent-grandchild")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	diags := recorder.SnapshotPublishes()
	if len(diags) != 1 || len(diags[0].RoutedRecipients) != 1 {
		t.Fatalf("publish diagnostics = %#v", diags)
	}
	if got := diags[0].RoutedRecipients[0].LocalizedEvent; got != "grandchild/micro.done" {
		t.Fatalf("localized_event = %q, want grandchild/micro.done", got)
	}
}

func TestEventBusPublish_RecordsNestedTemplateInstanceLocalizedEvent(t *testing.T) {
	grandchild := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
		},
		Path: "child/grandchild",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {
				ID:           "worker-{instance_id}",
				SubscribesTo: []string{"micro.done"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.done": {},
		},
	}
	child := runtimecontracts.FlowContractView{
		Paths:    runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Path:     "child",
		Children: []runtimecontracts.FlowContractView{grandchild},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child":      &root.Children[0],
				"grandchild": &root.Children[0].Children[0],
			},
		},
	}
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimecontracts.SystemNodeContract{}, runtimeflowidentity.DeriveRoute("child/grandchild", "inst-1")); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	if err := eb.Publish(ctx, (events.Event{
		Type: "child/grandchild/inst-1/micro.done",
	}).WithEntityID("ent-grandchild")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	diags := recorder.SnapshotPublishes()
	if len(diags) != 1 || len(diags[0].RoutedRecipients) != 1 {
		t.Fatalf("publish diagnostics = %#v", diags)
	}
	if got := diags[0].RoutedRecipients[0].LocalizedEvent; got != "micro.done" {
		t.Fatalf("localized_event = %q, want micro.done", got)
	}
}
