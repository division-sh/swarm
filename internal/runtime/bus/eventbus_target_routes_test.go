package bus

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type targetRouteMemoryStore struct {
	events map[string]events.Event
	routes map[string][]events.DeliveryRoute
	scopes map[string]replayclaim.CommittedReplayScope
}

func newTargetRouteMemoryStore() *targetRouteMemoryStore {
	return &targetRouteMemoryStore{
		events: map[string]events.Event{},
		routes: map[string][]events.DeliveryRoute{},
		scopes: map[string]replayclaim.CommittedReplayScope{},
	}
}

func (s *targetRouteMemoryStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.events[evt.ID] = evt
	return nil
}

func (s *targetRouteMemoryStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return nil
}

func (s *targetRouteMemoryStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	var out []string
	for _, route := range s.routes[eventID] {
		if route.SubscriberType == "agent" {
			out = append(out, route.SubscriberID)
		}
	}
	return uniqueStrings(out), nil
}

func (s *targetRouteMemoryStore) SupportsPersistedReplay() bool { return true }

func (s *targetRouteMemoryStore) PersistEventWithDeliveryRouteSetAndScope(_ context.Context, evt events.Event, deliveryRoutes []events.DeliveryRoute, scope replayclaim.CommittedReplayScope) error {
	s.events[evt.ID] = evt
	s.routes[evt.ID] = events.NormalizeDeliveryRoutes(deliveryRoutes)
	s.scopes[evt.ID] = scope
	return nil
}

func (s *targetRouteMemoryStore) ListEventDeliveryRoutes(_ context.Context, eventID string) ([]events.DeliveryRoute, error) {
	return append([]events.DeliveryRoute(nil), s.routes[eventID]...), nil
}

func (s *targetRouteMemoryStore) LoadCommittedReplayScope(_ context.Context, eventID string) (replayclaim.CommittedReplayScope, error) {
	scope := s.scopes[eventID]
	if scope == "" {
		return "", replayclaim.ErrMissingCommittedReplayScope
	}
	return scope, nil
}

func TestEventBusPublish_TargetSetInternalDeliveryUsesPerTargetRoutes(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(string) []Subscriber {
				return []Subscriber{
					{ID: "child-a-listener", Type: "node", Path: "child-a/inst-1"},
					{ID: "child-b-listener", Type: "node", Path: "child-b/inst-1"},
				}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("child/output.done"))
	evt := (events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("child/output.done"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithTargetSet([]events.RouteIdentity{
		{FlowInstance: "child-a/inst-1", EntityID: "ent-a"},
		{FlowInstance: "child-b/inst-1", EntityID: "ent-b"},
	})

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	assertTargetRouteDeliveries(t, ch, "ent-a", "ent-b")

	persisted := store.events[evt.ID]
	if got := persisted.EntityID(); got != "" {
		t.Fatalf("persisted EntityID() = %q, want empty target_set projection", got)
	}
	if got := persisted.FlowInstance(); got != "" {
		t.Fatalf("persisted FlowInstance() = %q, want empty target_set projection", got)
	}
	if got := store.routes[evt.ID]; len(got) != 2 {
		t.Fatalf("persisted delivery routes = %#v, want 2", got)
	}
	for _, route := range store.routes[evt.ID] {
		if route.SubscriberType != "node" || route.SubscriberID != "workflow-runtime" {
			t.Fatalf("delivery route = %#v, want node/workflow-runtime", route)
		}
		if route.Target.Empty() {
			t.Fatalf("delivery route target is empty: %#v", route)
		}
	}

	if err := eb.PublishPersistedRecipients(context.Background(), evt, nil); err != nil {
		t.Fatalf("PublishPersistedRecipients: %v", err)
	}
	assertTargetRouteDeliveries(t, ch, "ent-a", "ent-b")
}

func TestEventBusPublish_NoTargetConcreteRoutedNodeUsesWorkflowCarrierAndNodeDeliveryRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	source := semanticview.Wrap(routedNodeTemplateBundle())
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimecontracts.SystemNodeContract{}, runtimeflowidentity.DeriveRoute("operating", "inst-1")); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("operating/opco.product_initialization_requested"))
	evt := (events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("operating/inst-1/opco.product_initialization_requested"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-operating").WithFlowInstance("operating/inst-1")

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "concrete routed node event delivery")
	if got.FlowInstance() != "operating/inst-1" {
		t.Fatalf("delivered flow instance = %q, want operating/inst-1", got.FlowInstance())
	}

	routes := store.routes[evt.ID]
	if len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want one workflow-runtime carrier route", routes)
	}
	route := routes[0]
	if route.SubscriberType != "node" || route.SubscriberID != "workflow-runtime" {
		t.Fatalf("delivery route = %#v, want node/workflow-runtime carrier", route)
	}
	if route.Target.FlowInstance != "operating/inst-1" || route.Target.EntityID != "ent-operating" {
		t.Fatalf("delivery target = %#v, want operating/inst-1 ent-operating", route.Target)
	}

	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, replayclaim.CommittedReplayScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if len(live) != 1 || live[0] != "workflow-runtime" {
		t.Fatalf("replay live recipients = %#v, want workflow-runtime", live)
	}
	if len(internal) != 1 || internal[0] != "workflow-runtime" {
		t.Fatalf("replay internal recipients = %#v, want workflow-runtime", internal)
	}
	if len(replayRoutes) != 1 || replayRoutes[0].SubscriberID != "workflow-runtime" {
		t.Fatalf("replay routes = %#v, want workflow-runtime carrier route", replayRoutes)
	}
}

func TestEventBusPublish_NoTargetRootRoutedNodeUsesSemanticNodeDeliveryRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	source := semanticview.Wrap(loadTargetRouteTempBundle(t, routedRootNodeFixtureFiles()))
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := eb.SubscribeInternal("portfolio-node", events.EventType("opco.spinup_requested"))
	evt := (events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("opco.spinup_requested"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-root")

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "root routed node event delivery")
	if got.FlowInstance() != "" {
		t.Fatalf("delivered flow instance = %q, want root event", got.FlowInstance())
	}

	routes := store.routes[evt.ID]
	if len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want one semantic root node route", routes)
	}
	route := routes[0]
	if route.SubscriberType != "node" || route.SubscriberID != "portfolio-node" {
		t.Fatalf("delivery route = %#v, want node/portfolio-node", route)
	}
	if !route.Target.Empty() {
		t.Fatalf("delivery target = %#v, want empty root target", route.Target)
	}

	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, replayclaim.CommittedReplayScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if len(live) != 1 || live[0] != "portfolio-node" {
		t.Fatalf("replay live recipients = %#v, want portfolio-node", live)
	}
	if len(internal) != 1 || internal[0] != "portfolio-node" {
		t.Fatalf("replay internal recipients = %#v, want portfolio-node", internal)
	}
	if len(replayRoutes) != 1 || replayRoutes[0].SubscriberID != "portfolio-node" || !replayRoutes[0].Target.Empty() {
		t.Fatalf("replay routes = %#v, want empty node/portfolio-node route", replayRoutes)
	}

	if err := eb.PublishPersistedRecipients(context.Background(), evt, nil); err != nil {
		t.Fatalf("PublishPersistedRecipients: %v", err)
	}
	got = requireBusEvent(t, ch, "root routed node replay delivery")
	if got.FlowInstance() != "" {
		t.Fatalf("replayed flow instance = %q, want root event", got.FlowInstance())
	}
}

func TestEventBusPublish_NoTargetRootRoutedNodePersistsSemanticRouteWithoutInternalSubscription(t *testing.T) {
	store := newTargetRouteMemoryStore()
	source := semanticview.Wrap(loadTargetRouteTempBundle(t, routedRootNodeFixtureFiles()))
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := (events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("opco.spinup_requested"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-root")

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	routes := store.routes[evt.ID]
	if len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want one semantic root node route without an internal subscription", routes)
	}
	route := routes[0]
	if route.SubscriberType != "node" || route.SubscriberID != "portfolio-node" {
		t.Fatalf("delivery route = %#v, want node/portfolio-node", route)
	}
	if !route.Target.Empty() {
		t.Fatalf("delivery target = %#v, want empty root target", route.Target)
	}
	if got := store.scopes[evt.ID]; got != replayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, replayclaim.CommittedReplayScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if len(live) != 0 {
		t.Fatalf("replay live recipients = %#v, want none without an internal carrier", live)
	}
	if len(internal) != 0 {
		t.Fatalf("replay internal recipients = %#v, want none without an internal carrier", internal)
	}
	if len(replayRoutes) != 1 || replayRoutes[0].SubscriberType != "node" || replayRoutes[0].SubscriberID != "portfolio-node" {
		t.Fatalf("replay routes = %#v, want retained semantic node/portfolio-node evidence", replayRoutes)
	}
	if err := eb.PublishPersistedRecipients(context.Background(), evt, nil); err != nil {
		t.Fatalf("PublishPersistedRecipients without internal carrier: %v", err)
	}
}

func assertTargetRouteDeliveries(t *testing.T, ch <-chan events.Event, wantEntityIDs ...string) {
	t.Helper()
	seen := map[string]struct{}{}
	for range wantEntityIDs {
		got := requireBusEvent(t, ch, "target route delivery")
		if len(got.TargetRoutes()) != 0 {
			t.Fatalf("delivered event target_set = %#v, want singular delivery target", got.TargetRoutes())
		}
		target := got.TargetRoute()
		if target.Empty() {
			t.Fatalf("delivered target route is empty: %#v", got)
		}
		seen[target.EntityID] = struct{}{}
	}
	for _, want := range wantEntityIDs {
		if _, ok := seen[want]; !ok {
			t.Fatalf("missing target delivery for %q; saw %#v", want, seen)
		}
	}
}

func loadTargetRouteTempBundle(t *testing.T, files map[string]string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("load target route temp bundle: %v", err)
	}
	return bundle
}

func routedRootNodeFixtureFiles() map[string]string {
	return map[string]string{
		"package.yaml": `name: test
version: 1.0.0
`,
		"events.yaml": `opco.spinup_requested:
  entity_id: string
`,
		"nodes.yaml": `portfolio-node:
  id: portfolio-node
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  event_handlers:
    opco.spinup_requested: {}
`,
	}
}

func routedNodeTemplateBundle() *runtimecontracts.WorkflowContractBundle {
	operating := runtimecontracts.FlowContractView{
		Path:  "operating",
		Paths: runtimecontracts.FlowContractPaths{ID: "operating", Flow: "operating"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{
				Event: "opco.product_initialization_requested",
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"opco.product_initialization_requested": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"lifecycle-orchestrator": {
				ID:            "lifecycle-orchestrator",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"opco.product_initialization_requested"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"opco.product_initialization_requested": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{operating}}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"operating": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"operating": {
				Mode: "template",
				AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{
					Event: "opco.product_initialization_requested",
				},
			},
		},
	}
}
