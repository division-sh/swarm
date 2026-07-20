package bus_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"time"
)

func TestEventBusRemoveFlowInstanceDropsDerivedRoutes(t *testing.T) {
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Template: runtimecontracts.SystemNodeContract{
			ID:           "reviewer-{instance_id}",
			Produces:     []string{"task.started"},
			SubscribesTo: []string{"task.started"},
		},
		Identity: runtimeflowidentity.DeriveRoute("review", "inst-1"),
	}); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	if got := eb.RouteTable().Resolve("review/inst-1/task.started"); len(got) != 1 || got[0].ID != "reviewer-inst-1" {
		t.Fatalf("resolved subscribers after add = %#v", got)
	}
	if err := eb.RemoveFlowInstanceRoute(runtimeflowidentity.DeriveRoute("review", "inst-1")); err != nil {
		t.Fatalf("RemoveFlowInstance: %v", err)
	}
	if got := eb.RouteTable().Resolve("review/inst-1/task.started"); len(got) != 0 {
		t.Fatalf("resolved subscribers after remove = %#v, want none", got)
	}
}

func TestEventBusFlowInstanceTemplateDerivesSubscriptionsFromHandlerKeys(t *testing.T) {
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Template: runtimecontracts.SystemNodeContract{
			ID: "reviewer-{instance_id}",
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"task.started": {Emit: runtimecontracts.EmitSpec{Event: "task.started"}},
			},
		},
		Identity: runtimeflowidentity.DeriveRoute("review", "inst-1"),
	}); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	if got := eb.RouteTable().Resolve("review/inst-1/task.started"); len(got) != 1 || got[0].ID != "reviewer-inst-1" {
		t.Fatalf("resolved subscribers = %#v, want reviewer-inst-1", got)
	}
}

type routePersistenceTestStore struct {
	routes           map[string]runtimebus.FlowInstanceRouteRecord
	deliveries       map[string][]string
	upsertErr        error
	deleteErr        error
	rollbackCalls    []string
	deleteCalls      []runtimeflowidentity.Route
	upsertCalls      int
	upsertAfterWrite bool
}

func (s *routePersistenceTestStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return prepareTestCommitPublish(ctx, plan, &testCommitPublishTransaction{finalize: func(ctx context.Context, req runtimebus.CommitPublishRequest) error {
		return s.InsertEventDeliveryRoutes(ctx, req.Event.ID(), req.DeliveryRoutes)
	}})
}

func (s *routePersistenceTestStore) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	if s.deliveries == nil {
		s.deliveries = map[string][]string{}
	}
	s.deliveries[eventID] = append([]string(nil), agentIDs...)
	return nil
}
func (*routePersistenceTestStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}

func (s *routePersistenceTestStore) UpsertFlowInstanceRoute(_ context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	s.upsertCalls++
	if s.routes == nil {
		s.routes = map[string]runtimebus.FlowInstanceRouteRecord{}
	}
	s.routes[route.Identity.ScopeKey+"/"+route.Identity.InstanceID] = route
	if s.upsertAfterWrite && s.upsertErr != nil {
		return s.upsertErr
	}
	if s.upsertErr != nil {
		delete(s.routes, route.Identity.ScopeKey+"/"+route.Identity.InstanceID)
		return s.upsertErr
	}
	return nil
}

func TestEventBusRestorePersistedFlowInstanceRouteDoesNotRewritePersistence(t *testing.T) {
	store := &routePersistenceTestStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	req := runtimebus.FlowInstanceRouteMaterializationRequest{
		Template: runtimecontracts.SystemNodeContract{
			ID:           "reviewer-{instance_id}",
			Produces:     []string{"task.started"},
			SubscribesTo: []string{"task.started"},
		},
		Identity: runtimeflowidentity.DeriveRoute("review", "inst-1"),
	}
	if err := eb.RestorePersistedFlowInstanceRoute(req); err != nil {
		t.Fatalf("RestorePersistedFlowInstanceRoute: %v", err)
	}
	if store.upsertCalls != 0 || len(store.routes) != 0 {
		t.Fatalf("route recovery rewrote persistence: calls=%d routes=%#v", store.upsertCalls, store.routes)
	}
	if got := eb.RouteTable().Resolve("review/inst-1/task.started"); len(got) != 1 || got[0].ID != "reviewer-inst-1" {
		t.Fatalf("restored route subscribers = %#v, want reviewer-inst-1", got)
	}
}

func (s *routePersistenceTestStore) RollbackFlowInstanceRoute(_ context.Context, identity runtimeflowidentity.Route) error {
	s.rollbackCalls = append(s.rollbackCalls, identity.ScopeKey+"/"+identity.InstanceID)
	delete(s.routes, identity.ScopeKey+"/"+identity.InstanceID)
	return nil
}

func (s *routePersistenceTestStore) DeleteFlowInstanceRoute(_ context.Context, identity runtimeflowidentity.Route) error {
	s.deleteCalls = append(s.deleteCalls, identity)
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.routes, identity.ScopeKey+"/"+identity.InstanceID)
	return nil
}

func TestEventBusFlowInstanceRouteIdentityOwnerRejectsMismatchedExplicitPath(t *testing.T) {
	store := &routePersistenceTestStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	installed := runtimeflowidentity.DeriveRoute("review", "inst-1")
	req := runtimebus.FlowInstanceRouteMaterializationRequest{
		Template: runtimecontracts.SystemNodeContract{
			ID:           "reviewer-{instance_id}",
			Produces:     []string{"task.started"},
			SubscribesTo: []string{"task.started"},
		},
		Identity: installed,
	}
	if err := eb.AddFlowInstanceRoute(req); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(req); err != nil {
		t.Fatalf("exact AddFlowInstanceRoute replay: %v", err)
	}
	if got := eb.RouteTable().Resolve("review/inst-1/task.started"); len(got) != 1 || got[0].ID != "reviewer-inst-1" {
		t.Fatalf("routes after exact replay = %#v, want one installed owner route", got)
	}
	mismatched := runtimeflowidentity.StoredRoute("worker", "other", installed.InstancePath)
	if eb.RouteTable().HasFlowInstanceRoute(mismatched) {
		t.Fatal("HasFlowInstanceRoute accepted a different identity at the installed path")
	}
	mismatchedReq := req
	mismatchedReq.Identity = mismatched
	if err := eb.AddFlowInstanceRoute(mismatchedReq); err == nil || !strings.Contains(err.Error(), "is owned by scope") {
		t.Fatalf("mismatched AddFlowInstanceRoute error = %v, want complete-owner conflict", err)
	}
	if err := eb.RemoveFlowInstanceRoute(mismatched); err == nil || !strings.Contains(err.Error(), "is owned by scope") {
		t.Fatalf("mismatched RemoveFlowInstanceRoute error = %v, want complete-owner conflict", err)
	}
	if len(store.deleteCalls) != 0 {
		t.Fatalf("persistence delete calls = %#v, want none for rejected removal", store.deleteCalls)
	}
	if !eb.RouteTable().HasFlowInstanceRoute(installed) {
		t.Fatal("installed identity disappeared after mismatched add/remove")
	}
	if got := eb.RouteTable().Resolve("review/inst-1/task.started"); len(got) != 1 || got[0].ID != "reviewer-inst-1" {
		t.Fatalf("routes after mismatched add/remove = %#v, want owner authority unchanged", got)
	}
	normalizedRemoval := runtimeflowidentity.Route{
		ScopeKey:     " /review/ ",
		InstanceID:   " inst-1 ",
		InstancePath: " /review/inst-1/ ",
	}
	if err := eb.RemoveFlowInstanceRoute(normalizedRemoval); err != nil {
		t.Fatalf("RemoveFlowInstanceRoute owner: %v", err)
	}
	if len(store.deleteCalls) != 1 || store.deleteCalls[0] != installed {
		t.Fatalf("persistence delete calls = %#v, want one canonical owner", store.deleteCalls)
	}
	if err := eb.RemoveFlowInstanceRoute(normalizedRemoval); err != nil {
		t.Fatalf("exact RemoveFlowInstanceRoute replay: %v", err)
	}
	if len(store.deleteCalls) != 1 {
		t.Fatalf("persistence delete calls after absent replay = %#v, want no duplicate delete", store.deleteCalls)
	}
}

func (s *routePersistenceTestStore) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error) {
	out := make([]runtimeflowidentity.Route, 0, len(s.routes))
	for _, route := range s.routes {
		out = append(out, route.Identity)
	}
	return out, nil
}

func TestEventBusFlowInstanceRoutesPersistAcrossAddAndRemove(t *testing.T) {
	store := &routePersistenceTestStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Template: runtimecontracts.SystemNodeContract{
			ID:           "reviewer-{instance_id}",
			Produces:     []string{"task.started"},
			SubscribesTo: []string{"task.started"},
		},
		Identity: runtimeflowidentity.DeriveRoute("review", "inst-1"),
	}); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	if _, ok := store.routes["review/inst-1"]; !ok {
		t.Fatalf("persisted routes = %#v, want review/inst-1", store.routes)
	}
	if err := eb.RemoveFlowInstanceRoute(runtimeflowidentity.DeriveRoute("review", "inst-1")); err != nil {
		t.Fatalf("RemoveFlowInstance: %v", err)
	}
	if len(store.routes) != 0 {
		t.Fatalf("persisted routes after remove = %#v, want none", store.routes)
	}
}

func TestEventBusAddFlowInstanceRouteRollsBackPersistedRouteOnActiveInstallFailure(t *testing.T) {
	store := &routePersistenceTestStore{
		upsertErr:        context.DeadlineExceeded,
		upsertAfterWrite: true,
		deleteErr:        context.Canceled,
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	err = eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Template: runtimecontracts.SystemNodeContract{
			ID:           "reviewer-{instance_id}",
			Produces:     []string{"task.started"},
			SubscribesTo: []string{"task.started"},
		},
		Identity: runtimeflowidentity.DeriveRoute("review", "inst-1"),
	})
	if err == nil {
		t.Fatal("expected AddFlowInstanceRoute to fail")
	}
	if len(store.routes) != 0 {
		t.Fatalf("persisted routes after rollback = %#v, want none", store.routes)
	}
	if len(store.rollbackCalls) != 1 || store.rollbackCalls[0] != "review/inst-1" {
		t.Fatalf("rollback calls = %#v, want [review/inst-1]", store.rollbackCalls)
	}
	if got := eb.RouteTable().Resolve("review/inst-1/task.started"); len(got) != 0 {
		t.Fatalf("resolved subscribers after failed add = %#v, want none", got)
	}
}

func TestEventBusFlowInstanceRoutePersistsAndDeliversRenderedActivationConfigSubscriber(t *testing.T) {
	store := &routePersistenceTestStore{}
	bundle := routeMaterializationConfigVarBundle()
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	identity := runtimeflowidentity.DeriveRoute("operating", "11111111-1111-4111-8111-111111111111")
	if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: identity,
		ActivationVariables: map[string]string{
			"vertical_id": "11111111-1111-4111-8111-111111111111",
		},
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	route, ok := store.routes["operating/11111111-1111-4111-8111-111111111111"]
	if !ok {
		t.Fatalf("persisted routes = %#v, want operating instance route", store.routes)
	}
	if route.SubscriberID != "ceo-11111111-1111-4111-8111-111111111111" {
		t.Fatalf("persisted subscriber_id = %q, want rendered ceo id", route.SubscriberID)
	}

	eb.Subscribe("ceo-11111111-1111-4111-8111-111111111111")
	defer eb.Unsubscribe("ceo-11111111-1111-4111-8111-111111111111")
	evt := eventtest.RunCreatingRootIngress(eventtest.UUID("event-rendered-route-delivery"),
		events.EventType("operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := store.deliveries[evt.ID()]
	if len(got) != 1 || got[0] != "ceo-11111111-1111-4111-8111-111111111111" {
		t.Fatalf("delivery recipients = %#v, want rendered ceo id", got)
	}
}

func TestEventBusRemoveNestedFlowInstanceDropsDerivedRoutes(t *testing.T) {
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Template: runtimecontracts.SystemNodeContract{
			ID:           "worker-{instance_id}",
			Produces:     []string{"micro.started"},
			SubscribesTo: []string{"micro.started"},
		},
		Identity: runtimeflowidentity.DeriveRoute("child/grandchild", "inst-1"),
	}); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	if got := eb.RouteTable().Resolve("child/grandchild/inst-1/micro.started"); len(got) != 1 || got[0].ID != "worker-inst-1" {
		t.Fatalf("resolved subscribers after add = %#v", got)
	}
	if err := eb.RemoveFlowInstanceRoute(runtimeflowidentity.DeriveRoute("child/grandchild", "inst-1")); err != nil {
		t.Fatalf("RemoveFlowInstance: %v", err)
	}
	if got := eb.RouteTable().Resolve("child/grandchild/inst-1/micro.started"); len(got) != 0 {
		t.Fatalf("resolved subscribers after remove = %#v, want none", got)
	}
}

func TestRouteTableConcreteTemplateInstanceNodeSubscriberResolvesBeforeDeliveryPlanning(t *testing.T) {
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
	bundle := &runtimecontracts.WorkflowContractBundle{
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
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	got := rt.Resolve("operating/inst-1/opco.product_initialization_requested")
	if len(got) != 1 {
		t.Fatalf("resolved subscribers = %#v, want one lifecycle-orchestrator route", got)
	}
	if got[0].ID != "lifecycle-orchestrator" || got[0].Type != "node" || got[0].Path != "operating/inst-1" {
		t.Fatalf("resolved subscriber = %#v, want node lifecycle-orchestrator at operating/inst-1", got[0])
	}
}

func TestRouteTableFlowInstanceRouteRendersSubscriberWithActivationConfigVars(t *testing.T) {
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(routeMaterializationConfigVarBundle()))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	identity := runtimeflowidentity.DeriveRoute("operating", "11111111-1111-4111-8111-111111111111")
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: identity,
		ActivationVariables: map[string]string{
			"vertical_id": "11111111-1111-4111-8111-111111111111",
		},
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}

	got := rt.Resolve("operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested")
	if len(got) != 1 {
		t.Fatalf("resolved subscribers = %#v, want one ceo route", got)
	}
	if got[0].ID != "ceo-11111111-1111-4111-8111-111111111111" {
		t.Fatalf("resolved subscriber id = %q, want rendered ceo id", got[0].ID)
	}
	routes := rt.MaterializedRoutes(identity)
	if len(routes) != 1 || routes[0].SubscriberID != "ceo-11111111-1111-4111-8111-111111111111" {
		t.Fatalf("materialized routes = %#v, want rendered ceo subscriber", routes)
	}
}

func routeMaterializationConfigVarBundle() *runtimecontracts.WorkflowContractBundle {
	operating := runtimecontracts.FlowContractView{
		Path:  "operating",
		Paths: runtimecontracts.FlowContractPaths{ID: "operating", Flow: "operating"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"opco.product_initialization_requested"}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"opco.product_initialization_requested": {},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"ceo": {
				ID:            "ceo-{vertical_id}",
				Type:          "generic",
				Role:          "ceo",
				Subscriptions: []string{"opco.product_initialization_requested"},
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
				Pins: runtimecontracts.FlowPins{
					Inputs: runtimecontracts.FlowInputPins{Events: []string{"opco.product_initialization_requested"}},
				},
			},
		},
	}
}

func TestRouteTableTemplateOutputPinWildcardSubscriberResolvesThroughDerivedInstance(t *testing.T) {
	root := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "root", Flow: "root"},
		Path:  "",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"operating-accumulator": {
				ID:           "operating-accumulator",
				SubscribesTo: []string{"component-scaffold/*/component.scaffolded"},
			},
		},
		Children: []runtimecontracts.FlowContractView{
			{
				Paths: runtimecontracts.FlowContractPaths{ID: "component-scaffold", Flow: "component-scaffold"},
				Path:  "component-scaffold",
				Schema: runtimecontracts.FlowSchemaDocument{
					Mode: "template",
					Pins: runtimecontracts.FlowPins{
						Outputs: runtimecontracts.FlowOutputPins{Events: []string{"component.scaffolded"}},
					},
				},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"root":               &root,
				"component-scaffold": &root.Children[0],
			},
		},
	}
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	identity := runtimeflowidentity.DeriveRoute("component-scaffold", "component-a")
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}

	got := rt.Resolve("component-scaffold/component-a/component.scaffolded")
	if len(got) != 1 {
		t.Fatalf("resolved subscribers = %#v, want one operating-accumulator route", got)
	}
	if got[0].ID != "operating-accumulator" || got[0].Type != "node" || got[0].MatchPattern != "component-scaffold/*/component.scaffolded" {
		t.Fatalf("resolved subscriber = %#v, want operating-accumulator wildcard route", got[0])
	}

	if err := rt.RemoveFlowInstanceRoute(identity); err != nil {
		t.Fatalf("RemoveFlowInstanceRoute: %v", err)
	}
	if got := rt.Resolve("component-scaffold/component-a/component.scaffolded"); len(got) != 0 {
		t.Fatalf("resolved subscribers after remove = %#v, want none", got)
	}
	if got := rt.Resolve("component-scaffold/component-b/component.scaffolded"); len(got) != 0 {
		t.Fatalf("resolved subscribers for never-added instance = %#v, want none", got)
	}
}

func TestDeriveRouteTable_InputPinsDoNotAutoWireFromProducerOutput(t *testing.T) {
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
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := rt.Resolve("producer/scan.requested"); len(got) != 0 {
		t.Fatalf("Resolve(producer/scan.requested) = %#v, want none for retired sibling auto-wire", got)
	}
	if got := rt.Resolve("scan.requested"); len(got) != 0 {
		t.Fatalf("Resolve(scan.requested) = %#v, want none", got)
	}
	if got := rt.Resolve("discovery/scan.requested"); len(got) != 1 || got[0].ID != "scan-orchestrator" {
		t.Fatalf("Resolve(discovery/scan.requested) = %#v, want scan-orchestrator local input route", got)
	}
}

func TestDeriveRouteTable_HandlerOnlyInputPinsDoNotAutoWireFromProducerOutput(t *testing.T) {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "producer",
	}
	consumer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "consumer", Flow: "consumer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "consumer",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"consumer-node": {
				ID: "consumer-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"scan.requested": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, consumer}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer": &root.Children[0],
				"consumer": &root.Children[1],
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"consumer-node": {
					"scan.requested": {},
				},
			},
		},
	}
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := rt.Resolve("producer/scan.requested"); len(got) != 0 {
		t.Fatalf("Resolve(producer/scan.requested) = %#v, want none for retired sibling auto-wire", got)
	}
	got := rt.Resolve("consumer/scan.requested")
	if len(got) != 1 || got[0].ID != "consumer-node" {
		t.Fatalf("Resolve(consumer/scan.requested) = %#v, want consumer-node local input route", got)
	}
}

func TestDeriveRouteTable_StaticChildFlowInputSubscriptionsResolveCanonicalNodeOwners(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	for _, tc := range []struct {
		fixture     string
		eventType   string
		nodeID      string
		flowPath    string
		routeMatch  string
		routeSource string
	}{
		{
			fixture:     "test-child-flow-local-events",
			eventType:   "child/child.start",
			nodeID:      "child-intake",
			flowPath:    "child",
			routeMatch:  "child/child.start",
			routeSource: "subscription",
		},
		{
			fixture:     "test-nested-three-levels",
			eventType:   "child/step.begin",
			nodeID:      "child-relay",
			flowPath:    "child",
			routeMatch:  "step.begin",
			routeSource: "receiver_carrier",
		},
		{
			fixture:     "test-nested-three-levels",
			eventType:   "child/grandchild/micro.start",
			nodeID:      "grandchild-worker",
			flowPath:    "child/grandchild",
			routeMatch:  "micro.start",
			routeSource: "receiver_carrier",
		},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", tc.fixture)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
			if err != nil {
				t.Fatalf("DeriveRouteTable: %v", err)
			}
			got := rt.Resolve(tc.eventType)
			if len(got) != 1 ||
				got[0].ID != tc.nodeID ||
				got[0].Type != "node" ||
				got[0].Path != tc.flowPath ||
				got[0].MatchPattern != tc.routeMatch ||
				got[0].RouteSource != tc.routeSource {
				t.Fatalf("Resolve(%s) = %#v, want %s %s %s from %s", tc.eventType, got, tc.nodeID, tc.flowPath, tc.routeMatch, tc.routeSource)
			}
		})
	}
}

func TestDeriveRouteTable_RuntimeProducedFollowUpSubscriptionsResolveCanonicalNodeOwners(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	for _, tc := range []struct {
		name       string
		fixture    string
		eventType  string
		nodeID     string
		flowPath   string
		routeMatch string
	}{
		{
			name:       "root-local timer follow-up",
			fixture:    filepath.Join("tests", "tier5-flow-lifecycle", "test-timer-fire"),
			eventType:  "timer.check",
			nodeID:     "test-node",
			flowPath:   "",
			routeMatch: "timer.check",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixtureRoot := filepath.Join(repoRoot, tc.fixture)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
			if err != nil {
				t.Fatalf("DeriveRouteTable: %v", err)
			}
			got := rt.Resolve(tc.eventType)
			found := false
			for _, subscriber := range got {
				if subscriber.ID == tc.nodeID &&
					subscriber.Type == "node" &&
					subscriber.Path == tc.flowPath &&
					subscriber.MatchPattern == tc.routeMatch {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("Resolve(%s) = %#v, want %s %s %s", tc.eventType, got, tc.nodeID, tc.flowPath, tc.routeMatch)
			}
		})
	}
}

func TestDeriveRouteTable_AmbiguousInputPinsFailClosedWithoutEscapeHatch(t *testing.T) {
	producerA := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer_a", Flow: "producer_a"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"ticket.ready"}},
			},
		},
		Path: "producer_a",
	}
	producerB := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer_b", Flow: "producer_b"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"ticket.ready"}},
			},
		},
		Path: "producer_b",
	}
	consumer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "consumer", Flow: "consumer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"ticket.ready"}},
			},
		},
		Path: "consumer",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"consumer-node": {
				ID: "consumer-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"ticket.ready": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producerA, producerB, consumer}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer_a": &root.Children[0],
				"producer_b": &root.Children[1],
				"consumer":   &root.Children[2],
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"consumer-node": {
					"ticket.ready": {},
				},
			},
		},
	}
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := rt.Resolve("producer_a/ticket.ready"); len(got) != 0 {
		t.Fatalf("Resolve(producer_a/ticket.ready) = %#v, want none", got)
	}
	if got := rt.Resolve("producer_b/ticket.ready"); len(got) != 0 {
		t.Fatalf("Resolve(producer_b/ticket.ready) = %#v, want none", got)
	}
}

func TestDeriveRouteTable_InputPinsStayLocalWithoutExternalProducer(t *testing.T) {
	scoring := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "scoring", Flow: "scoring"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"score.dimension_complete"}},
			},
		},
		Path: "scoring",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"score.dimension_complete": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scoring-node": {
				ID:           "scoring-node",
				SubscribesTo: []string{"score.dimension_complete"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{scoring}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"scoring": &root.Children[0],
			},
		},
	}
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	got := rt.Resolve("scoring/score.dimension_complete")
	if len(got) != 1 || got[0].ID != "scoring-node" {
		t.Fatalf("Resolve(scoring/score.dimension_complete) = %#v, want scoring-node", got)
	}
	if got := rt.Resolve("score.dimension_complete"); len(got) != 0 {
		t.Fatalf("Resolve(score.dimension_complete) = %#v, want none", got)
	}
}

func TestDeriveRouteTable_NestedPackageConnectLocalizesWithinParentFlow(t *testing.T) {
	grandchild := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild", PackageKey: "flows/child/flows/grandchild"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Name: "micro_done", Event: "micro.done"}}},
			},
		},
		Path: "child/grandchild",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.done": {},
		},
	}
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child", PackageKey: "flows/child"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs:  runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Name: "micro_done", Event: "micro.done"}}},
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"step.result"}},
			},
		},
		Path: "child",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"child-aggregator": {
				ID:           "child-aggregator",
				SubscribesTo: []string{"micro.done"},
			},
		},
		Events:   map[string]runtimecontracts.EventCatalogEntry{"micro.done": {}},
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
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"child":      child.Schema,
			"grandchild": grandchild.Schema,
		},
		Semantics: runtimecontracts.WorkflowSemanticView{CompositionConnects: []runtimecontracts.FlowPackageConnect{{
			PackageKey: "flows/child",
			SourceFile: "flows/child/package.yaml",
			SourceLine: 10,
			From:       "grandchild.micro_done",
			To:         ".micro_done",
		}}},
	}
	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(issues) != 0 || len(plans) != 1 {
		t.Fatalf("nested connect plans = %#v issues = %#v, want one valid plan", plans, issues)
	}
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := rt.Resolve("child/grandchild/micro.done"); len(got) != 0 {
		t.Fatalf("direct descendant route = %#v, connect dispatch must own the boundary edge", got)
	}
	got := rt.Resolve("child/micro.done")
	if len(got) != 1 || got[0].ID != "child-aggregator" {
		t.Fatalf("Resolve(child/micro.done) = %#v, want receiver-local child-aggregator carrier", got)
	}
	if got[0].Path != "child" {
		t.Fatalf("receiver carrier path = %q, want child", got[0].Path)
	}
}

func TestDeriveRouteTable_NestedTemplateInstancesPersistSemanticScopeKey(t *testing.T) {
	grandchild := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
		},
		Path: "child/grandchild",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {
				ID:           "worker-{instance_id}",
				SubscribesTo: []string{"micro.started"},
				Produces:     []string{"micro.started"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.started": {},
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
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	identity := runtimeflowidentity.DeriveRoute("child/grandchild", "inst-1")
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	routes := rt.MaterializedRoutes(identity)
	if len(routes) != 1 {
		t.Fatalf("MaterializedRoutes = %#v, want 1 route", routes)
	}
	if routes[0].Identity.ScopeKey != "child/grandchild" {
		t.Fatalf("ScopeKey = %q, want child/grandchild", routes[0].Identity.ScopeKey)
	}
	if routes[0].Identity.InstanceID != "inst-1" {
		t.Fatalf("InstanceID = %q, want inst-1", routes[0].Identity.InstanceID)
	}
	if routes[0].SourceFlow != "child/grandchild" {
		t.Fatalf("SourceFlow = %q, want child/grandchild", routes[0].SourceFlow)
	}
}
