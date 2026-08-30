package pipeline

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestClassifyDeliveryTargetOwnershipClosedUnion(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("delivery-target-classification"), events.EventType("review/one/work.ready"),
		"", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{},
	)
	entityID := eventtest.UUID("existing-delivery-target")
	tests := []struct {
		name       string
		nodeID     string
		candidates []DeliveryTargetOwnerCandidate
		assert     func(*testing.T, events.DeliveryTargetOwnership)
	}{
		{
			name: "existing entity", nodeID: "existing",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: entityID}}},
			assert: func(t *testing.T, owner events.DeliveryTargetOwnership) {
				if !owner.ExistingEntity() || owner.Route().EntityID != entityID {
					t.Fatalf("owner = %#v, want existing entity %s", owner, entityID)
				}
			},
		},
		{
			name: "first delivery materializes exact future entity", nodeID: "materializer",
			assert: func(t *testing.T, owner events.DeliveryTargetOwnership) {
				if !owner.MaterializingEntity() || owner.Route().EntityID != FlowInstanceEntityID("review/one") {
					t.Fatalf("owner = %#v, want canonical materializing entity", owner)
				}
			},
		},
		{
			name: "stateful first delivery materializes exact future entity", nodeID: "transitioner",
			assert: func(t *testing.T, owner events.DeliveryTargetOwnership) {
				if !owner.MaterializingEntity() || owner.Route().EntityID != FlowInstanceEntityID("review/one") {
					t.Fatalf("owner = %#v, want canonical stateful materializing entity", owner)
				}
			},
		},
		{
			name: "entityless receiver", nodeID: "entityless",
			assert: func(t *testing.T, owner events.DeliveryTargetOwnership) {
				if !owner.EntitylessReceiver() || owner.Route().EntityID != "" {
					t.Fatalf("owner = %#v, want explicit entityless receiver", owner)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := pipelineNode(t, "review", test.nodeID)
			handler, err := AdmitDeliveryTargetHandler(source, node)
			if err != nil {
				t.Fatalf("admit handler: %v", err)
			}
			owner, err := ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
				Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
				Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"},
				Handler:   handler.ForEvent("work.ready"), Candidates: test.candidates,
			})
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			test.assert(t, owner)
		})
	}
}

func TestClassifyDeliveryTargetOwnershipFailsClosedOnMissingOrContradictoryEvidence(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("delivery-target-hostile"), events.EventType("review/one/work.ready"),
		"", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{},
	)
	canonicalID := FlowInstanceEntityID("review/one")
	tests := []struct {
		name       string
		nodeID     string
		candidates []DeliveryTargetOwnerCandidate
		want       string
	}{
		{name: "entity scoped handler without owner", nodeID: "entity-reader", want: "owner is missing"},
		{
			name: "existing and materializing contradiction", nodeID: "materializer",
			candidates: []DeliveryTargetOwnerCandidate{
				{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: canonicalID}},
				{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: canonicalID}, Materializing: true},
			},
			want: "contradictory existing and materializing",
		},
		{
			name: "wrong future identity", nodeID: "materializer",
			candidates: []DeliveryTargetOwnerCandidate{{
				Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: eventtest.UUID("wrong-future-target")}, Materializing: true,
			}},
			want: "disagrees with canonical handler identity",
		},
		{
			name: "malformed candidate missing instance", nodeID: "entity-reader",
			candidates: []DeliveryTargetOwnerCandidate{{
				Route: events.RouteIdentity{FlowID: "review", EntityID: canonicalID},
			}},
			want: "requires exact flow instance and entity identity",
		},
		{
			name: "malformed candidate missing entity", nodeID: "entity-reader",
			candidates: []DeliveryTargetOwnerCandidate{{
				Route: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"},
			}},
			want: "requires exact flow instance and entity identity",
		},
		{
			name: "exact candidate disagrees with blueprint entity", nodeID: "entity-reader",
			candidates: []DeliveryTargetOwnerCandidate{{
				Route: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: eventtest.UUID("contradictory-owner")},
			}},
			want: "disagrees with receiver entity",
		},
		{
			name: "raw blueprint entity is not ownership evidence", nodeID: "entity-reader",
			want: "owner is missing",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := pipelineNode(t, "review", test.nodeID)
			handler, err := AdmitDeliveryTargetHandler(source, node)
			if err != nil {
				t.Fatalf("admit handler: %v", err)
			}
			blueprint := events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"}
			if test.name == "raw blueprint entity is not ownership evidence" || test.name == "exact candidate disagrees with blueprint entity" {
				blueprint.EntityID = canonicalID
			}
			_, err = ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
				Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
				Blueprint: blueprint,
				Handler:   handler.ForEvent("work.ready"), Candidates: test.candidates,
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("classification error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestClassifyDeliveryTargetOwnershipNeverPromotesSameFlowSibling(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("delivery-target-handler-flow"), events.EventType("review/one/work.ready"),
		"", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{},
	)
	node := pipelineNode(t, "review", "entity-reader")
	handler, err := AdmitDeliveryTargetHandler(source, node)
	if err != nil {
		t.Fatalf("admit handler: %v", err)
	}
	entityID := eventtest.UUID("handler-flow-owner")
	request := DeliveryTargetOwnershipRequest{
		Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
		Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/producer-child"},
		Handler:   handler.ForEvent("work.ready"),
		Candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{
			FlowID: "review", FlowInstance: "review/one", EntityID: entityID,
		}}},
	}
	if _, err := ClassifyDeliveryTargetOwnership(request); err == nil || !strings.Contains(err.Error(), "owner is missing") {
		t.Fatalf("single sibling classification error = %v, want exact-owner rejection", err)
	}

	request.Candidates = append(request.Candidates, DeliveryTargetOwnerCandidate{Route: events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/two", EntityID: eventtest.UUID("second-handler-flow-owner"),
	}})
	if _, err := ClassifyDeliveryTargetOwnership(request); err == nil || !strings.Contains(err.Error(), "owner is missing") {
		t.Fatalf("multiple sibling classification error = %v, want same exact-owner rejection", err)
	}

	exactEntityID := eventtest.UUID("exact-handler-flow-owner")
	request.Candidates = append(request.Candidates, DeliveryTargetOwnerCandidate{Route: events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/producer-child", EntityID: exactEntityID,
	}})
	owner, err := ClassifyDeliveryTargetOwnership(request)
	if err != nil {
		t.Fatalf("classify exact owner with hostile siblings: %v", err)
	}
	if !owner.ExistingEntity() || owner.Route().FlowInstance != "review/producer-child" || owner.Route().EntityID != exactEntityID {
		t.Fatalf("owner = %#v, want exact owner with siblings untouched", owner)
	}
}

func TestClassifyDeliveryTargetOwnershipPreservesExactOwnerForEntityOptionalHandler(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("delivery-target-optional-owner"), events.EventType("review/one/work.ready"),
		"", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{},
	)
	node := pipelineNode(t, "review", "entityless")
	handler, err := AdmitDeliveryTargetHandler(source, node)
	if err != nil {
		t.Fatal(err)
	}
	entityID := eventtest.UUID("optional-existing-owner")
	owner, err := ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
		Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
		Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"},
		Handler:   handler.ForEvent("work.ready"), Candidates: []DeliveryTargetOwnerCandidate{{
			Route: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: entityID},
		}},
	})
	if err != nil {
		t.Fatalf("classify optional exact owner: %v", err)
	}
	if !owner.ExistingEntity() || owner.Route().EntityID != entityID {
		t.Fatalf("owner = %#v, want exact existing owner", owner)
	}
}

func TestClassifyDeliveryTargetOwnershipProjectsRootHandlerOntoSelectedRun(t *testing.T) {
	runID := eventtest.UUID("selected-root-run")
	existingEntityID := eventtest.UUID("selected-root-owner")
	flow := runtimecontracts.FlowContractView{
		Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: "."},
		Schema: runtimecontracts.FlowSchemaDocument{InitialState: "waiting", States: []string{"waiting", "done"}},
		Events: map[string]runtimecontracts.EventCatalogEntry{"timer.cancel": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"controller": {
				ID: "controller", SubscribesTo: []string{"timer.cancel"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"timer.cancel": {AdvancesTo: "done"},
				},
			},
		},
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "timer-proof"},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &flow, ByID: map[string]*runtimecontracts.FlowContractView{".": &flow},
		},
	})
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("selected-root-event"), "timer.cancel", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{},
	)
	node := pipelineNode(t, ".", "controller")
	handler, err := AdmitDeliveryTargetHandler(source, node)
	if err != nil {
		t.Fatalf("admit handler: %v", err)
	}
	request := DeliveryTargetOwnershipRequest{
		Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
		Blueprint: events.RouteIdentity{FlowID: ".", FlowInstance: runID},
		Handler:   handler.ForEvent("timer.cancel"),
	}

	request.Candidates = []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: runID, EntityID: existingEntityID}}}
	owner, err := ClassifyDeliveryTargetOwnership(request)
	if err != nil {
		t.Fatalf("classify existing selected-run owner: %v", err)
	}
	wantExisting := events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: existingEntityID}
	if !owner.ExistingEntity() || owner.Route() != wantExisting {
		t.Fatalf("existing owner = %#v, want %#v", owner, wantExisting)
	}

	request.Candidates = nil
	owner, err = ClassifyDeliveryTargetOwnership(request)
	if err != nil {
		t.Fatalf("classify first-delivery selected-run owner: %v", err)
	}
	wantMaterializing := events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: FlowInstanceEntityID(runID)}
	if !owner.MaterializingEntity() || owner.Route() != wantMaterializing {
		t.Fatalf("materializing owner = %#v, want %#v", owner, wantMaterializing)
	}
}

func TestClassifyDeliveryTargetOwnershipConsumesExactInputAcquisitionMode(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	entityID := eventtest.UUID("selected-input-owner")
	tests := []struct {
		name       string
		nodeID     string
		eventType  events.EventType
		candidates []DeliveryTargetOwnerCandidate
		wantKind   string
		wantEntity string
		wantError  string
	}{
		{
			name: "create pin consumes canonical materialization evidence", nodeID: "pin-creator", eventType: "work.created",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: FlowInstanceEntityID("review/one")}, Materializing: true}},
			wantKind:   "materializing_entity", wantEntity: FlowInstanceEntityID("review/one"),
		},
		{
			name: "create pin owns exact first delivery materialization", nodeID: "pin-creator", eventType: "work.created",
			wantKind: "materializing_entity", wantEntity: FlowInstanceEntityID("review/one"),
		},
		{
			name: "select or create pin owns exact zero match materialization", nodeID: "pin-upserter", eventType: "work.upserted",
			wantKind: "materializing_entity", wantEntity: FlowInstanceEntityID("review/one"),
		},
		{
			name: "select pin consumes existing", nodeID: "pin-selector", eventType: "work.selected",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: entityID}}},
			wantKind:   "existing_entity",
		},
		{name: "select pin rejects missing owner", nodeID: "pin-selector", eventType: "work.selected", wantError: "owner is missing"},
		{
			name: "select pin consumes exact ordered materialization", nodeID: "pin-selector", eventType: "work.selected",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: FlowInstanceEntityID("review/one")}, Materializing: true}},
			wantKind:   "materializing_entity", wantEntity: FlowInstanceEntityID("review/one"),
		},
		{
			name: "create pin rejects wrong future owner", nodeID: "pin-creator", eventType: "work.created",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: eventtest.UUID("wrong-acquisition-owner")}, Materializing: true}},
			wantError:  "disagrees with canonical handler identity",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evt := eventtest.RunCreatingRootIngress(
				eventtest.UUID("input-acquisition-"+test.name), events.EventType("review/one/"+string(test.eventType)),
				"", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{},
			)
			node := pipelineNode(t, "review", test.nodeID)
			handler, err := AdmitDeliveryTargetHandler(source, node)
			if err != nil {
				t.Fatalf("admit handler: %v", err)
			}
			owner, err := ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
				Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
				Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"},
				Handler:   handler.ForEvent(test.eventType), Candidates: test.candidates,
			})
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("classification error = %v, want %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			if owner.Code() != test.wantKind {
				t.Fatalf("owner = %s %#v, want %s", owner.Code(), owner.Route(), test.wantKind)
			}
			if test.wantEntity != "" && owner.Route().EntityID != test.wantEntity {
				t.Fatalf("owner entity = %q, want exact future entity %q", owner.Route().EntityID, test.wantEntity)
			}
		})
	}
}

func TestClassifyDeliveryTargetOwnershipDeclaredKeyAcquisitionMatrix(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("declared-key-target"), events.EventType("review/work.keyed"),
		"", "", mustJSON(map[string]any{"account_id": "account-1"}), 0, "", "", events.EventEnvelope{}, time.Time{},
	)
	instance := WorkflowInstance{
		EntityID: eventtest.UUID("declared-key-existing"), WorkflowName: "review", InstanceID: "one",
		StorageRef: "review/one", Status: "active", CurrentState: "active", Fields: map[string]any{"account_id": "account-1"},
		EntityType: "review_entity",
	}
	tests := []struct {
		name      string
		nodeID    string
		reader    deliveryTargetWorkflowReader
		wantKind  string
		wantRoute string
		wantError string
	}{
		{name: "select zero", nodeID: "key-selector", reader: deliveryTargetWorkflowReader{}, wantError: "select_entity_no_match"},
		{name: "select one ignores supplied path", nodeID: "key-selector", reader: deliveryTargetWorkflowReader{selected: []WorkflowInstance{instance}}, wantKind: "existing_entity", wantRoute: "review/one"},
		{name: "select many", nodeID: "key-selector", reader: deliveryTargetWorkflowReader{selected: []WorkflowInstance{instance, withDeliveryTargetInstanceIdentity(instance, "two", eventtest.UUID("declared-key-second"))}}, wantError: "select_entity_ambiguous"},
		{name: "select or create zero", nodeID: "key-upserter", reader: deliveryTargetWorkflowReader{}, wantKind: "materializing_entity"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := pipelineNode(t, "review", test.nodeID)
			handler, err := AdmitDeliveryTargetHandler(source, node)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
				Context: context.Background(), Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
				Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/hostile-preselection"},
				Handler:   handler.ForEvent("work.keyed"), WorkflowInstances: test.reader,
			})
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("classification error = %v, want %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if owner.Code() != test.wantKind {
				t.Fatalf("owner kind = %s, want %s", owner.Code(), test.wantKind)
			}
			if test.wantRoute != "" && owner.Route().FlowInstance != test.wantRoute {
				t.Fatalf("owner route = %#v, want %s", owner.Route(), test.wantRoute)
			}
			if owner.Route().FlowInstance == "review/hostile-preselection" {
				t.Fatalf("declared-key acquisition retained supplied preselection: %#v", owner.Route())
			}
		})
	}
}

func TestClassifyDeliveryTargetOwnershipTargetedEventPreservesExactOwnerBeforeDeclaredKeyAcquisition(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	exact := events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/exact", EntityID: eventtest.UUID("declared-key-explicit-target"),
	}.Normalized()
	competing := WorkflowInstance{
		EntityID: eventtest.UUID("declared-key-competing-owner"), WorkflowName: "review", InstanceID: "competing",
		StorageRef: "review/competing", Status: "active", CurrentState: "active", Fields: map[string]any{"account_id": "account-1"},
		EntityType: "review_entity",
	}
	for _, nodeID := range []string{"key-selector", "key-upserter"} {
		t.Run(nodeID, func(t *testing.T) {
			node := pipelineNode(t, "review", nodeID)
			handler, err := AdmitDeliveryTargetHandler(source, node)
			if err != nil {
				t.Fatal(err)
			}
			evt := eventtest.RunCreatingRootIngress(
				eventtest.UUID("declared-key-explicit-target-"+nodeID), events.EventType("review/work.keyed"),
				"", "", mustJSON(map[string]any{"account_id": "account-1"}), 0, "", "",
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, exact), time.Time{},
			)
			owner, err := ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
				Context: context.Background(), Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
				Blueprint: exact, Handler: handler.ForEvent("work.keyed"),
				Candidates:        []DeliveryTargetOwnerCandidate{{Route: exact}},
				WorkflowInstances: deliveryTargetWorkflowReader{selected: []WorkflowInstance{competing}},
			})
			if err != nil {
				t.Fatalf("classify targeted %s: %v", nodeID, err)
			}
			if !owner.ExistingEntity() || owner.Route() != exact {
				t.Fatalf("targeted owner = %s %#v, want exact existing owner %#v", owner.Code(), owner.Route(), exact)
			}
		})
	}
}

func TestClassifyDeliveryTargetOwnershipJoinOccurrencePreservesDeclarationOwnerBeforeDeclaredKeyAcquisition(t *testing.T) {
	bundle := workflowJoinLifecycleBundle(t)
	node := bundle.Nodes["join-node"]
	handler := node.EventHandlers["item.completed"]
	handler.SelectEntity = &runtimecontracts.SelectEntitySpec{Bindings: []runtimecontracts.SelectEntityKeyBinding{{
		Field: "portfolio_id", Ref: "payload.portfolio_id", RefPath: paths.Parse("payload.portfolio_id"),
	}}}
	node.EventHandlers["item.completed"] = handler
	bundle.Nodes["join-node"] = node

	plan := bundle.Semantics.Joins[0]
	plan.Node = mustPipelineNode("", "join-node")
	source := exactWorkflowJoinSource{
		Source: workflowJoinLifecycleRootAndFlowSource(bundle), plans: []runtimecontracts.WorkflowJoinPlan{plan},
		nodeFlowID: "", overrideNodeOwner: true,
	}
	entityID := eventtest.UUID("join-declaration-owner")
	routingSource, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	handle := pipelineJoinHandle(t, "", timeridentity.TimerHandleJoinTimeout)
	evt := exactJoinOccurrenceEvent(t, "join-declaration-owner", handle, routingSource, events.EventEnvelope{EntityID: entityID})
	target := events.RouteIdentity{
		FlowID: ".", FlowInstance: evt.RunID(), EntityID: entityID,
	}.Normalized()
	targetHandler, err := NewDeliveryTargetHandler(plan.Node)
	if err != nil {
		t.Fatal(err)
	}
	_, resolvedTarget, resolvedHandler, resolved, resolveErr := ResolveWorkflowJoinOccurrenceDeliveryTarget(source, evt)
	if resolveErr != nil || !resolved {
		t.Fatalf("resolve declaration-bound join occurrence: target=%#v handler=%#v resolved=%t err=%v", resolvedTarget, resolvedHandler, resolved, resolveErr)
	}
	if _, admitted := resolvedHandler.resolve(source, evt.Type()); !admitted {
		t.Fatalf("resolved declaration-bound join handler %s/%s is not executable for %s: admission=%#v", resolvedHandler.FlowID(), resolvedHandler.NodeID(), evt.Type(), semanticview.ClassifyExecutableNodeSubscription(source, resolvedHandler.Node(), "item.completed"))
	}
	owner, err := ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
		Context: context.Background(), Source: source, Event: evt,
		Recipient: events.MustNodeDeliveryRecipient(plan.Node), Blueprint: target,
		Handler:    targetHandler.ForEvent(events.EventType(handle.EventType())),
		Candidates: []DeliveryTargetOwnerCandidate{{Route: target}},
	})
	if err != nil {
		t.Fatalf("classify declaration-bound join occurrence without selector payload: %v", err)
	}
	if !owner.ExistingEntity() || owner.Route() != target {
		t.Fatalf("join occurrence owner = %s %#v, want exact declaration owner %#v", owner.Code(), owner.Route(), target)
	}
}

func TestClassifyDeliveryTargetOwnershipSelectOrCreateAcceptsExactSameKeyAppearanceAndRejectsConflict(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("declared-key-race"), events.EventType("review/work.keyed"),
		"", "", mustJSON(map[string]any{"account_id": "account-1"}), 0, "", "", events.EventEnvelope{}, time.Time{},
	)
	instanceID, err := selectOrCreateEntityInstanceID(source, "review", map[string]any{"account_id": "account-1"})
	if err != nil {
		t.Fatal(err)
	}
	identity := deriveFlowInstanceIdentity(source, "review", instanceID)
	matching := WorkflowInstance{
		EntityID: identity.EntityID, WorkflowName: "review", InstanceID: identity.InstanceID, StorageRef: identity.InstancePath,
		Status: "active", CurrentState: "active", Fields: map[string]any{"account_id": "account-1"},
		EntityType: "review_entity",
	}
	node := pipelineNode(t, "review", "key-upserter")
	handler, err := AdmitDeliveryTargetHandler(source, node)
	if err != nil {
		t.Fatal(err)
	}
	classify := func(reader deliveryTargetWorkflowReader) (events.DeliveryTargetOwnership, error) {
		return ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
			Context: context.Background(), Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
			Handler: handler.ForEvent("work.keyed"), WorkflowInstances: reader,
		})
	}
	owner, err := classify(deliveryTargetWorkflowReader{loaded: map[string]WorkflowInstance{identity.InstancePath: matching}})
	if err != nil {
		t.Fatalf("same-key exact appearance: %v", err)
	}
	if !owner.ExistingEntity() || owner.Route().FlowInstance != identity.InstancePath {
		t.Fatalf("owner = %#v, want exact appearing row", owner)
	}
	conflicting := matching
	conflicting.Fields = map[string]any{"account_id": "other"}
	if _, err := classify(deliveryTargetWorkflowReader{loaded: map[string]WorkflowInstance{identity.InstancePath: conflicting}}); err == nil || !strings.Contains(err.Error(), "select_or_create_entity_conflict") {
		t.Fatalf("conflicting exact appearance error = %v", err)
	}
}

type deliveryTargetWorkflowReader struct {
	selected       []WorkflowInstance
	loaded         map[string]WorkflowInstance
	selectedStates []WorkflowEntityStatePersistenceRecord
	loadedStates   map[string]WorkflowEntityStatePersistenceRecord
}

func (r deliveryTargetWorkflowReader) LoadWorkflowInstance(_ context.Context, route runtimeflowidentity.Route) (WorkflowInstance, bool, error) {
	instance, ok := r.loaded[route.InstancePath]
	return instance, ok, nil
}

func (r deliveryTargetWorkflowReader) ListWorkflowInstances(context.Context) ([]WorkflowInstance, error) {
	return append([]WorkflowInstance(nil), r.selected...), nil
}

func (r deliveryTargetWorkflowReader) LoadWorkflowEntityState(_ context.Context, route runtimeflowidentity.Route, _ runtimeidentity.EntityID) (WorkflowEntityStatePersistenceRecord, bool, error) {
	if record, ok := r.loadedStates[route.InstancePath]; ok {
		return record, true, nil
	}
	instance, ok := r.loaded[route.InstancePath]
	if !ok {
		return WorkflowEntityStatePersistenceRecord{}, false, nil
	}
	return deliveryTargetStateRecord(instance), true, nil
}

func (r deliveryTargetWorkflowReader) SelectActiveWorkflowEntityStates(context.Context, WorkflowEntityStateSelectionOwner, []WorkflowInstanceFieldSelector, []string) ([]WorkflowEntityStatePersistenceRecord, error) {
	if r.selectedStates != nil {
		return append([]WorkflowEntityStatePersistenceRecord(nil), r.selectedStates...), nil
	}
	records := make([]WorkflowEntityStatePersistenceRecord, 0, len(r.selected))
	for _, instance := range r.selected {
		records = append(records, deliveryTargetStateRecord(instance))
	}
	return records, nil
}

func (r deliveryTargetWorkflowReader) SelectActiveWorkflowInstances(context.Context, string, []WorkflowInstanceFieldSelector, []string) ([]WorkflowInstance, error) {
	return append([]WorkflowInstance(nil), r.selected...), nil
}

func deliveryTargetStateRecord(instance WorkflowInstance) WorkflowEntityStatePersistenceRecord {
	marshal := func(value any) json.RawMessage {
		raw, _ := json.Marshal(value)
		return raw
	}
	return WorkflowEntityStatePersistenceRecord{
		EntityID: instance.EntityID, FlowInstance: instance.StorageRef,
		EntityType: instance.EntityType,
		Slug:       instance.Slug, Name: instance.Name, CurrentState: instance.CurrentState, Revision: instance.Revision,
		EnteredStageAt: instance.EnteredStageAt, Gates: marshal(instance.Gates), Fields: marshal(instance.Fields),
		Bookkeeping: marshal(instance.Bookkeeping), Accumulator: marshal(instance.StateBuckets),
		CreatedAt: instance.CreatedAt, UpdatedAt: instance.UpdatedAt,
	}
}

func withDeliveryTargetInstanceIdentity(instance WorkflowInstance, instanceID, entityID string) WorkflowInstance {
	instance.InstanceID = instanceID
	instance.StorageRef = "review/" + instanceID
	instance.EntityID = entityID
	return instance
}

func TestValidateStampedDeliveryTargetOwnershipRejectsWrongAcquisitionFutureID(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("stamped-acquisition-owner"), events.EventType("review/one/work.created"),
		"", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{},
	)
	node := pipelineNode(t, "review", "pin-creator")
	handlerFact, err := AdmitDeliveryTargetHandler(source, node)
	if err != nil {
		t.Fatal(err)
	}
	handlerFact = handlerFact.ForEvent("work.created")
	handler, ok := handlerFact.resolve(source, "work.created")
	if !ok {
		t.Fatal("resolve admitted create-pin handler")
	}
	wrong := events.MustMaterializingEntityTarget(events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/one", EntityID: eventtest.UUID("wrong-stamped-acquisition-owner"),
	})
	err = ValidateStampedDeliveryTargetOwnership(source, evt, events.MustNodeDeliveryRecipient(node), handlerFact, handler, wrong)
	if err == nil || !strings.Contains(err.Error(), "canonical future entity") {
		t.Fatalf("ValidateStampedDeliveryTargetOwnership error = %v, want canonical future identity rejection", err)
	}
}

func TestHandlerEntityClassifierCoversCompleteExecutableShape(t *testing.T) {
	assertClassifierFields := func(t *testing.T, typ reflect.Type, classifiers any) {
		t.Helper()
		classifierValue := reflect.ValueOf(classifiers)
		if classifierValue.Len() != typ.NumField() {
			t.Fatalf("%s classifier count = %d, want %d fields", typ.Name(), classifierValue.Len(), typ.NumField())
		}
		for index := 0; index < typ.NumField(); index++ {
			field := typ.Field(index)
			if !classifierValue.MapIndex(reflect.ValueOf(field.Name)).IsValid() {
				t.Errorf("%s.%s has no entity semantic classifier", typ.Name(), field.Name)
			}
		}
	}

	assertClassifierFields(t, reflect.TypeOf(runtimecontracts.SystemNodeEventHandler{}), systemNodeEventHandlerEntityClassifiers)
	assertClassifierFields(t, reflect.TypeOf(runtimecontracts.HandlerRuleEntry{}), handlerRuleEntryEntityClassifiers)
}

func TestHandlerEntityClassifierRejectsEntitylessOwnershipAcrossNestedOperators(t *testing.T) {
	tests := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
	}{
		{name: "group by source", handler: runtimecontracts.SystemNodeEventHandler{GroupBy: &runtimecontracts.GroupBySpec{ItemsFrom: "entity.items"}}},
		{name: "on success emission", handler: runtimecontracts.SystemNodeEventHandler{OnSuccess: runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "work.done", From: "entity"}}}},
		{name: "join membership", handler: runtimecontracts.SystemNodeEventHandler{Join: &runtimecontracts.JoinSpec{Members: runtimecontracts.JoinMembersSpec{From: "entity.expected"}}}},
		{name: "loop lifecycle", handler: runtimecontracts.SystemNodeEventHandler{Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision"}}},
		{name: "activity input", handler: runtimecontracts.SystemNodeEventHandler{Activity: runtimecontracts.ActivitySpec{Input: map[string]runtimecontracts.ExpressionValue{"state": runtimecontracts.RefExpression("entity.status")}}}},
		{name: "guard escalation", handler: runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{OnFailSpec: runtimecontracts.GuardFailureSpec{Action: runtimecontracts.GuardFailureActionEscalate, Escalation: runtimecontracts.EmitSpec{Event: "guard.failed", From: "entity"}}}}},
		{name: "platform entity identity", handler: runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{Check: `_entity.id != ""`}}},
		{name: "platform entity state", handler: runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{Check: `_entity.current_state == "active"`}}},
		{name: "platform entity gate", handler: runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{Check: `_entity.gates.ready`}}},
		{name: "nested rule activity", handler: runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{{Activity: runtimecontracts.ActivitySpec{Input: map[string]runtimecontracts.ExpressionValue{"owner": runtimecontracts.CELExpression("entity.owner")}}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if handlerExecutionEntityRequirement(nil, "review", test.handler) == DeliveryTargetEntityOptional {
				t.Fatalf("handler execution requirement = entityless-safe, want entity ownership for %s", test.name)
			}
		})
	}
	if handlerExecutionEntityRequirement(nil, "review", runtimecontracts.SystemNodeEventHandler{
		FanOut: &runtimecontracts.FanOutSpec{ItemsFrom: "payload.items", Emit: runtimecontracts.EmitSpec{Event: "work.item", From: "payload"}},
	}) != DeliveryTargetEntityOptional {
		t.Fatal("payload-only fan out classified as entity-scoped")
	}
	if handlerExecutionEntityRequirement(nil, "review", runtimecontracts.SystemNodeEventHandler{
		Guard: &runtimecontracts.GuardSpec{Check: `_entity.flow_instance == "review/one"`},
	}) != DeliveryTargetEntityOptional {
		t.Fatal("flow-instance-only platform metadata classified as entity-scoped")
	}
}

func TestCompiledFanOutEntityRequirementRejectsEntitylessOwnershipAcrossSites(t *testing.T) {
	bundle := loadWorkflowTempBundle(t, map[string]string{

		"schema.yaml": "name: compiled-fan-out-entity-requirement\n",
		"review/schema.yaml": `name: review
mode: template
initial_state: active
states: [active]
`,
		"review/entities.yaml": `review_entity:
  items:
    type: "[text]"
`,
		"review/events.yaml": `top.ready: {}
nested.ready: {}
item.requested:
  item: string
`,
		"review/nodes.yaml": `top-level-reader:
  id: top-level-reader
  execution_type: system_node
  subscribes_to: [top.ready]
  event_handlers:
    top.ready:
      fan_out:
        items_from: entity.items
        as: row
        identity: row
        emit:
          event: item.requested
          fields:
            item: row
nested-reader:
  id: nested-reader
  execution_type: system_node
  subscribes_to: [nested.ready]
  event_handlers:
    nested.ready:
      rules:
        entity-rows:
          condition: "true"
          fan_out:
            items_from: entity.items
            as: row
            identity: row
            emit:
              event: item.requested
              fields:
                item: row
`,
	})
	source := semanticview.Wrap(bundle)
	for _, test := range []struct{ nodeID, eventType string }{
		{nodeID: "top-level-reader", eventType: "top.ready"},
		{nodeID: "nested-reader", eventType: "nested.ready"},
	} {
		t.Run(test.nodeID, func(t *testing.T) {
			node := pipelineNode(t, "review", test.nodeID)
			handlerFact, err := AdmitDeliveryTargetHandler(source, node)
			if err != nil {
				t.Fatal(err)
			}
			handlerFact = handlerFact.ForEvent(events.EventType(test.eventType))
			handler, ok := handlerFact.resolve(source, events.EventType(test.eventType))
			if !ok {
				t.Fatal("resolve admitted fan-out handler")
			}
			if got := handlerExecutionEntityRequirementForNode(source, node, events.EventType(test.eventType), "review", handler); got == DeliveryTargetEntityOptional {
				t.Fatalf("compiled fan-out requirement = %v, want entity ownership", got)
			}
		})
	}
}

func TestHandlerExecutionEntityRequirementOwnsDurableBehaviorCapabilities(t *testing.T) {
	tests := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
		want    DeliveryTargetEntityDependency
	}{
		{name: "payload accumulator persists state bucket", handler: runtimecontracts.SystemNodeEventHandler{
			Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"},
		}, want: DeliveryTargetExistingEntityRequired},
		{name: "approval activity persists proposed effect", handler: runtimecontracts.SystemNodeEventHandler{
			Activity: runtimecontracts.ActivitySpec{Tool: "review", Approval: &runtimecontracts.ActivityApprovalSpec{Decision: "review_change"}},
		}, want: DeliveryTargetExistingEntityRequired},
		{name: "nested approval activity persists proposed effect", handler: runtimecontracts.SystemNodeEventHandler{
			Rules: []runtimecontracts.HandlerRuleEntry{{Activity: runtimecontracts.ActivitySpec{Tool: "review", Approval: &runtimecontracts.ActivityApprovalSpec{Decision: "review_change"}}}},
		}, want: DeliveryTargetExistingEntityRequired},
		{name: "guard kill persists lifecycle transition", handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{Check: "false", OnFail: "kill"},
		}, want: DeliveryTargetExistingEntityRequired},
		{name: "accumulator clear persists state bucket mutation", handler: runtimecontracts.SystemNodeEventHandler{
			Clear: &runtimecontracts.ClearSpec{Targets: []string{"accumulator_state"}},
		}, want: DeliveryTargetExistingEntityRequired},
		{name: "pending dedup clear persists metadata mutation", handler: runtimecontracts.SystemNodeEventHandler{
			Clear: &runtimecontracts.ClearSpec{Targets: []string{"pending_dedup"}},
		}, want: DeliveryTargetExistingEntityRequired},
		{name: "unrooted clear persists entity field mutation", handler: runtimecontracts.SystemNodeEventHandler{
			Clear: &runtimecontracts.ClearSpec{Targets: []string{"revision_count"}},
		}, want: DeliveryTargetExistingEntityRequired},
		{name: "explicit creation is acquisition independent from execution", handler: runtimecontracts.SystemNodeEventHandler{
			CreateEntity: true,
		}, want: DeliveryTargetEntityOptional},
		{name: "creation preserves platform entity identity dependency", handler: runtimecontracts.SystemNodeEventHandler{
			CreateEntity: true,
			Guard:        &runtimecontracts.GuardSpec{Check: `_entity.id != ""`},
		}, want: DeliveryTargetExistingEntityRequired},
		{name: "payload-only fanout is entityless safe", handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{ItemsFrom: "payload.items", Emit: runtimecontracts.EmitSpec{Event: "work.item", From: "payload"}},
		}, want: DeliveryTargetEntityOptional},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := handlerExecutionEntityRequirement(nil, "review", test.handler); got != test.want {
				t.Fatalf("handler execution requirement = %d, want %d", got, test.want)
			}
		})
	}
}

func TestHandlerExecutionEntityRequirementIgnoresUnevaluatedFields(t *testing.T) {
	tests := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
	}{
		{
			name: "filter predicate",
			handler: runtimecontracts.SystemNodeEventHandler{Filter: &runtimecontracts.FilterSpec{
				Predicate: "entity.items",
			}},
		},
		{
			name: "reduce params",
			handler: runtimecontracts.SystemNodeEventHandler{Reduce: &runtimecontracts.ReduceSpec{
				Params: map[string]runtimecontracts.ExpressionValue{"value": runtimecontracts.RefExpression("entity.items")},
			}},
		},
		{
			name:    "filter source shadowed by items from",
			handler: runtimecontracts.SystemNodeEventHandler{Filter: &runtimecontracts.FilterSpec{Source: "entity.items", ItemsFrom: "payload.items"}},
		},
		{
			name:    "reduce source shadowed by items from",
			handler: runtimecontracts.SystemNodeEventHandler{Reduce: &runtimecontracts.ReduceSpec{Source: "entity.items", ItemsFrom: "payload.items"}},
		},
		{
			name:    "count source shadowed by items from",
			handler: runtimecontracts.SystemNodeEventHandler{Count: &runtimecontracts.CountSpec{Source: "entity.items", ItemsFrom: "payload.items"}},
		},
		{
			name: "guard check shadowed by checks",
			handler: runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{
				Check:  "entity.items",
				Checks: []runtimecontracts.GuardCheck{{ID: "ready", Check: "payload.ready"}},
			}},
		},
		{
			name: "nested query row",
			handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{
				Queries: []runtimecontracts.QuerySpec{{Source: "entity.items"}},
			}},
		},
		{
			name: "on complete activity input",
			handler: runtimecontracts.SystemNodeEventHandler{OnComplete: []runtimecontracts.HandlerRuleEntry{{
				Activity: runtimecontracts.ActivitySpec{Input: map[string]runtimecontracts.ExpressionValue{"owner": runtimecontracts.CELExpression("entity.owner")}},
			}}},
		},
		{
			name: "on complete action input",
			handler: runtimecontracts.SystemNodeEventHandler{OnComplete: []runtimecontracts.HandlerRuleEntry{{
				Action: runtimecontracts.ActionSpec{ID: "create_flow_instance", InstanceIDFrom: "entity.owner"},
			}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := handlerExecutionEntityRequirement(nil, "review", tc.handler); got != DeliveryTargetEntityOptional {
				t.Fatalf("handler execution requirement = %d, want entityless-safe for unevaluated field", got)
			}
		})
	}
}

func TestDeliveryTargetWorkflowInstanceAvailabilityIsActiveOnly(t *testing.T) {
	source := handlerEntityRequirementExecutionSource()
	tests := []struct {
		name        string
		status      string
		state       string
		terminated  time.Time
		unavailable bool
	}{
		{name: "active", status: "active", state: "active"},
		{name: "draining", status: "draining", state: "active", unavailable: true},
		{name: "terminated", status: "terminated", state: "active", unavailable: true},
		{name: "retired inactive spelling", status: "inactive", state: "active", unavailable: true},
		{name: "unknown fails closed", status: "failed", state: "active", unavailable: true},
		{name: "missing fails closed", state: "active", unavailable: true},
		{name: "termination timestamp", status: "active", state: "active", terminated: time.Now().UTC(), unavailable: true},
		{name: "terminal entity stage", status: "active", state: "killed", unavailable: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			instance := WorkflowInstance{Status: testCase.status, CurrentState: testCase.state, TerminatedAt: testCase.terminated,
				EntityType: "review_entity"}
			if got := deliveryTargetWorkflowInstanceUnavailable(source, ".", instance); got != testCase.unavailable {
				t.Fatalf("delivery target unavailable = %t, want %t for status=%q state=%q", got, testCase.unavailable, testCase.status, testCase.state)
			}
		})
	}
}

func deliveryTargetOwnershipSource(t *testing.T) semanticview.Source {
	t.Helper()
	flow := runtimecontracts.FlowContractView{
		Path: "review", Paths: runtimecontracts.FlowContractPaths{FlowPath: "review"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: runtimecontracts.FlowModeTemplate, InitialState: "active", States: []string{"active", "done"},
			Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{
				{Event: "work.created", Resolution: runtimecontracts.FlowInputPinResolution{Mode: runtimecontracts.FlowInputResolutionModeCreate}},
				{Event: "work.selected", Resolution: runtimecontracts.FlowInputPinResolution{Mode: runtimecontracts.FlowInputResolutionModeSelect}},
				{Event: "work.upserted", Resolution: runtimecontracts.FlowInputPinResolution{Mode: runtimecontracts.FlowInputResolutionModeSelectOrCreate}},
			}}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.ready": {}, "work.created": {}, "work.selected": {}, "work.upserted": {}, "work.keyed": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"existing":     deliveryTargetOwnershipNode("existing", runtimecontracts.SystemNodeEventHandler{AdvancesTo: "done"}),
			"materializer": deliveryTargetOwnershipNode("materializer", runtimecontracts.SystemNodeEventHandler{CreateEntity: true}),
			"transitioner": deliveryTargetOwnershipNode("transitioner", runtimecontracts.SystemNodeEventHandler{AdvancesTo: "done"}),
			"entityless":   deliveryTargetOwnershipNode("entityless", runtimecontracts.SystemNodeEventHandler{}),
			"entity-reader": deliveryTargetOwnershipNode("entity-reader", runtimecontracts.SystemNodeEventHandler{
				Condition: "entity.status == 'ready'",
			}),
			"pin-creator":  deliveryTargetOwnershipEventNode("pin-creator", "work.created", runtimecontracts.SystemNodeEventHandler{}),
			"pin-selector": deliveryTargetOwnershipEventNode("pin-selector", "work.selected", runtimecontracts.SystemNodeEventHandler{}),
			"pin-upserter": deliveryTargetOwnershipEventNode("pin-upserter", "work.upserted", runtimecontracts.SystemNodeEventHandler{}),
			"key-selector": deliveryTargetOwnershipEventNode("key-selector", "work.keyed", runtimecontracts.SystemNodeEventHandler{
				SelectEntity: &runtimecontracts.SelectEntitySpec{Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "account_id", Ref: "payload.account_id", RefPath: paths.Parse("payload.account_id")}}},
			}),
			"key-upserter": deliveryTargetOwnershipEventNode("key-upserter", "work.keyed", runtimecontracts.SystemNodeEventHandler{
				SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "account_id", Ref: "payload.account_id", RefPath: paths.Parse("payload.account_id")}}},
			}),
		},
	}
	root := runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Children: []runtimecontracts.FlowContractView{flow}}
	bundle := admitSyntheticEntityContractsForTest(t, &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{"review": &root.Children[0]},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"review": flow.Schema},
	}, "", map[string]string{"review": "review_entity"})
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatalf("compile delivery-target ownership semantics: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func deliveryTargetOwnershipNode(id string, handler runtimecontracts.SystemNodeEventHandler) runtimecontracts.SystemNodeContract {
	return deliveryTargetOwnershipEventNode(id, "work.ready", handler)
}

func deliveryTargetOwnershipEventNode(id, eventType string, handler runtimecontracts.SystemNodeEventHandler) runtimecontracts.SystemNodeContract {
	return runtimecontracts.SystemNodeContract{
		ID: id, SubscribesTo: []string{eventType},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{eventType: handler},
	}
}
