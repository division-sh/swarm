package pipeline

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestClassifyDeliveryTargetOwnershipClosedUnion(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(eventtest.UUID("delivery-target-classification"), "work.ready", "", "", nil, 0, testPipelineRunID, "", events.EventEnvelope{}, time.Time{})
	for _, nodeID := range []string{"existing", "materializer", "transitioner", "entityless", "entity-reader"} {
		for _, prospective := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/prospective=%t", nodeID, prospective), func(t *testing.T) {
				node := pipelineNode(t, "review", nodeID)
				target := events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: FlowInstanceEntityID("review/one")}
				owner, err := ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
					Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node), Blueprint: target,
					Handler:    MustDeliveryTargetHandler(node).ForEvent("work.ready"),
					Candidates: []DeliveryTargetOwnerCandidate{{Route: target, Materializing: prospective}},
				})
				if err != nil || owner.Route() != target || owner.MaterializingEntity() != prospective || owner.ExistingEntity() == prospective {
					t.Fatalf("constructor candidate ownership: owner=%#v err=%v", owner, err)
				}
			})
		}
	}
}

func TestClassifyDeliveryTargetOwnershipFailsClosedOnMissingOrContradictoryEvidence(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("delivery-target-hostile"), events.EventType("review/one/work.ready"),
		"", "", nil, 0, testPipelineRunID, "", events.EventEnvelope{}, time.Time{},
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
			want: "constructor target evidence disagrees with canonical instance identity",
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
		"", "", nil, 0, testPipelineRunID, "", events.EventEnvelope{}, time.Time{},
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
		"", "", nil, 0, testPipelineRunID, "", events.EventEnvelope{}, time.Time{},
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
		Schema: runtimecontracts.FlowSchemaDocument{StageDeclarations: runtimecontracts.FlowStageDeclarations{Declared: true, Entries: []runtimecontracts.FlowStageDeclaration{{ID: "waiting"}, {ID: "done"}}}},
		Events: map[string]runtimecontracts.EventCatalogEntry{"timer.cancel": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"controller": {
				SubscribesTo: []string{"timer.cancel"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"timer.cancel": {AdvancesTo: "done"},
				},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "timer-proof"},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &flow, ByID: map[string]*runtimecontracts.FlowContractView{".": &flow},
		},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
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
	if err == nil || !owner.Empty() || !strings.Contains(err.Error(), "construct it before handler delivery") {
		t.Fatalf("missing root construction acquired ownership: owner=%#v err=%v", owner, err)
	}
	wantMaterializing := events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: FlowInstanceEntityID(runID)}
	request.Candidates = []DeliveryTargetOwnerCandidate{{Route: wantMaterializing, Materializing: true}}
	owner, err = ClassifyDeliveryTargetOwnership(request)
	if err != nil {
		t.Fatalf("classify explicit selected-run construction evidence: %v", err)
	}
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
			name: "creating input consumes canonical construction evidence", nodeID: "pin-creator", eventType: "work.created",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: FlowInstanceEntityID("review/one")}, Materializing: true}},
			wantKind:   "materializing_entity", wantEntity: FlowInstanceEntityID("review/one"),
		},
		{
			name: "creating input cannot replace missing constructor", nodeID: "pin-creator", eventType: "work.created",
			wantError: "construct it before handler delivery",
		},
		{
			name: "upsert input cannot replace missing constructor", nodeID: "pin-upserter", eventType: "work.upserted",
			wantError: "construct it before handler delivery",
		},
		{
			name: "selected input consumes existing", nodeID: "pin-selector", eventType: "work.selected",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: entityID}}},
			wantKind:   "existing_entity",
		},
		{name: "selected input rejects missing owner", nodeID: "pin-selector", eventType: "work.selected", wantError: "owner is missing"},
		{
			name: "selected input consumes exact ordered construction", nodeID: "pin-selector", eventType: "work.selected",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: FlowInstanceEntityID("review/one")}, Materializing: true}},
			wantKind:   "materializing_entity", wantEntity: FlowInstanceEntityID("review/one"),
		},
		{
			name: "creating input rejects wrong construction owner", nodeID: "pin-creator", eventType: "work.created",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: eventtest.UUID("wrong-acquisition-owner")}, Materializing: true}},
			wantError:  "constructor target evidence disagrees with canonical instance identity",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evt := eventtest.RunCreatingRootIngress(
				eventtest.UUID("input-acquisition-"+test.name), events.EventType("review/one/"+string(test.eventType)),
				"", "", nil, 0, testPipelineRunID, "", events.EventEnvelope{}, time.Time{},
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

func TestClassifyDeliveryTargetOwnershipPreservesCompositionInstanceMatrix(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("composition-target"), events.EventType("review/work.keyed"),
		"", "", mustJSON(map[string]any{"account_id": "unrelated-business-key"}), 0, testPipelineRunID, "", events.EventEnvelope{}, time.Time{},
	)
	identity := deriveFlowInstanceIdentity(source, "review", "one")
	exact := events.RouteIdentity{FlowID: "review", FlowInstance: identity.InstancePath, EntityID: identity.EntityID}
	sibling := events.RouteIdentity{FlowID: "review", FlowInstance: "review/two", EntityID: eventtest.UUID("composition-sibling")}
	for _, tc := range []struct {
		name, node, kind, failure string
		candidates                []DeliveryTargetOwnerCandidate
	}{
		{name: "required absent", node: "key-selector", failure: "owner is missing"},
		{name: "terminal exact cannot be reinitialized", node: "key-upserter", failure: "owner is unavailable", candidates: []DeliveryTargetOwnerCandidate{{Route: exact, Availability: NewDeliveryTargetAvailability("done", "active", false)}}},
		{name: "draining exact cannot be reinitialized", node: "key-upserter", failure: "owner is unavailable", candidates: []DeliveryTargetOwnerCandidate{{Route: exact, Availability: NewDeliveryTargetAvailability("active", "draining", false)}}},
		{name: "terminated exact cannot be reinitialized", node: "key-upserter", failure: "owner is unavailable", candidates: []DeliveryTargetOwnerCandidate{{Route: exact, Availability: NewDeliveryTargetAvailability("active", "active", true)}}},
		{name: "active duplicate cannot hide terminal evidence", node: "key-selector", failure: "owner is unavailable", candidates: []DeliveryTargetOwnerCandidate{{Route: exact}, {Route: exact, Availability: NewDeliveryTargetAvailability("done", "active", false)}}},
		{name: "terminal sibling cannot veto active receiver", node: "key-selector", kind: "existing_entity", candidates: []DeliveryTargetOwnerCandidate{{Route: sibling, Availability: NewDeliveryTargetAvailability("done", "terminated", true)}, {Route: exact}}},
		{name: "required exact", node: "key-selector", kind: "existing_entity", candidates: []DeliveryTargetOwnerCandidate{{Route: exact}}},
		{name: "required only sibling", node: "key-selector", failure: "owner is missing", candidates: []DeliveryTargetOwnerCandidate{{Route: sibling}}},
		{name: "required exact and sibling", node: "key-selector", kind: "existing_entity", candidates: []DeliveryTargetOwnerCandidate{{Route: sibling}, {Route: exact}}},
		{name: "missing constructor cannot initialize absent", node: "key-upserter", failure: "construct it before handler delivery"},
		{name: "initialize existing", node: "key-upserter", kind: "existing_entity", candidates: []DeliveryTargetOwnerCandidate{{Route: exact}}},
		{name: "sibling cannot replace missing constructor", node: "key-upserter", failure: "construct it before handler delivery", candidates: []DeliveryTargetOwnerCandidate{{Route: sibling}}},
		{name: "initialize exact and sibling", node: "key-upserter", kind: "existing_entity", candidates: []DeliveryTargetOwnerCandidate{{Route: exact}, {Route: sibling}}},
		{name: "contradictory exact owners", node: "key-upserter", failure: "ambiguous", candidates: []DeliveryTargetOwnerCandidate{{Route: exact}, {Route: events.RouteIdentity{FlowID: "review", FlowInstance: exact.FlowInstance, EntityID: eventtest.UUID("contradictory-owner")}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := pipelineNode(t, "review", tc.node)
			handler, err := AdmitDeliveryTargetHandler(source, node)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
				Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
				Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: identity.InstancePath},
				Handler:   handler.ForEvent("work.keyed"), Candidates: tc.candidates,
			})
			if tc.failure != "" {
				if err == nil || !strings.Contains(err.Error(), tc.failure) {
					t.Fatalf("error=%v, want %q", err, tc.failure)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if owner.Code() != tc.kind || owner.Route() != exact {
				t.Fatalf("owner=%s %#v, want %s %#v", owner.Code(), owner.Route(), tc.kind, exact)
			}
		})
	}
}

func TestClassifyDeliveryTargetOwnershipTargetedEventPreservesExactOwnerBeforeDeclaredKeyAcquisition(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	exact := events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/exact", EntityID: deriveFlowInstanceIdentity(source, "review", "exact").EntityID,
	}.Normalized()
	competing := events.RouteIdentity{FlowID: "review", FlowInstance: "review/competing", EntityID: eventtest.UUID("declared-key-competing-owner")}
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
				Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
				Blueprint: exact, Handler: handler.ForEvent("work.keyed"),
				Candidates: []DeliveryTargetOwnerCandidate{{Route: exact}, {Route: competing}},
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

	plan := exactCompiledJoinPlanForTest(bundle, ".")
	source := exactWorkflowJoinSource{
		Source: workflowJoinLifecycleRootAndFlowSource(bundle), plans: []runtimecontracts.WorkflowJoinPlan{plan},
		nodeFlowID: "", overrideNodeOwner: true,
	}
	entityID := eventtest.UUID("join-declaration-owner")
	routingSource, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	handle := pipelineJoinHandle(t, "", timeridentity.TimerHandleJoinTimeout, testPipelineRunID, testPipelineRunID, entityID)
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
		Source: source, Event: evt,
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

func TestValidateStampedDeliveryTargetOwnershipRejectsWrongAcquisitionFutureID(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("stamped-acquisition-owner"), events.EventType("review/one/work.created"),
		"", "", nil, 0, testPipelineRunID, "", events.EventEnvelope{}, time.Time{},
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
	if err == nil || !strings.Contains(err.Error(), "materializing_entity ownership disagrees with canonical instance identity") {
		t.Fatalf("ValidateStampedDeliveryTargetOwnership error = %v, want canonical construction identity rejection", err)
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
			err := NewDeliveryTargetAvailability(testCase.state, testCase.status, !testCase.terminated.IsZero()).Validate(source, ".")
			var terminal *TerminalReceiverError
			if errors.As(err, &terminal) != (testCase.state == "killed") {
				t.Fatalf("terminal receiver classification=%#v err=%v", terminal, err)
			}
		})
	}
}

func deliveryTargetOwnershipSource(t *testing.T) semanticview.Source {
	t.Helper()
	flow := runtimecontracts.FlowContractView{
		Path: "review", Paths: runtimecontracts.FlowContractPaths{FlowPath: "review"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Instance:          mustDeliveryTargetTemplateField(t),
			StageDeclarations: runtimecontracts.FlowStageDeclarations{Declared: true, Entries: []runtimecontracts.FlowStageDeclaration{{ID: "active"}, {ID: "done", Final: true}}},
			Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{
				{Event: "work.created"},
				{Event: "work.selected"},
				{Event: "work.upserted"},
			}}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.ready": {}, "work.created": {}, "work.selected": {}, "work.upserted": {}, "work.keyed": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"existing":     deliveryTargetOwnershipNode("existing", runtimecontracts.SystemNodeEventHandler{AdvancesTo: "done"}),
			"materializer": deliveryTargetOwnershipNode("materializer", runtimecontracts.SystemNodeEventHandler{}),
			"transitioner": deliveryTargetOwnershipNode("transitioner", runtimecontracts.SystemNodeEventHandler{AdvancesTo: "done"}),
			"entityless":   deliveryTargetOwnershipNode("entityless", runtimecontracts.SystemNodeEventHandler{}),
			"entity-reader": deliveryTargetOwnershipNode("entity-reader", runtimecontracts.SystemNodeEventHandler{
				Condition: "entity.status == 'ready'",
			}),
			"pin-creator":  deliveryTargetOwnershipEventNode("pin-creator", "work.created", runtimecontracts.SystemNodeEventHandler{}),
			"pin-selector": deliveryTargetOwnershipEventNode("pin-selector", "work.selected", runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}),
			"pin-upserter": deliveryTargetOwnershipEventNode("pin-upserter", "work.upserted", runtimecontracts.SystemNodeEventHandler{}),
			"key-selector": deliveryTargetOwnershipEventNode("key-selector", "work.keyed", runtimecontracts.SystemNodeEventHandler{
				Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"},
			}),
			"key-upserter": deliveryTargetOwnershipEventNode("key-upserter", "work.keyed", runtimecontracts.SystemNodeEventHandler{}),
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

func mustDeliveryTargetTemplateField(t testing.TB) runtimecontracts.TemplateInstanceField {
	t.Helper()
	field, err := runtimecontracts.ParseTemplateInstanceField("instance_key")
	if err != nil {
		t.Fatal(err)
	}
	return field
}

func deliveryTargetOwnershipEventNode(id, eventType string, handler runtimecontracts.SystemNodeEventHandler) runtimecontracts.SystemNodeContract {
	return runtimecontracts.SystemNodeContract{
		SubscribesTo:  []string{eventType},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{eventType: handler},
	}
}
