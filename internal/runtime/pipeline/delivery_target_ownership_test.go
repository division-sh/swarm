package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestClassifyDeliveryTargetOwnershipClosedUnion(t *testing.T) {
	source := deliveryTargetOwnershipSource()
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
			name: "selected row establishes existing ownership for empty handler", nodeID: "entityless",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: entityID}}},
			assert: func(t *testing.T, owner events.DeliveryTargetOwnership) {
				if !owner.ExistingEntity() || owner.Route().EntityID != entityID {
					t.Fatalf("owner = %#v, want selected existing entity", owner)
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
			handler, err := AdmitDeliveryTargetHandler(source, "review", test.nodeID)
			if err != nil {
				t.Fatalf("admit handler: %v", err)
			}
			owner, err := ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
				Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(test.nodeID),
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
	source := deliveryTargetOwnershipSource()
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, err := AdmitDeliveryTargetHandler(source, "review", test.nodeID)
			if err != nil {
				t.Fatalf("admit handler: %v", err)
			}
			_, err = ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
				Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(test.nodeID),
				Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"},
				Handler:   handler.ForEvent("work.ready"), Candidates: test.candidates,
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("classification error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestClassifyDeliveryTargetOwnershipUsesUnambiguousHandlerFlowOwner(t *testing.T) {
	source := deliveryTargetOwnershipSource()
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("delivery-target-handler-flow"), events.EventType("review/one/work.ready"),
		"", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{},
	)
	handler, err := AdmitDeliveryTargetHandler(source, "review", "entityless")
	if err != nil {
		t.Fatalf("admit handler: %v", err)
	}
	entityID := eventtest.UUID("handler-flow-owner")
	request := DeliveryTargetOwnershipRequest{
		Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient("entityless"),
		Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/producer-child"},
		Handler:   handler.ForEvent("work.ready"),
		Candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{
			FlowID: "review", FlowInstance: "review/one", EntityID: entityID,
		}}},
	}
	owner, err := ClassifyDeliveryTargetOwnership(request)
	if err != nil {
		t.Fatalf("classify unambiguous handler-flow owner: %v", err)
	}
	if !owner.ExistingEntity() || owner.Route().FlowInstance != "review/one" || owner.Route().EntityID != entityID {
		t.Fatalf("owner = %#v, want exact existing handler-flow owner", owner)
	}

	request.Candidates = append(request.Candidates, DeliveryTargetOwnerCandidate{Route: events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/two", EntityID: eventtest.UUID("second-handler-flow-owner"),
	}})
	if _, err := ClassifyDeliveryTargetOwnership(request); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous handler-flow classification error = %v, want fail closed", err)
	}
}

func TestClassifyDeliveryTargetOwnershipProjectsRootHandlerOntoSelectedRun(t *testing.T) {
	runID := eventtest.UUID("selected-root-run")
	existingEntityID := eventtest.UUID("selected-root-owner")
	flow := runtimecontracts.FlowContractView{
		Path: "timer-proof", Paths: runtimecontracts.FlowContractPaths{ID: "timer-proof", Flow: "timer-proof"},
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
			Root: &flow, ByID: map[string]*runtimecontracts.FlowContractView{"timer-proof": &flow},
		},
	})
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("selected-root-event"), "timer.cancel", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{},
	)
	handler, err := AdmitDeliveryTargetHandler(source, "timer-proof", "controller")
	if err != nil {
		t.Fatalf("admit handler: %v", err)
	}
	request := DeliveryTargetOwnershipRequest{
		Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient("controller"),
		Blueprint: events.RouteIdentity{FlowID: "timer-proof"},
		Handler:   handler.ForEvent("timer.cancel"),
	}

	request.Candidates = []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: runID, EntityID: existingEntityID}}}
	owner, err := ClassifyDeliveryTargetOwnership(request)
	if err != nil {
		t.Fatalf("classify existing selected-run owner: %v", err)
	}
	wantExisting := events.RouteIdentity{FlowID: "timer-proof", FlowInstance: runID, EntityID: existingEntityID}
	if !owner.ExistingEntity() || owner.Route() != wantExisting {
		t.Fatalf("existing owner = %#v, want %#v", owner, wantExisting)
	}

	request.Candidates = nil
	owner, err = ClassifyDeliveryTargetOwnership(request)
	if err != nil {
		t.Fatalf("classify first-delivery selected-run owner: %v", err)
	}
	wantMaterializing := events.RouteIdentity{FlowID: "timer-proof", FlowInstance: runID, EntityID: FlowInstanceEntityID(runID)}
	if !owner.MaterializingEntity() || owner.Route() != wantMaterializing {
		t.Fatalf("materializing owner = %#v, want %#v", owner, wantMaterializing)
	}
}

func TestClassifyDeliveryTargetOwnershipConsumesExactInputAcquisitionMode(t *testing.T) {
	source := deliveryTargetOwnershipSource()
	entityID := eventtest.UUID("selected-input-owner")
	tests := []struct {
		name       string
		nodeID     string
		eventType  events.EventType
		candidates []DeliveryTargetOwnerCandidate
		wantKind   string
		wantError  string
	}{
		{
			name: "create pin consumes canonical materialization evidence", nodeID: "pin-creator", eventType: "work.created",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: FlowInstanceEntityID("review/one")}, Materializing: true}},
			wantKind:   "materializing_entity",
		},
		{
			name: "select pin consumes existing", nodeID: "pin-selector", eventType: "work.selected",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: entityID}}},
			wantKind:   "existing_entity",
		},
		{name: "select pin rejects missing owner", nodeID: "pin-selector", eventType: "work.selected", wantError: "owner is missing"},
		{
			name: "select pin consumes planned future owner", nodeID: "pin-selector", eventType: "work.selected",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: entityID}, Materializing: true}},
			wantKind:   "materializing_entity",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evt := eventtest.RunCreatingRootIngress(
				eventtest.UUID("input-acquisition-"+test.name), events.EventType("review/one/"+string(test.eventType)),
				"", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{},
			)
			handler, err := AdmitDeliveryTargetHandler(source, "review", test.nodeID)
			if err != nil {
				t.Fatalf("admit handler: %v", err)
			}
			owner, err := ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
				Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(test.nodeID),
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
		})
	}
}

func deliveryTargetOwnershipSource() semanticview.Source {
	flow := runtimecontracts.FlowContractView{
		Path: "review", Paths: runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
		Schema: runtimecontracts.FlowSchemaDocument{
			InitialState: "active", States: []string{"active", "done"},
			Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{
				{Name: "work_created", Event: "work.created", Resolution: runtimecontracts.FlowInputPinResolution{Mode: runtimecontracts.FlowInputResolutionModeCreate}},
				{Name: "work_selected", Event: "work.selected", Resolution: runtimecontracts.FlowInputPinResolution{Mode: runtimecontracts.FlowInputResolutionModeSelect}},
			}}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.ready": {}, "work.created": {}, "work.selected": {}},
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
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{"review": &root.Children[0]},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"review": flow.Schema},
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
