package pipeline

import (
	"reflect"
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
		{
			name: "raw blueprint entity is not ownership evidence", nodeID: "entity-reader",
			want: "owner is missing",
		},
		{
			name: "selected entity contradicts entityless handler", nodeID: "entityless",
			candidates: []DeliveryTargetOwnerCandidate{{
				Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: canonicalID},
			}},
			want: "entityless-safe handler has selected entity ownership evidence",
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
			if test.name == "raw blueprint entity is not ownership evidence" {
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

func TestClassifyDeliveryTargetOwnershipUsesUnambiguousHandlerFlowOwner(t *testing.T) {
	source := deliveryTargetOwnershipSource()
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("delivery-target-handler-flow"), events.EventType("review/one/work.ready"),
		"", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{},
	)
	node := pipelineNode(t, "review", "existing")
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
	node := pipelineNode(t, "timer-proof", "controller")
	handler, err := AdmitDeliveryTargetHandler(source, node)
	if err != nil {
		t.Fatalf("admit handler: %v", err)
	}
	request := DeliveryTargetOwnershipRequest{
		Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
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
			name: "select pin consumes planned future owner", nodeID: "pin-selector", eventType: "work.selected",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: FlowInstanceEntityID("review/one")}, Materializing: true}},
			wantKind:   "materializing_entity",
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

func TestValidateStampedDeliveryTargetOwnershipRejectsWrongAcquisitionFutureID(t *testing.T) {
	source := deliveryTargetOwnershipSource()
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
		{name: "fan out source", handler: runtimecontracts.SystemNodeEventHandler{FanOut: &runtimecontracts.FanOutSpec{ItemsFrom: "entity.items"}}},
		{name: "group by source", handler: runtimecontracts.SystemNodeEventHandler{GroupBy: &runtimecontracts.GroupBySpec{ItemsFrom: "entity.items"}}},
		{name: "on success emission", handler: runtimecontracts.SystemNodeEventHandler{OnSuccess: runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "work.done", From: "entity"}}}},
		{name: "join membership", handler: runtimecontracts.SystemNodeEventHandler{Join: &runtimecontracts.JoinSpec{Members: runtimecontracts.JoinMembersSpec{From: "entity.expected"}}}},
		{name: "loop lifecycle", handler: runtimecontracts.SystemNodeEventHandler{Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision"}}},
		{name: "activity input", handler: runtimecontracts.SystemNodeEventHandler{Activity: runtimecontracts.ActivitySpec{Input: map[string]runtimecontracts.ExpressionValue{"state": runtimecontracts.RefExpression("entity.status")}}}},
		{name: "guard escalation", handler: runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{OnFailSpec: runtimecontracts.GuardFailureSpec{Action: runtimecontracts.GuardFailureActionEscalate, Escalation: runtimecontracts.EmitSpec{Event: "guard.failed", From: "entity"}}}}},
		{name: "platform entity identity", handler: runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{Check: `_entity.id != ""`}}},
		{name: "platform entity state", handler: runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{Check: `_entity.current_state == "active"`}}},
		{name: "platform entity gate", handler: runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{Check: `_entity.gates.ready`}}},
		{name: "nested rule fan out", handler: runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{{FanOut: &runtimecontracts.FanOutSpec{ItemsFrom: "entity.items"}}}}},
		{name: "nested rule activity", handler: runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{{Activity: runtimecontracts.ActivitySpec{Input: map[string]runtimecontracts.ExpressionValue{"owner": runtimecontracts.CELExpression("entity.owner")}}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if handlerExecutionEntityRequirement(nil, "review", test.handler) == handlerEntitylessSafe {
				t.Fatalf("handler execution requirement = entityless-safe, want entity ownership for %s", test.name)
			}
		})
	}
	if handlerExecutionEntityRequirement(nil, "review", runtimecontracts.SystemNodeEventHandler{
		FanOut: &runtimecontracts.FanOutSpec{ItemsFrom: "payload.items", Emit: runtimecontracts.EmitSpec{Event: "work.item", From: "payload"}},
	}) != handlerEntitylessSafe {
		t.Fatal("payload-only fan out classified as entity-scoped")
	}
	if handlerExecutionEntityRequirement(nil, "review", runtimecontracts.SystemNodeEventHandler{
		Guard: &runtimecontracts.GuardSpec{Check: `_entity.flow_instance == "review/one"`},
	}) != handlerEntitylessSafe {
		t.Fatal("flow-instance-only platform metadata classified as entity-scoped")
	}
}

func TestHandlerExecutionEntityRequirementOwnsDurableBehaviorCapabilities(t *testing.T) {
	tests := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
		want    handlerEntityRequirement
	}{
		{name: "payload accumulator persists state bucket", handler: runtimecontracts.SystemNodeEventHandler{
			Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"},
		}, want: handlerExistingEntityRequired},
		{name: "approval activity persists proposed effect", handler: runtimecontracts.SystemNodeEventHandler{
			Activity: runtimecontracts.ActivitySpec{Tool: "review", Approval: &runtimecontracts.ActivityApprovalSpec{Decision: "review_change"}},
		}, want: handlerExistingEntityRequired},
		{name: "nested approval activity persists proposed effect", handler: runtimecontracts.SystemNodeEventHandler{
			Rules: []runtimecontracts.HandlerRuleEntry{{Activity: runtimecontracts.ActivitySpec{Tool: "review", Approval: &runtimecontracts.ActivityApprovalSpec{Decision: "review_change"}}}},
		}, want: handlerExistingEntityRequired},
		{name: "guard kill persists lifecycle transition", handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{Check: "false", OnFail: "kill"},
		}, want: handlerExistingEntityRequired},
		{name: "accumulator clear persists state bucket mutation", handler: runtimecontracts.SystemNodeEventHandler{
			Clear: &runtimecontracts.ClearSpec{Targets: []string{"accumulator_state"}},
		}, want: handlerExistingEntityRequired},
		{name: "pending dedup clear persists metadata mutation", handler: runtimecontracts.SystemNodeEventHandler{
			Clear: &runtimecontracts.ClearSpec{Targets: []string{"pending_dedup"}},
		}, want: handlerExistingEntityRequired},
		{name: "unrooted clear persists entity field mutation", handler: runtimecontracts.SystemNodeEventHandler{
			Clear: &runtimecontracts.ClearSpec{Targets: []string{"revision_count"}},
		}, want: handlerExistingEntityRequired},
		{name: "explicit creation may materialize", handler: runtimecontracts.SystemNodeEventHandler{
			CreateEntity: true,
		}, want: handlerMaterializingEntity},
		{name: "creation dominates platform entity identity", handler: runtimecontracts.SystemNodeEventHandler{
			CreateEntity: true,
			Guard:        &runtimecontracts.GuardSpec{Check: `_entity.id != ""`},
		}, want: handlerMaterializingEntity},
		{name: "payload-only fanout is entityless safe", handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{ItemsFrom: "payload.items", Emit: runtimecontracts.EmitSpec{Event: "work.item", From: "payload"}},
		}, want: handlerEntitylessSafe},
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
			if got := handlerExecutionEntityRequirement(nil, "review", tc.handler); got != handlerEntitylessSafe {
				t.Fatalf("handler execution requirement = %d, want entityless-safe for unevaluated field", got)
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
				{Name: "work_upserted", Event: "work.upserted", Resolution: runtimecontracts.FlowInputPinResolution{Mode: runtimecontracts.FlowInputResolutionModeSelectOrCreate}},
			}}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.ready": {}, "work.created": {}, "work.selected": {}, "work.upserted": {}},
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
