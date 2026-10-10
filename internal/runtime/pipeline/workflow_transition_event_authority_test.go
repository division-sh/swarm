package pipeline

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

func VerifyPipelineTransitionRejectsCoherentEventSubstitutionOnBothStoresForTest(t *testing.T, backend string, open func(*testing.T, semanticview.Source, string) WorkflowHandlerNativeFixtureForTest) {
	source := transitionMutationSource(t)
	node := pipelineNode(t, ".", "router")
	handler := source.ExecutableNodeEventHandlers(node)["advance"]
	graph, ok := semanticview.WorkflowStageTopology(source, ".")
	if !ok {
		t.Fatal("source did not compile root topology")
	}
	compiled, err := graph.AdmitTransition(contracts.WorkflowTransitionSite{
		Node: node, HandlerEvent: "advance", AdvanceCarrier: contracts.HandlerAdvanceCarrierHandler,
	}, "ready", "done")
	if err != nil {
		t.Fatal(err)
	}
	ref, ok := handler.Rules[0].DeclarationIdentity()
	if !ok {
		t.Fatal("source did not qualify selected rule")
	}
	selection, err := handlerselection.Selected(handlerselection.ContextRules, ref, "chosen")
	if err != nil {
		t.Fatal(err)
	}
	cause, err := workflowlifecycle.NewCompiledTransition(compiled, selection, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"accepted_control", "direct_execution_control", "other_persisted_id", "missing_id", "type", "time", "other_persisted_event", "foreign_accepted_handler", "missing_execution_event", "application_without_execution_event", "contradictory_inbound_event"} {
		t.Run(variant, func(t *testing.T) {
			fixture := open(t, source, variant)
			pc := fixture.NewCoordinator(PipelineCoordinatorOptions{Module: staticSemanticWorkflowModule{source: source}})
			ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
			if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
				t.Fatal(err)
			}
			entityID := eventtest.UUID("transition-event-authority-" + backend + "-" + variant)
			address := testEngineStateAddress(".", testPipelineRunID, entityID)
			instance := materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: testPipelineRunID, StorageRef: testPipelineRunID, EntityID: entityID,
				WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), Mode: contracts.FlowModeStatic,
				CurrentState: "ready", Fields: map[string]any{"marker": "unchanged"}, EntityType: "test_entity",
			})
			if err := fixture.Construct(ctx, instance); err != nil {
				t.Fatal(err)
			}
			at := time.Now().UTC().Truncate(time.Microsecond)
			acceptedID := eventtest.UUID("accepted-" + backend + "-" + variant)
			otherID := eventtest.UUID("other-" + backend + "-" + variant)
			acceptedType := events.EventType("advance")
			if variant == "foreign_accepted_handler" {
				acceptedType = "foreign"
			}
			accepted := eventtest.ExistingRunRootIngressWithRoutingSource(acceptedID, acceptedType, "", "", nil, 0, testPipelineRunID,
				handlerTestWorkflowEnvelope(".", testPipelineRunID, entityID), testWorkflowRoutingSource(".", testPipelineRunID, entityID), at)
			other := eventtest.ExistingRunRootIngressWithRoutingSource(otherID, "foreign", "", "", nil, 0, testPipelineRunID,
				handlerTestWorkflowEnvelope(".", testPipelineRunID, entityID), testWorkflowRoutingSource(".", testPipelineRunID, entityID), at.Add(time.Second))
			target := events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: entityID}
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target)}
			fixture.Publish(ctx, accepted, route)
			fixture.Publish(ctx, other, route)
			acceptedHandler := source.ExecutableNodeEventHandlers(node)[string(acceptedType)]
			application, err := pc.prepareDeliveryTargetApplication(ctx, node.Key(), MustDeliveryTargetHandler(node).ForEvent(acceptedType), acceptedHandler, accepted, events.MustExistingEntityTarget(target))
			if err != nil {
				t.Fatalf("prepare actual delivery application: %v", err)
			}
			publicationEffect, err := workflowlifecycle.NewAcceptedEvent(address.FlowInstance.Route, identity.NormalizeEntityID(entityID), accepted.ID(), string(accepted.Type()), accepted.ExecutionMode(), accepted.CreatedAt(), &cause)
			if err != nil {
				t.Fatal(err)
			}
			id, err := deliverylifecycle.DeliveryID(accepted.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			pending, err := pc.deliveryStore.Snapshot(ctx, id)
			if err != nil || pending.EventID != accepted.ID() || pending.Route.Target != route.Target || pending.Route.Recipient != route.Recipient || pending.Status != deliverylifecycle.StatusPending {
				t.Fatalf("component publication differs from its exact pending delivery: %+v err=%v", pending, err)
			}
			publicationEffect, err = publicationEffect.WithExecutionOccurrence("delivery", pending.DeliveryID)
			if err != nil {
				t.Fatal(err)
			}
			entry, hasEntry, err := publicationEffect.StageEntry(address.FlowInstance)
			if err != nil || !hasEntry {
				t.Fatalf("actual publication occurrence: found=%v err=%v", hasEntry, err)
			}
			if variant != "direct_execution_control" && variant != "missing_execution_event" {
				ctx = withDeliveryTargetApplication(ctx, application)
			}
			ctx = runtimecorrelation.WithInboundEvent(ctx, application.Event())
			if variant == "missing_execution_event" || variant == "application_without_execution_event" {
				ctx = runtimecorrelation.WithInboundEvent(ctx, events.Event{})
			} else if variant == "contradictory_inbound_event" {
				ctx = runtimecorrelation.WithInboundEvent(ctx, other)
			}
			state := testEngineStateMutation(map[string]any{"marker": "must-not-persist"}, nil, nil)
			state.NextState, state.Transition = "done", &cause
			state.TriggerEventID, state.TriggerEventType, state.TriggeredAt = acceptedID, string(acceptedType), at
			switch variant {
			case "other_persisted_id":
				state.TriggerEventID = otherID
			case "missing_id":
				state.TriggerEventID = eventtest.UUID("never-persisted")
			case "type":
				state.TriggerEventType = "foreign"
			case "time":
				state.TriggeredAt = at.Add(time.Second)
			case "other_persisted_event":
				state.TriggerEventID, state.TriggerEventType, state.TriggeredAt = otherID, "foreign", at.Add(time.Second)
			}
			// Deliberately keep both projections internally consistent. Their
			// agreement must not substitute for the accepted execution event.
			effect, err := workflowlifecycle.NewAcceptedEvent(address.FlowInstance.Route, identity.NormalizeEntityID(entityID),
				state.TriggerEventID, state.TriggerEventType, accepted.ExecutionMode(), state.TriggeredAt, &cause)
			if err != nil {
				t.Fatal(err)
			}
			effect, err = effect.WithExecutionOccurrence("delivery", entry.OccurrenceID)
			if err != nil {
				t.Fatal(err)
			}
			mutation := engine.EngineMutation{Address: address, State: state, HandlerRuleSelection: selection, LifecycleEffects: []workflowlifecycle.Effect{effect}}
			if err := mutation.ValidateTransitionEvidence(); err != nil {
				t.Fatalf("probe must reach independent accepted-event authority: %v", err)
			}
			before, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, address.FlowInstance)
			if err != nil || !found {
				t.Fatalf("before snapshot: found=%v err=%v", found, err)
			}
			mutation.EvaluatedState, found, err = workflowEngineEvaluationSnapshot(source, address.FlowID.String(), address, before, nil)
			if err != nil || !found {
				t.Fatalf("capture transition R1: found=%v error=%v", found, err)
			}
			snapshotRows := func() map[string][]string {
				raw, err := fixture.ApplicationStorage(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var snapshot map[string]struct{ Rows []string }
				if err := json.Unmarshal(raw, &snapshot); err != nil {
					t.Fatal(err)
				}
				out := map[string][]string{}
				for _, table := range []string{
					"entity_state", "flow_instances", "entity_mutations", "workflow_instance_initial_materializations",
					"events", "event_deliveries", "event_receipts", "timers", "activity_attempts",
					"event_delivery_handler_rule_selections", "event_delivery_attempts",
					"author_activity_occurrences", "fan_out_intents", "fan_out_outcomes",
				} {
					value, found := snapshot[table]
					if !found {
						t.Fatalf("native typed snapshot omitted %s", table)
					}
					out[table] = value.Rows
				}
				return out
			}
			beforeRows := snapshotRows()
			_, commitErr := (pipelineEngineMutationOwner{store: pc.workflowStore, state: pipelineEngineStateRepo{coordinator: pc}}).CommitEngineMutation(ctx, mutation)
			after, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, address.FlowInstance)
			if err != nil || !found {
				t.Fatalf("after snapshot: found=%v err=%v", found, err)
			}
			if variant == "accepted_control" || variant == "direct_execution_control" {
				if commitErr != nil || after.CurrentState != "done" || after.Revision != before.Revision+1 || len(after.TransitionHistory) != 1 || after.TransitionHistory[0].TriggerEventID != acceptedID {
					t.Fatalf("accepted control: err=%v state=%#v", commitErr, after)
				}
				return
			}
			if commitErr == nil {
				t.Errorf("coherent %s substitution COMMITTED: accepted=%s/%s cause_handler=%s trigger=%s/%s before_revision=%d after_revision=%d history=%#v",
					variant, accepted.ID(), accepted.Type(), compiled.Edge().HandlerEvent, state.TriggerEventID, state.TriggerEventType, before.Revision, after.Revision, after.TransitionHistory)
			} else {
				want := "transition trigger contradicts its accepted execution event"
				switch variant {
				case "foreign_accepted_handler":
					want = "transition handler contradicts its accepted execution selection"
				case "missing_execution_event", "application_without_execution_event":
					want = "transition requires its accepted execution event"
				case "contradictory_inbound_event":
					want = "transition execution event contradicts its admitted delivery application"
				}
				if !strings.Contains(commitErr.Error(), want) {
					t.Errorf("wrong rejection: got %v, want %q", commitErr, want)
				}
			}
			if !reflect.DeepEqual(before, after) {
				t.Error("hostile event substitution changed business state/history")
			}
			afterRows := snapshotRows()
			for table, rows := range beforeRows {
				if !slices.Equal(rows, afterRows[table]) {
					t.Errorf("hostile event substitution changed %s row contents: before=%q after=%q", table, rows, afterRows[table])
				}
			}
		})
	}
}
