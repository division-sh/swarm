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
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func VerifyExistingOwnerExecutionSemanticsPersistOnSQLiteAndPostgresForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := handlerEntityRequirementExecutionSource()
	fixture := open(t, source)
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})

	t.Run("accumulator", func(t *testing.T) {
		ctx := nativeWorkflowHandlerRunContextForTest(t, fixture)
		nodeKey := pipelineNode(t, ".", "node-a").Key()
		instance, result := executeExistingOwnerBehavior(t, fixture, ctx, pc, "accumulator", "work.ready", json.RawMessage(`{"item_id":"a"}`), nil, nil)
		if !result.handled {
			t.Fatal("accumulator execution was not handled")
		}
		nodeBucket, ok := instance.StateBuckets[nodeKey].(map[string]any)
		if !ok {
			t.Fatalf("accumulator node bucket = %#v, want persisted node-a bucket", instance.StateBuckets)
		}
		if _, ok := nodeBucket["handler_accumulators"]; !ok {
			t.Fatalf("accumulator bucket = %#v, want persisted handler_accumulators", nodeBucket)
		}
	})

	t.Run("clear", func(t *testing.T) {
		ctx := nativeWorkflowHandlerRunContextForTest(t, fixture)
		initialMetadata := map[string]any{
			"revision_count":    3,
			"dedup_key":         "pending-a",
			"accumulated_count": 1,
		}
		instance, result := executeExistingOwnerBehavior(t, fixture, ctx, pc, "clear", "work.clear", nil, initialMetadata, nil)
		if !result.handled {
			t.Fatal("clear execution was not handled")
		}
		for _, field := range []string{"revision_count", "dedup_key", "accumulated_count"} {
			if _, ok := instance.Fields[field]; ok {
				t.Fatalf("clear retained field %q in %#v", field, instance.Fields)
			}
		}
	})

	t.Run("guard_kill", func(t *testing.T) {
		ctx := nativeWorkflowHandlerRunContextForTest(t, fixture)
		instance, result := executeExistingOwnerBehavior(t, fixture, ctx, pc, "guard-kill", "work.kill", nil, nil, nil)
		if !result.handled || (result.status != "" && result.status != HandlerOutcomeKilled) {
			t.Fatalf("guard kill outcome = handled:%t status:%q, want handled killed outcome", result.handled, result.status)
		}
		if got := strings.TrimSpace(instance.CurrentState); got != "killed" {
			t.Fatalf("guard kill current state = %q, want killed", got)
		}
	})
}

func VerifyNativeEntitylessNodeContractEmissionDoesNotMaterializeWorkflowStateOnSQLiteAndPostgresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml": "stages:\n  active: {}\n",
				"events.yaml": "work.ready:\n  item_id: text\nwork.emitted:\n",
				"nodes.yaml":  "node-a:\n  execution_type: system_node\n  subscribes_to: [work.ready]\n  event_handlers:\n    work.ready:\n      emit: work.emitted\n",
			})
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			instancePath := runID
			// Fieldless is not entityless: the constructor owns a header, but
			// declarative emission must not create an entity_state row.
			constructNativePipelineScenarioForTest(t, fixture, pc, ctx, ".")
			evt := eventtest.ExistingRunRootIngress(
				uuid.NewString(), "work.ready", "", "", json.RawMessage(`{"item_id":"a"}`), 0, runID,
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: instancePath, EntityID: runID}), time.Now().UTC(),
			)
			node := pipelineNode(t, ".", "node-a")
			route := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target: events.MustExistingEntityTarget(events.RouteIdentity{
					FlowID: ".", FlowInstance: instancePath, EntityID: runID,
				}),
			}
			if err := fixture.PublishNode(ctx, evt, route); err != nil {
				t.Fatal(err)
			}
			deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, route)

			outcome, err := executeNativeClaimedPipelineHandlerForTest(t, pc, deliveryCtx, node,
				runtimecontracts.SystemNodeEventHandler{Emit: runtimecontracts.EmitSpec{Event: "work.emitted"}},
				workflowTriggerContext{Event: evt, HandlerEventKey: "work.ready"})
			if err != nil {
				t.Fatalf("execute entityless declarative handler: %v", err)
			}
			if !outcome.Handled {
				t.Fatalf("entityless declarative outcome = %#v, want handled", outcome)
			}
			if bus.committedPublications != 1 || len(outcome.FollowUp.Emissions) != 1 || outcome.FollowUp.Emissions[0].Event.Type() != events.EventType("work.emitted") {
				t.Fatalf("entityless durable publications = %#v/%d, want one work.emitted event", outcome.FollowUp.Emissions, bus.committedPublications)
			}
			emitted := outcome.FollowUp.Emissions[0].Event
			persisted, err := fixture.PublishedEvent(ctx, emitted.ID())
			if err != nil || !reflect.DeepEqual(persisted, emitted) {
				t.Fatalf("entityless publication lacks its exact native receipt: %+v/%v", persisted, err)
			}

			counts, err := fixture.PhysicalCounts(ctx)
			if err != nil || counts.EntityStates != 0 || counts.ConstructedHeaders != 1 {
				t.Fatalf("entityless physical state = %+v/%v, want zero entity states and one constructed header", counts, err)
			}
		})
	}
}

func executeExistingOwnerBehavior(
	t *testing.T,
	fixture WorkflowHandlerNativeFixtureForTest,
	ctx context.Context,
	pc *PipelineCoordinator,
	name string,
	eventType string,
	payload json.RawMessage,
	metadata map[string]any,
	stateBuckets map[string]any,
) (WorkflowInstance, existingOwnerExecutionResult) {
	t.Helper()
	runID := runtimecorrelation.RunIDFromContext(ctx)
	flowInstance := runID
	entityID := runID
	seedMetadata := cloneStringAnyMap(metadata)
	if seedMetadata == nil {
		seedMetadata = map[string]any{}
	}
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      runID,
		StorageRef:      flowInstance,
		EntityID:        entityID,
		WorkflowName:    ".",
		WorkflowVersion: pc.SemanticSource().WorkflowVersion(),
		CurrentState:    "active",
		Fields:          seedMetadata,
		StateBuckets:    stateBuckets,
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed %s workflow instance: %v", name, err)
	}

	node := pipelineNode(t, ".", "node-a")
	handler, found := pc.SemanticSource().ExecutableNodeEventHandlers(node)[eventType]
	if !found {
		t.Fatalf("missing declared handler %q", eventType)
	}
	sourceEvent := eventtest.ExistingRunRootIngressWithRoutingSource(
		uuid.NewString(), events.EventType(eventType), "", "", payload, 0, runID,
		handlerTestWorkflowEnvelope(".", flowInstance, entityID), testWorkflowRoutingSource(".", flowInstance, entityID), time.Now().UTC(),
	)
	target := events.RouteIdentity{FlowID: ".", FlowInstance: flowInstance, EntityID: entityID}
	evt := eventtest.TargetRouted(sourceEvent, target)
	route := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(node),
		Target:    events.MustExistingEntityTarget(target),
	}
	fixture.Publish(ctx, sourceEvent, route)
	deliveryCtx, stopHeartbeat := claimNativeWorkflowHandlerPublicationForTest(t, pc, ctx, evt, route)
	defer func() {
		if err := stopHeartbeat(); err != nil {
			t.Errorf("native handler heartbeat cleanup: %v", err)
		}
	}()
	executed, err := pc.executeNodeContractHandler(deliveryCtx, node, handler, workflowTriggerContext{
		Event: evt, HandlerEventKey: eventType,
		State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(flowInstance), entityID),
	}, false)
	if err != nil {
		t.Fatalf("execute %s handler: %v", name, err)
	}
	result := existingOwnerExecutionResult{handled: executed.Handled}
	for _, intent := range executed.FollowUp.Emissions {
		result.emissions = append(result.emissions, intent.Event)
	}
	if executed.Outcome != nil {
		result.status = executed.Outcome.Status
	}
	instance, ok, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, flowInstance))
	if err != nil || !ok {
		t.Fatalf("load %s workflow instance: found=%t err=%v", name, ok, err)
	}
	return instance, result
}

type existingOwnerExecutionResult struct {
	handled   bool
	status    HandlerOutcomeStatus
	emissions []events.Event
}

func handlerEntityRequirementExecutionSource() semanticview.Source {
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides("../../..", "testdata/entity-requirement-execution", "")
	if err != nil {
		panic(err)
	}
	return semanticview.Wrap(bundle)
}

func VerifyNativeEntitylessPayloadGuardDoesNotPublishOrMaterializeOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml": "stages:\n  active: {}\n",
				"events.yaml": "work.ready:\n  item_id: text\n",
				"nodes.yaml": `node-a:
  execution_type: system_node
  subscribes_to: [work.ready]
  event_handlers:
    work.ready:
      guard:
        check: payload.item_id == "a"
`,
			})
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			instancePath := runID
			constructNativePipelineScenarioForTest(t, fixture, pc, ctx, ".")
			evt := eventtest.ExistingRunRootIngressWithRoutingSource(
				uuid.NewString(), "work.ready", "", "", json.RawMessage(`{"item_id":"a"}`), 0, runID,
				events.EnvelopeForTargetRoute(handlerTestWorkflowEnvelope(".", instancePath, runID), events.RouteIdentity{FlowID: ".", FlowInstance: instancePath, EntityID: runID}), testWorkflowRoutingSource(".", instancePath, runID), time.Now().UTC(),
			)
			node := pipelineNode(t, ".", "node-a")
			route := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target: events.MustExistingEntityTarget(events.RouteIdentity{
					FlowID: ".", FlowInstance: instancePath, EntityID: runID,
				}),
			}
			if err := fixture.PublishNode(ctx, evt, route); err != nil {
				t.Fatal(err)
			}
			deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, route)

			outcome, err := executeNativeClaimedPipelineHandlerForTest(t, pc, deliveryCtx, node,
				runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{Check: `payload.item_id == "a"`}},
				workflowTriggerContext{Event: evt, HandlerEventKey: "work.ready"})
			if err != nil {
				t.Fatalf("execute entityless payload guard handler: %v", err)
			}
			if !outcome.Handled {
				t.Fatalf("entityless payload guard outcome = %#v, want handled", outcome)
			}
			rejected := eventtest.ExistingRunRootIngressWithRoutingSource(
				uuid.NewString(), "work.ready", "", "", json.RawMessage(`{"item_id":"b"}`), 0, runID,
				events.EnvelopeForTargetRoute(handlerTestWorkflowEnvelope(".", instancePath, runID), events.RouteIdentity{FlowID: ".", FlowInstance: instancePath, EntityID: runID}), testWorkflowRoutingSource(".", instancePath, runID), time.Now().UTC(),
			)
			if err := fixture.PublishNode(ctx, rejected, route); err != nil {
				t.Fatal(err)
			}
			outcome, err = executeNativeClaimedPipelineHandlerForTest(t, pc, deliveryCtx, node,
				runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{Check: `payload.item_id == "a"`}},
				workflowTriggerContext{Event: rejected, HandlerEventKey: "work.ready"})
			if err != nil || outcome.Outcome == nil || len(outcome.Outcome.ActionsExecuted) != 1 || outcome.Outcome.ActionsExecuted[0] != "reject" {
				t.Fatalf("payload guard rejection = %#v, %v", outcome, err)
			}
			if bus.committedPublications != 0 || bus.publishedCount() != 0 {
				t.Fatalf("entityless durable publications = %d/%d, want no publication", bus.committedPublications, bus.publishedCount())
			}

			counts, err := fixture.PhysicalCounts(ctx)
			if err != nil || counts.EntityStates != 0 || counts.ConstructedHeaders != 1 {
				t.Fatalf("entityless physical state = %+v/%v, want zero entity states and one constructed header", counts, err)
			}
		})
	}
}
