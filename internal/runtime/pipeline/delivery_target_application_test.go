package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func VerifyDeliveryTargetApplicationPreservesCompositionTargetOnSQLiteAndPostgresForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := deliveryTargetOwnershipSource(t)
	fixture := open(t, source)
	ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
	if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})

	node := pipelineNode(t, "review", "key-upserter")
	handlerFact, err := AdmitDeliveryTargetHandler(source, node)
	if err != nil {
		t.Fatal(err)
	}
	handlerFact = handlerFact.ForEvent("work.keyed")
	handler, ok := handlerFact.resolve(source, "work.keyed")
	if !ok {
		t.Fatal("resolve select-or-create handler")
	}
	evt := handlerTestRootIngress(
		uuid.NewString(), "work.keyed", "", "", mustJSON(map[string]any{"account_id": "account-1"}), 0,
		testPipelineRunID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
	expected := map[string]any{"account_id": "account-1"}
	instanceID := "composition-selected"
	identity := deriveFlowInstanceIdentity(source, "review", instanceID)
	target := events.RouteIdentity{FlowID: "review", FlowInstance: identity.InstancePath, EntityID: identity.EntityID}
	owner := events.MustExistingEntityTarget(target)

	application, err := pc.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, owner)
	if !errors.Is(err, runtimeengine.ErrUnconstructedWorkflowTarget) {
		t.Fatalf("unconstructed target became runnable: application=%+v err=%v", application, err)
	}

	exact := materializedWorkflowInstanceForSource(t, source, ctx, WorkflowInstance{
		InstanceID: instanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID,
		WorkflowName: "review", WorkflowVersion: source.WorkflowVersion(), CurrentState: "active", Fields: expected,
		EntityType: "review_entity",
	})
	if err := fixture.Construct(ctx, exact); err != nil {
		t.Fatalf("seed exact appearing target: %v", err)
	}
	application, err = pc.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, owner)
	if err != nil {
		t.Fatalf("prepare exact same-key appearance: %v", err)
	}
	if application.State().EntityID != identity.EntityID {
		t.Fatalf("appearing exact target state = %#v", application.State())
	}

	siblingID := "later-matching-sibling"
	siblingPath := "review/" + siblingID
	siblingEntityID := eventtest.UUID("later-matching-sibling")
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, source, ctx, WorkflowInstance{
		InstanceID: siblingID, StorageRef: siblingPath, EntityID: siblingEntityID,
		WorkflowName: "review", WorkflowVersion: source.WorkflowVersion(), CurrentState: "active", Fields: expected,
		EntityType: "review_entity",
	})); err != nil {
		t.Fatalf("seed later matching sibling: %v", err)
	}
	restarted := fixture.NewCoordinator(PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})
	application, err = restarted.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, owner)
	if err != nil {
		t.Fatalf("prepare committed target after restart and later match: %v", err)
	}
	if application.Route().InstancePath != identity.InstancePath || application.EntityID() != identity.EntityID {
		t.Fatalf("committed target rerouted after restart: route=%#v entity=%q", application.Route(), application.EntityID())
	}

	exact.EntityType = "wrong_entity_type"
	if changed, err := fixture.ConflictingEntityType(ctx, testPipelineRunID, exact.EntityID); err != nil || changed != 1 {
		t.Fatalf("seed exact target conflict: %v", err)
	}
	if _, err := restarted.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, owner); err == nil || !strings.Contains(err.Error(), "field row disagrees with constructed header contract") {
		t.Fatalf("conflicting exact target error = %v", err)
	}
	sibling, exists, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, siblingPath))
	if err != nil || !exists || sibling.EntityID != siblingEntityID || sibling.Fields["account_id"] != "account-1" {
		t.Fatalf("hostile conflict mutated sibling: found=%t err=%v instance=%#v", exists, err, sibling)
	}
}

func VerifyDeliveryTargetApplicationConsumesDeclarationBoundJoinTargetWithoutPayloadSelectorOnBothStoresForTest(t *testing.T, backend string, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	bundle := workflowJoinLifecycleBundle(t)
	handler := bundle.Nodes["join-node"].EventHandlers["item.completed"]

	plan := exactCompiledJoinPlanForTest(bundle, ".")
	source := exactWorkflowJoinSource{
		Source: workflowJoinLifecycleRootAndFlowSource(bundle), plans: []runtimecontracts.WorkflowJoinPlan{plan},
	}
	fixture := open(t, source)
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})
	ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
	if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}

	entityID := eventtest.UUID("join-declaration-application-owner-" + backend)
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: testPipelineRunID, StorageRef: testPipelineRunID, EntityID: entityID,
		WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), CurrentState: "awaiting",
		Fields: map[string]any{"portfolio_id": "portfolio-main"}, EntityType: "test_entity",
	})); err != nil {
		t.Fatalf("seed exact declaration target: %v", err)
	}
	routingSource, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	handle := pipelineJoinHandle(t, "", timeridentity.TimerHandleJoinComplete, testPipelineRunID, testPipelineRunID, entityID)
	payload, err := json.Marshal(handle.PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	evt := eventtest.RuntimeControlWithRoutingSource(
		"join-declaration-application-"+backend, events.EventType(handle.EventType()),
		"runtime.generic_schedule", handle.TaskID(), payload, 0, testPipelineRunID, "",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), routingSource, time.Now().UTC(),
	)
	target := events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: entityID}.Normalized()
	handlerFact, err := NewDeliveryTargetHandler(plan.Node)
	if err != nil {
		t.Fatal(err)
	}
	application, err := pc.prepareDeliveryTargetApplication(
		ctx, plan.Node.Key(), handlerFact.ForEvent("item.completed"), handler, evt, events.MustExistingEntityTarget(target),
	)
	if err != nil {
		t.Fatalf("prepare declaration-bound join target without selector payload: %v", err)
	}
	if !application.Owner().ExistingEntity() || application.Route().InstancePath != testPipelineRunID || application.EntityID() != entityID {
		t.Fatalf("declaration-bound application = owner:%#v route:%#v entity:%q", application.Owner(), application.Route(), application.EntityID())
	}
}

func VerifyDeliveryTargetApplicationRejectsMissingExactExistingTargetWithoutMutationForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := handlerEntityRequirementExecutionSource()
	fixture := open(t, source)
	ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
	if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})
	node := pipelineNode(t, ".", "node-a")
	handlerFact := MustDeliveryTargetHandler(node).ForEvent("work.ready")
	handler := runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}
	target := events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: eventtest.UUID("missing-existing-target")}
	evt := handlerTestRootIngress(uuid.NewString(), "work.ready", "", "", nil, 0, testPipelineRunID, "", events.EventEnvelope{}, time.Now().UTC())
	if _, err := pc.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(target)); !errors.Is(err, runtimeengine.ErrUnconstructedWorkflowTarget) {
		t.Fatalf("missing exact target error = %v", err)
	}
	instances, err := pc.ListWorkflowInstances(ctx, testPipelineRunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(instances) != 0 {
		t.Fatalf("missing exact target materialized state: %#v", instances)
	}
}

func VerifyDeliveryTargetApplicationRejectsWrongRunRootTargetsBeforeMutationOnBothStoresForTest(t *testing.T, backend string, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	const wrongRunID = "88888888-8888-8888-8888-888888888888"
	for _, testCase := range []struct {
		name  string
		owner func(events.RouteIdentity) events.DeliveryTargetOwnership
		seed  string
	}{
		{name: "complete-existing", owner: events.MustExistingEntityTarget, seed: "complete"},
		{name: "state-only-existing", owner: events.MustExistingEntityTarget, seed: "state-only"},
		{name: "materializing", owner: events.MustMaterializingEntityTarget},
		{name: "entityless", owner: func(route events.RouteIdentity) events.DeliveryTargetOwnership {
			route.EntityID = ""
			return events.MustEntitylessReceiverTarget(route)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			source := handlerEntityRequirementExecutionSource()
			fixture := open(t, source)
			newCoordinator := func() *PipelineCoordinator {
				return fixture.NewCoordinator(PipelineCoordinatorOptions{
					Module: staticSemanticWorkflowModule{source: source},
				})
			}
			ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
			if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
				t.Fatal(err)
			}
			entityID := eventtest.UUID("wrong-run-root-" + backend + "-" + testCase.name)
			now := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
			switch testCase.seed {
			case "complete":
				if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
					InstanceID: wrongRunID, StorageRef: wrongRunID, EntityID: entityID,
					WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), Mode: runtimecontracts.FlowModeStatic,
					CurrentState: "active", Fields: map[string]any{"marker": "unchanged"},
					EntityType: "test_entity",
				})); err != nil {
					t.Fatalf("seed complete wrong-run root: %v", err)
				}
			case "state-only":
				if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
					InstanceID: wrongRunID, StorageRef: wrongRunID, EntityID: entityID,
					WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), Mode: runtimecontracts.FlowModeStatic,
					CurrentState: "active", Fields: map[string]any{"marker": "unchanged"}, EntityType: "test_entity",
					EnteredStageAt: now, CreatedAt: now, UpdatedAt: now,
				})); err != nil {
					t.Fatalf("construct wrong-run root component: %v", err)
				}
				if changed, err := fixture.MissingHeader(ctx, testPipelineRunID, wrongRunID); err != nil || changed != 1 {
					t.Fatalf("seed state-only wrong-run root: rows=%d err=%v", changed, err)
				}
			}
			var before WorkflowTargetPersistenceRecord
			if testCase.seed != "" {
				var err error
				before, err = fixture.Persistence.store.LoadTargetPersistence(ctx, testRunScopedWorkflowInstanceFromContext(ctx, wrongRunID), runtimeidentity.NormalizeEntityID(entityID))
				expected := WorkflowTargetPersistenceComplete
				if testCase.seed == "state-only" {
					expected = WorkflowTargetPersistenceStateOnly
				}
				if err != nil || before.Presence != expected {
					t.Fatalf("wrong-run component presence = %v, want %v err=%v", before.Presence, expected, err)
				}
			}

			countRows := func(table string) int {
				t.Helper()
				return int(nativeWorkflowEnginePhysicalCountsForTest(t, fixture, ctx)[table])
			}
			stateBefore, lifecycleBefore := countRows("entity_state"), countRows("flow_instances")
			node := pipelineNode(t, ".", "node-a")
			handlerFact := MustDeliveryTargetHandler(node).ForEvent("work.ready")
			handler := runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}
			evt := handlerTestRootIngress(
				uuid.NewString(), "work.ready", "", "", nil, 0, testPipelineRunID, "",
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: wrongRunID, EntityID: entityID}), now,
			)
			route := events.RouteIdentity{FlowID: ".", FlowInstance: wrongRunID, EntityID: entityID}
			owner := testCase.owner(route)

			for attempt, coordinator := range []*PipelineCoordinator{newCoordinator(), newCoordinator()} {
				if _, err := coordinator.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, owner); err == nil || !strings.Contains(err.Error(), "disagrees with current root coordinate") {
					t.Fatalf("attempt %d wrong-run root error = %v", attempt+1, err)
				}
			}
			if stateAfter, lifecycleAfter := countRows("entity_state"), countRows("flow_instances"); stateAfter != stateBefore || lifecycleAfter != lifecycleBefore {
				t.Fatalf("wrong-run rejection mutated persistence: state %d->%d lifecycle %d->%d", stateBefore, stateAfter, lifecycleBefore, lifecycleAfter)
			}
			if testCase.seed != "" {
				persisted, found, err := fixture.Persistence.store.LoadEntityState(ctx, testRunScopedWorkflowInstanceFromContext(ctx, wrongRunID), runtimeidentity.NormalizeEntityID(entityID))
				marker := string(persisted.Fields)
				if err != nil || !found || !strings.Contains(marker, "unchanged") {
					t.Fatalf("wrong-run rejection changed seeded state: fields=%q err=%v", marker, err)
				}
				// Compare exact snapshots from one reader, not driver-local times against UTC.
				after, err := fixture.Persistence.store.LoadTargetPersistence(ctx, testRunScopedWorkflowInstanceFromContext(ctx, wrongRunID), runtimeidentity.NormalizeEntityID(entityID))
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("wrong-run refusal changed unasserted field facts or clocks: before=%#v after=%#v err=%v", before, after, err)
				}
			}
		})
	}
}

func VerifyDeliveryTargetApplicationRejectsStateOnlyChildRelabeledAsParentOnBothStoresForTest(t *testing.T, backend string, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := deliveryTargetNestedOwnershipSource(t)
	fixture := open(t, source)
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})
	ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
	if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}
	instancePath := "review/child/instance"
	entityID := eventtest.UUID("state-only-child-relabeled-as-parent-" + backend)
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: "instance", StorageRef: instancePath, EntityID: entityID,
		WorkflowName: "review/child", WorkflowVersion: source.WorkflowVersion(),
		CurrentState: "active", EntityType: "review_item", Fields: map[string]any{"marker": "unchanged"},
		EnteredStageAt: now, CreatedAt: now, UpdatedAt: now,
	})); err != nil {
		t.Fatalf("construct native child prestate: %v", err)
	}
	if changed, err := fixture.MissingHeader(ctx, testPipelineRunID, instancePath); err != nil || changed != 1 {
		t.Fatalf("remove exact child lifecycle header: rows=%d err=%v", changed, err)
	}
	childRoute := runtimeflowidentity.StoredRoute("review/child", runtimeflowidentity.LogicalInstanceID(instancePath), instancePath)
	childOwner := testRunScopedWorkflowInstanceForRun(testPipelineRunID, childRoute.InstancePath)
	prestate, err := fixture.Persistence.store.LoadTargetPersistence(ctx, childOwner, runtimeidentity.NormalizeEntityID(entityID))
	if err != nil || prestate.Presence != WorkflowTargetPersistenceStateOnly {
		t.Fatalf("native fault did not produce exact state-only child: presence=%v err=%v", prestate.Presence, err)
	}

	node := pipelineNode(t, "review", "existing")
	handlerFact, err := AdmitDeliveryTargetHandler(source, node)
	if err != nil {
		t.Fatal(err)
	}
	handlerFact = handlerFact.ForEvent("work.ready")
	handler, ok := handlerFact.resolve(source, "work.ready")
	if !ok {
		t.Fatal("resolve parent delivery handler")
	}
	hostile := events.RouteIdentity{FlowID: "review", FlowInstance: instancePath, EntityID: entityID}
	evt := handlerTestRootIngress(
		uuid.NewString(), "work.ready", "", "", nil, 0, testPipelineRunID, "",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, hostile), now,
	)
	if _, err := pc.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(hostile)); !errors.Is(err, runtimeengine.ErrUnconstructedWorkflowTarget) {
		t.Fatalf("state-only child relabeling error = %v", err)
	}

	route := runtimeflowidentity.StoredRoute("review", runtimeflowidentity.LogicalInstanceID(instancePath), instancePath)
	persisted, found, err := fixture.Persistence.store.LoadEntityState(ctx, testRunScopedWorkflowInstanceForRun(testPipelineRunID, route.InstancePath), runtimeidentity.NormalizeEntityID(entityID))
	var fields map[string]any
	fieldsErr := json.Unmarshal(persisted.Fields, &fields)
	if err != nil || !found || persisted.CurrentState != "active" || persisted.Revision != 1 || fieldsErr != nil || fields["marker"] != "unchanged" {
		t.Fatalf("rejected child state changed: found=%t err=%v state=%#v", found, err, persisted)
	}
	after, err := fixture.Persistence.store.LoadTargetPersistence(ctx, childOwner, runtimeidentity.NormalizeEntityID(entityID))
	if err != nil || !reflect.DeepEqual(prestate, after) {
		t.Fatalf("rejected child changed an unasserted field or lifecycle clock: before=%#v after=%#v err=%v", prestate, after, err)
	}
}

func deliveryTargetNestedOwnershipSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadWorkflowTempSource(t, map[string]string{
		"schema.yaml":                "name: nested-delivery-target\n",
		"review/schema.yaml":         "name: review\ninstance: instance_key\nstages:\n  active: {}\n  done: {final: true}\n",
		"review/entities.yaml":       "review_entity:\n  instance_key: text\n",
		"review/events.yaml":         "work.ready:\n",
		"review/nodes.yaml":          "existing:\n  execution_type: system_node\n  subscribes_to: [work.ready]\n  event_handlers:\n    work.ready:\n      advances_to: done\n",
		"review/child/schema.yaml":   "name: child\nstages:\n  active: {}\n  done: {final: true}\n",
		"review/child/entities.yaml": "review_item:\n  marker: text\n",
	})
}

func VerifyDeliveryTargetApplicationRejectsInvalidPersistencePresenceAndLifecycleWithoutMutationForTest(t *testing.T, backend string, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	for _, testCase := range []struct {
		name      string
		wantError string
	}{
		{name: "lifecycle-only", wantError: "workflow_target_unconstructed"},
		{name: "wrong-descriptor", wantError: "lifecycle descriptor conflicts with compiled receiver"},
		{name: "terminated-status", wantError: "lifecycle is not active"},
		{name: "draining-status", wantError: "lifecycle is not active"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			source := handlerEntityRequirementExecutionSource()
			fixture := open(t, source)
			pc := fixture.NewCoordinator(PipelineCoordinatorOptions{
				Module: staticSemanticWorkflowModule{source: source},
			})
			ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
			if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
				t.Fatal(err)
			}
			instancePath := testPipelineRunID
			entityID := eventtest.UUID("invalid-persistence-" + backend + "-" + testCase.name)
			instance := materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: runtimeflowidentity.LogicalInstanceID(instancePath), StorageRef: instancePath, EntityID: entityID,
				WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), Mode: "static",
				CurrentState: "active", Fields: map[string]any{"marker": "unchanged"}, EntityType: "test_entity",
			})
			switch testCase.name {
			case "lifecycle-only":
				instance.Fields = map[string]any{}
			case "wrong-descriptor":
				instance.WorkflowName, instance.Mode = "other-flow", "template"
			}
			if err := fixture.Construct(ctx, instance); err != nil {
				t.Fatalf("construct %s native prestate: %v", testCase.name, err)
			}
			var changed int64
			var err error
			switch testCase.name {
			case "lifecycle-only":
				changed, err = fixture.MissingFields(ctx, testPipelineRunID, entityID)
			case "terminated-status":
				changed, err = fixture.Terminated(ctx, testPipelineRunID, instancePath, time.Now().UTC())
			case "draining-status":
				changed, err = fixture.Draining(ctx, testPipelineRunID, instancePath)
			}
			if testCase.name != "wrong-descriptor" && (err != nil || changed != 1) {
				t.Fatalf("exact %s native fault: rows=%d err=%v", testCase.name, changed, err)
			}
			stateOwner := testRunScopedWorkflowInstanceFromContext(ctx, instancePath)
			before, err := fixture.Persistence.store.LoadTargetPersistence(ctx, stateOwner, runtimeidentity.NormalizeEntityID(entityID))
			expectedPresence := WorkflowTargetPersistenceComplete
			if testCase.name == "lifecycle-only" {
				expectedPresence = WorkflowTargetPersistenceLifecycleOnly
			}
			if err != nil || before.Presence != expectedPresence {
				t.Fatalf("hostile prestate presence=%v want=%v err=%v", before.Presence, expectedPresence, err)
			}
			switch testCase.name {
			case "wrong-descriptor":
				if before.Lifecycle.WorkflowName != "other-flow" || before.Lifecycle.Mode != "template" {
					t.Fatal("wrong descriptor was not persisted")
				}
			case "terminated-status":
				if before.Lifecycle.Status != "terminated" || before.Lifecycle.TerminatedAt.IsZero() {
					t.Fatal("terminated status/time was not persisted")
				}
			case "draining-status":
				if before.Lifecycle.Status != "draining" || !before.Lifecycle.TerminatedAt.IsZero() {
					t.Fatal("draining status/NULL termination time was not persisted")
				}
			}

			node := pipelineNode(t, ".", "node-a")
			handlerFact := MustDeliveryTargetHandler(node).ForEvent("work.ready")
			handler := runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}
			target := events.RouteIdentity{FlowID: ".", FlowInstance: instancePath, EntityID: entityID}
			evt := handlerTestRootIngress(uuid.NewString(), "work.ready", "", "", nil, 0, testPipelineRunID, "", events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Now().UTC())
			if _, err := pc.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(target)); err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("invalid %s persistence error = %v, want %q", testCase.name, err, testCase.wantError)
			}

			after, err := fixture.Persistence.store.LoadTargetPersistence(ctx, stateOwner, runtimeidentity.NormalizeEntityID(entityID))
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("invalid lifecycle rejection changed exact persistence: before=%+v after=%+v err=%v", before, after, err)
			}
			if testCase.name == "lifecycle-only" {
				_, found, err := fixture.Persistence.store.LoadEntityState(ctx, stateOwner, runtimeidentity.NormalizeEntityID(entityID))
				if err != nil || found {
					t.Fatalf("lifecycle-only rejection state rows = %t, err=%v", found, err)
				}
				return
			}
			persisted, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instancePath))
			if err != nil || !found || persisted.Revision != 1 || persisted.Fields["marker"] != "unchanged" {
				t.Fatalf("invalid lifecycle rejection mutated target: found=%t err=%v instance=%#v", found, err, persisted)
			}
		})
	}
}

func VerifyNonActiveDeliveryTargetRejectsDelayedAndReplayedExecutionBeforeMutationOnBothStoresForTest(t *testing.T, backend string, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := handlerEntityRequirementExecutionSource()
	fixture := open(t, source)
	newCoordinator := func() *PipelineCoordinator {
		return fixture.NewCoordinator(PipelineCoordinatorOptions{
			Module: staticSemanticWorkflowModule{source: source},
		})
	}
	first := newCoordinator()
	ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
	if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}
	instancePath := testPipelineRunID
	entityID := eventtest.UUID("non-active-delivery-target-" + backend)
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: instancePath, StorageRef: instancePath, EntityID: entityID,
		WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), Mode: "static", CurrentState: "active",
		Fields: map[string]any{"marker": "unchanged"}, EntityType: "test_entity",
	})); err != nil {
		t.Fatalf("seed delayed target: %v", err)
	}
	if changed, err := fixture.Draining(ctx, testPipelineRunID, instancePath); err != nil || changed != 1 {
		t.Fatalf("drain delayed target: rows=%d err=%v", changed, err)
	}
	evt := eventtest.ExistingRunRootIngressWithRoutingSource(
		uuid.NewString(), "work.ready", "", "", json.RawMessage(`{"item_id":"a"}`), 0,
		testPipelineRunID, handlerTestWorkflowEnvelope(".", instancePath, entityID), testWorkflowRoutingSource(".", instancePath, entityID), time.Now().UTC(),
	)
	node := pipelineNode(t, ".", "node-a")
	route := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(node),
		Target:    events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: instancePath, EntityID: entityID}),
	}
	fixture.Publish(ctx, evt, route)
	deliveryID, err := deliverylifecycle.DeliveryID(evt.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := first.deliveryStore.Snapshot(ctx, deliveryID)
	if err != nil || pending.Status != deliverylifecycle.StatusPending {
		t.Fatalf("native pending occurrence not established: %+v err=%v", pending, err)
	}
	readStorage := func() json.RawMessage {
		value, err := fixture.ApplicationStorage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	before := readStorage()
	countsBefore := nativeWorkflowEnginePhysicalCountsForTest(t, fixture, ctx)
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"},
		Emit:       runtimecontracts.EmitSpec{Event: "work.emitted"},
	}
	execute := func(label string, coordinator *PipelineCoordinator) {
		t.Helper()
		deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, route)
		_, err := coordinator.executeNodeContractHandler(deliveryCtx, node, handler, workflowTriggerContext{Event: evt, HandlerEventKey: "work.ready"}, false)
		if err == nil || !strings.Contains(err.Error(), "lifecycle is not active") {
			t.Fatalf("%s non-active target error = %v, want fail-closed status conflict", label, err)
		}
		if !bytes.Equal(before, readStorage()) {
			t.Fatalf("%s non-active target persisted an effect", label)
		}
	}
	execute("delayed", first)
	execute("replayed after coordinator reconstruction", newCoordinator())

	persisted, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instancePath))
	if err != nil || !found || persisted.Revision != 1 || persisted.Fields["marker"] != "unchanged" || persisted.Status != "draining" {
		t.Fatalf("non-active replay changed target: found=%t err=%v instance=%#v", found, err, persisted)
	}
	if after := nativeWorkflowEnginePhysicalCountsForTest(t, fixture, ctx); after["entity_mutations"] != countsBefore["entity_mutations"] {
		t.Fatalf("non-active replay added mutation rows: before=%v after=%v", countsBefore, after)
	}
	delivery, err := first.deliveryStore.Snapshot(ctx, deliveryID)
	if err != nil || delivery.Status != deliverylifecycle.StatusPending || !reflect.DeepEqual(pending, delivery) {
		t.Fatalf("non-active replay changed exact pending occurrence: before=%+v after=%+v err=%v", pending, delivery, err)
	}
}

func VerifyDeliveryTargetApplicationCarriesConstructedScenarioPreStateThroughMutationOnSQLiteAndPostgresForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := handlerEntityRequirementExecutionSource()
	fixture := open(t, source)
	ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
	if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})

	instancePath := testPipelineRunID
	entityID := testPipelineRunID
	occurredAt := time.Date(2026, time.January, 4, 12, 0, 0, 0, time.UTC)
	// Explicit private component setup supplies both constructor facts and
	// declared scenario state. Imported fields alone remain non-runnable.
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: instancePath, StorageRef: instancePath, EntityID: entityID,
		WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), Mode: "static", EntityType: "test_entity",
		CurrentState: "active", Fields: map[string]any{"marker": "preserved"}, Gates: map[string]bool{"./approved": true},
		CreatedAt: occurredAt, EnteredStageAt: occurredAt,
	})); err != nil {
		t.Fatalf("seed exact constructed scenario pre-state: %v", err)
	}

	evt := eventtest.ExistingRunRootIngressWithRoutingSource(
		uuid.NewString(), "work.ready", "", "", json.RawMessage(`{"item_id":"a"}`), 0,
		testPipelineRunID, events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: instancePath, EntityID: entityID}), testWorkflowRoutingSource(".", instancePath, entityID), occurredAt.Add(time.Minute),
	)
	node := pipelineNode(t, ".", "node-a")
	target := events.RouteIdentity{FlowID: ".", FlowInstance: instancePath, EntityID: entityID}
	route := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(node),
		Target:    events.MustExistingEntityTarget(target),
	}
	fixture.Publish(ctx, evt, route)
	evt = eventtest.TargetRouted(evt, target)
	deliveryCtx, stopHeartbeat := claimNativeWorkflowHandlerPublicationForTest(t, pc, ctx, evt, route)
	defer func() {
		if err := stopHeartbeat(); err != nil {
			t.Errorf("native scenario heartbeat cleanup: %v", err)
		}
	}()
	result, err := pc.executeNodeContractHandler(deliveryCtx, node, runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"},
	}, workflowTriggerContext{Event: evt}, false)
	if err != nil {
		t.Fatalf("execute exact entity-only pre-state: %v", err)
	}
	if !result.Handled {
		t.Fatal("entity-only pre-state handler was not handled")
	}
	instance, exists, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instancePath))
	if err != nil || !exists {
		t.Fatalf("load first-mutation workflow instance: found=%t err=%v", exists, err)
	}
	if instance.EntityID != entityID || instance.Fields["marker"] != "preserved" || !instance.Gates["./approved"] || instance.Revision != 2 || !instance.CreatedAt.Equal(occurredAt) || !instance.EnteredStageAt.Equal(occurredAt) {
		t.Fatalf("first-mutation state = %#v, want exact preserved pre-state at revision 2", instance)
	}
}

func VerifyDeliveryTargetApplicationReloadsCurrentScopedStateOnSQLiteAndPostgresForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := handlerEntityRequirementExecutionSource()
	fixture := open(t, source)
	ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
	if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})

	instancePath := testPipelineRunID
	entityID := eventtest.UUID("scoped-gates-existing-target")
	persisted := materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: "scoped-gates", StorageRef: instancePath, EntityID: entityID,
		WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), Mode: "static", CurrentState: "active",
		Fields: map[string]any{"marker": "durable"}, Gates: map[string]bool{"./approved": true},
		EntityType: "test_entity",
	})
	if err := fixture.Construct(ctx, persisted); err != nil {
		t.Fatalf("seed exact scoped-gate target: %v", err)
	}

	node := pipelineNode(t, ".", "node-a")
	handlerFact := MustDeliveryTargetHandler(node).ForEvent("work.ready")
	handler := runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}
	evt := handlerTestRootIngress(
		uuid.NewString(), "work.ready", "", "", nil, 0, testPipelineRunID, "",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: instancePath, EntityID: entityID}), time.Now().UTC(),
	)
	target := events.RouteIdentity{FlowID: ".", FlowInstance: instancePath, EntityID: entityID}
	application, err := pc.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(target))
	if err != nil {
		t.Fatalf("prepare scoped-gate application: %v", err)
	}
	persisted.Fields["marker"] = "current"
	if err := fixture.Persistence.store.mutateE(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instancePath), func(current *WorkflowInstance) error {
		current.Fields["marker"] = persisted.Fields["marker"]
		return nil
	}); err != nil {
		t.Fatalf("advance target after application preparation: %v", err)
	}

	executionCtx := withDeliveryTargetApplication(ctx, application)
	stateRepo := pipelineEngineStateRepo{coordinator: pc}
	address := testEngineStateAddress(".", instancePath, entityID)
	snapshot, exists, err := stateRepo.LoadState(executionCtx, address)
	if err != nil || !exists {
		t.Fatalf("load execution snapshot: exists=%t err=%v", exists, err)
	}
	if !snapshot.StateCarrier.Gates["approved"] || !snapshot.StateCarrier.Gates["./approved"] || snapshot.StateCarrier.Fields["marker"] != "current" {
		t.Fatalf("execution gates = %#v, want local and qualified facts", snapshot.StateCarrier.Gates)
	}
	snapshot.StateCarrier.Gates["approved"] = false
	snapshot.StateCarrier.Fields["marker"] = "mutated"

	fresh, exists, err := stateRepo.LoadState(executionCtx, address)
	if err != nil || !exists || !fresh.StateCarrier.Gates["approved"] || fresh.StateCarrier.Fields["marker"] != "current" {
		t.Fatalf("fresh immutable snapshot = %#v exists=%t err=%v", fresh, exists, err)
	}
	stored, exists, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instancePath))
	if err != nil || !exists || stored.Gates["approved"] || !stored.Gates["./approved"] || stored.Fields["marker"] != "current" {
		t.Fatalf("durable state changed by execution projection: exists=%t err=%v instance=%#v", exists, err, stored)
	}
}

func TestDeliveryTargetApplicationRequiresExactPreviewOwnerAndRejectsConflict(t *testing.T) {
	source := handlerEntityRequirementExecutionSource()
	pc := &PipelineCoordinator{module: staticSemanticWorkflowModule{source: source}}
	node := pipelineNode(t, ".", "node-a")
	handlerFact := MustDeliveryTargetHandler(node).ForEvent("work.ready")
	handler := runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}
	entityID := eventtest.UUID("preview-exact-target")
	target := events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: entityID}
	evt := handlerTestRootIngress(
		uuid.NewString(), "work.ready", "", "", nil, 0, testPipelineRunID, "",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Now().UTC(),
	)

	_, err := pc.prepareDeliveryTargetApplication(
		context.Background(), node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(target),
		WorkflowState{Stage: "active", Metadata: map[string]any{"marker": "preview"}},
	)
	if err == nil || !strings.Contains(err.Error(), "entity disagrees with admitted owner") {
		t.Fatalf("empty preview became execution identity: %v", err)
	}
	preview := WorkflowState{EntityID: entityID, Stage: "active", Metadata: map[string]any{"marker": "preview"}, Control: runtimeengine.StateControl{
		EntityType: "test_entity", FlowPath: target.FlowInstance, StorageRef: target.FlowInstance, InstanceID: target.FlowInstance,
	}}
	application, err := pc.prepareDeliveryTargetApplication(context.Background(), node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(target), preview)
	if err != nil {
		t.Fatalf("exact preview owner refused: %v", err)
	}
	if got := application.State(); got.EntityID != entityID || got.Control.FlowPath != target.FlowInstance || got.Metadata["marker"] != "preview" {
		t.Fatalf("projected preview state = %#v", got)
	}

	preview.EntityID = eventtest.UUID("conflicting-preview-target")
	_, err = pc.prepareDeliveryTargetApplication(
		context.Background(), node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(target),
		preview,
	)
	if err == nil || !strings.Contains(err.Error(), "entity disagrees with admitted owner") {
		t.Fatalf("conflicting preview error = %v", err)
	}
}
