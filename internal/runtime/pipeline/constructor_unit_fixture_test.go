package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func newConstructorHandlerUnitCoordinator(t *testing.T, module WorkflowModule) (*PipelineCoordinator, *recordingPipelineBus, context.Context) {
	t.Helper()
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus: bus, module: module,
		workflowStore:  newSQLiteWorkflowInstanceStoreForTest(t, newSQLiteWorkflowInstanceStoreTestDB(t)),
		expressionEval: newWorkflowExpressionEvaluator(), entityLocks: map[string]*sync.Mutex{},
	}
	configureWorkflowLifecycleForTest(t, pc)
	configurePipelineTestDeliveryOwner(t, pc)
	return pc, bus, sqliteExactOnceRunContext(t, pc.workflowStore.testDB())
}

// Explicit unit setup consumes constructor values; this private fixture does
// not qualify selected-store construction receipts or physical attachment.
func constructorUnitIdentity(t *testing.T, source semanticview.Source, runID, flowID string) flowidentity.Instance {
	t.Helper()
	if flowID == semanticview.RootExecutionFlowID(source) {
		return flowidentity.Stored(source, flowID, runID, runID, runID, "")
	}
	bundle, found := semanticview.Bundle(source)
	if !found {
		t.Fatal("unit construction requires the authored flow tree")
	}
	view, found := bundle.FlowViewByID(flowID)
	if !found || view.Parent == nil {
		t.Fatalf("unit construction requires the exact parent of %s", flowID)
	}
	parent := constructorUnitIdentity(t, source, runID, view.Parent.Paths.FlowPath)
	child, err := flowidentity.KeylessChild(source, parent, flowID)
	if err != nil {
		t.Fatal(err)
	}
	return child
}

func seedConstructorUnitInstance(t *testing.T, pc *PipelineCoordinator, ctx context.Context, flowID string) WorkflowInstance {
	t.Helper()
	source := pc.SemanticSource()
	constructor, err := CompileFlowConstructor(source, flowID, "")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := constructor.InitialFields(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	graph, found := semanticview.WorkflowStageTopology(source, flowID)
	if !found {
		t.Fatal("unit constructor requires its compiled stage topology")
	}
	initial, err := graph.InitialStoredStage()
	if err != nil {
		t.Fatal(err)
	}
	runID := correlation.RunIDFromContext(ctx)
	constructed := constructorUnitIdentity(t, source, runID, flowID)
	contract, _ := entityruntime.ResolveForFlow(source, flowID)
	at := time.Now().UTC()
	instance := WorkflowInstance{
		InstanceID: constructed.InstanceID, StorageRef: constructed.InstancePath, EntityID: constructed.EntityID,
		ParentFlowID: constructed.ParentRoute.FlowID, ParentFlowInstance: constructed.ParentRoute.FlowInstance, ParentEntityID: constructed.ParentEntityID,
		WorkflowName: flowID, WorkflowVersion: source.WorkflowVersion(), Mode: "static",
		EntityType: contract.EntityType, Fields: fields, InitialFieldValues: cloneStringAnyMap(fields),
		CurrentState: initial.ID(), StageDefined: graph.StageCount() != 0, CreatedAt: at, EnteredStageAt: at,
	}
	if err := pc.workflowStore.create(ctx, instance); err != nil {
		t.Fatalf("seed constructor-derived unit instance: %v", err)
	}
	return instance
}

func prepareConstructorUnitDelivery(t *testing.T, pc *PipelineCoordinator, ctx context.Context, flowID, nodeID string, event events.Event) (context.Context, WorkflowState) {
	t.Helper()
	seedExactOnceEvent(t, pc.workflowStore, ctx, event)
	instance := seedConstructorUnitInstance(t, pc, correlation.WithInboundEvent(ctx, event), flowID)
	ctx = withClaimedWorkflowNodePublicationForTest(t, pc, ctx, event, events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(pipelineSourceNode(t, pc.SemanticSource(), flowID, nodeID)),
		Target: events.MustExistingEntityTarget(events.RouteIdentity{
			FlowID: flowID, FlowInstance: instance.StorageRef, EntityID: instance.EntityID,
		}),
	})
	return ctx, mustCurrentWorkflowState(t, pc, ctx, flowidentity.StoredRoute(flowID, instance.InstanceID, instance.StorageRef), instance.EntityID)
}
