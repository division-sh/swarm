package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

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

func materializedWorkflowInstanceForSource(t testing.TB, source semanticview.Source, ctx context.Context, instance WorkflowInstance) WorkflowInstance {
	t.Helper()
	instance = materializedWorkflowInstanceForTest(instance)
	runID := correlation.RunIDFromContext(ctx)
	if instance.WorkflowName == semanticview.RootExecutionFlowID(source) {
		if instance.StorageRef != runID || instance.EntityID != flowidentity.EntityID(runID) {
			t.Fatal("root component fixture requires its exact run coordinate")
		}
		return instance
	}
	bundle, found := semanticview.Bundle(source)
	if !found {
		t.Fatal("component fixture requires its admitted flow tree")
	}
	view, found := bundle.FlowViewByID(instance.WorkflowName)
	if !found || view.Parent == nil {
		t.Fatal("component fixture requires its exact child declaration")
	}
	parent, err := flowidentity.StandingForGeneration(source, view.Parent.Paths.FlowPath, runID)
	if err != nil {
		t.Fatal(err)
	}
	var constructed flowidentity.Instance
	if view.Schema.Instance.Empty() {
		constructed, err = flowidentity.KeylessChild(source, parent, instance.WorkflowName)
	} else {
		constructed, err = flowidentity.KeyedChild(source, parent, instance.WorkflowName, instance.InstanceID)
	}
	if err != nil || constructed.InstancePath != instance.StorageRef {
		t.Fatalf("component fixture constructor path: constructed=%+v err=%v", constructed, err)
	}
	instance.ParentFlowID = constructed.ParentRoute.FlowID
	instance.ParentFlowInstance = constructed.ParentRoute.FlowInstance
	instance.ParentEntityID = constructed.ParentEntityID
	return instance
}

func constructedScenarioInstanceForTest(t *testing.T, source semanticview.Source, ctx context.Context, flowID string) WorkflowInstance {
	t.Helper()
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
		t.Fatal("scenario constructor requires its compiled stage topology")
	}
	initial, err := graph.InitialStoredStage()
	if err != nil {
		t.Fatal(err)
	}
	runID := correlation.RunIDFromContext(ctx)
	constructed := constructorUnitIdentity(t, source, runID, flowID)
	contract, _ := entityruntime.ResolveForFlow(source, flowID)
	at := time.Now().UTC()
	return WorkflowInstance{
		InstanceID: constructed.InstanceID, StorageRef: constructed.InstancePath, EntityID: constructed.EntityID,
		ParentFlowID: constructed.ParentRoute.FlowID, ParentFlowInstance: constructed.ParentRoute.FlowInstance, ParentEntityID: constructed.ParentEntityID,
		WorkflowName: flowID, WorkflowVersion: source.WorkflowVersion(), Mode: "static",
		EntityType: contract.EntityType, Fields: fields, InitialFieldValues: cloneStringAnyMap(fields),
		CurrentState: initial.ID(), StageDefined: graph.StageCount() != 0, CreatedAt: at, EnteredStageAt: at,
	}
}
