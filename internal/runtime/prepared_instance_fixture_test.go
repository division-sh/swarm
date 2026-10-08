package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
)

// This native component fixture carries prepared instance data, not proof of
// public constructor eligibility or installed runtime attachment.
func seedRuntimeTestPreparedInstance(t testing.TB, ctx context.Context, selected bus.FlowInstanceActivationCommitOwner, pc *pipeline.PipelineCoordinator, instance pipeline.WorkflowInstance) pipeline.CommittedFlowInstanceActivation {
	t.Helper()
	ctx = testLiveExecutionContext(ctx)
	source := pc.SemanticSource()
	runID := correlation.RunIDFromContext(ctx)
	if instance.WorkflowName != semanticview.RootExecutionFlowID(source) && instance.ParentFlowID == "" && instance.ParentFlowInstance == "" && instance.ParentEntityID == "" {
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
		instance.ParentFlowID, instance.ParentFlowInstance, instance.ParentEntityID = constructed.ParentRoute.FlowID, constructed.ParentRoute.FlowInstance, constructed.ParentEntityID
	}
	at := instance.CreatedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	initialized, lifecycle, err := pc.PrepareInitialEntryLifecycle(ctx, flowidentity.RunScopedFlowInstance{
		RunID: correlation.RunIDFromContext(ctx), Route: flowidentity.Stored(nil, instance.WorkflowName, instance.StorageRef, instance.InstanceID, instance.EntityID, instance.ParentEntityID).Route(),
	}, instance, at)
	if err != nil {
		t.Fatalf("prepare component initial lifecycle: %v", err)
	}
	command, err := flowactivationfixture.Command(ctx, initialized, lifecycle, at)
	if err != nil {
		t.Fatalf("prepare component construction command: %v", err)
	}
	committed, err := selected.CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged {
		t.Fatalf("commit component instance: %+v %v", committed, err)
	}
	if err := committed.Validate(); err != nil {
		t.Fatalf("validate component commit: %v", err)
	}
	if committed.Created {
		if err := pc.FinalizeInitialEntryLifecycle(ctx, committed.Lifecycle); err != nil {
			t.Fatalf("dispatch component initial lifecycle: %v", err)
		}
	}
	return committed
}

func seedRuntimeTestKeylessSource(t testing.TB, ctx context.Context, selected bus.FlowInstanceActivationCommitOwner, pc *pipeline.PipelineCoordinator, flowID string) flowidentity.Instance {
	t.Helper()
	source := pc.SemanticSource()
	runID := correlation.RunIDFromContext(ctx)
	var result flowidentity.Instance
	for _, id := range []string{semanticview.RootExecutionFlowID(source), flowID} {
		instance, err := flowidentity.StandingForGeneration(source, id, runID)
		if err != nil {
			t.Fatal(err)
		}
		schema, found := source.FlowSchemaByID(id)
		if !found || !schema.Instance.Empty() {
			t.Fatal("source fixture requires its exact keyless declaration")
		}
		topology, found := semanticview.WorkflowStageTopology(source, id)
		if !found {
			t.Fatal("source fixture requires its compiled stage catalog")
		}
		initial, err := topology.InitialStoredStage()
		if err != nil {
			t.Fatal(err)
		}
		committed := seedRuntimeTestPreparedInstance(t, ctx, selected, pc, pipeline.WorkflowInstance{
			WorkflowName: id, WorkflowVersion: source.WorkflowVersion(), StorageRef: instance.InstancePath,
			InstanceID: instance.InstanceID, EntityID: instance.EntityID,
			ParentFlowID: instance.ParentRoute.FlowID, ParentFlowInstance: instance.ParentRoute.FlowInstance, ParentEntityID: instance.ParentEntityID,
			CurrentState: initial.ID(), StageDefined: !initial.IsStatelessPosture(), Fields: map[string]any{}, EntityType: "test_entity",
		})
		result = committed.Plan.Identity
	}
	return result
}
