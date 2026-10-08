package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
)

// These explicit component inputs exercise lifecycle persistence, not public
// constructor eligibility or parent-driven eager construction.
func commitA2FixtureConstruction(t *testing.T, coordinator *pipeline.PipelineCoordinator, selected any, ctx context.Context, owner flowidentity.RunScopedFlowInstance, initial pipeline.WorkflowInstance, at time.Time) pipeline.FlowInstanceActivationPlan {
	t.Helper()
	source := coordinator.SemanticSource()
	if initial.WorkflowName != semanticview.RootExecutionFlowID(source) && initial.ParentFlowID == "" && initial.ParentFlowInstance == "" && initial.ParentEntityID == "" {
		bundle, found := semanticview.Bundle(source)
		if !found {
			t.Fatal("component construction requires its admitted flow tree")
		}
		view, found := bundle.FlowViewByID(initial.WorkflowName)
		if !found || view.Parent == nil {
			t.Fatal("component construction requires its explicit child declaration")
		}
		parent, err := flowidentity.StandingForGeneration(source, view.Parent.Paths.FlowPath, owner.RunID)
		if err != nil {
			t.Fatal(err)
		}
		var constructed flowidentity.Instance
		if view.Schema.Instance.Empty() {
			constructed, err = flowidentity.KeylessChild(source, parent, initial.WorkflowName)
		} else {
			constructed, err = flowidentity.KeyedChild(source, parent, initial.WorkflowName, owner.Route.InstanceID)
		}
		if err != nil || constructed.InstancePath != initial.StorageRef {
			t.Fatalf("component constructor path: constructed=%+v err=%v", constructed, err)
		}
		initial.ParentFlowID, initial.ParentFlowInstance, initial.ParentEntityID = constructed.ParentRoute.FlowID, constructed.ParentRoute.FlowInstance, constructed.ParentEntityID
	}
	initialized, lifecycle, err := coordinator.PrepareInitialEntryLifecycle(ctx, owner, initial, at)
	if err != nil {
		t.Fatalf("prepare component lifecycle: %v", err)
	}
	command, err := flowactivationfixture.Command(ctx, initialized, lifecycle, at)
	if err != nil {
		t.Fatalf("assemble component construction: %v", err)
	}
	committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged {
		t.Fatalf("commit component construction: acknowledged=%v err=%v", committed.Acknowledged, err)
	}
	if committed.Created {
		if err := coordinator.FinalizeInitialEntryLifecycle(ctx, committed.Lifecycle); err != nil {
			t.Fatalf("finalize acknowledged component lifecycle: %v", err)
		}
	}
	return command.Plan
}
