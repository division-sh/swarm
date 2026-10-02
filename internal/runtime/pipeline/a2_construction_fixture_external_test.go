package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
)

// These explicit component inputs exercise lifecycle persistence, not public
// constructor eligibility or parent-driven eager construction.
func commitA2FixtureConstruction(t *testing.T, coordinator *pipeline.PipelineCoordinator, selected any, ctx context.Context, owner flowidentity.RunScopedFlowInstance, initial pipeline.WorkflowInstance, at time.Time) pipeline.FlowInstanceActivationPlan {
	t.Helper()
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
