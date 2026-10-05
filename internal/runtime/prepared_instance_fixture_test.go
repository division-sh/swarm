package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
)

// This native component fixture carries prepared instance data, not proof of
// public constructor eligibility or installed runtime attachment.
func seedRuntimeTestPreparedInstance(t testing.TB, ctx context.Context, selected bus.FlowInstanceActivationCommitOwner, pc *pipeline.PipelineCoordinator, instance pipeline.WorkflowInstance) {
	t.Helper()
	ctx = testLiveExecutionContext(ctx)
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
}
