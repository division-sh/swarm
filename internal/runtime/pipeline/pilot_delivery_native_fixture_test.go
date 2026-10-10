package pipeline

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func nativePilotPipelineForTest(t *testing.T, backend string, bundle *contracts.WorkflowContractBundle, open pipelineDeliveryNativeOpenerForTest) (*PipelineDeliveryNativeFixtureForTest, *PipelineCoordinator, context.Context) {
	t.Helper()
	source := semanticview.Wrap(bundle)
	fixture := open(t, backend, source)
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		t.Fatal(err)
	}
	module := &previewWorkflowModule{bundle: bundle, workflowNodes: nodes, guardRegistry: NewContractGuardRegistry(source)}
	pc, ctx := nativePipelineDeliveryCoordinatorForTest(t, fixture, module)
	run := uuid.NewString()
	ctx = correlation.WithRunID(ctx, run)
	if err := fixture.RequireRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	return fixture, pc, ctx
}
