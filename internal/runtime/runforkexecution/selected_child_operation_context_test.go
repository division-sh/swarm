package runforkexecution

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func selectedForkParentRuntimeContext(t *testing.T, ctx context.Context, selected startupownership.Store, source correlation.SourceArtifactFact) (context.Context, string) {
	t.Helper()
	process := selectedContractTestProcessCapability(t, ctx, selected)
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: source.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatal(err)
	}
	processEvidence, err := process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{
		BundleHash: source.BundleHash(), RuntimeInstanceID: processEvidence.RuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := grant.Retire(context.WithoutCancel(ctx)); err != nil {
			t.Error(err)
		}
	})
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	evidence, err := grant.AdmitExecution(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ctx = correlation.WithSourceArtifactFact(ctx, source)
	ctx = correlation.WithRuntimeInstanceID(ctx, evidence.RuntimeInstanceID)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(evidence.RuntimeInstanceID, evidence.BundleHash))
	ctx, err = startupownership.WithRuntimeMutationAuthority(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		original, err := startupownership.CaptureMutationAuthority(ctx)
		if err != nil || original.GrantEvidence().GrantID != evidence.GrantID {
			t.Errorf("selected child changed its parent original: %v", err)
		}
		if err := grant.ProveCurrent(ctx); err != nil {
			t.Errorf("selected child retired its parent grant: %v", err)
		}
	})
	return ctx, evidence.GrantID
}
