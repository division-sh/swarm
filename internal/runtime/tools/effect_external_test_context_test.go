package tools_test

import (
	"context"
	"testing"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"
const authorActivityTestBundleHash = sourceartifactfixture.BundleHash

var authorActivityTestSourceArtifactFact = mustAuthorActivityTestSourceArtifactFact()

func mustAuthorActivityTestSourceArtifactFact() runtimecorrelation.SourceArtifactFact {
	fact, err := runtimecorrelation.NewSourceArtifactFact(authorActivityTestBundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}

func unmanagedToolTestContext() context.Context {
	ctx := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestSourceArtifactFact.BundleHash(),
	))
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, authorActivityTestSourceArtifactFact)
	return runtimeeffects.WithDifferentOwner(ctx, runtimeeffects.OwnerBuildTestInfrastructure)
}

func toolFixtureProcessContext(t testing.TB, ctx context.Context, selected any) context.Context {
	t.Helper()
	if startupownership.HasMutationAuthority(ctx) {
		return ctx
	}
	owner, ok := selected.(agentfixture.Store)
	if !ok {
		t.Fatalf("tool fixture %T has no native process owner", selected)
	}
	process, err := agentfixture.ProcessCapability(t, ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = startupownership.WithProcessMutationAuthority(ctx, process)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}
