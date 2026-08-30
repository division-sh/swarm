package tools_test

import (
	"context"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"
const authorActivityTestBundleHash = sourceartifactfixture.BundleHash
const authorActivityTestSourceArtifact = "ephemeral"

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
