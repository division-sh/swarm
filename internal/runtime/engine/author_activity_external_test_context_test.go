package engine_test

import (
	"context"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestSourceArtifactFact = mustAuthorActivityTestSourceArtifactFact()

func mustAuthorActivityTestSourceArtifactFact() runtimecorrelation.SourceArtifactFact {
	return sourceartifactfixture.Fact()
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestSourceArtifactFact.BundleHash(),
	))
	return runtimecorrelation.WithSourceArtifactFact(ctx, authorActivityTestSourceArtifactFact)
}
