package tools_test

import (
	"context"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"
const authorActivityTestBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const authorActivityTestBundleSource = "ephemeral"

var authorActivityTestBundleSourceFact = mustAuthorActivityTestBundleSourceFact()

func mustAuthorActivityTestBundleSourceFact() runtimecorrelation.BundleSourceFact {
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(authorActivityTestBundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}

func unmanagedToolTestContext() context.Context {
	ctx := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestBundleSourceFact.BundleHash(),
	))
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, authorActivityTestBundleSourceFact)
	return runtimeeffects.WithDifferentOwner(ctx, runtimeeffects.OwnerBuildTestInfrastructure)
}
