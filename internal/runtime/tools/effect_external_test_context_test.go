package tools_test

import (
	"context"
	"strings"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestBundleSourceFact = runtimecorrelation.BundleSourceFact{
	BundleHash:        "bundle-v1:sha256:" + strings.Repeat("a", 64),
	BundleSource:      "ephemeral",
	BundleFingerprint: "sha256:" + strings.Repeat("a", 64),
}

func unmanagedToolTestContext() context.Context {
	ctx := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestBundleSourceFact.BundleHash,
	))
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, authorActivityTestBundleSourceFact)
	return runtimeeffects.WithDifferentOwner(ctx, runtimeeffects.OwnerBuildTestInfrastructure)
}
