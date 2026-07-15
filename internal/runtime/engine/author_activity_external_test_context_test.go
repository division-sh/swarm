package engine_test

import (
	"context"
	"strings"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestBundleSourceFact = runtimecorrelation.BundleSourceFact{
	BundleHash:        "bundle-v1:sha256:" + strings.Repeat("a", 64),
	BundleSource:      "ephemeral",
	BundleFingerprint: "sha256:" + strings.Repeat("a", 64),
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestBundleSourceFact.BundleHash,
	))
	return runtimecorrelation.WithBundleSourceFact(ctx, authorActivityTestBundleSourceFact)
}
