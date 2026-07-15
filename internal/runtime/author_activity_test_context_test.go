package runtime

import (
	"context"
	"strings"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

func testAuthorActivityContext(ctx context.Context) context.Context {
	return testAuthorActivityContextForBundle(ctx, "bundle-v1:sha256:"+strings.Repeat("a", 64))
}

func testAuthorActivityContextForBundle(ctx context.Context, bundleHash string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		strings.TrimSpace(bundleHash),
	))
}

func newScopedTestRuntime(ctx context.Context, deps RuntimeDeps) (*Runtime, error) {
	if strings.TrimSpace(deps.Options.RuntimeInstanceID) == "" {
		deps.Options.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if strings.TrimSpace(deps.Options.BundleSourceFact.BundleHash) == "" {
		deps.Options.BundleSourceFact = runtimecorrelation.BundleSourceFact{
			BundleHash:        "bundle-v1:sha256:" + strings.Repeat("a", 64),
			BundleSource:      "ephemeral",
			BundleFingerprint: "sha256:" + strings.Repeat("a", 64),
		}
	}
	return NewRuntime(ctx, deps)
}
