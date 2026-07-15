package runforkexecution

import (
	"context"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
)

const runForkTestRuntimeInstanceID = "22222222-2222-4222-8222-222222222222"
const runForkTestBundleHash = "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"

func runForkTestContext() context.Context {
	return runtimeauthoractivity.WithScope(
		context.Background(),
		runtimeauthoractivity.BundleScope(runForkTestRuntimeInstanceID, runForkTestBundleHash),
	)
}
