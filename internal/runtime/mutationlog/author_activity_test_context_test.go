package mutationlog

import (
	"context"
	"strings"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
)

func testAuthorActivityContext(ctx context.Context) context.Context {
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		"11111111-1111-1111-1111-111111111111",
		"bundle-v1:sha256:"+strings.Repeat("a", 64),
	))
}

func testAuthorActivityRuntimeContext(ctx context.Context) context.Context {
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.RuntimeScope(
		"11111111-1111-1111-1111-111111111111",
	))
}
