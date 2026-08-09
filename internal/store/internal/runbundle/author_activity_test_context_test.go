package runbundle

import (
	"context"
	"strings"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
)

func testAuthorActivityContext() context.Context {
	return runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(
		"11111111-1111-1111-1111-111111111111",
		"bundle-v1:sha256:"+strings.Repeat("a", 64),
	))
}
