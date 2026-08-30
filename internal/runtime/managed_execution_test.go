package runtime

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
)

func managedExecutionTestContext(t testing.TB, ctx context.Context) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"runtime-test-authority",
		1,
		"",
		"runtime-test-actors",
		"bundle-v2:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("managedexecution.New: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}
