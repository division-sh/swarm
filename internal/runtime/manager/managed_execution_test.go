package manager

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
)

func managedExecutionTestContext(t testing.TB, ctx context.Context) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"manager-test-authority",
		1,
		"",
		"manager-test-actors",
		"manager-test-bundle",
		nil,
	)
	if err != nil {
		t.Fatalf("managedexecution.New: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}
