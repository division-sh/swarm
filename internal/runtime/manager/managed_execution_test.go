package manager

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func managedExecutionTestContext(t testing.TB, ctx context.Context) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"manager-test-authority",
		1,
		"",
		"manager-test-actors",
		"bundle-v2:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("managedexecution.New: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}

func managedExecutionTestContextForCapturedSource(t testing.TB, ctx context.Context) context.Context {
	t.Helper()
	fact, present := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !present {
		t.Fatal("managed execution fixture requires a captured source fact")
	}
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"manager-test-authority",
		1,
		"",
		"manager-test-actors",
		fact.BundleHash(),
		nil,
	)
	if err != nil {
		t.Fatalf("managedexecution.New: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}
