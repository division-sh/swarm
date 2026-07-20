package pipeline

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

type pipelineTestWorkFixture struct {
	process *worklifetime.Process
	runtime *worklifetime.RuntimeOccurrence
}

var pipelineTestWorkFixtures sync.Map

func pipelineTestWorkOwner(t *testing.T) *worklifetime.RuntimeOccurrence {
	t.Helper()
	if existing, ok := pipelineTestWorkFixtures.Load(t); ok {
		return existing.(*pipelineTestWorkFixture).runtime
	}
	fixture := &pipelineTestWorkFixture{process: worklifetime.NewProcess()}
	owner, err := fixture.process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "pipeline-test-runtime",
		BundleHash:        "pipeline-test-bundle",
	})
	if err != nil {
		t.Fatalf("create pipeline test work owner: %v", err)
	}
	fixture.runtime = owner
	actual, loaded := pipelineTestWorkFixtures.LoadOrStore(t, fixture)
	if loaded {
		return actual.(*pipelineTestWorkFixture).runtime
	}
	t.Cleanup(func() {
		defer pipelineTestWorkFixtures.Delete(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := fixture.runtime.RetireAndWait(ctx); err != nil {
			t.Errorf("retire pipeline test work owner: %v", err)
			return
		}
		if _, err := fixture.process.Join(ctx); err != nil {
			t.Errorf("join pipeline test process owner: %v", err)
		}
	})
	return owner
}

func testAuthorActivityContext(t *testing.T, ctx context.Context) context.Context {
	t.Helper()
	ctx = worklifetime.WithOccurrence(ctx, pipelineTestWorkOwner(t))
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		"11111111-1111-1111-1111-111111111111",
		"bundle-v1:sha256:"+strings.Repeat("a", 64),
	))
}
