package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const pipelineTestBundleHash = sourceartifactfixture.BundleHash

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
		BundleHash:        pipelineTestBundleHash,
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
	fact, err := runtimecorrelation.NewSourceArtifactFact(pipelineTestBundleHash)
	if err != nil {
		t.Fatalf("create pipeline test bundle source fact: %v", err)
	}
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		"11111111-1111-1111-1111-111111111111",
		pipelineTestBundleHash,
	))
}

func mustPipelineTestSourceArtifactFact(bundleHash string) runtimecorrelation.SourceArtifactFact {
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}
