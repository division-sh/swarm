package runforkexecution

import (
	"context"
	"sync"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

const runForkTestRuntimeInstanceID = "22222222-2222-4222-8222-222222222222"
const runForkTestBundleHash = "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"

type runForkTestWorkFixture struct {
	process *worklifetime.Process
	runtime *worklifetime.RuntimeOccurrence
}

var runForkTestWorkFixtures sync.Map

func testGatewayWorkOwner(t testing.TB) *worklifetime.RuntimeOccurrence {
	t.Helper()
	if existing, ok := runForkTestWorkFixtures.Load(t); ok {
		return existing.(*runForkTestWorkFixture).runtime
	}
	fixture := &runForkTestWorkFixture{process: worklifetime.NewProcess()}
	runtimeOwner, err := fixture.process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "selected-contract-gateway-test",
		BundleHash:        "selected-contract-gateway-bundle",
	})
	if err != nil {
		t.Fatalf("create selected-fork test work owner: %v", err)
	}
	fixture.runtime = runtimeOwner
	actual, loaded := runForkTestWorkFixtures.LoadOrStore(t, fixture)
	if loaded {
		return actual.(*runForkTestWorkFixture).runtime
	}
	t.Cleanup(func() {
		defer runForkTestWorkFixtures.Delete(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := fixture.runtime.RetireAndWait(ctx); err != nil {
			t.Errorf("retire selected-fork test work owner: %v", err)
			return
		}
		if _, err := fixture.process.Join(ctx); err != nil {
			t.Errorf("join selected-fork test process owner: %v", err)
		}
	})
	return runtimeOwner
}

func runForkTestContext(t testing.TB) context.Context {
	t.Helper()
	ctx := worklifetime.WithOccurrence(context.Background(), testGatewayWorkOwner(t))
	return runtimeauthoractivity.WithScope(
		ctx,
		runtimeauthoractivity.BundleScope(runForkTestRuntimeInstanceID, runForkTestBundleHash),
	)
}
