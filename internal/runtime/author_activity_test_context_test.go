package runtime

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

type runtimeTestWorkFixture struct {
	process *worklifetime.Process
}

var runtimeTestWorkFixtures sync.Map
var runtimeTestEventBusOwners sync.Map

func runtimeTestProcessWorkOwner(t testing.TB) *worklifetime.Process {
	t.Helper()
	if existing, ok := runtimeTestWorkFixtures.Load(t); ok {
		return existing.(*runtimeTestWorkFixture).process
	}
	fixture := &runtimeTestWorkFixture{process: worklifetime.NewProcess()}
	actual, loaded := runtimeTestWorkFixtures.LoadOrStore(t, fixture)
	if loaded {
		return actual.(*runtimeTestWorkFixture).process
	}
	t.Cleanup(func() {
		defer runtimeTestWorkFixtures.Delete(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := fixture.process.Join(ctx); err != nil {
			t.Errorf("join runtime test process owner: %v", err)
		}
	})
	return fixture.process
}

func runtimeTestOccurrence(t testing.TB, bundleHash string) *worklifetime.RuntimeOccurrence {
	t.Helper()
	owner, err := runtimeTestProcessWorkOwner(t).NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		BundleHash:        strings.TrimSpace(bundleHash),
	})
	if err != nil {
		t.Fatalf("create runtime test occurrence: %v", err)
	}
	t.Cleanup(func() {
		owner.Retire()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := owner.RetireAndWait(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("retire runtime test occurrence: %v", err)
		}
	})
	return owner
}

func newRuntimeTestEventBus(t testing.TB, store runtimebus.EventStore) (*runtimebus.EventBus, error) {
	t.Helper()
	return newRuntimeTestEventBusWithOptions(t, store, runtimebus.EventBusOptions{})
}

func newRuntimeTestEventBusWithOptions(t testing.TB, store runtimebus.EventStore, opts runtimebus.EventBusOptions) (*runtimebus.EventBus, error) {
	t.Helper()
	if opts.WorkOwner == nil {
		opts.WorkOwner = runtimeTestOccurrence(t, "bundle-v1:sha256:"+strings.Repeat("a", 64))
	}
	if opts.PipelineObligations == nil {
		if provider, ok := store.(interface {
			PipelineObligations() runtimepipelineobligation.Store
		}); ok {
			opts.PipelineObligations = provider.PipelineObligations()
		}
	}
	bus, err := runtimebus.NewEventBusWithOptions(store, opts)
	if err != nil {
		return nil, err
	}
	runtimeTestEventBusOwners.Store(bus, opts.WorkOwner)
	t.Cleanup(func() { runtimeTestEventBusOwners.Delete(bus) })
	return bus, nil
}

func runtimeTestEventBusWorkOwner(t testing.TB, bus *runtimebus.EventBus) worklifetime.Occurrence {
	t.Helper()
	owner, ok := runtimeTestEventBusOwners.Load(bus)
	if !ok {
		t.Fatal("runtime test event bus has no registered work owner")
	}
	return owner.(worklifetime.Occurrence)
}

func runtimeTestEventBusRuntimeOccurrence(t testing.TB, bus *runtimebus.EventBus) *worklifetime.RuntimeOccurrence {
	t.Helper()
	owner := runtimeTestEventBusWorkOwner(t, bus)
	runtimeOwner, ok := owner.(*worklifetime.RuntimeOccurrence)
	if !ok {
		t.Fatalf("runtime test event bus owner is %T, want runtime occurrence", owner)
	}
	return runtimeOwner
}

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

func newScopedTestRuntime(t testing.TB, ctx context.Context, deps RuntimeDeps) (*Runtime, error) {
	t.Helper()
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
	if deps.Options.ProcessWorkOwner == nil {
		deps.Options.ProcessWorkOwner = runtimeTestProcessWorkOwner(t)
	}
	if deps.Stores.PipelineObligations == nil {
		if provider, ok := deps.Stores.EventStore.(interface {
			PipelineObligations() runtimepipelineobligation.Store
		}); ok {
			deps.Stores.PipelineObligations = provider.PipelineObligations()
		}
	}
	runtime, err := NewRuntime(ctx, deps)
	if err == nil {
		t.Cleanup(func() {
			if shutdownErr := runtime.Shutdown(); shutdownErr != nil {
				t.Errorf("shutdown runtime test fixture: %v", shutdownErr)
			}
		})
	}
	return runtime, err
}
