package builder

import (
	"context"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/google/uuid"
)

type testRuntimeAcquirer struct {
	runtime *runtimepkg.Runtime
	owner   *worklifetime.RuntimeOccurrence
	process *worklifetime.Process
}

type testRuntimeUse struct {
	runtime *runtimepkg.Runtime
	lease   *worklifetime.Lease
	ctx     context.Context
}

func newTestRuntimeAcquirer(t testing.TB, rt *runtimepkg.Runtime) RuntimeAcquirer {
	t.Helper()
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: uuid.NewString(),
		BundleHash:        "builder-test-bundle",
	})
	if err != nil {
		t.Fatalf("new builder test runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(ctx); err != nil {
			t.Errorf("retire builder test runtime occurrence: %v", err)
			return
		}
		if _, err := process.Join(ctx); err != nil {
			t.Errorf("join builder test process: %v", err)
		}
	})
	return &testRuntimeAcquirer{runtime: rt, owner: owner, process: process}
}

func newTestOwnedEventBus(t testing.TB, store runtimebus.EventStore, opts runtimebus.EventBusOptions) (*runtimepkg.Runtime, *testRuntimeAcquirer) {
	t.Helper()
	acquirer := newTestRuntimeAcquirer(t, nil).(*testRuntimeAcquirer)
	opts.WorkOwner = acquirer.owner
	bus, err := runtimebus.NewEventBusWithOptions(store, opts)
	if err != nil {
		t.Fatalf("new owned builder event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	acquirer.runtime = rt
	return rt, acquirer
}

func (a *testRuntimeAcquirer) AcquireCurrentRuntime(ctx context.Context) (RuntimeUse, error) {
	return a.acquire(ctx)
}

func (a *testRuntimeAcquirer) AcquireRunRuntime(ctx context.Context, _ string) (RuntimeUse, error) {
	return a.acquire(ctx)
}

func (a *testRuntimeAcquirer) acquire(ctx context.Context) (RuntimeUse, error) {
	ctx = worklifetime.WithProcess(ctx, a.process)
	ctx = worklifetime.WithOccurrence(ctx, a.owner)
	lease, err := a.owner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	workCtx := worklifetime.WithProcess(lease.Context(), a.process)
	workCtx = worklifetime.WithOccurrence(workCtx, a.owner)
	return &testRuntimeUse{runtime: a.runtime, lease: lease, ctx: workCtx}, nil
}

func (u *testRuntimeUse) Runtime() *runtimepkg.Runtime { return u.runtime }
func (u *testRuntimeUse) WorkContext() context.Context { return u.ctx }
func (u *testRuntimeUse) Done() error                  { return u.lease.Done() }
