package manager

import (
	"context"
	"sync"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type managerTestWorkFixture struct {
	process *worklifetime.Process
	runtime *worklifetime.RuntimeOccurrence
}

var managerTestWorkFixtures sync.Map

func admitManagerTestBusContext(ctx context.Context) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx, nil
}

func newTestManagerWorkOwner(t *testing.T) worklifetime.Occurrence {
	t.Helper()
	if existing, ok := managerTestWorkFixtures.Load(t); ok {
		return existing.(*managerTestWorkFixture).runtime
	}
	fixture := &managerTestWorkFixture{process: worklifetime.NewProcess()}
	runtimeOwner, err := fixture.process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "manager-test-runtime",
		BundleHash:        "manager-test-bundle",
	})
	if err != nil {
		t.Fatalf("create manager test work owner: %v", err)
	}
	fixture.runtime = runtimeOwner
	actual, loaded := managerTestWorkFixtures.LoadOrStore(t, fixture)
	if loaded {
		return actual.(*managerTestWorkFixture).runtime
	}
	t.Cleanup(func() {
		defer managerTestWorkFixtures.Delete(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := fixture.runtime.RetireAndWait(ctx); err != nil {
			t.Errorf("retire manager test work owner: %v", err)
			return
		}
		if _, err := fixture.process.Join(ctx); err != nil {
			t.Errorf("join manager test process owner: %v", err)
		}
	})
	return runtimeOwner
}

func newTestAgentManager(t *testing.T, bus Bus, factory AgentFactory, stores ...ManagerPersistence) *AgentManager {
	t.Helper()
	return newTestAgentManagerWithOptions(t, bus, factory, AgentManagerOptions{}, stores...)
}

func newTestAgentManagerWithOptions(t *testing.T, bus Bus, factory AgentFactory, opts AgentManagerOptions, stores ...ManagerPersistence) *AgentManager {
	t.Helper()
	if opts.WorkOwner == nil {
		opts.WorkOwner = newTestManagerWorkOwner(t)
	}
	if opts.DeliveryStore == nil && len(stores) > 0 {
		if deliveryStore, ok := any(stores[0]).(runtimedelivery.Store); ok {
			opts.DeliveryStore = deliveryStore
		}
	}
	if opts.DeliveryStore == nil {
		opts.DeliveryStore = newManagerDeliveryTestStore(t)
	}
	if authorityStore, ok := opts.DeliveryStore.(interface {
		managerTestDeliveryAuthority() runtimedelivery.ExecutionAuthority
	}); ok {
		authority := authorityStore.managerTestDeliveryAuthority()
		if setter, ok := bus.(interface {
			SetDeliveryAuthority(runtimedelivery.ExecutionAuthority) error
		}); ok {
			if err := setter.SetDeliveryAuthority(authority); err != nil {
				t.Fatalf("set manager test delivery authority: %v", err)
			}
		}
		if setter, ok := bus.(interface {
			SetDeliveryContinuationOwner(runtimebus.DeliveryContinuationOwner) error
		}); ok {
			if err := setter.SetDeliveryContinuationOwner(runtimebustest.NewDeliveryContinuationOwner(true)); err != nil {
				t.Fatalf("set manager test delivery continuation owner: %v", err)
			}
		}
	}
	manager := NewAgentManagerWithOptions(bus, factory, opts, stores...)
	t.Cleanup(func() {
		if err := manager.ShutdownWithOptions(ShutdownOptions{Grace: 5 * time.Second}); err != nil {
			t.Errorf("shutdown manager test work owner: %v", err)
		}
	})
	return manager
}
