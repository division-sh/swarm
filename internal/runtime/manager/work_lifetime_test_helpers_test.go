package manager

import (
	"context"
	"sync"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

type managerTestWorkFixture struct {
	process *worklifetime.Process
	runtime *worklifetime.RuntimeOccurrence
}

var managerTestWorkFixtures sync.Map

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
	manager := NewAgentManagerWithOptions(bus, factory, opts, stores...)
	t.Cleanup(func() {
		if err := manager.ShutdownWithOptions(ShutdownOptions{Grace: 5 * time.Second}); err != nil {
			t.Errorf("shutdown manager test work owner: %v", err)
		}
	})
	return manager
}
