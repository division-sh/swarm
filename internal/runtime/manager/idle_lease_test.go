package manager

import (
	"context"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
)

func TestIssue2269SettlingLeasePreservesOtherAcquisitions(t *testing.T) {
	bus := newProjectionTestBus()
	factory := &projectionTestFactory{handled: make(chan int, 1)}
	am := newProjectionTestManager(t, bus, factory.Build)
	identity := runtimeagentidentitytest.RootRuntime(t, "idle-lease", "idle-lease-test")
	if err := spawnManagerTestAgent(am, managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live", ID: identity.AgentID(), Identity: identity, Subscriptions: []string{"test.old"},
	})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancel()
	if err := am.Run(managedExecutionTestContext(t, ctx)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := am.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	settling, err := am.lifecycle.acquireExecutionIdentity(ctx, identity, "settling test carrier", true)
	if err != nil {
		t.Fatal(err)
	}
	defer settling.Release()
	other, err := am.lifecycle.acquireExecutionIdentity(ctx, identity, "other admitted work", true)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Release()
	am.lifecycle.mu.Lock()
	execution := am.lifecycle.cells[identity].execution
	drained := execution.leaseDrained
	count := execution.leases
	am.lifecycle.mu.Unlock()
	if count != 2 || drained == nil {
		t.Fatalf("two exact acquisitions: count=%d drained=%v", count, drained)
	}
	settling.Release()
	settling.Release()
	am.lifecycle.mu.Lock()
	count = execution.leases
	am.lifecycle.mu.Unlock()
	if count != 1 {
		t.Fatalf("settling carrier released another acquisition: %d", count)
	}
	select {
	case <-drained:
		t.Fatal("other work falsely declared drained")
	default:
	}
	other.Release()
	select {
	case <-drained:
	default:
		t.Fatal("identified last acquisition did not release its join")
	}
}
