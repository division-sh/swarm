package manager

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/google/uuid"
)

func ProveAgentRowIndividualSourceSetReplacement(t *testing.T, previous AgentLifecyclePersistence, reader AgentLifecycleStateReader, record PersistedAgent, advance func() (AgentLifecyclePersistence, agenttopology.Admission)) {
	t.Helper()
	bus := newProjectionTestBus()
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{LifecycleStore: previous, PersistenceRoles: PersistenceRoles{LifecycleState: reader}})
	ctx := testAuthorActivityContext(context.Background())
	agent := &reconfigureTestAgent{id: record.Config.ID}
	if err := am.lifecycle.registerExecution(ctx, record, false, agent, testManagerSubscriptionAdmission(t, record.Config)); err != nil {
		t.Fatal(err)
	}
	if err := am.Run(managedExecutionTestContext(t, ctx)); err != nil {
		t.Fatalf("install original running Manager projection: %v", err)
	}
	t.Cleanup(func() {
		if err := am.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	before, _, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil {
		t.Fatal(err)
	}
	am.lifecycle.mu.Lock()
	predecessor := am.lifecycle.cells[record.Config.Identity].execution
	am.lifecycle.mu.Unlock()
	successor, topology := advance()
	am.lifecycle.replacePersistence(successor)
	if _, err := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "restart", uuid.NewString(), nil, am.semanticSource, &topology, nil); err != nil {
		t.Fatalf("real individual source-set replacement: %v", err)
	}
	after, found, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found || after.Generation != before.Generation+1 || after.Phase != AgentLifecycleRunning || !after.Topology.Equal(topology) {
		t.Fatalf("native successor row: before=%#v after=%#v found=%v err=%v", before, after, found, err)
	}
	am.lifecycle.mu.Lock()
	cell := am.lifecycle.cells[record.Config.Identity]
	valid := cell.execution != predecessor && cell.execution.agent == agent && predecessor.fenced &&
		cell.execution.token == lifecycleToken(record.Config.Identity, after.RuntimeEpoch, after.Generation) &&
		am.lifecycle.executionRunnableCurrentOccurrenceLocked(cell)
	am.lifecycle.mu.Unlock()
	if !valid {
		t.Fatal("native generation advanced without its installed runnable successor projection")
	}
	if err := am.Shutdown(); err != nil {
		t.Fatal(err)
	}
}
