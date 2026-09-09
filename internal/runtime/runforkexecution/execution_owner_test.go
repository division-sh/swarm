package runforkexecution

import (
	"context"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
)

func selectedContractExecutionOwnerForTest(t testing.TB, selected *store.PostgresStore) SelectedContractExecutionOwner {
	return selectedForkBoundOwnerForTest(t, selected, func() SelectedContractExecutionOwner {
		return newSelectedContractExecutionOwnerForTest(t, selected)
	})
}

func newSelectedContractExecutionOwnerForTest(t testing.TB, selected *store.PostgresStore) SelectedContractExecutionOwner {
	t.Helper()
	_ = runForkTestContext(t)
	if selected == nil {
		t.Fatal("selected postgres store is required")
	}
	durable := runtimebus.DurableDependencies{
		ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
		FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected, FlowRouteTopology: selected, FlowRouteRollback: selected,
		ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected, WorkflowInstances: selected, PreparedEvents: selected,
		TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
	}
	roles := runtimemanager.PersistenceRoles{
		LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected, EffectsRecovery: selected,
		DeliveryQuiescence: selected, EventExistence: selected, DirectiveOperations: selected, DirectiveTargets: selected,
		FlowRoutes: selected, StandingRestarts: selected,
	}
	owner, err := NewSelectedContractExecutionOwner(
		runtimepipeline.NewWorkflowPersistence(selected), selected, selected, selected,
		selected, durable, selected.PipelineObligations(), selected, roles,
		selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected,
	)
	if err != nil {
		t.Fatalf("NewSelectedContractExecutionOwner: %v", err)
	}
	t.Cleanup(func() {
		if err := owner.RetireSelectedContexts(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return owner
}

func selectedContractSQLiteExecutionOwnerForTest(t testing.TB, selected *store.SQLiteRuntimeStore) SelectedContractExecutionOwner {
	return selectedForkBoundOwnerForTest(t, selected, func() SelectedContractExecutionOwner {
		return newSelectedContractSQLiteExecutionOwnerForTest(t, selected)
	})
}

func newSelectedContractSQLiteExecutionOwnerForTest(t testing.TB, selected *store.SQLiteRuntimeStore) SelectedContractExecutionOwner {
	t.Helper()
	_ = runForkTestContext(t)
	durable := runtimebus.DurableDependencies{
		ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
		FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected, FlowRouteTopology: selected, FlowRouteRollback: selected,
		ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected, WorkflowInstances: selected, PreparedEvents: selected,
		TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
	}
	roles := runtimemanager.PersistenceRoles{
		LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected, EffectsRecovery: selected,
		DeliveryQuiescence: selected, EventExistence: selected, DirectiveOperations: selected, DirectiveTargets: selected,
		FlowRoutes: selected, StandingRestarts: selected,
	}
	owner, err := NewSelectedContractExecutionOwner(
		runtimepipeline.NewWorkflowPersistence(selected), selected, selected, selected,
		selected, durable, selected.PipelineObligations(), selected, roles,
		selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.RetireSelectedContexts(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return owner
}

func selectedForkBoundOwnerForTest(t testing.TB, selected startupownership.Store, construct func() SelectedContractExecutionOwner) SelectedContractExecutionOwner {
	t.Helper()
	ctx := runForkTestContext(t)
	capability := selectedContractTestProcessCapability(t, ctx, selected)
	value, _ := runForkTestWorkFixtures.Load(t)
	fixture := value.(*runForkTestWorkFixture)
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if owner, ok := fixture.owners[selected]; ok {
		return owner
	}
	owner := construct()
	if err := owner.BindSelectedProcess(ctx, fixture.process, capability); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live)); err != nil {
		t.Fatal(err)
	}
	if fixture.owners == nil {
		fixture.owners = make(map[startupownership.Store]SelectedContractExecutionOwner)
	}
	fixture.owners[selected] = owner
	return owner
}
