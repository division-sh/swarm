package runforkexecution

import (
	"context"
	"strings"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestSelectedContractExecutionOwnerRequiresEmitFeedback(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var owner SelectedContractExecutionOwner
			if backend == "sqlite" {
				owner = selectedContractSQLiteExecutionOwnerForTest(t, storetest.StartSQLiteRuntimeStore(t))
			} else {
				selected, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)
				owner = selectedContractExecutionOwnerForTest(t, selected)
			}
			ports := owner.ports
			durable := ports.busDurable
			durable.EmitFeedback = nil
			_, err := NewSelectedContractExecutionOwner(
				ports.workflow, ports.fork, ports.runtimeExecution, ports.replay,
				ports.events, durable, ports.pipelineObligations, ports.manager, ports.managerRoles,
				ports.effects, ports.completion, ports.completionHeartbeat, ports.liveSessions, ports.managedCapabilities,
				ports.budget, ports.logs, ports.decisionCards, ports.proposedEffects, ports.humanTasks,
				ports.decisionCardDraftExpiry, ports.humanTaskExpiry,
			)
			if err == nil || !strings.Contains(err.Error(), "event emit feedback") {
				t.Fatalf("missing emit feedback admitted past selected construction: %v", err)
			}
		})
	}
}

func selectedContractExecutionOwnerForTest(t testing.TB, selected *store.PostgresStore) SelectedContractExecutionOwner {
	return selectedForkBoundOwnerForTest(t, selected, func() SelectedContractExecutionOwner {
		return newSelectedContractExecutionOwnerForTest(t, selected)
	})
}

func selectedContractExecutionOwnerWithProcessForTest(t testing.TB, selected *store.PostgresStore, capability startupownership.ProcessCapability) SelectedContractExecutionOwner {
	t.Helper()
	return selectedForkBoundOwnerWithProcessForTest(t, selected, capability, func() SelectedContractExecutionOwner {
		return newSelectedContractExecutionOwnerForTest(t, selected)
	})
}

func newSelectedContractExecutionOwnerForTest(t testing.TB, selected *store.PostgresStore) SelectedContractExecutionOwner {
	t.Helper()
	_ = runForkTestContext(t)
	if selected == nil {
		t.Fatal("selected postgres store is required")
	}
	workflow := runtimepipeline.NewWorkflowPersistence(selected)
	durable := runtimebus.DurableDependencies{
		EmitFeedback: selected,
		Instances: workflow, ConstructionPublications: workflow,
		ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
		FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteTopology: selected,
		ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected, PreparedEvents: selected,
		TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
	}
	roles := runtimemanager.PersistenceRoles{
		LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected, EffectsRecovery: selected,
		DeliveryQuiescence: selected, EventExistence: selected, DirectiveOperations: selected, DirectiveTargets: selected,
		StandingRestarts: selected,
	}
	owner, err := NewSelectedContractExecutionOwner(
		workflow, selected, selected, selected,
		selected, durable, selected.PipelineObligations(), selected, roles,
		selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected,
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
	workflow := runtimepipeline.NewWorkflowPersistence(selected)
	durable := runtimebus.DurableDependencies{
		EmitFeedback: selected,
		Instances: workflow, ConstructionPublications: workflow,
		ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
		FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteTopology: selected,
		ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected, PreparedEvents: selected,
		TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
	}
	roles := runtimemanager.PersistenceRoles{
		LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected, EffectsRecovery: selected,
		DeliveryQuiescence: selected, EventExistence: selected, DirectiveOperations: selected, DirectiveTargets: selected,
		StandingRestarts: selected,
	}
	owner, err := NewSelectedContractExecutionOwner(
		workflow, selected, selected, selected,
		selected, durable, selected.PipelineObligations(), selected, roles,
		selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected,
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
	return selectedForkBoundOwnerWithProcessForTest(t, selected, capability, construct)
}

func selectedForkBoundOwnerWithProcessForTest(t testing.TB, selected startupownership.Store, capability startupownership.ProcessCapability, construct func() SelectedContractExecutionOwner) SelectedContractExecutionOwner {
	t.Helper()
	ctx := runForkTestContext(t)
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
	if _, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live), SelectedForkRecoveryEnvironment{}); err != nil {
		t.Fatal(err)
	}
	if fixture.owners == nil {
		fixture.owners = make(map[startupownership.Store]SelectedContractExecutionOwner)
	}
	fixture.owners[selected] = owner
	return owner
}
