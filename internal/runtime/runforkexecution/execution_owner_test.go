package runforkexecution

import (
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store"
)

func selectedContractExecutionOwnerForTest(t testing.TB, selected *store.PostgresStore) SelectedContractExecutionOwner {
	t.Helper()
	if selected == nil {
		t.Fatal("selected postgres store is required")
	}
	durable := runtimebus.DurableDependencies{
		ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
		FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected, FlowRouteTopology: selected, FlowRouteRollback: selected,
		ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected, DeliveryTargets: selected, DeliveryRouteSets: selected,
		TargetFailureRecorder: selected, RunOrigins: selected,
	}
	roles := runtimemanager.PersistenceRoles{
		LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected, EffectsRecovery: selected,
		DeliveryQuiescence: selected, EventExistence: selected, DirectiveOperations: selected, DirectiveTargets: selected,
		FlowRoutes: selected,
	}
	owner, err := NewSelectedContractExecutionOwner(
		runtimepipeline.NewWorkflowPersistence(selected), selected, selected, selected,
		selected, durable, selected.PipelineObligations(), selected, roles,
		selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected,
	)
	if err != nil {
		t.Fatalf("NewSelectedContractExecutionOwner: %v", err)
	}
	return owner
}
