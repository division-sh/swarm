package manager_test

import (
	"context"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/selected/selectedtest"
	"testing"
)

func openManagerNativeSelectedDelivery(t *testing.T, backend string) *manager.ManagerDeliveryNativeFixture {
	t.Helper()
	fixture := openManagerNativeDelivery(t, backend)
	selected := fixture.Store.(selectedtest.SelectedDeliveryFixtureStore)
	execution := selectedtest.OpenSelectedDeliveryExecution(t, manager.TerminalPanicFixtureContext(), selected)
	fixture.Authority, fixture.NormalAuthority = execution.Authority, execution.NormalAuthority
	fixture.SelectedAdmission, fixture.SelectedRoute = execution.SelectedAdmission, execution.SelectedRoute
	fixture.SelectedEvent, fixture.SemanticSource, fixture.Context = execution.SelectedEvent, execution.SemanticSource, execution.Context
	fixture.SelectedOrigin = execution.DiagnosticOrigin
	fixture.PrepareActor = func(t *testing.T, cfg actors.AgentConfig) (context.Context, manager.PersistedAgent, manager.AgentLifecyclePersistence, agenttopology.SourceSetPlan) {
		t.Helper()
		if cfg.Identity.Normalize() != fixture.SelectedRoute.AgentIdentity {
			t.Fatal("selected actor preparation must name its actual issued declaration")
		}
		record, grant := execution.PreparePersistedActor(t)
		fixture.Context = execution.Context
		return execution.Context, record, grant, agenttopology.SourceSetPlan{}
	}
	return fixture
}

func TestManagerNativeSelectedDeliveryFixtureUsesRealIssuedClaimBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openManagerNativeSelectedDelivery(t, backend)
			if fixture.Authority.Kind() != deliverylifecycle.ExecutionAuthoritySelectedContractFork || fixture.SelectedAdmission.Kind != managedexecution.KindSelectedContractFork {
				t.Fatal("selected manager fixture lacks exact native issued claim")
			}
		})
	}
}
