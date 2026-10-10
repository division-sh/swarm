package manager_test

import (
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
