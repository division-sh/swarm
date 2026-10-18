package manager_test

import (
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/selected/selectedtest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"testing"
)

func openManagerNativeSelectedDelivery(t *testing.T, backend string) *manager.ManagerDeliveryNativeFixture {
	t.Helper()
	fixture := openManagerNativeDelivery(t, backend)
	selected := fixture.Store.(selectedtest.SelectedDeliveryFixtureStore)
	original, err := agentfixture.ProcessCapability(t, fixture.Context, fixture.Store.(agentfixture.Store))
	if err != nil {
		t.Fatal(err)
	}
	execution := selectedtest.OpenSelectedDeliveryExecution(t, fixture.Context, selected, original)
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
			t.Run("borrowed_owner_survives_joined_child", func(t *testing.T) {
				normal := openManagerNativeDelivery(t, backend)
				original, err := agentfixture.ProcessCapability(t, normal.Context, normal.Store.(agentfixture.Store))
				if err != nil {
					t.Fatal(err)
				}
				before, err := original.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				t.Run("selected_child", func(t *testing.T) {
					selectedtest.OpenSelectedDeliveryExecution(t, normal.Context, normal.Store.(selectedtest.SelectedDeliveryFixtureStore), original)
				})
				if after, err := original.Evidence(); err != nil || after != before {
					t.Fatalf("joined selected child retired or replaced its borrowed original: before=%+v after=%+v error=%v", before, after, err)
				}
				if err := original.ProveCurrent(normal.Context); err != nil {
					t.Fatalf("joined selected child lost its outer original: %v", err)
				}
			})
		})
	}
}
