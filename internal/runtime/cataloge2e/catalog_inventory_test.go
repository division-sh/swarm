package cataloge2e

import (
	"testing"

	"github.com/division-sh/swarm/internal/testcatalog"
)

func catalogInventory(t testing.TB) *testcatalog.Inventory {
	t.Helper()
	inventory, err := testcatalog.Load(repoRootFromCatalogE2E(t))
	if err != nil {
		t.Fatalf("load catalog inventory: %v", err)
	}
	return inventory
}

func catalogRuntimeFixtures(t testing.TB, claimID string) []testcatalog.Fixture {
	t.Helper()
	fixtures := catalogInventory(t).Select(claimID, testcatalog.DispositionRuntime)
	if len(fixtures) == 0 {
		t.Fatalf("catalog claim %s has no runtime fixtures", claimID)
	}
	return fixtures
}

func catalogRuntimeFixture(t testing.TB, claimID, name string) testcatalog.Fixture {
	t.Helper()
	for _, fixture := range catalogRuntimeFixtures(t, claimID) {
		if fixture.Name == name {
			return fixture
		}
	}
	t.Fatalf("catalog claim %s has no runtime fixture %s", claimID, name)
	return testcatalog.Fixture{}
}
