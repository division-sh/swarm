package construction_test

import (
	"context"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type reopenCleanupStore interface {
	storetest.RunFixtureStore
	runtimebus.RunLifecycleReadPersistence
	sourceartifactfixture.Writer
}

func TestRuntimeLocationReopenCleanupFollowsConsumerJoin(t *testing.T) {
	backends := []struct {
		name string
		open func(*testing.T) (reopenCleanupStore, func() reopenCleanupStore)
	}{
		{"sqlite", func(t *testing.T) (reopenCleanupStore, func() reopenCleanupStore) {
			selected, reopen := storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background())
			return selected, func() reopenCleanupStore { return reopen() }
		}},
		{"postgres", func(t *testing.T) (reopenCleanupStore, func() reopenCleanupStore) {
			selected, reopen := storetest.StartPostgresRuntimeStoreWithReopen(t)
			return selected, func() reopenCleanupStore { return reopen() }
		}},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			var original, peer reopenCleanupStore
			joined := false
			ctx := context.Background()
			runID := uuid.NewString()
			t.Run("fixture", func(t *testing.T) {
				var reopen func() reopenCleanupStore
				original, reopen = backend.open(t)
				sourceartifactfixture.Require(t, ctx, original)
				storetest.RequireRun(t, ctx, original, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID})
				t.Cleanup(func() {
					for _, owner := range []reopenCleanupStore{original, peer} {
						if _, err := owner.LoadRunLifecycleSnapshot(ctx, runID); err != nil {
							t.Errorf("store closed before consumer join: %v", err)
						}
					}
					joined = true
				})
				peer = reopen()
				if peer == original {
					t.Fatal("reopen reused the original native owner")
				}
			})
			if !joined {
				t.Fatal("consumer cleanup was not executed")
			}
			for _, owner := range []reopenCleanupStore{original, peer} {
				if _, err := owner.LoadRunLifecycleSnapshot(ctx, runID); err == nil {
					t.Fatal("fixture cleanup retained native read authority")
				}
			}
		})
	}
}
