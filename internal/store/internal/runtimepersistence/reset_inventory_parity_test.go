package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestResetInventoryAndOperationLeaseBothStores(t *testing.T) {
	runID := uuid.NewString()
	var baseline *destructivereset.Inventory
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected interface {
				destructivereset.InventoryReader
				destructivereset.LockManager
			}
			var db *sql.DB
			if backend == "sqlite" {
				s := newBootstrappedSQLiteRuntimeStoreForTest(t)
				selected, db = s, s.backend.ConstructionHandle()
			} else {
				_, database, _ := testutil.StartPostgres(t)
				selected, db = newTestPostgresStore(t, database), database
			}
			ctx := testAuthorActivityContext()
			seed := runlifecyclefixture.Fixture{RunID: runID, Origin: runlifecyclefixture.ScenarioSetupOrigin()}
			if backend == "sqlite" {
				runlifecyclefixture.RequireSQLite(t, ctx, db, seed)
			} else {
				runlifecyclefixture.RequirePostgres(t, ctx, db, seed)
			}
			inventory, err := selected.ReadResetInventory(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !inventory.CleanupRunSetKnown || len(inventory.CleanupRuns) != 1 || inventory.CleanupRuns[0].RunID != runID || !reflect.DeepEqual(inventory.ActiveRuns, inventory.CleanupRuns) {
				t.Fatalf("inventory did not capture canonical run: %+v", inventory)
			}
			if baseline == nil {
				baseline = &inventory
			} else if !reflect.DeepEqual(*baseline, inventory) {
				t.Fatalf("backend inventory differs: %+v vs %+v", inventory, *baseline)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if lease, acquired, err := selected.AcquireDestructiveReset(cancelled); !errors.Is(err, context.Canceled) || acquired || lease != nil {
				t.Fatalf("cancelled acquisition = %v, %t, %v", lease, acquired, err)
			}
			first, acquired, err := selected.AcquireDestructiveReset(ctx)
			if err != nil || !acquired || first == nil {
				t.Fatalf("first acquisition = %v, %t, %v", first, acquired, err)
			}
			t.Cleanup(func() {
				if err := first.Release(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if other, acquired, err := selected.AcquireDestructiveReset(ctx); err != nil || acquired || other != nil {
				t.Fatalf("overlap = %v, %t, %v", other, acquired, err)
			}
			if err := first.Release(cancelled); err != nil {
				t.Fatal(err)
			}
			second, acquired, err := selected.AcquireDestructiveReset(ctx)
			if err != nil || !acquired || second == nil {
				t.Fatalf("reacquisition = %v, %t, %v", second, acquired, err)
			}
			t.Cleanup(func() {
				if err := second.Release(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if err := first.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			if other, acquired, err := selected.AcquireDestructiveReset(ctx); err != nil || acquired || other != nil {
				t.Fatalf("old lease released its successor = %v, %t, %v", other, acquired, err)
			}
		})
	}
}
