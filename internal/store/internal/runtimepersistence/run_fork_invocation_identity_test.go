package runtimepersistence

import (
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunForkFreshInvocationsAtSameCutHaveIndependentChildrenBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var owner runForkSelectedLifecycleStore
			var db *sql.DB
			if backend == "sqlite" {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				owner, db = store, store.backend.ConstructionHandle()
			} else {
				_, db, _ = testutil.StartPostgres(t)
				owner = newPostgresStoreWithBackend(mustPostgresBackend(db))
			}
			runID, eventID := uuid.NewString(), uuid.NewString()
			at := time.Now().UTC().Truncate(time.Microsecond)
			requireRunFixtureForTest(t, ctx, owner, semanticRunFixture{
				Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: at.Add(-time.Minute),
				BundleHash: authorActivityTestBundleHash,
			})
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			seedRunForkRevisionMatrixEvent(t, ctx, tx, runID, eventID, at, backend == "postgres")
			effects, err := runforkrevision.ForRun(runID, runforkrevision.AllFamilies()...)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := finalizeRunForkRevisionMatrix(ctx, tx, backend == "postgres", effects); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			request := runfork.RunForkMaterializeRequest{SourceRunID: runID, At: eventID}
			first, err := owner.MaterializeRunFork(ctx, request)
			if err != nil {
				t.Fatalf("first invocation: %v", err)
			}
			second, err := owner.MaterializeRunFork(ctx, request)
			if err != nil {
				t.Fatalf("second fresh invocation: %v", err)
			}
			if first.ForkRunID == second.ForkRunID {
				t.Fatalf("distinct fresh invocations collapsed to the same child %s at fixed cut %s", first.ForkRunID, eventID)
			}
			if first.SourceRunID != runID || second.SourceRunID != runID || first.ForkPoint != second.ForkPoint {
				t.Fatalf("independent identity changed the fixed source cut: first=%+v second=%+v", first, second)
			}
		})
	}
}
