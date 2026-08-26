package runtimepersistence

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestMutationLogPrivateAdapterRequiresExactActiveRunSource(t *testing.T) {
	const bundleA = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const bundleB = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	tests := []struct {
		name         string
		seedRun      bool
		deleteBundle bool
		contextHash  string
		want         string
	}{
		{name: "exact source", seedRun: true, contextHash: bundleA},
		{name: "missing run", contextHash: bundleA, want: "run not found"},
		{name: "foreign source", seedRun: true, contextHash: bundleB, want: "does not match active run"},
		{name: "deleted persisted source", seedRun: true, deleteBundle: true, contextHash: bundleA, want: "persisted bundle source unavailable"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			selected := admitTestPostgresStore(t, db)
			runID := uuid.NewString()
			seedStoreTestPersistedBundle(t, db, bundleA)
			if tc.contextHash == bundleB {
				seedStoreTestPersistedBundle(t, db, bundleB)
			}
			if tc.seedRun {
				requireRunFixtureForTest(t, testAuthorActivityContextForBundle(bundleA), selected, semanticRunFixture{
					Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID,
					State: runtimerunlifecycle.StateRunning, BundleHash: bundleA,
					BundleSource: runtimerunlifecycle.BundleSourcePersisted, StartedAt: time.Date(2026, 8, 2, 14, 0, 0, 0, time.UTC),
				})
			}
			if tc.deleteBundle {
				if _, err := db.ExecContext(context.Background(), `DELETE FROM bundles WHERE bundle_hash = $1`, bundleA); err != nil {
					t.Fatalf("delete persisted bundle: %v", err)
				}
			}

			fact, err := runtimecorrelation.NewPersistedBundleSourceFact(tc.contextHash)
			if err != nil {
				t.Fatal(err)
			}
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(tc.contextHash), runID)
			ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
			err = insertMutationLogPrivateAdapter(ctx, selected, runtimemutationlog.Record{
				EntityID: uuid.NewString(), Domain: runtimemutationlog.DomainLifecycleState, NewValue: "active",
				WriterType: "system_node", WriterID: "review",
			})
			if tc.want == "" {
				if err != nil {
					t.Fatalf("insert exact mutation log: %v", err)
				}
				if got := countMutationLogAdapterRows(t, db, runID, "entity_mutations", ""); got != 1 {
					t.Fatalf("entity mutation rows = %d, want 1", got)
				}
				if got := countMutationLogAdapterRows(t, db, runID, "author_activity_occurrences", "kind = 'entity.lifecycle'"); got != 1 {
					t.Fatalf("entity mutation story rows = %d, want 1", got)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("insert error = %v, want %q", err, tc.want)
			}
			if got := countMutationLogAdapterRows(t, db, runID, "entity_mutations", ""); got != 0 {
				t.Fatalf("entity mutation rows after rejection = %d, want 0", got)
			}
			if got := countMutationLogAdapterRows(t, db, runID, "author_activity_occurrences", "kind = 'entity.lifecycle'"); got != 0 {
				t.Fatalf("entity mutation story rows after rejection = %d, want 0", got)
			}
		})
	}
}

func insertMutationLogPrivateAdapter(ctx context.Context, selected *PostgresStore, record runtimemutationlog.Record) error {
	return selected.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		return privatemutationlog.InsertWithStory(txctx, tx, postgresActiveRunSourceOwner(selected, tx), story, privaterunforkrevision.NewEffects(), record)
	})
}

func countMutationLogAdapterRows(t *testing.T, db *sql.DB, runID, table, predicate string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM ` + table + ` WHERE run_id = $1::uuid`
	if predicate != "" {
		query += ` AND ` + predicate
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, runID).Scan(&count); err != nil {
		t.Fatalf("count %s rows: %v", table, err)
	}
	return count
}
