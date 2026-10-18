package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storerunfork "github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestMutationLogPrivateAdapterRequiresExactActiveRunSource(t *testing.T) {
	artifactA := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  mutation-log-a: {}\n"))
	artifactB := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  mutation-log-b: {}\n"))
	bundleA := artifactA.BundleHash()
	bundleB := artifactB.BundleHash()

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
		{name: "deleted persisted source", seedRun: true, deleteBundle: true, contextHash: bundleA, want: "source artifact unavailable"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			selected := admitTestPostgresStore(t, db)
			runID := uuid.NewString()
			sourceartifactfixture.RequireArtifact(t, testAuthorActivityContext(), selected, artifactA)
			if tc.contextHash == bundleB {
				sourceartifactfixture.RequireArtifact(t, testAuthorActivityContext(), selected, artifactB)
			}
			if tc.seedRun {
				requireRunFixtureForTest(t, testAuthorActivityContextForBundle(bundleA), selected, semanticRunFixture{
					Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID,
					State: runtimerunlifecycle.StateRunning, BundleHash: bundleA,
					StartedAt: time.Date(2026, 8, 2, 14, 0, 0, 0, time.UTC),
				})
			}
			if tc.deleteBundle {
				if _, err := db.ExecContext(context.Background(), `DELETE FROM source_artifacts WHERE bundle_hash = $1`, bundleA); err != nil {
					t.Fatalf("delete persisted source artifact: %v", err)
				}
			}

			fact, err := runtimecorrelation.NewSourceArtifactFact(tc.contextHash)
			if err != nil {
				t.Fatal(err)
			}
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(tc.contextHash), runID)
			ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
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

func insertMutationLogPrivateAdapter(ctx context.Context, selected any, record runtimemutationlog.Record) error {
	return runSelectedFixtureMutation(ctx, selected, "test mutation log adapter", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			switch store := selected.(type) {
			case *PostgresStore:
				return privatemutationlog.Insert(txctx, attempt, postgresActiveRunSourceOwner(store, tx), record)
			case *SQLiteRuntimeStore:
				owner := storerunfork.ActiveRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
					return store.runLifecycleSQLiteOwner.RequireActiveSourceTx(ctx, tx, runID)
				})
				return privatemutationlog.InsertSQLite(txctx, attempt, owner, record)
			default:
				return fmt.Errorf("mutation log fixture store %T is unsupported", selected)
			}
		})
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
