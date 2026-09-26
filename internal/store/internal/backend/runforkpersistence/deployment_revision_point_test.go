package runforkpersistence

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestDeploymentForkPointSelectsExactCommittedRevisionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, opened, cleanup := testutil.StartEmptyPostgres(t)
				db = opened
				t.Cleanup(cleanup)
			} else {
				var err error
				db, err = sql.Open("sqlite", ":memory:")
				if err != nil {
					t.Fatal(err)
				}
				db.SetMaxOpenConns(1)
				t.Cleanup(func() { _ = db.Close() })
			}
			for _, ddl := range []string{
				`CREATE TABLE runs (run_id TEXT PRIMARY KEY, origin_kind TEXT NOT NULL)`,
				`CREATE TABLE run_fork_revision_heads (run_id TEXT PRIMARY KEY, last_revision BIGINT NOT NULL)`,
				`CREATE TABLE run_fork_revisions (run_id TEXT, revision BIGINT, PRIMARY KEY(run_id,revision))`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			runID := uuid.NewString()
			if _, err := db.Exec(`INSERT INTO runs VALUES($1,'deployment')`, runID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO run_fork_revision_heads VALUES($1,2)`, runID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO run_fork_revisions VALUES($1,1),($1,2)`, runID); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			point, err := resolveRunForkRevisionPoint(ctx, tx, runID, "")
			if err != nil {
				t.Fatal(err)
			}
			if point.Kind != runfork.RunForkPointDeploymentRevision || point.Revision != 2 || point.EventID != "" {
				t.Fatalf("wrong eventless point: %+v", point)
			}
			fixed, err := resolveFixedRunForkRevisionPoint(ctx, tx, runID,
				runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 1}, resolveRunForkRevisionPoint)
			if err != nil || fixed.Kind != runfork.RunForkPointDeploymentRevision || fixed.Revision != 1 {
				t.Fatalf("materialized retry resolved latest instead of R1: %+v err=%v", fixed, err)
			}
			if _, err := resolveFixedRunForkRevisionPoint(ctx, tx, runID,
				runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 3}, resolveRunForkRevisionPoint); err == nil {
				t.Fatal("uncommitted deployment revision was accepted")
			}
		})
	}
}
