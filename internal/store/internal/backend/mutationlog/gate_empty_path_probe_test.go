package mutationlog

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

// An isolated query-only schema deliberately lacks the constraint. Its accepted
// row proves the probe reports nil, not synthetic refusal, and is rolled back.
func TestGateEmptyPathProbeReportsActualAcceptanceAndExactPayloadBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			var db *sql.DB
			var err error
			if postgres {
				_, native, cleanup := testutil.StartEmptyPostgres(t)
				t.Cleanup(cleanup)
				db = native
			} else {
				db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "query-only.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Errorf("close query-only schema: %v", err)
					}
				})
			}
			mutationSchema := `CREATE TABLE entity_mutations (mutation_id TEXT,run_id TEXT,entity_id TEXT,domain TEXT,path TEXT,old_value TEXT,new_value TEXT,writer_type TEXT,writer_id TEXT,handler_step TEXT)`
			if postgres {
				mutationSchema = `CREATE TABLE entity_mutations (mutation_id UUID DEFAULT gen_random_uuid(),run_id UUID,entity_id UUID,domain TEXT,path TEXT,old_value JSONB,new_value JSONB,writer_type TEXT,writer_id TEXT,handler_step TEXT)`
			}
			for _, statement := range []string{`CREATE TABLE entity_state (run_id TEXT,entity_id TEXT)`, mutationSchema} {
				if _, err := db.Exec(statement); err != nil {
					t.Fatal(err)
				}
			}
			run, entity := uuid.NewString(), uuid.NewString()
			if _, err := db.Exec(`INSERT INTO entity_state VALUES ($1,$2)`, run, entity); err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err := ProbeEmptyGatePathForTest(context.Background(), tx, postgres, uuid.NewString(), entity); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("foreign run was not exact entity absence: %v", err)
			}
			if err := ProbeEmptyGatePathForTest(context.Background(), tx, postgres, run, entity); err != nil {
				t.Fatalf("accepted malformed write was incorrectly reported as refusal: %v", err)
			}
			var actualRun, actualEntity, domain, path, old, value, writer, id, step string
			if err := tx.QueryRow(`SELECT CAST(run_id AS TEXT),CAST(entity_id AS TEXT),domain,path,CAST(old_value AS TEXT),CAST(new_value AS TEXT),writer_type,writer_id,handler_step FROM entity_mutations`).Scan(&actualRun, &actualEntity, &domain, &path, &old, &value, &writer, &id, &step); err != nil {
				t.Fatal(err)
			}
			if actualRun != run || actualEntity != entity || domain != "gate" || path != "" || old != "null" || value != "true" || writer != "platform" || id != "test" || step != "seed" {
				t.Fatalf("exact original malformed gate write changed: %s/%s %q/%q %s->%s %s/%s/%s", actualRun, actualEntity, domain, path, old, value, writer, id, step)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			var remaining int
			if err := db.QueryRow(`SELECT COUNT(*) FROM entity_mutations`).Scan(&remaining); err != nil || remaining != 0 {
				t.Fatalf("accepted probe retained corrupt history: %d err=%v", remaining, err)
			}
		})
	}
}
