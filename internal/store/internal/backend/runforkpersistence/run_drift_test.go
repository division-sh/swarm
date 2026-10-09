package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func runDriftDatabase(t *testing.T, backend string) (*sql.DB, string, string) {
	t.Helper()
	db := historicalSnapshotDatabase(t, backend)
	jsonType := "TEXT"
	if backend == "postgres" {
		jsonType = "JSONB"
	}
	for _, query := range []string{
		`CREATE TABLE run_fork_revision_heads(run_id TEXT PRIMARY KEY,last_revision BIGINT NOT NULL)`,
		`CREATE TABLE run_fork_revisions(run_id TEXT NOT NULL,revision BIGINT NOT NULL,PRIMARY KEY(run_id,revision))`,
		fmt.Sprintf(`CREATE TABLE entity_mutations(mutation_id TEXT PRIMARY KEY,run_id TEXT,entity_id TEXT,domain TEXT,path TEXT,new_value %s,created_at TIMESTAMP)`, jsonType),
		fmt.Sprintf(`CREATE TABLE entity_state(run_id TEXT,entity_id TEXT,current_state TEXT,fields %s,bookkeeping %s,gates %s,accumulator %s,PRIMARY KEY(run_id,entity_id))`, jsonType, jsonType, jsonType, jsonType),
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	run, entity := uuid.NewString(), uuid.NewString()
	requireHistoricalSnapshotRun(t, db, run)
	for _, query := range []string{`INSERT INTO run_fork_revision_heads VALUES($1,1)`, `INSERT INTO run_fork_revisions VALUES($1,1)`} {
		if _, err := db.Exec(query, run); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO entity_state VALUES($1,$2,'queued','{"value":"original"}','{}','{}','{}')`, run, entity); err != nil {
		t.Fatal(err)
	}
	for _, row := range []runForkRevisionEntityMutation{
		{MutationID: uuid.NewString(), EntityID: entity, Domain: "lifecycle_state", NewValue: json.RawMessage(`"queued"`), CreatedAt: time.Now().UTC()},
		{MutationID: uuid.NewString(), EntityID: entity, Domain: "authored_field", Path: "value", NewValue: json.RawMessage(`"original"`), CreatedAt: time.Now().UTC()},
	} {
		insertRunDriftMutation(t, db, run, row, 1)
	}
	return db, run, entity
}

func insertRunDriftMutation(t *testing.T, db *sql.DB, run string, row runForkRevisionEntityMutation, revision int64) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO entity_mutations VALUES($1,$2,$3,$4,$5,$6,$7)`, row.MutationID, run, row.EntityID, row.Domain, row.Path, string(row.NewValue), row.CreatedAt); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO run_fork_fact_revisions VALUES($1,'entity_mutations',$2,$3,$4,true)`, run, row.MutationID, revision, string(body)); err != nil {
		t.Fatal(err)
	}
}

func inspectRunDriftTest(t *testing.T, db *sql.DB, run string) (mutationlog.DriftReport, error) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	}()
	return inspectRunMutationDrift(context.Background(), tx, run)
}

func TestVerifyRunMutationDriftBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, domain := range []mutationlog.Domain{mutationlog.DomainAuthoredField, mutationlog.DomainLifecycleState, mutationlog.DomainBookkeeping, mutationlog.DomainGate, mutationlog.DomainAccumulator} {
				t.Run(string(domain), func(t *testing.T) {
					db, run, entity := runDriftDatabase(t, backend)
					clean, err := inspectRunDriftTest(t, db, run)
					if err != nil || len(clean.Rows) != 0 || clean.EntitiesChecked != 1 {
						t.Fatalf("clean=%+v err=%v", clean, err)
					}
					column := map[mutationlog.Domain]string{mutationlog.DomainAuthoredField: "fields", mutationlog.DomainLifecycleState: "current_state", mutationlog.DomainBookkeeping: "bookkeeping", mutationlog.DomainGate: "gates", mutationlog.DomainAccumulator: "accumulator"}[domain]
					value := `{"node.key":"bypass"}`
					wantPath := "node.key"
					if domain == mutationlog.DomainAuthoredField {
						value = `{"value":"bypass"}`
						wantPath = "value"
					}
					if domain == mutationlog.DomainLifecycleState {
						value = "done"
						wantPath = ""
					}
					if _, err := db.Exec(`UPDATE entity_state SET `+column+`=$1 WHERE run_id=$2 AND entity_id=$3`, value, run, entity); err != nil {
						t.Fatal(err)
					}
					got, err := inspectRunDriftTest(t, db, run)
					if err != nil || len(got.Rows) != 1 || got.Rows[0].EntityID != entity || got.Rows[0].Domain != domain || *got.Rows[0].Path != wantPath {
						t.Fatalf("got=%+v err=%v", got, err)
					}
				})
			}
		})
	}
}

func TestVerifyRunHistoryAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, cut := range []string{"changed_physical", "deleted_physical", "missing_order", "uncommitted_order", "tombstone", "foreign_order"} {
				t.Run(cut, func(t *testing.T) {
					db, run, _ := runDriftDatabase(t, backend)
					var mutation string
					if err := db.QueryRow(`SELECT mutation_id FROM entity_mutations WHERE run_id=$1 AND domain='authored_field'`, run).Scan(&mutation); err != nil {
						t.Fatal(err)
					}
					query := map[string]string{
						"changed_physical":  `UPDATE entity_mutations SET new_value='"changed"' WHERE mutation_id=$1`,
						"deleted_physical":  `DELETE FROM entity_mutations WHERE mutation_id=$1`,
						"missing_order":     `DELETE FROM run_fork_fact_revisions WHERE fact_key=$1`,
						"uncommitted_order": `UPDATE run_fork_fact_revisions SET revision=2 WHERE fact_key=$1`,
						"tombstone":         `UPDATE run_fork_fact_revisions SET present=false WHERE fact_key=$1`,
						"foreign_order":     `UPDATE run_fork_fact_revisions SET run_id='00000000-0000-0000-0000-000000000001' WHERE fact_key=$1`,
					}[cut]
					if _, err := db.Exec(query, mutation); err != nil {
						t.Fatal(err)
					}
					got, err := inspectRunDriftTest(t, db, run)
					if cut == "changed_physical" {
						if err != nil || len(got.Rows) != 1 || got.Rows[0].FoldedValue != "changed" {
							t.Fatalf("ledger substituted physical value: %+v %v", got, err)
						}
						return
					}
					var history *mutationlog.HistoryError
					if !errors.As(err, &history) || history.RunID != run || history.MutationID != mutation || len(got.Rows) != 0 {
						t.Fatalf("cut=%s report=%+v error=%v", cut, got, err)
					}
				})
			}
		})
	}
}

func TestVerifyRunSharedForkOrderBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, run, entity := runDriftDatabase(t, backend)
			if _, err := db.Exec(`INSERT INTO run_fork_revisions VALUES($1,2)`, run); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE run_fork_revision_heads SET last_revision=2 WHERE run_id=$1`, run); err != nil {
				t.Fatal(err)
			}
			at := time.Unix(10, 0).UTC()
			// Occurrence time is deliberately backdated. The second same-revision
			// write sorts first by UUID: characterize the existing order honestly.
			for _, row := range []runForkRevisionEntityMutation{
				{MutationID: "ffffffff-ffff-ffff-ffff-ffffffffffff", EntityID: entity, Domain: "authored_field", Path: "value", NewValue: json.RawMessage(`"first-occurrence"`), CreatedAt: at},
				{MutationID: "00000000-0000-0000-0000-000000000001", EntityID: entity, Domain: "authored_field", Path: "value", NewValue: json.RawMessage(`"last-occurrence"`), CreatedAt: at.Add(time.Second)},
			} {
				insertRunDriftMutation(t, db, run, row, 2)
			}
			if _, err := db.Exec(`UPDATE entity_state SET fields='{"value":"last-occurrence"}' WHERE run_id=$1`, run); err != nil {
				t.Fatal(err)
			}
			got, err := inspectRunDriftTest(t, db, run)
			if err != nil || len(got.Rows) != 1 || got.Rows[0].FoldedValue != "first-occurrence" {
				t.Fatalf("order changed/hidden: %+v %v", got, err)
			}
		})
	}
}

func TestVerifyRunMissingAndEmptyExistingRunBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, _, _ := runDriftDatabase(t, backend)
			run := uuid.NewString()
			got, err := inspectRunDriftTest(t, db, run)
			var missing *runlifecycle.RunNotFoundError
			if !errors.As(err, &missing) || missing.RunID != run || len(got.Rows) != 0 {
				t.Fatalf("missing=%+v %v", got, err)
			}
			requireHistoricalSnapshotRun(t, db, run)
			got, err = inspectRunDriftTest(t, db, run)
			if err != nil || got.EntitiesChecked != 0 || len(got.Rows) != 0 {
				t.Fatalf("empty existing run=%+v %v", got, err)
			}
		})
	}
}

func TestVerifyRunSnapshotIsolationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, run, entity := runDriftDatabase(t, backend)
			ctx := context.Background()
			snapshot, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := snapshot.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
					t.Error(err)
				}
			}()
			before, err := inspectRunMutationDrift(ctx, snapshot, run)
			if err != nil || len(before.Rows) != 0 {
				t.Fatalf("before=%+v %v", before, err)
			}
			writer, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := writer.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
					t.Error(err)
				}
			}()
			if _, err := writer.Exec(`INSERT INTO run_fork_revisions VALUES($1,2)`, run); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Exec(`UPDATE run_fork_revision_heads SET last_revision=2 WHERE run_id=$1`, run); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Exec(`UPDATE entity_state SET fields='{"value":"next"}' WHERE run_id=$1 AND entity_id=$2`, run, entity); err != nil {
				t.Fatal(err)
			}
			added := uuid.NewString()
			if _, err := writer.Exec(`INSERT INTO entity_state VALUES($1,$2,'queued','{}','{}','{}','{}')`, run, added); err != nil {
				t.Fatal(err)
			}
			for _, row := range []runForkRevisionEntityMutation{
				{MutationID: uuid.NewString(), EntityID: entity, Domain: "authored_field", Path: "value", NewValue: json.RawMessage(`"next"`), CreatedAt: time.Now().UTC()},
				{MutationID: uuid.NewString(), EntityID: added, Domain: "lifecycle_state", NewValue: json.RawMessage(`"queued"`), CreatedAt: time.Now().UTC()},
			} {
				if _, err := writer.Exec(`INSERT INTO entity_mutations VALUES($1,$2,$3,$4,$5,$6,$7)`, row.MutationID, run, row.EntityID, row.Domain, row.Path, string(row.NewValue), row.CreatedAt); err != nil {
					t.Fatal(err)
				}
				body, err := json.Marshal(row)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := writer.Exec(`INSERT INTO run_fork_fact_revisions VALUES($1,'entity_mutations',$2,2,$3,true)`, run, row.MutationID, string(body)); err != nil {
					t.Fatal(err)
				}
			}
			if err := writer.Commit(); err != nil {
				t.Fatal(err)
			}
			retained, err := inspectRunMutationDrift(ctx, snapshot, run)
			if err != nil || retained.EntitiesChecked != 1 || len(retained.Rows) != 0 {
				t.Fatalf("mixed snapshot: %+v %v", retained, err)
			}
			if err := snapshot.Rollback(); err != nil {
				t.Fatal(err)
			}
			fresh, err := inspectRunDriftTest(t, db, run)
			if err != nil || fresh.EntitiesChecked != 2 || len(fresh.Rows) != 0 {
				t.Fatalf("new snapshot lost update/membership: %+v %v", fresh, err)
			}
		})
	}
}

func TestVerifyRunEntityMembershipBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, cut := range []string{"state_only", "history_only", "present_null"} {
				t.Run(cut, func(t *testing.T) {
					db, run, entity := runDriftDatabase(t, backend)
					switch cut {
					case "state_only":
						entity = uuid.NewString()
						if _, err := db.Exec(`INSERT INTO entity_state VALUES($1,$2,'','{}','{}','{}','{}')`, run, entity); err != nil {
							t.Fatal(err)
						}
					case "history_only":
						if _, err := db.Exec(`DELETE FROM entity_state WHERE run_id=$1 AND entity_id=$2`, run, entity); err != nil {
							t.Fatal(err)
						}
					case "present_null":
						if _, err := db.Exec(`UPDATE entity_state SET fields='{"value":"original","optional":null}' WHERE run_id=$1`, run); err != nil {
							t.Fatal(err)
						}
					}
					got, err := inspectRunDriftTest(t, db, run)
					if err != nil || len(got.Rows) != 1 || got.Rows[0].EntityID != entity {
						t.Fatalf("union/presence: %+v %v", got, err)
					}
					row := got.Rows[0]
					if cut == "present_null" {
						if row.Kind != "value" || row.FoldedPresent || !row.StoredPresent || row.StoredType != "null" {
							t.Fatalf("null became absence: %+v", row)
						}
					} else if row.Kind != "entity_presence" || row.Domain != "" || row.Path != nil {
						t.Fatalf("fieldless entity disappeared: %+v", row)
					}
				})
			}
		})
	}
}
