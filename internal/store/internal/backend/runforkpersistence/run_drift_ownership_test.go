package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
	"github.com/google/uuid"
)

func insertRunDriftHeader(t *testing.T, db *sql.DB, run, entity, path string, entityType any, stage string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO flow_instances VALUES($1,$2,$3,$4,'worker',$5,1,'{}','{}','{}','terminated')`, run, entity, path, entityType, stage); err != nil {
		t.Fatal(err)
	}
}

func insertRunDriftLifecycle(t *testing.T, db *sql.DB, run, entity, stage string) {
	t.Helper()
	value, err := json.Marshal(stage)
	if err != nil {
		t.Fatal(err)
	}
	insertRunDriftMutation(t, db, run, runForkRevisionEntityMutation{MutationID: uuid.NewString(), EntityID: entity,
		Domain: "lifecycle_state", NewValue: value, CreatedAt: time.Now().UTC()}, 1)
}

func TestVerifyRunCanonicalMembershipBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, run, fielded := runDriftDatabase(t, backend)
			insertRunDriftHeader(t, db, run, fielded, fielded, "company", "queued")
			child := uuid.NewString()
			for _, member := range []struct{ entity, path string }{{run, run}, {child, "worker/child"}} {
				insertRunDriftHeader(t, db, run, member.entity, member.path, nil, "done")
				insertRunDriftLifecycle(t, db, run, member.entity, "done")
			}
			report, err := inspectRunDriftTest(t, db, run)
			if err != nil || report.EntitiesChecked != 3 || len(report.Rows) != 0 {
				t.Fatalf("mixed fielded/fieldless final non-active members lost: %+v %v", report, err)
			}
			if _, err := db.Exec(`DELETE FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, run, child); err != nil {
				t.Fatal(err)
			}
			report, err = inspectRunDriftTest(t, db, run)
			if err != nil || len(report.Rows) != 1 || report.Rows[0].EntityID != child || report.Rows[0].Kind != "entity_presence" || !report.Rows[0].FoldedPresent || report.Rows[0].StoredPresent {
				t.Fatalf("history-only constructed member disappeared: %+v %v", report, err)
			}
			added := uuid.NewString()
			insertRunDriftHeader(t, db, run, added, "worker/added", nil, "done")
			report, err = inspectRunDriftTest(t, db, run)
			if err != nil || report.EntitiesChecked != 4 || len(report.Rows) != 2 {
				t.Fatalf("unlogged current member disappeared: %+v %v", report, err)
			}
			found := false
			for _, row := range report.Rows {
				if row.EntityID == added && row.Kind == "entity_presence" && !row.FoldedPresent && row.StoredPresent {
					found = true
				}
			}
			if !found {
				t.Fatal("added header did not produce its exact presence mismatch")
			}
		})
	}
}

func TestVerifyRunCanonicalHeaderDomainsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, domain := range []mutationlog.Domain{mutationlog.DomainLifecycleState, mutationlog.DomainBookkeeping, mutationlog.DomainGate, mutationlog.DomainAccumulator, mutationlog.DomainAuthoredField} {
				t.Run(string(domain), func(t *testing.T) {
					db, run, entity := runDriftDatabase(t, backend)
					insertRunDriftHeader(t, db, run, entity, entity, "company", "queued")
					if _, err := db.Exec(`UPDATE entity_state SET current_state='obsolete',bookkeeping='[]',gates='null',accumulator='7' WHERE run_id=$1`, run); err != nil {
						t.Fatal(err)
					}
					report, err := inspectRunDriftTest(t, db, run)
					if err != nil || report.EntitiesChecked != 1 || len(report.Rows) != 0 {
						t.Fatalf("obsolete copies overrode/vetoed canonical header: %+v %v", report, err)
					}
					// Restore copies to history: an unlogged header change must still win.
					if _, err := db.Exec(`UPDATE entity_state SET current_state='queued',bookkeeping='{}',gates='{}',accumulator='{}' WHERE run_id=$1`, run); err != nil {
						t.Fatal(err)
					}
					table, column, value, path := "flow_instances", "current_state", "done", ""
					switch domain {
					case mutationlog.DomainBookkeeping:
						column, value, path = "bookkeeping", `{"node.key":"bypass"}`, "node.key"
					case mutationlog.DomainGate:
						column, value, path = "gates", `{"node.key":"bypass"}`, "node.key"
					case mutationlog.DomainAccumulator:
						column, value, path = "accumulator", `{"node.key":"bypass"}`, "node.key"
					case mutationlog.DomainAuthoredField:
						table, column, value, path = "entity_state", "fields", `{"value":"bypass"}`, "value"
					}
					if _, err := db.Exec(`UPDATE `+table+` SET `+column+`=$1 WHERE run_id=$2 AND entity_id=$3`, value, run, entity); err != nil {
						t.Fatal(err)
					}
					report, err = inspectRunDriftTest(t, db, run)
					if err != nil || len(report.Rows) != 1 || report.Rows[0].EntityID != entity || report.Rows[0].Domain != domain || report.Rows[0].Path == nil || *report.Rows[0].Path != path {
						t.Fatalf("canonical domain bypass missed: %+v %v", report, err)
					}
				})
			}
		})
	}
}

func TestVerifyRunConstructedPairAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, cut := range []string{"missing_fields", "fieldless_fields", "wrong_entity", "wrong_path", "wrong_type", "conflicting_fields", "invalid_header_json", "invalid_fields_json", "invalid_revision", "empty_declared_type"} {
				t.Run(cut, func(t *testing.T) {
					db, run, entity := runDriftDatabase(t, backend)
					insertRunDriftHeader(t, db, run, entity, entity, "company", "queued")
					query := map[string]string{
						"missing_fields":      `DELETE FROM entity_state WHERE run_id=$1`,
						"fieldless_fields":    `UPDATE flow_instances SET entity_type=NULL WHERE run_id=$1`,
						"wrong_entity":        `UPDATE entity_state SET entity_id='00000000-0000-0000-0000-000000000099' WHERE run_id=$1`,
						"wrong_path":          `UPDATE entity_state SET flow_instance='foreign/route' WHERE run_id=$1`,
						"wrong_type":          `UPDATE entity_state SET entity_type='foreign' WHERE run_id=$1`,
						"conflicting_fields":  `INSERT INTO entity_state SELECT run_id,'00000000-0000-0000-0000-000000000099',current_state,fields,bookkeeping,gates,accumulator,flow_instance,entity_type FROM entity_state WHERE run_id=$1`,
						"invalid_header_json": `UPDATE flow_instances SET bookkeeping='[]' WHERE run_id=$1`,
						"invalid_fields_json": `UPDATE entity_state SET fields='[]' WHERE run_id=$1`,
						"invalid_revision":    `UPDATE flow_instances SET revision=0 WHERE run_id=$1`,
						"empty_declared_type": `UPDATE flow_instances SET entity_type='' WHERE run_id=$1`,
					}[cut]
					if _, err := db.Exec(query, run); err != nil {
						t.Fatal(err)
					}
					report, err := inspectRunDriftTest(t, db, run)
					var history *mutationlog.HistoryError
					if !errors.As(err, &history) || history.Code != "invalid_entity_projection" || history.RunID != run || history.EntityID != entity || report.EntitiesChecked != 0 || len(report.Rows) != 0 {
						t.Fatalf("malformed pair became clean/drift or lost coordinates: %+v %v", report, err)
					}
					// The writer and reader must reject the same malformed pairing.
					tx, err := db.BeginTx(context.Background(), nil)
					if err != nil {
						t.Fatal(err)
					}
					_, _, mutationErr := workflowheader.LoadForMutation(context.Background(), tx, backend == "postgres", run, entity, entity)
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					var projection *workflowheader.ProjectionError
					if !errors.As(mutationErr, &projection) || projection.EntityID != entity {
						t.Fatalf("mutation reader admission diverged: %v", mutationErr)
					}
				})
			}
		})
	}
}

func TestVerifyRunSharedHeaderReadScopeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, run, entity := runDriftDatabase(t, backend)
			insertRunDriftHeader(t, db, run, entity, entity, "company", "queued")
			tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, _, err = workflowheader.LoadForRead(ctx, tx, run, entity, entity)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled exact read lost cause: %v", err)
			}
			_, err = workflowheader.InventoryForRead(ctx, tx, run)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled inventory lost cause: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			_, _, err = workflowheader.LoadForRead(context.Background(), tx, run, entity, entity)
			if !errors.Is(err, sql.ErrTxDone) {
				t.Fatalf("escaped exact read became usable: %v", err)
			}
			_, err = workflowheader.InventoryForRead(context.Background(), tx, run)
			if !errors.Is(err, sql.ErrTxDone) {
				t.Fatalf("escaped inventory became usable: %v", err)
			}
		})
	}
}

func TestVerifyRunStateOnlyDoesNotAcquireConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, run, entity := runDriftDatabase(t, backend)
			tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			inventory, err := workflowheader.InventoryForRead(context.Background(), tx, run)
			if err != nil || len(inventory.Constructed) != 0 || len(inventory.StateOnly) != 1 || inventory.StateOnly[0].EntityID != entity {
				t.Fatalf("imported state classified as constructed: %+v %v", inventory, err)
			}
			_, found, err := workflowheader.LoadForRead(context.Background(), tx, run, entity, entity)
			if err != nil || found {
				t.Fatalf("state-only acquired receiver authority: found=%t err=%v", found, err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			report, err := inspectRunDriftTest(t, db, run)
			if err != nil || len(report.Rows) != 0 || report.EntitiesChecked != 1 {
				t.Fatalf("valid state-only import lost: %+v %v", report, err)
			}
		})
	}
}

func TestVerifyRunSharedHeaderReadPreservesMutationLocks(t *testing.T) {
	db, run, entity := runDriftDatabase(t, "postgres")
	insertRunDriftHeader(t, db, run, entity, entity, "company", "queued")
	for _, mutation := range []bool{false, true} {
		for _, table := range []string{"flow_instances", "entity_state"} {
			tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: !mutation})
			if err != nil {
				t.Fatal(err)
			}
			var found bool
			if mutation {
				_, found, err = workflowheader.LoadForMutation(context.Background(), tx, true, run, entity, entity)
			} else {
				_, found, err = workflowheader.LoadForRead(context.Background(), tx, run, entity, entity)
			}
			if err != nil || !found {
				_ = tx.Rollback()
				t.Fatalf("load mutation=%t: found=%t err=%v", mutation, found, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, writeErr := db.ExecContext(ctx, `UPDATE `+table+` SET current_state=current_state WHERE run_id=$1 AND entity_id=$2`, run, entity)
			cancel()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if mutation && writeErr == nil {
				t.Fatalf("mutation reader no longer locks %s", table)
			}
			if !mutation && writeErr != nil {
				t.Fatalf("read-only projection acquired mutation lock on %s: %v", table, writeErr)
			}
		}
	}
}
