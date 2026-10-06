package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestReceiverJoinedInventoryPreservesExactPhysicalOracleBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			runID, sibling := seedReceiverJoinedInventoryStorage(t, fixture, ctx)
			want := receiverJoinedInventoryStorageOracle(t, ctx, fixture.db, runID)
			if len(want) != 3 || want[0].CurrentState != "active" || want[1].CurrentState != "done" || want[2].CurrentState != "queued" {
				t.Fatalf("independent join fixture lost ordering or terminal/static history: %+v", want)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := ReadReceiverJoinedInventoryStorageForTest(ctx, fixture.store, runID)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("exact join, header state, raw field bytes or ordering changed: got=%+v want=%+v err=%v", got, want, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("receiver inventory escaped the original joined read owner: %+v", counts)
			}
			restore()
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("receiver inventory mutated storage: err=%v", err)
			}
			got[0].Fields = "changed detached evidence"
			if again, err := ReadReceiverJoinedInventoryStorageForTest(ctx, fixture.store, runID); err != nil || !reflect.DeepEqual(again, want) {
				t.Fatalf("caller changed persisted inventory: %+v %v", again, err)
			}
			if got, err := ReadReceiverJoinedInventoryStorageForTest(ctx, fixture.store, sibling); err != nil || len(got) != 1 || got[0].FlowInstance != "inventory/foreign" {
				t.Fatalf("foreign run was not isolated: %+v %v", got, err)
			}
			if got, err := ReadReceiverJoinedInventoryStorageForTest(ctx, fixture.store, uuid.NewString()); err != nil || got != nil {
				t.Fatalf("missing inventory fabricated rows: %+v %v", got, err)
			}
			// Deliberately duplicate one physical header in an independent view.
			// The reader must preserve the join's multiplicity, not hide it.
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `ALTER TABLE flow_instances RENAME TO retained_inventory_headers`); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `CREATE VIEW flow_instances AS SELECT * FROM retained_inventory_headers
					UNION ALL SELECT * FROM retained_inventory_headers WHERE instance_path='inventory/active'`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			restored := false
			restoreHeader := func() error {
				if restored {
					return nil
				}
				err := runUnrevisionedEventFixtureTransactionForTest(context.WithoutCancel(ctx), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					if _, err := tx.ExecContext(ctx, `DROP VIEW flow_instances`); err != nil {
						return err
					}
					_, err := tx.ExecContext(ctx, `ALTER TABLE retained_inventory_headers RENAME TO flow_instances`)
					return err
				})
				if err == nil {
					restored = true
				}
				return err
			}
			defer func() {
				if err := restoreHeader(); err != nil {
					t.Error(err)
				}
			}()
			duplicateWant := receiverJoinedInventoryStorageOracle(t, ctx, fixture.db, runID)
			duplicates, err := ReadReceiverJoinedInventoryStorageForTest(ctx, fixture.store, runID)
			if err != nil || len(duplicates) != 4 || !reflect.DeepEqual(duplicates, duplicateWant) || duplicates[0] != duplicates[1] {
				t.Fatalf("duplicate-owner cardinality was hidden: got=%+v want=%+v err=%v", duplicates, duplicateWant, err)
			}
			if err := restoreHeader(); err != nil {
				t.Fatal(err)
			}
			if after, err := ReadReceiverJoinedInventoryStorageForTest(ctx, fixture.store, runID); err != nil || !reflect.DeepEqual(after, want) {
				t.Fatalf("duplicate control did not restore original inventory: %+v %v", after, err)
			}
		})
	}
}

func TestReceiverJoinedInventoryRefusesInvalidCancelledClosedAndPartialEvidenceBothStores(t *testing.T) {
	ctx, runID := context.Background(), "abcdef01-1111-4111-8111-111111111111"
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadReceiverJoinedInventoryStorageForTest(ctx, selected, runID); err == nil || got != nil {
			t.Fatalf("invalid original owner leaked rows: %+v %v", got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			runID, _ := seedReceiverJoinedInventoryStorage(t, fixture, ctx)
			want := receiverJoinedInventoryStorageOracle(t, ctx, fixture.db, runID)
			for _, invalid := range []string{"", "bad", uuid.Nil.String(), strings.ToUpper("abcdef01-1111-4111-8111-111111111111"), " " + runID} {
				if got, err := ReadReceiverJoinedInventoryStorageForTest(ctx, fixture.store, invalid); err == nil || got != nil {
					t.Fatalf("invalid run identity leaked rows: %+v %v", got, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadReceiverJoinedInventoryStorageForTest(cancelled, fixture.store, runID); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled inventory leaked evidence: %+v %v", got, err)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `ALTER TABLE entity_state RENAME TO retained_inventory_fields`); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE VIEW entity_state AS
					SELECT run_id,entity_id,flow_instance,entity_type,
					CASE WHEN entity_id='%s' THEN NULL ELSE fields END AS fields FROM retained_inventory_fields`, want[len(want)-1].EntityID))
				return err
			}); err != nil {
				t.Fatal(err)
			}
			restored := false
			restoreFields := func() error {
				if restored {
					return nil
				}
				err := runUnrevisionedEventFixtureTransactionForTest(context.WithoutCancel(ctx), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					if _, err := tx.ExecContext(ctx, `DROP VIEW entity_state`); err != nil {
						return err
					}
					_, err := tx.ExecContext(ctx, `ALTER TABLE retained_inventory_fields RENAME TO entity_state`)
					return err
				})
				if err == nil {
					restored = true
				}
				return err
			}
			defer func() {
				if err := restoreFields(); err != nil {
					t.Error(err)
				}
			}()
			if got, err := ReadReceiverJoinedInventoryStorageForTest(ctx, fixture.store, runID); err == nil || got != nil || !strings.Contains(strings.ToLower(err.Error()), "null") {
				t.Fatalf("late NULL scan returned partial evidence: %+v %v", got, err)
			}
			if err := restoreFields(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadReceiverJoinedInventoryStorageForTest(ctx, fixture.store, runID); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("failed observation changed original rows: %+v %v", got, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadReceiverJoinedInventoryStorageForTest(ctx, fixture.store, runID); err == nil || got != nil {
				t.Fatalf("closed original owner leaked rows: %+v %v", got, err)
			}
		})
	}
}

// Frozen original receiver join, independent of the new pipeline reader.
func receiverJoinedInventoryStorageOracle(t *testing.T, ctx context.Context, db *sql.DB, runID string) []ReceiverJoinedInventoryStorageRow {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT es.entity_id,es.flow_instance,es.entity_type,fi.current_state,CAST(es.fields AS TEXT)
		FROM entity_state es JOIN flow_instances fi ON fi.run_id=es.run_id AND fi.entity_id=es.entity_id AND fi.instance_path=es.flow_instance
		WHERE es.run_id=$1 ORDER BY es.entity_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []ReceiverJoinedInventoryStorageRow
	for rows.Next() {
		var row ReceiverJoinedInventoryStorageRow
		if err := rows.Scan(&row.EntityID, &row.FlowInstance, &row.EntityType, &row.CurrentState, &row.Fields); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func seedReceiverJoinedInventoryStorage(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context) (string, string) {
	t.Helper()
	runID, sibling := uuid.NewString(), uuid.NewString()
	seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	seedAuthorActivityReceiptRun(t, fixture, ctx, sibling)
	now := time.Now().UTC()
	rows := []struct {
		entity, entityPath, headerRun, headerEntity, headerPath, state string
	}{
		{"10000000-1111-4111-8111-111111111111", "inventory/active", runID, "10000000-1111-4111-8111-111111111111", "inventory/active", "active"},
		{"20000000-1111-4111-8111-111111111111", "inventory/terminal", runID, "20000000-1111-4111-8111-111111111111", "inventory/terminal", "done"},
		{"30000000-1111-4111-8111-111111111111", "inventory/static", runID, "30000000-1111-4111-8111-111111111111", "inventory/static", "queued"},
		{"40000000-1111-4111-8111-111111111111", "inventory/wrong-path", runID, "40000000-1111-4111-8111-111111111111", "inventory/other-path", "active"},
		{"50000000-1111-4111-8111-111111111111", "inventory/wrong-entity", runID, "51000000-1111-4111-8111-111111111111", "inventory/wrong-entity", "active"},
		{"70000000-1111-4111-8111-111111111111", "inventory/wrong-run", sibling, "70000000-1111-4111-8111-111111111111", "inventory/wrong-run", "active"},
	}
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
		for _, row := range rows {
			if _, err := tx.ExecContext(ctx, `INSERT INTO entity_state
				(run_id,entity_id,flow_instance,entity_type,current_state,fields,bookkeeping,revision)
				VALUES($1,$2,$3,'inventory_entity','stale-business-state',$4,'{}',7)`, runID, row.entity, row.entityPath,
				`{"integer":7,"decimal":7.0,"text":"exact bytes","nested":[null,{"flag":true}]}`); err != nil {
				return err
			}
			status, ended := "active", any(nil)
			if row.state == "done" {
				status, ended = "terminated", now
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO flow_instances
				(run_id,entity_id,instance_path,current_state,flow_template,mode,status,config,stage_defined,
				gates,bookkeeping,accumulator,revision,entered_state_at,created_at,updated_at,terminated_at)
				VALUES($1,$2,$3,$4,'inventory','static',$5,'{}',TRUE,'{}','{}','{}',1,$6,$6,$6,$7)`,
				row.headerRun, row.headerEntity, row.headerPath, row.state, status, now, ended); err != nil {
				return err
			}
		}
		entity := "60000000-1111-4111-8111-111111111111"
		if _, err := tx.ExecContext(ctx, `INSERT INTO entity_state
			(run_id,entity_id,flow_instance,entity_type,current_state,fields,bookkeeping,revision)
			VALUES($1,$2,'inventory/foreign','inventory_entity','queued','{}','{}',1)`, sibling, entity); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO flow_instances
			(run_id,entity_id,instance_path,current_state,flow_template,mode,status,config,stage_defined,
			gates,bookkeeping,accumulator,revision,entered_state_at,created_at,updated_at)
			VALUES($1,$2,'inventory/foreign','queued','inventory','static','active','{}',TRUE,'{}','{}','{}',1,$3,$3,$3)`, sibling, entity, now); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE runs SET status='completed' WHERE run_id=$1`, runID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return runID, sibling
}
