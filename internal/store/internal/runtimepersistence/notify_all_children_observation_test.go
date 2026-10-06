package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestNotifyAllChildrenObservationPreservesPhysicalItemMetadataAndDiagnosticScopeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			active, sibling := seedDeliveryRecoveryClaim(t, fixture, ctx), seedDeliveryRecoveryClaim(t, fixture, ctx)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, active, 2)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, sibling, 1)
			instance, now := "physical/shared-instance", time.Now().UTC().Truncate(time.Microsecond)
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				// These are private physical readback controls, not a claim of
				// lawful child issuance; the real public journey proves issuance.
				if _, err := tx.ExecContext(ctx, `UPDATE events SET event_class='child',produced_by_type='platform',event_name='portfolio/account.notify.requested',source_event_id=$1 WHERE event_id IN (SELECT event_id FROM fan_out_outcomes WHERE run_id=$2)`, active.Snapshot.EventID, active.Claim.RunID()); err != nil {
					return err
				}
				for i, run := range []string{active.Claim.RunID(), sibling.Claim.RunID()} {
					if _, err := tx.ExecContext(ctx, `INSERT INTO entity_state(run_id,entity_id,flow_instance,entity_type,current_state,revision,fields,accumulator,updated_at) VALUES($1,$2,$3,'physical','ready',1,$4,'{}',$5)`, run, uuid.NewString(), instance, []string{`{"mark":"earlier", "number":1.0}`, `{"mark":"later", "number":2.0}`}[i], now.Add(time.Duration(i)*time.Second)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := ReadNotifyAllChildrenItemStorageForTest(ctx, fixture.store, active.Claim.RunID(), active.Snapshot.EventID)
			if err != nil || len(got) != 2 {
				t.Fatalf("item rows=%d/%v", len(got), err)
			}
			query := `SELECT e.event_id::text,e.payload,e.created_at,o.ordinal FROM fan_out_outcomes o JOIN events e ON e.event_id=o.event_id AND e.run_id=o.run_id WHERE o.run_id=$1::uuid AND e.event_name=$2 AND e.source_event_id=$3::uuid AND o.outcome_kind='committed' ORDER BY o.ordinal`
			if backend.name == "sqlite" {
				query = `SELECT e.event_id,e.payload,e.created_at,o.ordinal FROM fan_out_outcomes o JOIN events e ON e.event_id=o.event_id AND e.run_id=o.run_id WHERE o.run_id=? AND e.event_name=? AND e.source_event_id=? AND o.outcome_kind='committed' ORDER BY o.ordinal`
			}
			rows, err := fixture.db.QueryContext(ctx, query, active.Claim.RunID(), "portfolio/account.notify.requested", active.Snapshot.EventID)
			if err != nil {
				t.Fatal(err)
			}
			var want []NotifyAllChildrenItemStorage
			for rows.Next() {
				var row NotifyAllChildrenItemStorage
				var raw, at any
				if err := rows.Scan(&row.ID, &raw, &at, &row.Ordinal); err != nil {
					t.Fatal(err)
				}
				switch value := raw.(type) {
				case []byte:
					row.Payload = string(value)
				case string:
					row.Payload = value
				default:
					row.Payload = fmt.Sprint(raw)
				}
				row.CreatedAt = fmt.Sprint(at)
				want = append(want, row)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) || got[0].Ordinal != 0 || got[1].Ordinal != 1 {
				t.Fatal("physical item predicate, bytes, timestamp or order changed")
			}
			metadata, err := ReadNotifyAllChildrenMetadataStorageForTest(ctx, fixture.store, instance)
			if err != nil {
				t.Fatal(err)
			}
			var raw []byte
			if err := fixture.db.QueryRowContext(ctx, `SELECT fields FROM entity_state WHERE flow_instance = $1 ORDER BY updated_at DESC LIMIT 1`, instance).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			if metadata != string(raw) {
				t.Fatal("latest whole-store metadata bytes changed")
			}
			sections, err := ReadNotifyAllChildrenDiagnosticStorageForTest(ctx, fixture.store)
			if err != nil || len(sections) != 6 {
				t.Fatalf("sections=%d/%v", len(sections), err)
			}
			queries := []string{
				`SELECT event_name, event_id, payload FROM events ORDER BY created_at, event_id`,
				`SELECT event_id, subscriber_type, subscriber_id, outcome, COALESCE(reason_code, ''), COALESCE(CAST(failure AS TEXT), '') FROM event_receipts ORDER BY event_id, subscriber_type, subscriber_id`,
				`SELECT event_id, subscriber_type, subscriber_id, status, COALESCE(reason_code, ''), COALESCE(CAST(failure AS TEXT), ''), COALESCE(CAST(delivery_target_route AS TEXT), '') FROM event_deliveries ORDER BY event_id, subscriber_type, subscriber_id`,
				`SELECT flow_instance, current_state, fields FROM entity_state ORDER BY flow_instance`,
				`SELECT run_id, instance_path, flow_template, status, config FROM flow_instances ORDER BY run_id, instance_path`,
				`SELECT original_event_id, failure FROM dead_letters ORDER BY created_at`,
			}
			for i, query := range queries {
				rows, err := fixture.db.QueryContext(ctx, query)
				if err != nil {
					t.Fatal(err)
				}
				columns, err := rows.Columns()
				if err != nil {
					t.Fatal(err)
				}
				var values [][]any
				for rows.Next() {
					row, dest := make([]any, len(columns)), make([]any, len(columns))
					for j := range row {
						dest[j] = &row[j]
					}
					if err := rows.Scan(dest...); err != nil {
						t.Fatal(err)
					}
					for j, v := range row {
						if raw, ok := v.([]byte); ok {
							row[j] = string(raw)
						}
					}
					values = append(values, row)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if err := rows.Close(); err != nil {
					t.Fatal(err)
				}
				if sections[i].Failure != "" || !reflect.DeepEqual(sections[i].Columns, columns) || !reflect.DeepEqual(sections[i].Rows, values) {
					t.Fatalf("diagnostic section %d changed", i)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 8 || counts.Total.ReadCommits != 8 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("original reader escaped: %+v", counts)
			}
			if rows, err := ReadNotifyAllChildrenItemStorageForTest(ctx, fixture.store, active.Claim.RunID(), sibling.Snapshot.EventID); err != nil || len(rows) != 0 {
				t.Fatalf("source sibling=%v/%v", rows, err)
			}
			if rows, err := ReadNotifyAllChildrenItemStorageForTest(ctx, fixture.store, sibling.Claim.RunID(), active.Snapshot.EventID); err != nil || len(rows) != 0 {
				t.Fatalf("run sibling=%v/%v", rows, err)
			}
		})
	}
}

func TestNotifyAllChildrenObservationRefusesRawCancelledClosedAndFailedSectionsBothStores(t *testing.T) {
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if rows, err := ReadNotifyAllChildrenItemStorageForTest(context.Background(), owner, uuid.NewString(), uuid.NewString()); err == nil || rows != nil {
			t.Fatalf("raw items=%v/%v", rows, err)
		}
		if raw, err := ReadNotifyAllChildrenMetadataStorageForTest(context.Background(), owner, "physical"); err == nil || raw != "" {
			t.Fatalf("raw metadata=%s/%v", raw, err)
		}
		if rows, err := ReadNotifyAllChildrenDiagnosticStorageForTest(context.Background(), owner); err == nil || rows != nil {
			t.Fatalf("raw diagnostic=%v/%v", rows, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), context.Background()
			refuse := func(ctx context.Context) {
				if rows, err := ReadNotifyAllChildrenItemStorageForTest(ctx, fixture.store, uuid.NewString(), uuid.NewString()); err == nil || rows != nil {
					t.Fatalf("failed items=%v/%v", rows, err)
				}
				if raw, err := ReadNotifyAllChildrenMetadataStorageForTest(ctx, fixture.store, "physical"); err == nil || raw != "" {
					t.Fatalf("failed metadata=%s/%v", raw, err)
				}
				sections, err := ReadNotifyAllChildrenDiagnosticStorageForTest(ctx, fixture.store)
				if err != nil {
					if sections != nil {
						t.Fatal("partial owner evidence")
					}
					return
				}
				if len(sections) != 6 {
					t.Fatal("failed diagnostics lost sections")
				}
				for _, s := range sections {
					if s.Failure == "" || s.Rows != nil {
						t.Fatal("failed section granted success")
					}
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			refuse(cancelled)
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE events RENAME TO unavailable_notify_observer_events`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			sections, err := ReadNotifyAllChildrenDiagnosticStorageForTest(ctx, fixture.store)
			if err != nil || len(sections) != 6 || sections[0].Failure == "" || sections[0].Rows != nil {
				t.Fatalf("failed first section=%v/%v", sections, err)
			}
			for _, s := range sections[1:] {
				if s.Failure != "" {
					t.Fatalf("first failure poisoned later section: %s", s.Failure)
				}
			}
			if got, err := ReadNotifyAllChildrenItemStorageForTest(ctx, fixture.store, uuid.NewString(), uuid.NewString()); err == nil || got != nil {
				t.Fatalf("unavailable items=%v/%v", got, err)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_notify_observer_events RENAME TO events`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(ctx)
		})
	}
}
