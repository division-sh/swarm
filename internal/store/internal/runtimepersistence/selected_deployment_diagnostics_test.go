package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedDeploymentForkDiagnosticsKeepExactRunAndReadSnapshotBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			active, sibling := seedDeliveryRecoveryClaim(t, fixture, ctx), seedDeliveryRecoveryClaim(t, fixture, ctx)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, active, 2)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, sibling, 1)
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id,control_status,controlled_by) VALUES($1,'paused','diagnostic-storage-proof')`, active.Claim.RunID())
				return err
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			var first []SelectedDeploymentForkDiagnosticSection
			for _, cell := range []struct {
				run, delivery, event string
				outcomes, events     string
			}{
				{active.Claim.RunID(), active.Snapshot.DeliveryID, active.Snapshot.EventID, "2", "3"},
				{sibling.Claim.RunID(), sibling.Snapshot.DeliveryID, sibling.Snapshot.EventID, "1", "2"},
			} {
				sections, err := ReadSelectedDeploymentForkDiagnosticsForTest(ctx, fixture.store, cell.run)
				if err != nil || len(sections) != 15 {
					t.Fatalf("incomplete native diagnostics: %+v %v", sections, err)
				}
				byName := make(map[string]SelectedDeploymentForkDiagnosticSection, len(sections))
				for _, section := range sections {
					if section.Name == "" || len(section.Columns) == 0 {
						t.Fatalf("unnamed diagnostic fields: %+v", section)
					}
					if _, exists := byName[section.Name]; exists {
						t.Fatalf("duplicate diagnostic section %s", section.Name)
					}
					for _, row := range section.Rows {
						if len(row) != len(section.Columns) {
							t.Fatalf("diagnostic column/value mismatch: %+v", section)
						}
					}
					byName[section.Name] = section
				}
				for name, want := range map[string]string{
					"outcomes": cell.outcomes, "events": cell.events, "deliveries": "1", "pending_deliveries": "1",
					"pipeline_receipts": cell.outcomes, "failed_deliveries": "0", "stamped_handoffs": "0", "receiver_instances": "0",
				} {
					section := byName[name]
					if !reflect.DeepEqual(section.Rows, [][]SelectedDeploymentForkDiagnosticCell{{{Text: want, Present: true}}}) {
						t.Fatalf("diagnostic crossed run/count: %s want=%s actual=%+v", name, want, section)
					}
				}
				for name, columns := range map[string]int{"run_control": 2, "feeds": 10, "executions": 5, "receipts": 6, "delivery_rows": 11, "attempts": 6, "runtime_logs": 2} {
					if len(byName[name].Columns) != columns {
						t.Fatalf("diagnostic lost original fields: %s %+v", name, byName[name])
					}
				}
				if cell.run == active.Claim.RunID() {
					if !reflect.DeepEqual(byName["run_control"].Rows, [][]SelectedDeploymentForkDiagnosticCell{{{Text: "running", Present: true}, {Text: "paused", Present: true}}}) || byName["run_control"].Failure != "" {
						t.Fatalf("diagnostic lost physical run/control correspondence: %+v", byName["run_control"])
					}
				} else if len(byName["run_control"].Rows) != 0 || byName["run_control"].Failure != sql.ErrNoRows.Error() {
					t.Fatalf("diagnostic replaced missing control with run absence: %+v", byName["run_control"])
				}
				delivery := byName["delivery_rows"]
				if len(delivery.Rows) != 1 || delivery.Rows[0][0] != (SelectedDeploymentForkDiagnosticCell{Text: cell.delivery, Present: true}) ||
					delivery.Rows[0][1] != (SelectedDeploymentForkDiagnosticCell{Text: cell.event, Present: true}) || delivery.Rows[0][8].Present || delivery.Rows[0][10].Present {
					t.Fatalf("diagnostic lost exact delivery identity or NULL markers: %+v", delivery)
				}
				if first == nil {
					first = sections
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("diagnostic did not use its selected read snapshot: %+v", counts)
			}
			if got, err := ReadSelectedDeploymentForkDiagnosticsForTest(ctx, fixture.store, uuid.NewString()); err != nil || len(got) != 15 ||
				got[0].Failure != sql.ErrNoRows.Error() || len(got[0].Rows) != 0 ||
				!reflect.DeepEqual(got[3].Rows, [][]SelectedDeploymentForkDiagnosticCell{{{Text: "0", Present: true}}}) {
				t.Fatalf("missing run fabricated diagnostic evidence or lost original absence: %+v %v", got, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadSelectedDeploymentForkDiagnosticsForTest(cancelled, fixture.store, active.Claim.RunID()); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled read returned diagnostic evidence: %+v %v", got, err)
			}
			if got, err := ReadSelectedDeploymentForkDiagnosticsForTest(ctx, fixture.store, active.Claim.RunID()); err != nil || !reflect.DeepEqual(got, first) || probe.Snapshot().Total.WriteCommits != 0 {
				t.Fatalf("diagnostic changed durable state: %+v %v", got, err)
			}
			first[0].Rows[0][0].Text = "caller mutation"
			if got, err := ReadSelectedDeploymentForkDiagnosticsForTest(ctx, fixture.store, active.Claim.RunID()); err != nil || got[0].Rows[0][0].Text == "caller mutation" {
				t.Fatalf("diagnostic leaked mutable observation state: %+v %v", got, err)
			}
		})
	}
}

func TestSelectedDeploymentForkDiagnosticsRefuseInvalidAuthority(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []any{nil, struct{}{}, (*SQLiteRuntimeStore)(nil), (*PostgresStore)(nil), &SQLiteRuntimeStore{}, &PostgresStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadSelectedDeploymentForkDiagnosticsForTest(ctx, owner, uuid.NewString()); err == nil || got != nil {
			t.Fatalf("foreign/uninitialized owner%T returned diagnostics: %+v %v", owner, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, run := range []string{"", "not-a-run", uuid.Nil.String(), "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"} {
				if got, err := ReadSelectedDeploymentForkDiagnosticsForTest(ctx, fixture.store, run); err == nil || got != nil {
					t.Fatalf("inexact run returned diagnostics: %+v %v", got, err)
				}
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadSelectedDeploymentForkDiagnosticsForTest(ctx, fixture.store, uuid.NewString()); err == nil || got != nil {
				t.Fatalf("unavailable read returned diagnostics: %+v %v", got, err)
			}
		})
	}
}
