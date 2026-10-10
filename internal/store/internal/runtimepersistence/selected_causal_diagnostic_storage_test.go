package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedCausalDiagnosticStoragePreservesGlobalMultiplicityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, _ := selectedForkDiscardTestStore(t, backend)
			owner := selected.(diagnosticProjectionTestStore)
			ctx := context.Background()
			foreignRun := uuid.NewString()
			origin := runtimemanager.LifecycleDiagnosticOrigin{Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticObservation}
			first := enqueueDiagnosticTestAgent(t, owner, "first")
			foreign := enqueueDiagnosticTestIdentity(t, owner, mustTestAgentIdentityForRun(foreignRun, "foreign", "consumer"), origin, executionmode.Live)
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			before, err := ReadSelectedCausalDiagnosticConservationForTest(ctx, selected, first.OutboxID)
			if err != nil || before != (SelectedCausalDiagnosticConservation{Pending: 1}) {
				t.Fatalf("pending exact outbox conservation: %+v %v", before, err)
			}
			pending, err := ReadSelectedCausalDiagnosticStorageForTest(ctx, selected, first.OutboxID)
			if err != nil || pending.Events != nil || pending.Projection != nil {
				t.Fatalf("pending NULL projection changed: %+v %v", pending, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("diagnostic witnesses escaped their original coherent read owner: %+v", counts)
			}
			logger := runtimepkg.NewRuntimeLogger(owner, executionposture.Live, nil)
			if err := logger.ProjectLifecycleDiagnostic(ctx, first); err != nil {
				t.Fatal(err)
			}
			if err := logger.ProjectLifecycleDiagnostic(ctx, foreign); err != nil {
				t.Fatal(err)
			}
			cause := uuid.NewString()
			if _, err := db.ExecContext(ctx, `UPDATE events SET source_event_id=$1 WHERE event_id=$2`, cause, diagnosticEventID(first.OutboxID)); err != nil {
				t.Fatal(err)
			}
			// A wrong-name, wrong-run duplicate must remain visible under the
			// same payload outbox identity, including its independent causal edge.
			foreignCause := uuid.NewString()
			if _, err := db.ExecContext(ctx, `UPDATE events SET payload=(SELECT payload FROM events WHERE event_id=$1),event_name='platform.inbound_recorded',run_id=$2,source_event_id=$3 WHERE event_id=$4`, diagnosticEventID(first.OutboxID), foreignRun, foreignCause, diagnosticEventID(foreign.OutboxID)); err != nil {
				t.Fatal(err)
			}
			storage, err := ReadSelectedCausalDiagnosticStorageForTest(ctx, selected, first.OutboxID)
			if err != nil || len(storage.Events) != 2 || len(storage.Projection) == 0 {
				t.Fatalf("global duplicate evidence lost: %+v %v", storage, err)
			}
			got := map[string]string{}
			for _, row := range storage.Events {
				got[row.RunID] = row.SourceEventID
			}
			if !reflect.DeepEqual(got, map[string]string{first.Identity.RunID: cause, foreignRun: foreignCause}) {
				t.Fatalf("foreign run/cause was hidden: %v", got)
			}
			var raw []byte
			if err := db.QueryRowContext(ctx, `SELECT projection FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1`, first.OutboxID).Scan(&raw); err != nil || !bytes.Equal(raw, storage.Projection) {
				t.Fatalf("projection is not verbatim: %s %s %v", raw, storage.Projection, err)
			}
			conserved, err := ReadSelectedCausalDiagnosticConservationForTest(ctx, selected, first.OutboxID)
			if err != nil || conserved != (SelectedCausalDiagnosticConservation{Events: 2}) {
				t.Fatalf("global count/acknowledgment changed: %+v %v", conserved, err)
			}
			missing, err := ReadSelectedCausalDiagnosticConservationForTest(ctx, selected, uuid.NewString())
			if err != nil || missing != (SelectedCausalDiagnosticConservation{}) {
				t.Fatalf("missing identity borrowed evidence: %+v %v", missing, err)
			}
			if storage, err := ReadSelectedCausalDiagnosticStorageForTest(ctx, selected, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) || storage.Events != nil || storage.Projection != nil {
				t.Fatalf("missing receipt granted evidence: %+v %v", storage, err)
			}
		})
	}
}

func TestSelectedCausalDiagnosticStorageDiscardsFailedCutsBothStores(t *testing.T) {
	ctx := context.Background()
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		storage, err := ReadSelectedCausalDiagnosticStorageForTest(ctx, selected, uuid.NewString())
		if err == nil || storage.Events != nil || storage.Projection != nil {
			t.Fatalf("foreign owner returned evidence: %+v %v", storage, err)
		}
		counts, err := ReadSelectedCausalDiagnosticConservationForTest(ctx, selected, uuid.NewString())
		if err == nil || counts != (SelectedCausalDiagnosticConservation{}) {
			t.Fatalf("foreign owner returned conservation: %+v %v", counts, err)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"invalid-id", "cancelled", "closed", "events-unavailable", "receipt-unavailable", "null-run", "null-cause"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, db, _ := selectedForkDiscardTestStore(t, backend)
				owner := selected.(diagnosticProjectionTestStore)
				item := enqueueDiagnosticTestAgent(t, owner, "failed-cut")
				if err := runtimepkg.NewRuntimeLogger(owner, executionposture.Live, nil).ProjectLifecycleDiagnostic(ctx, item); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `UPDATE events SET source_event_id=$1 WHERE event_id=$2`, uuid.NewString(), diagnosticEventID(item.OutboxID)); err != nil {
					t.Fatal(err)
				}
				readCtx, id := ctx, item.OutboxID
				switch cut {
				case "invalid-id":
					id = "invalid"
				case "cancelled":
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					readCtx = canceled
				case "closed":
					if err := selected.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				case "events-unavailable":
					if _, err := db.ExecContext(ctx, `ALTER TABLE events RENAME TO unavailable_causal_diagnostic_events`); err != nil {
						t.Fatal(err)
					}
				case "receipt-unavailable":
					if _, err := db.ExecContext(ctx, `ALTER TABLE agent_lifecycle_diagnostic_outbox RENAME TO unavailable_causal_diagnostic_outbox`); err != nil {
						t.Fatal(err)
					}
				case "null-run":
					if _, err := db.ExecContext(ctx, `UPDATE events SET run_id=NULL,source_event_id=NULL WHERE event_id=$1`, diagnosticEventID(item.OutboxID)); err != nil {
						t.Fatal(err)
					}
				case "null-cause":
					if _, err := db.ExecContext(ctx, `UPDATE events SET source_event_id=NULL WHERE event_id=$1`, diagnosticEventID(item.OutboxID)); err != nil {
						t.Fatal(err)
					}
				}
				storage, err := ReadSelectedCausalDiagnosticStorageForTest(readCtx, selected, id)
				if err == nil || storage.Events != nil || storage.Projection != nil || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("failed cut retained partial evidence: %+v %v", storage, err)
				}
				if cut == "null-run" || cut == "null-cause" {
					// Conservation counts physical rows even when strict lineage
					// readback refuses nullable attribution.
					counts, err := ReadSelectedCausalDiagnosticConservationForTest(readCtx, selected, id)
					if err != nil || counts != (SelectedCausalDiagnosticConservation{Events: 1}) {
						t.Fatalf("NULL row disappeared from conservation: %+v %v", counts, err)
					}
					return
				}
				counts, err := ReadSelectedCausalDiagnosticConservationForTest(readCtx, selected, id)
				if err == nil || counts != (SelectedCausalDiagnosticConservation{}) || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("failed conservation retained partial counts: %+v %v", counts, err)
				}
			})
		}
	}
}
