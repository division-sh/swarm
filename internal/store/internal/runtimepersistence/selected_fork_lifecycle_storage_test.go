package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

func TestSelectedForkLifecycleStorageRetainsPendingAndGlobalAttributionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, _ := selectedForkDiscardTestStore(t, backend)
			owner := selected.(diagnosticProjectionTestStore)
			ctx := context.Background()
			runID, foreignRun := uuid.NewString(), uuid.NewString()
			origin := runtimemanager.LifecycleDiagnosticOrigin{Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticObservation}
			first := enqueueDiagnosticTestIdentity(t, owner, mustTestAgentIdentityForRun(runID, "first", "consumer"), origin, executionmode.Live)
			pending := enqueueDiagnosticTestIdentity(t, owner, mustTestAgentIdentityForRun(runID, "pending", "consumer"), origin, executionmode.Live)
			foreign := enqueueDiagnosticTestIdentity(t, owner, mustTestAgentIdentityForRun(foreignRun, "foreign", "consumer"), origin, executionmode.Live)
			produced, err := owner.ListPendingAgentLifecycleDiagnostics(ctx, 100)
			if err != nil {
				t.Fatal(err)
			}
			wantReceipts := map[string]bool{}
			for _, row := range produced {
				if row.Identity.RunID == runID {
					wantReceipts[row.OutboxID] = true
				}
			}
			if !wantReceipts[first.OutboxID] || !wantReceipts[pending.OutboxID] || wantReceipts[foreign.OutboxID] {
				t.Fatalf("producer did not retain exact fixture diagnostic identities: %v", wantReceipts)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			initial, err := ReadSelectedForkLifecycleDiagnosticStorageForTest(ctx, selected, runID)
			if err != nil || len(initial.Receipts) != len(wantReceipts) || len(initial.Logs) != 0 {
				t.Fatalf("pending physical evidence: %+v %v", initial, err)
			}
			for _, row := range initial.Receipts {
				if !wantReceipts[row.OutboxID] || row.Projection != nil {
					t.Fatalf("pending NULL or exact-run membership lost: %+v", row)
				}
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("diagnostic observation mutated its original owner: %v", err)
			}
			logger := runtimepkg.NewRuntimeLogger(owner, executionposture.Live, nil)
			if err := logger.ProjectLifecycleDiagnostic(ctx, first); err != nil {
				t.Fatal(err)
			}
			if err := logger.ProjectLifecycleDiagnostic(ctx, foreign); err != nil {
				t.Fatal(err)
			}
			// Keep the same outbox payload in a foreign physical log row. A
			// run-filtered or payload-only reader would conceal the counterexample.
			if _, err := db.ExecContext(ctx, `UPDATE events SET payload=(SELECT payload FROM events WHERE event_id=$1) WHERE event_id=$2`, diagnosticEventID(first.OutboxID), diagnosticEventID(foreign.OutboxID)); err != nil {
				t.Fatal(err)
			}
			storage, err := ReadSelectedForkLifecycleDiagnosticStorageForTest(ctx, selected, runID)
			if err != nil || len(storage.Receipts) != len(wantReceipts) || len(storage.Logs) != 2 {
				t.Fatalf("projected, pending and global duplicate cardinality: %+v %v", storage, err)
			}
			for _, row := range storage.Receipts {
				var raw []byte
				if err := db.QueryRowContext(ctx, `SELECT projection FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1`, row.OutboxID).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(row.Projection, raw) || (row.OutboxID == pending.OutboxID && row.Projection != nil) || (row.OutboxID == first.OutboxID && len(row.Projection) == 0) {
					t.Fatalf("stored projection bytes or NULL changed: %+v raw=%s", row, raw)
				}
			}
			physicalRuns := map[string]int{}
			for _, row := range storage.Logs {
				var payload struct {
					Details map[string]any `json:"details"`
				}
				if err := json.Unmarshal(row.Payload, &payload); err != nil {
					t.Fatal(err)
				}
				if !row.RunPresent || payload.Details["outbox_id"] != first.OutboxID || payload.Details["run_id"] != runID {
					t.Fatalf("global attribution witness lost stored or payload coordinates: %+v %+v", row, payload)
				}
				physicalRuns[row.RunID]++
			}
			if !reflect.DeepEqual(physicalRuns, map[string]int{runID: 1, foreignRun: 1}) {
				t.Fatalf("foreign duplicate log hidden: %v", physicalRuns)
			}
			if _, err := db.ExecContext(ctx, `UPDATE events SET run_id=NULL WHERE event_id=$1`, diagnosticEventID(foreign.OutboxID)); err != nil {
				t.Fatal(err)
			}
			nullable, err := ReadSelectedForkLifecycleDiagnosticStorageForTest(ctx, selected, uuid.NewString())
			if err != nil || len(nullable.Receipts) != 0 || len(nullable.Logs) != 2 {
				t.Fatalf("missing-run receipts borrowed foreign evidence or hid global logs: %+v %v", nullable, err)
			}
			var nullLogs int
			for _, row := range nullable.Logs {
				if !row.RunPresent {
					if row.RunID != "" || len(row.Payload) == 0 {
						t.Fatalf("NULL physical run changed its payload: %+v", row)
					}
					nullLogs++
				}
			}
			if nullLogs != 1 {
				t.Fatalf("NULL physical-run cardinality=%d", nullLogs)
			}
			retained := append([]byte(nil), storage.Logs[0].Payload...)
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(retained, storage.Logs[0].Payload) {
				t.Fatal("diagnostic evidence depended on the closed store lifetime")
			}
		})
	}
}

func TestSelectedForkLifecycleStorageDiscardsEveryPartialReadBothStores(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		storage, err := ReadSelectedForkLifecycleDiagnosticStorageForTest(ctx, owner, uuid.NewString())
		if err == nil || storage.Receipts != nil || storage.Logs != nil {
			t.Fatalf("unowned diagnostic evidence: %+v %v", storage, err)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"invalid-run", "cancelled", "closed", "receipt-read-failed", "log-read-failed"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, db, _ := selectedForkDiscardTestStore(t, backend)
				item := enqueueDiagnosticTestAgent(t, selected.(diagnosticProjectionTestStore), "selected-diagnostic")
				runID, readCtx := item.Identity.RunID, ctx
				switch cut {
				case "invalid-run":
					runID = "not-canonical"
				case "cancelled":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					readCtx = cancelled
				case "closed":
					if err := selected.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				case "receipt-read-failed":
					if _, err := db.ExecContext(ctx, `ALTER TABLE agent_lifecycle_diagnostic_outbox RENAME TO unavailable_selected_diagnostic_receipts`); err != nil {
						t.Fatal(err)
					}
				case "log-read-failed":
					if _, err := db.ExecContext(ctx, `ALTER TABLE events RENAME TO unavailable_selected_diagnostic_logs`); err != nil {
						t.Fatal(err)
					}
				}
				storage, err := ReadSelectedForkLifecycleDiagnosticStorageForTest(readCtx, selected, runID)
				if err == nil || storage.Receipts != nil || storage.Logs != nil || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("failed diagnostic read retained partial evidence: %+v %v", storage, err)
				}
			})
		}
	}
}
