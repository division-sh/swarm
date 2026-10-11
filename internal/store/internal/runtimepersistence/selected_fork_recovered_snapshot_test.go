package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedForkRecoveredSnapshotPreservesExactRunInventoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, _ := selectedForkDiscardTestStore(t, backend)
			owner := selected.(diagnosticProjectionTestStore)
			ctx := context.Background()
			item := enqueueDiagnosticTestAgent(t, owner, "recovery-snapshot")
			if err := runtimepkg.NewRuntimeLogger(owner, executionposture.Live, nil).ProjectLifecycleDiagnostic(ctx, item); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			before, err := ReadSelectedForkRecoveredStorageSnapshotForTest(ctx, selected, item.Identity.RunID)
			if err != nil || len(before) != 4 || len(before["events"].Rows) != 1 {
				t.Fatalf("exact recovery inventory lost its event: %v %v", before, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("recovery inventory escaped original coherent read owner: %+v", counts)
			}
			for _, table := range []string{"entity_state", "flow_instances", "flow_instance_runtime_readiness", "events"} {
				if len(before[table].Columns) == 0 || before[table].Rows == nil {
					t.Fatalf("physical columns/empty family disappeared: %s %+v", table, before[table])
				}
			}
			var row []struct {
				Column, Type string
				Value        any
			}
			if err := json.Unmarshal([]byte(before["events"].Rows[0]), &row); err != nil {
				t.Fatal(err)
			}
			foundRun := false
			wantRun, wantType := item.Identity.RunID, "string"
			if backend == "postgres" {
				wantRun, wantType = base64.StdEncoding.EncodeToString([]byte(item.Identity.RunID)), "[]uint8"
			}
			for _, value := range row {
				if value.Column == "run_id" {
					if value.Value != wantRun || value.Type != wantType {
						t.Fatalf("physical run/driver type changed: %+v", value)
					}
					foundRun = true
				}
			}
			if !foundRun {
				t.Fatal("complete event row omitted run identity")
			}
			missing, err := ReadSelectedForkRecoveredStorageSnapshotForTest(ctx, selected, uuid.NewString())
			if err != nil || len(missing) != 4 {
				t.Fatalf("missing run lost fixed physical inventory: %v %v", missing, err)
			}
			for table, evidence := range missing {
				if len(evidence.Rows) != 0 || !reflect.DeepEqual(evidence.Columns, before[table].Columns) {
					t.Fatalf("missing run borrowed rows or lost columns: %s %+v", table, evidence)
				}
			}
			after, err := ReadSelectedForkRecoveredStorageSnapshotForTest(ctx, selected, item.Identity.RunID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("physical observation mutated or aliased its evidence: %v", err)
			}
		})
	}
}

func TestSelectedForkRecoveredSnapshotRefusesPartialAndForeignEvidenceBothStores(t *testing.T) {
	ctx := context.Background()
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if snapshot, err := ReadSelectedForkRecoveredStorageSnapshotForTest(ctx, selected, uuid.NewString()); err == nil || snapshot != nil {
			t.Fatalf("foreign recovery observer returned evidence: %v %v", snapshot, err)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"invalid-run", "cancelled", "closed", "late-table"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, db, _ := selectedForkDiscardTestStore(t, backend)
				readCtx, runID := ctx, uuid.NewString()
				switch cut {
				case "invalid-run":
					runID = "invalid"
				case "cancelled":
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					readCtx = canceled
				case "closed":
					if err := selected.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				case "late-table":
					if _, err := db.ExecContext(ctx, `ALTER TABLE events RENAME TO unavailable_recovery_snapshot_events`); err != nil {
						t.Fatal(err)
					}
				}
				snapshot, err := ReadSelectedForkRecoveredStorageSnapshotForTest(readCtx, selected, runID)
				if err == nil || snapshot != nil || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("failed cut leaked partial recovery tables: %v %v", snapshot, err)
				}
			})
		}
	}
}
