package runforkpersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestSelectedBindingPersistsDisjointForkPointsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, opened, cleanup := testutil.StartEmptyPostgres(t)
				db = opened
				t.Cleanup(cleanup)
			} else {
				opened, err := sql.Open("sqlite", ":memory:")
				if err != nil {
					t.Fatal(err)
				}
				db = opened
				db.SetMaxOpenConns(1)
				t.Cleanup(func() { _ = db.Close() })
			}
			if _, err := db.Exec(`CREATE TABLE run_fork_selected_contract_bindings (
				binding_id TEXT DEFAULT '00000000-0000-0000-0000-000000000001', fork_run_id TEXT,
				source_run_id TEXT, fork_point_kind TEXT, fork_revision BIGINT, fork_event_id TEXT,
				mode TEXT, bundle_hash TEXT, created_at TEXT)`); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			eventID := uuid.NewString()
			for _, point := range []runfork.RunForkPoint{
				{Kind: runfork.RunForkPointEvent, Revision: 4, Input: eventID, EventID: eventID, EventName: "worker.ready", Timestamp: time.Now().UTC()},
				{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7},
			} {
				req := runfork.RunForkSelectedContractBindingRequest{
					ForkRunID: uuid.NewString(), SourceRunID: uuid.NewString(), ForkPoint: point,
					ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
				}
				inserted, err := insertRunForkSelectedContractBinding(ctx, tx, req, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				loaded, err := loadRunForkSelectedContractBinding(ctx, tx, inserted.ForkRunID)
				if err != nil {
					t.Fatal(err)
				}
				if inserted.ForkPoint != loaded.ForkPoint {
					t.Fatalf("inserted binding invents non-durable point fields: inserted=%+v loaded=%+v", inserted.ForkPoint, loaded.ForkPoint)
				}
				if loaded.ForkPoint.Kind != point.Kind || loaded.ForkPoint.Revision != point.Revision ||
					loaded.ForkPoint.EventID != point.EventID || loaded.ForkEventID != point.EventID {
					t.Fatalf("point changed across selected binding: %+v want %+v", loaded, point)
				}
				if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_bindings SET fork_point_kind='deployment_revision' WHERE fork_run_id=$1`, inserted.ForkRunID); err != nil {
					t.Fatal(err)
				}
				if point.Kind == runfork.RunForkPointEvent {
					if _, err := loadRunForkSelectedContractBinding(ctx, tx, inserted.ForkRunID); err == nil {
						t.Fatal("mixed persisted fork point was accepted")
					}
				}
			}
		})
	}
}
