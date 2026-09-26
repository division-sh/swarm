package agentpersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestSelectedGenerationGrantExecutionRequiresExactForkPointBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, opened, cleanup := testutil.StartEmptyPostgres(t)
				db = opened
				t.Cleanup(cleanup)
			} else {
				opened, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "grant.db"))
				if err != nil {
					t.Fatal(err)
				}
				db = opened
				t.Cleanup(func() { _ = db.Close() })
			}
			db.SetMaxOpenConns(1)
			for _, ddl := range []string{
				`CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL)`,
				`CREATE TABLE run_fork_selected_contract_bindings (
					binding_id TEXT PRIMARY KEY, fork_run_id TEXT NOT NULL, source_run_id TEXT NOT NULL,
					fork_point_kind TEXT NOT NULL, fork_revision BIGINT NOT NULL, fork_event_id TEXT,
					mode TEXT NOT NULL, bundle_hash TEXT)`,
				`CREATE TABLE run_fork_selected_contract_runtime_executions (
					execution_id TEXT PRIMARY KEY, binding_id TEXT NOT NULL, fork_run_id TEXT NOT NULL,
					source_run_id TEXT NOT NULL, fork_point_kind TEXT NOT NULL, fork_revision BIGINT NOT NULL,
					fork_event_id TEXT, generation BIGINT NOT NULL, fence_generation BIGINT NOT NULL,
					execution_owner TEXT NOT NULL, admission_fingerprint TEXT NOT NULL,
					container_plan_fingerprint TEXT NOT NULL, actor_census_fingerprint TEXT NOT NULL,
					effective_config_fingerprint TEXT NOT NULL, declaration_plan_fingerprint TEXT NOT NULL,
					declaration_plan TEXT NOT NULL, preparation_fingerprint TEXT NOT NULL,
					preparation_binding TEXT NOT NULL, state TEXT NOT NULL, lease_expires_at TIMESTAMPTZ NOT NULL)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			for _, arm := range []struct {
				kind     string
				revision int64
				eventID  any
			}{
				{"event", 2, uuid.NewString()},
				{"deployment_revision", 3, nil},
			} {
				forkID, sourceID, bindingID, executionID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
				const bundleHash = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				if _, err := db.ExecContext(ctx, `INSERT INTO runs VALUES ($1,$2)`, forkID, bundleHash); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings VALUES ($1,$2,$3,$4,$5,$6,'selected_contracts',NULL)`,
					bindingID, forkID, sourceID, arm.kind, arm.revision, arm.eventID); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_runtime_executions VALUES
					($1,$2,$3,$4,$5,$6,$7,1,1,'owner','admission','container','actors','config','declaration','{}','preparation','{}','running',$8)`,
					executionID, bindingID, forkID, sourceID, arm.kind, arm.revision, arm.eventID, time.Now().Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				binding := startupownership.SelectedForkGrantBinding{ExecutionID: executionID}
				load := func(want bool) {
					t.Helper()
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					row, err := loadSelectedForkGrantExecutionTx(ctx, tx, binding, backend == "sqlite")
					if want && (err != nil || row.Binding.ForkRunID != forkID || row.BundleHash != bundleHash) {
						t.Fatalf("exact %s point refused: row=%+v err=%v", arm.kind, row, err)
					}
					if !want && err == nil {
						t.Fatalf("contradictory %s point admitted", arm.kind)
					}
				}
				load(true)
				if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET fork_revision=fork_revision+1 WHERE execution_id=$1`, executionID); err != nil {
					t.Fatal(err)
				}
				load(false)
				if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET fork_revision=fork_revision-1, fork_point_kind='event' WHERE execution_id=$1`, executionID); err != nil {
					t.Fatal(err)
				}
				if arm.kind == "deployment_revision" {
					load(false)
				}
				if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET fork_point_kind=$2, fork_event_id=$3 WHERE execution_id=$1`,
					executionID, arm.kind, uuid.NewString()); err != nil {
					t.Fatal(err)
				}
				load(false)
			}
		})
	}
}
