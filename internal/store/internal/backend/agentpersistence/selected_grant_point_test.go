package agentpersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
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
				`CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL, status TEXT NOT NULL)`,
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
				status   string
			}{
				{"event", 2, uuid.NewString(), "running"},
				{"deployment_revision", 3, nil, "running"},
				{"event", 2, uuid.NewString(), "paused"},
				{"event", 2, uuid.NewString(), "stopped"},
			} {
				forkID, sourceID, bindingID, executionID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
				const bundleHash = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				if _, err := db.ExecContext(ctx, `INSERT INTO runs VALUES ($1,$2,$3)`, forkID, bundleHash, arm.status); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings VALUES ($1,$2,$3,$4,$5,$6,'selected_contracts',NULL)`,
					bindingID, forkID, sourceID, arm.kind, arm.revision, arm.eventID); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_runtime_executions VALUES
					($1,$2,$3,$4,$5,$6,$7,1,1,'owner',$9,$9,$9,$9,$9,'{}',$9,'{}','running',$8)`,
					executionID, bindingID, forkID, sourceID, arm.kind, arm.revision, arm.eventID, time.Now().Add(time.Hour), "sha256:"+strings.Repeat("1", 64)); err != nil {
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
					if want {
						grant := startupownership.GrantEvidence{GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "owner",
							ProcessBootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1,
							BundleHash: bundleHash, StateVersion: 1, State: startupownership.GrantAdmitted, SelectedFork: &row.Binding}
						// A real read-only transaction must not acquire the mutation
						// fence or FOR UPDATE locks. Observation never authorizes work.
						_ = tx.Rollback()
						read, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
						if err != nil {
							t.Fatal(err)
						}
						defer read.Rollback()
						active, err := ObserveSelectedForkGenerationGrantTx(ctx, read, grant, backend == "sqlite")
						if err != nil || active != (arm.status == "running") {
							t.Fatalf("selected %s read-only observation status=%s active=%t error=%v", arm.kind, arm.status, active, err)
						}
						wrong := *grant.SelectedFork
						wrong.FenceGeneration++
						grant.SelectedFork = &wrong
						if active, err := ObserveSelectedForkGenerationGrantTx(ctx, read, grant, backend == "sqlite"); err != nil || active {
							t.Fatalf("read-only observation borrowed a replaced fence: active=%t error=%v", active, err)
						}
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
