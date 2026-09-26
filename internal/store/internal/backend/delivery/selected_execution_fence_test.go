package delivery

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSelectedDeliveryExecutionFenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			var adapter *Adapter
			if backend == "postgres" {
				_, db, _ = testutil.StartEmptyPostgres(t)
				adapter, _ = NewAdapter(DialectPostgres)
			} else {
				var err error
				db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "fence.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				adapter, _ = NewAdapter(DialectSQLite)
			}
			for _, ddl := range []string{
				`CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL)`,
				`CREATE TABLE run_fork_selected_contract_bindings (binding_id TEXT PRIMARY KEY, fork_run_id TEXT NOT NULL, mode TEXT NOT NULL, bundle_hash TEXT NOT NULL)`,
				`CREATE TABLE run_fork_selected_contract_runtime_executions (execution_id TEXT PRIMARY KEY, binding_id TEXT NOT NULL, fork_run_id TEXT NOT NULL, generation BIGINT NOT NULL, state TEXT NOT NULL, lease_expires_at TIMESTAMP)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			runID, bindingID, executionID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
			source, err := correlation.NewSourceArtifactFact(bundle)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := deliverylifecycle.NewSelectedExecutionAuthority(source, executionID, runID, 2)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO runs VALUES ($1,$2)`, runID, bundle); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_bindings VALUES ($1,$2,'selected_contracts','')`, bindingID, runID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_runtime_executions VALUES ($1,$2,$3,2,'running',$4)`, executionID, bindingID, runID, time.Now().UTC().Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			check := func(candidate deliverylifecycle.ExecutionAuthority, want bool) {
				t.Helper()
				readTx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				readCurrent, readErr := adapter.selectedExecutionCurrentRead(ctx, readTx, candidate)
				_ = readTx.Rollback()
				if readErr != nil || readCurrent != want {
					t.Fatalf("read-only selected execution current=%t want=%t err=%v", readCurrent, want, readErr)
				}
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				got, err := adapter.selectedExecutionCurrent(ctx, tx, candidate)
				if !want {
					_, scanErr := adapter.ScanContinuations(ctx, tx, candidate, deliverylifecycle.ContinuationCursor{}, 1)
					if scanErr == nil || !strings.Contains(scanErr.Error(), "fenced") {
						t.Fatalf("fenced selected continuation scan error=%v", scanErr)
					}
				}
				_ = tx.Rollback()
				if err != nil || got != want {
					t.Fatalf("selected execution current=%t want=%t err=%v", got, want, err)
				}
			}
			check(authority, true)
			for _, update := range []string{
				`UPDATE run_fork_selected_contract_runtime_executions SET state='failed'`,
				`UPDATE run_fork_selected_contract_runtime_executions SET state='running', lease_expires_at='2000-01-01T00:00:00Z'`,
				`UPDATE run_fork_selected_contract_runtime_executions SET lease_expires_at=NULL`,
				`UPDATE run_fork_selected_contract_runtime_executions SET lease_expires_at='2999-01-01T00:00:00Z', generation=3`,
				`UPDATE run_fork_selected_contract_runtime_executions SET generation=2, fork_run_id='` + uuid.NewString() + `'`,
			} {
				if _, err := db.Exec(update); err != nil {
					t.Fatal(err)
				}
				check(authority, false)
			}
			if _, err := db.Exec(`UPDATE run_fork_selected_contract_runtime_executions SET fork_run_id=$1`, runID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE runs SET bundle_hash='bundle-v2:sha256:` + strings.Repeat("b", 64) + `'`); err != nil {
				t.Fatal(err)
			}
			check(authority, false)
			if _, err := db.Exec(`UPDATE runs SET bundle_hash=$1`, bundle); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE run_fork_selected_contract_bindings SET mode='bundle_hash',bundle_hash='bundle-v2:sha256:` + strings.Repeat("b", 64) + `'`); err != nil {
				t.Fatal(err)
			}
			check(authority, false)
			if _, err := db.Exec(`UPDATE run_fork_selected_contract_bindings SET bundle_hash=$1`, bundle); err != nil {
				t.Fatal(err)
			}
			check(authority, true)
			if _, err := db.Exec(`UPDATE run_fork_selected_contract_runtime_executions SET state='failed' WHERE execution_id=$1`, executionID); err != nil {
				t.Fatal(err)
			}
			successorID := uuid.NewString()
			if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_runtime_executions VALUES ($1,$2,$3,3,'running',$4)`, successorID, bindingID, runID, time.Now().UTC().Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			successor, err := deliverylifecycle.NewSelectedExecutionAuthority(source, successorID, runID, 3)
			if err != nil {
				t.Fatal(err)
			}
			check(authority, false)
			check(successor, true)
		})
	}
}

func TestSelectedDeliveryFenceHoldsPostgresExecutionUntilCommit(t *testing.T) {
	_, db, _ := testutil.StartEmptyPostgres(t)
	adapter, err := NewAdapter(DialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	for _, ddl := range []string{
		`CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL)`,
		`CREATE TABLE run_fork_selected_contract_bindings (binding_id TEXT PRIMARY KEY, fork_run_id TEXT NOT NULL, mode TEXT NOT NULL, bundle_hash TEXT NOT NULL)`,
		`CREATE TABLE run_fork_selected_contract_runtime_executions (execution_id TEXT PRIMARY KEY, binding_id TEXT NOT NULL, fork_run_id TEXT NOT NULL, generation BIGINT NOT NULL, state TEXT NOT NULL, lease_expires_at TIMESTAMP)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	runID, bindingID, executionID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	source, err := correlation.NewSourceArtifactFact(bundle)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := deliverylifecycle.NewSelectedExecutionAuthority(source, executionID, runID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO runs VALUES ($1,$2)`, runID, bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_bindings VALUES ($1,$2,'selected_contracts','')`, bindingID, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_runtime_executions VALUES ($1,$2,$3,1,'running',$4)`, executionID, bindingID, runID, time.Now().UTC().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	claimTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer claimTx.Rollback()
	current, err := adapter.selectedExecutionCurrent(ctx, claimTx, authority)
	if err != nil || !current {
		t.Fatalf("selected claim fence current=%t err=%v", current, err)
	}
	updateCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	updated := make(chan error, 1)
	go func() {
		close(started)
		_, err := db.ExecContext(updateCtx, `UPDATE run_fork_selected_contract_runtime_executions SET state='failed' WHERE execution_id=$1`, executionID)
		updated <- err
	}()
	<-started
	select {
	case err := <-updated:
		t.Fatalf("execution fenced before claimant transaction ended: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := claimTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-updated:
		if err != nil {
			t.Fatal(err)
		}
	case <-updateCtx.Done():
		t.Fatalf("execution fencing did not resume after claimant rollback: %v", updateCtx.Err())
	}
}
