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

func TestSelectedSuccessorTransferFencesOnlyExactNonterminalDeliveriesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			var adapter *Adapter
			if backend == "postgres" {
				_, db, _ = testutil.StartEmptyPostgres(t)
				adapter, _ = NewAdapter(DialectPostgres)
			} else {
				var err error
				db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "selected-handoff.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				adapter, _ = NewAdapter(DialectSQLite)
			}
			for _, ddl := range []string{
				`CREATE TABLE runs (run_id TEXT PRIMARY KEY,bundle_hash TEXT NOT NULL)`,
				`CREATE TABLE run_fork_selected_contract_runtime_executions (
					execution_id TEXT PRIMARY KEY,fork_run_id TEXT NOT NULL,binding_id TEXT NOT NULL,
					generation BIGINT NOT NULL,state TEXT NOT NULL)`,
				`CREATE TABLE event_deliveries (
					delivery_id TEXT PRIMARY KEY,run_id TEXT NOT NULL,status TEXT NOT NULL,
					execution_authority_kind TEXT NOT NULL,authority_bundle_hash TEXT NOT NULL,
					execution_authority_id TEXT NOT NULL,execution_authority_generation BIGINT NOT NULL,
					selected_execution_id TEXT,selected_execution_generation BIGINT,
					current_attempt_version BIGINT,current_attempt_open BOOLEAN,
					continuation_handoff_at TIMESTAMP,updated_at TIMESTAMP NOT NULL)`,
				`CREATE TABLE event_delivery_attempts (
					delivery_id TEXT NOT NULL,claim_version BIGINT NOT NULL,open_marker BOOLEAN NOT NULL,
					started_at TIMESTAMP NOT NULL,lease_expires_at TIMESTAMP NOT NULL)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			runID, bindingID, predecessorID, successorID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
			source, err := correlation.NewSourceArtifactFact(bundle)
			if err != nil {
				t.Fatal(err)
			}
			successor, err := deliverylifecycle.NewSelectedExecutionAuthority(source, successorID, runID, 2)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO runs VALUES ($1,$2)`, runID, bundle); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_runtime_executions VALUES
				($1,$3,$4,1,'running'),($2,$3,$4,2,'prepared')`, predecessorID, successorID, runID, bindingID); err != nil {
				t.Fatal(err)
			}
			pendingID, claimedID, failedID, deliveredID, unhandedID, unhandedClaimedID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			updatedAt := time.Now().UTC().Add(-time.Minute)
			for _, cell := range []struct {
				id      string
				status  string
				version any
				open    any
				handoff any
			}{
				{pendingID, "pending", nil, nil, updatedAt},
				{claimedID, "in_progress", 1, true, updatedAt},
				{failedID, "failed", nil, nil, updatedAt},
				{deliveredID, "delivered", nil, nil, updatedAt},
				{unhandedID, "pending", nil, nil, nil},
				{unhandedClaimedID, "in_progress", 1, true, nil},
			} {
				if _, err := db.Exec(`INSERT INTO event_deliveries VALUES
					($1,$2,$3,'selected_contract_fork',$4,$5,1,$5,1,$6,$7,$8,$9)`,
					cell.id, runID, cell.status, bundle, predecessorID, cell.version, cell.open, cell.handoff, updatedAt); err != nil {
					t.Fatal(err)
				}
			}
			startedAt, originalLease := time.Now().UTC().Add(-time.Minute), time.Now().UTC().Add(time.Hour)
			for _, id := range []string{claimedID, unhandedClaimedID} {
				if _, err := db.Exec(`INSERT INTO event_delivery_attempts VALUES ($1,1,TRUE,$2,$3)`, id, startedAt, originalLease); err != nil {
					t.Fatal(err)
				}
			}
			transfer := func(previous string, want bool) {
				t.Helper()
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				err = adapter.transferSelectedSuccessorAuthorityTx(ctx, tx, previous, successor)
				if want {
					if err != nil {
						t.Fatalf("exact transfer: %v", err)
					}
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				} else if err == nil {
					t.Fatal("contradictory selected successor transferred deliveries")
				}
			}
			transfer(predecessorID, false)
			if _, err := db.Exec(`UPDATE run_fork_selected_contract_runtime_executions SET state='closed' WHERE execution_id=$1`, predecessorID); err != nil {
				t.Fatal(err)
			}
			transfer(uuid.NewString(), false)
			transfer(predecessorID, true)
			for _, cell := range []struct {
				id   string
				want string
			}{
				{pendingID, successorID}, {claimedID, successorID}, {failedID, successorID}, {deliveredID, predecessorID}, {unhandedID, predecessorID}, {unhandedClaimedID, predecessorID},
			} {
				var authorityID, selectedID string
				var generation, selectedGeneration int
				if err := db.QueryRow(`SELECT execution_authority_id,selected_execution_id,execution_authority_generation,selected_execution_generation
					FROM event_deliveries WHERE delivery_id=$1`, cell.id).
					Scan(&authorityID, &selectedID, &generation, &selectedGeneration); err != nil {
					t.Fatal(err)
				}
				wantGeneration := 2
				if cell.want == predecessorID {
					wantGeneration = 1
				}
				if authorityID != cell.want || selectedID != cell.want || generation != wantGeneration || selectedGeneration != wantGeneration {
					t.Fatalf("delivery %s authority=%s/%s generation=%d/%d", cell.id, authorityID, selectedID, generation, selectedGeneration)
				}
			}
			for _, id := range []string{claimedID, unhandedClaimedID} {
				var lease time.Time
				if err := db.QueryRow(`SELECT lease_expires_at FROM event_delivery_attempts WHERE delivery_id=$1`, id).Scan(&lease); err != nil {
					t.Fatal(err)
				}
				if !lease.Before(originalLease) || !lease.After(startedAt) || lease.After(time.Now().Add(time.Second)) {
					t.Fatalf("predecessor claim %s lease was not fenced: %s", id, lease)
				}
			}
			if _, err := db.Exec(`UPDATE run_fork_selected_contract_runtime_executions SET state='running' WHERE execution_id=$1`, successorID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE event_deliveries SET continuation_handoff_at=$1 WHERE delivery_id=$2`, updatedAt, unhandedID); err != nil {
				t.Fatal(err)
			}
			transfer(predecessorID, true)
			transfer(predecessorID, true)
			var lateAuthority string
			if err := db.QueryRow(`SELECT selected_execution_id FROM event_deliveries WHERE delivery_id=$1`, unhandedID).Scan(&lateAuthority); err != nil || lateAuthority != successorID {
				t.Fatalf("stamped continuation did not transfer once: authority=%s err=%v", lateAuthority, err)
			}
			foreignID, foreignExecution := uuid.NewString(), uuid.NewString()
			if _, err := db.Exec(`INSERT INTO event_deliveries VALUES
				($1,$2,'pending','selected_contract_fork',$3,$4,3,$4,3,NULL,NULL,$5,$5)`,
				foreignID, runID, bundle, foreignExecution, updatedAt); err != nil {
				t.Fatal(err)
			}
			transfer(predecessorID, false)
			if err := db.QueryRow(`SELECT selected_execution_id FROM event_deliveries WHERE delivery_id=$1`, foreignID).Scan(&lateAuthority); err != nil || lateAuthority != foreignExecution {
				t.Fatalf("foreign selected continuation was rewritten: authority=%s err=%v", lateAuthority, err)
			}
			if _, err := db.Exec(`DELETE FROM event_deliveries WHERE delivery_id=$1`, foreignID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO event_deliveries VALUES
				($1,$2,'pending','selected_contract_fork',$3,$4,3,NULL,NULL,NULL,NULL,$5,$5)`,
				foreignID, runID, bundle, foreignExecution, updatedAt); err != nil {
				t.Fatal(err)
			}
			transfer(predecessorID, false)
			var missing sql.NullString
			if err := db.QueryRow(`SELECT selected_execution_id FROM event_deliveries WHERE delivery_id=$1`, foreignID).Scan(&missing); err != nil || missing.Valid {
				t.Fatalf("corrupt selected continuation was rewritten: authority=%+v err=%v", missing, err)
			}
		})
	}
}
