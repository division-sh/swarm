package runforkpersistence

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestForkActivationLoadsExactMaterializedPointBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, point := range []runfork.RunForkPoint{
				{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7},
				{Kind: runfork.RunForkPointEvent, Revision: 7, EventID: uuid.NewString()},
			} {
				t.Run(string(point.Kind), func(t *testing.T) {
					db := forkOperationTestDatabase(t, backend)
					if _, err := db.Exec(`CREATE TABLE run_fork_selected_contract_bindings (
						binding_id TEXT PRIMARY KEY, fork_run_id TEXT NOT NULL, source_run_id TEXT NOT NULL,
						fork_point_kind TEXT NOT NULL, fork_revision BIGINT NOT NULL, fork_event_id TEXT,
						mode TEXT NOT NULL, bundle_hash TEXT
					)`); err != nil {
						t.Fatal(err)
					}
					ctx := context.Background()
					childID, bindingID := uuid.NewString(), uuid.NewString()
					request := runfork.ForkOperationRequest{
						OperationID: uuid.NewString(), Actor: "bearer:operator", IdempotencyKey: "exact-activation",
						TransportHash: "sha256:exact-activation", SourceRunID: uuid.NewString(), ForkEventID: point.EventID,
						TargetBundleHash: "sha256:bundle", ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
						ResolvedPoint: &point,
					}
					withForkOperationTx(t, db, func(tx *sql.Tx) {
						if _, _, err := bindForkOperationTx(ctx, tx, request, childID, bindingID, backend == "postgres"); err != nil {
							t.Fatal(err)
						}
						if _, err := tx.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings VALUES ($1,$2,$3,$4,$5,$6,'selected_contracts',NULL)`,
							bindingID, childID, request.SourceRunID, point.Kind, point.Revision, nullableForkEventID(point)); err != nil {
							t.Fatal(err)
						}
					})
					caller := request
					caller.ResolvedPoint = nil
					lineage := runForkActivationLineage{ForkRunID: childID, SourceRunID: request.SourceRunID,
						ForkBundleHash: request.TargetBundleHash, ForkPoint: point}
					check := func(caller runfork.ForkOperationRequest, lineage runForkActivationLineage, want bool) {
						t.Helper()
						withForkOperationTx(t, db, func(tx *sql.Tx) {
							stored, err := resolvedForkOperationForActivationTx(ctx, tx, caller, lineage, backend == "postgres")
							if want {
								if err != nil || stored.ResolvedPoint == nil || *stored.ResolvedPoint != point {
									t.Fatalf("exact activation point: request=%+v err=%v", stored, err)
								}
							} else if err == nil {
								t.Fatalf("contradictory activation admitted: %+v", stored)
							}
						})
					}
					check(caller, lineage, true)
					changed := caller
					changed.TransportHash = "sha256:other"
					check(changed, lineage, false)
					changed = caller
					changed.TargetBundleHash = "sha256:other"
					check(changed, lineage, false)
					other := lineage
					other.ForkPoint.Revision++
					check(caller, other, false)
					other = lineage
					other.SourceRunID = uuid.NewString()
					check(caller, other, false)
					if _, err := db.Exec(`UPDATE run_fork_selected_contract_bindings SET fork_revision=8 WHERE binding_id=$1`, bindingID); err != nil {
						t.Fatal(err)
					}
					check(caller, lineage, false)
					if _, err := db.Exec(`UPDATE run_fork_selected_contract_bindings SET fork_revision=7 WHERE binding_id=$1`, bindingID); err != nil {
						t.Fatal(err)
					}
					check(caller, lineage, true)
					var state string
					if err := db.QueryRow(`SELECT state FROM run_fork_operations WHERE operation_id=$1`, request.OperationID).Scan(&state); err != nil || state != string(runfork.ForkOperationMaterialized) {
						t.Fatalf("activation probe mutated operation: state=%q err=%v", state, err)
					}
				})
			}
		})
	}
}

func TestForkOperationPostgresReadOnlyResultLookup(t *testing.T) {
	db := forkOperationTestDatabase(t, "postgres")
	ctx := context.Background()
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 3}
	request := runfork.ForkOperationRequest{
		OperationID: uuid.NewString(), Actor: "bearer:operator", IdempotencyKey: "read-only-result",
		TransportHash: "sha256:read-only-result", SourceRunID: uuid.NewString(),
		TargetBundleHash: "sha256:bundle", ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
		ResolvedPoint: &point,
	}
	childID, bindingID := uuid.NewString(), uuid.NewString()
	withForkOperationTx(t, db, func(tx *sql.Tx) {
		if _, _, err := bindForkOperationTx(ctx, tx, request, childID, bindingID, true); err != nil {
			t.Fatal(err)
		}
		if err := completeForkOperationTx(ctx, tx, request, runfork.ForkOperationResult{
			SourceRunID: request.SourceRunID, SourceRunStatus: "running", ForkRunID: childID,
			ForkPoint: point, ForkRunStatus: runfork.RunForkActivatedStatus,
			BundleHash: request.TargetBundleHash,
		}, true); err != nil {
			t.Fatal(err)
		}
	})
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	record, found, err := loadForkOperationByIDTx(ctx, tx, request.OperationID, false)
	if err != nil || !found || record.Status != runfork.ForkOperationActivated ||
		record.Result == nil || record.Result.ForkPoint != point {
		t.Fatalf("read-only activated result: found=%t record=%+v err=%v", found, record, err)
	}
}
