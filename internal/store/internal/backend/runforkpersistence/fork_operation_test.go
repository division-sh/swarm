package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestForkOperationDurableReplayAndConflictBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			ctx := context.Background()
			request := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "bearer:operator", IdempotencyKey: "fork-request-1",
				TransportHash: "sha256:exact-transport", SourceRunID: uuid.NewString(), ForkEventID: uuid.NewString(),
				TargetBundleHash: "sha256:bundle", ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
			}
			request.ResolvedPoint = &runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 1, EventID: request.ForkEventID}
			childID, bindingID := uuid.NewString(), uuid.NewString()
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				if _, found, err := lookupForkOperationTx(ctx, tx, request.Actor, request.IdempotencyKey, request.TransportHash, backend == "postgres", true); err != nil || found {
					t.Fatalf("unbound operation lookup: found=%t err=%v", found, err)
				}
				bound, replay, err := bindForkOperationTx(ctx, tx, request, childID, bindingID, backend == "postgres")
				if err != nil || replay || bound.Status != runfork.ForkOperationMaterialized || bound.ForkRunID != childID {
					t.Fatalf("bind first operation: record=%+v replay=%t err=%v", bound, replay, err)
				}
			})

			// Simulate an API response lost after materialization. A new invocation
			// must find the durable request before resolving mutable source aliases.
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				loaded, found, err := lookupForkOperationTx(ctx, tx, request.Actor, request.IdempotencyKey, request.TransportHash, backend == "postgres", true)
				if err != nil || !found || loaded.ForkRunID != childID || loaded.BindingID != bindingID {
					t.Fatalf("lost-response replay: found=%t record=%+v err=%v", found, loaded, err)
				}
				retry := request
				retry.OperationID = uuid.NewString()
				bound, replay, err := bindForkOperationTx(ctx, tx, retry, childID, bindingID, backend == "postgres")
				if err != nil || !replay || bound.Request.OperationID != request.OperationID {
					t.Fatalf("exact retry minted a second operation: record=%+v replay=%t err=%v", bound, replay, err)
				}
			})

			result := runfork.ForkOperationResult{
				SourceRunID: request.SourceRunID, SourceRunStatus: "completed", ForkRunID: childID,
				ForkEventID: request.ForkEventID, ForkPoint: *request.ResolvedPoint, ForkRunStatus: runfork.RunForkActivatedStatus,
				BundleHash: request.TargetBundleHash, ExecutedEventCount: 2,
			}
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				if err := completeForkOperationTx(ctx, tx, request, result, backend == "postgres"); err != nil {
					t.Fatalf("activate operation: %v", err)
				}
			})
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				loaded, found, err := lookupForkOperationTx(ctx, tx, request.Actor, request.IdempotencyKey, request.TransportHash, backend == "postgres", false)
				if err != nil || !found || loaded.Status != runfork.ForkOperationActivated || loaded.Result == nil || loaded.Result.ExecutedEventCount != 2 {
					t.Fatalf("activated response replay: found=%t record=%+v err=%v", found, loaded, err)
				}
				if err := completeForkOperationTx(ctx, tx, request, result, backend == "postgres"); err == nil {
					t.Fatal("second activation rewrote immutable result")
				}
				changed := request
				changed.OperationID = uuid.NewString()
				changed.TargetBundleHash = "sha256:other"
				if _, _, err := bindForkOperationTx(ctx, tx, changed, childID, bindingID, backend == "postgres"); err == nil {
					t.Fatal("same keyed request changed its semantic target")
				}
				if _, _, err := lookupForkOperationTx(ctx, tx, request.Actor, request.IdempotencyKey, "sha256:other-transport", backend == "postgres", true); err == nil {
					t.Fatal("same key accepted a changed transport request")
				} else {
					var conflict *runfork.ForkOperationKeyConflictError
					if !errors.As(err, &conflict) || conflict.OriginalRequestHash != request.TransportHash || conflict.ConflictingRequestHash != "sha256:other-transport" {
						t.Fatalf("fork request conflict lost exact hashes: %v", err)
					}
				}
			})
		})
	}
}

func TestForkOperationKeylessIsInvocationScopedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			request := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "bearer_token:operator", TransportHash: "sha256:keyless",
				SourceRunID: uuid.NewString(), ForkEventID: uuid.NewString(), TargetBundleHash: "sha256:bundle",
				ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
			}
			request.ResolvedPoint = &runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 1, EventID: request.ForkEventID}
			childID, bindingID := uuid.NewString(), uuid.NewString()
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				if _, replay, err := bindForkOperationTx(context.Background(), tx, request, childID, bindingID, backend == "postgres"); err != nil || replay {
					t.Fatalf("first unkeyed bind replay=%t err=%v", replay, err)
				}
			})
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				record, found, err := loadForkOperationByIDTx(context.Background(), tx, request.OperationID, backend == "postgres")
				if err != nil || !found || record.Request.IdempotencyKey != "" || record.ForkRunID != childID {
					t.Fatalf("keyless result by exact invocation: found=%t record=%+v err=%v", found, record, err)
				}
				second := request
				second.OperationID = uuid.NewString()
				if _, _, err := bindForkOperationTx(context.Background(), tx, second, childID, bindingID, backend == "postgres"); err == nil {
					t.Fatal("second invocation claimed the first child's identity")
				}
			})
		})
	}
}

func TestForkOperationDeploymentRevisionIsDurableAndEventlessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			ctx := context.Background()
			request := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "bearer:operator", IdempotencyKey: "eventless-fork",
				TransportHash: "sha256:eventless", SourceRunID: uuid.NewString(),
				TargetBundleHash: "sha256:bundle", ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
				ResolvedPoint: &runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7},
			}
			childID, bindingID := uuid.NewString(), uuid.NewString()
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				if _, replay, err := bindForkOperationTx(ctx, tx, request, childID, bindingID, backend == "postgres"); err != nil || replay {
					t.Fatalf("bind eventless point replay=%t err=%v", replay, err)
				}
			})
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				retry := request
				retry.ResolvedPoint = nil
				retry.OperationID = uuid.NewString()
				record, replay, err := bindForkOperationTx(ctx, tx, retry, childID, bindingID, backend == "postgres")
				if err != nil || !replay || record.Request.ResolvedPoint == nil || record.Request.ResolvedPoint.Revision != 7 {
					t.Fatalf("response-loss retry changed selected revision: record=%+v replay=%t err=%v", record, replay, err)
				}
			})
			result := runfork.ForkOperationResult{
				SourceRunID: request.SourceRunID, SourceRunStatus: "running", ForkRunID: childID,
				ForkPoint: *request.ResolvedPoint, ForkRunStatus: runfork.RunForkActivatedStatus,
				BundleHash: request.TargetBundleHash, ExecutedEventCount: 0,
			}
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				if err := completeForkOperationTx(ctx, tx, request, result, backend == "postgres"); err != nil {
					t.Fatal(err)
				}
				loaded, found, err := lookupForkOperationTx(ctx, tx, request.Actor, request.IdempotencyKey, request.TransportHash, backend == "postgres", false)
				if err != nil || !found || loaded.Result == nil || loaded.Result.ForkPoint != *request.ResolvedPoint {
					t.Fatalf("eventless result lost exact point: %+v found=%t err=%v", loaded, found, err)
				}
			})
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				if _, err := tx.ExecContext(ctx, `UPDATE run_fork_operations SET fork_revision=8 WHERE operation_id=$1`, request.OperationID); err != nil {
					t.Fatal(err)
				}
			})
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				if _, _, err := lookupForkOperationTx(ctx, tx, request.Actor, request.IdempotencyKey, request.TransportHash, backend == "postgres", false); err == nil || !strings.Contains(err.Error(), "typed revision point") {
					t.Fatalf("contradictory durable revision admitted: %v", err)
				}
			})
		})
	}
}

func forkOperationTestDatabase(t *testing.T, backend string) *sql.DB {
	t.Helper()
	var db *sql.DB
	var err error
	jsonType := "TEXT"
	if backend == "postgres" {
		_, db, _ = testutil.StartEmptyPostgres(t)
		jsonType = "JSONB"
	} else {
		db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "fork-operation.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}
	db.SetMaxOpenConns(1)
	query := strings.ReplaceAll(`CREATE TABLE run_fork_operations (
		operation_id TEXT PRIMARY KEY, actor TEXT NOT NULL, idempotency_key TEXT,
		transport_hash TEXT NOT NULL, semantic_hash TEXT NOT NULL,
		request_json JSON_TYPE NOT NULL, source_run_id TEXT NOT NULL,
		fork_point_kind TEXT NOT NULL, fork_revision BIGINT NOT NULL, fork_event_id TEXT,
		fork_run_id TEXT NOT NULL UNIQUE, selected_binding_id TEXT NOT NULL UNIQUE,
		state TEXT NOT NULL, result_json JSON_TYPE, failure_json JSON_TYPE,
		created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
		UNIQUE (actor, idempotency_key)
	)`, "JSON_TYPE", jsonType)
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}
	return db
}

func withForkOperationTx(t *testing.T, db *sql.DB, run func(*sql.Tx)) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	run(tx)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
