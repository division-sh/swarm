package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func requireForkOperationMaterializationRequest(operation *runfork.ForkOperationRequest, sourceRunID string, point runfork.RunForkPoint, targetBundleHash string, selection runfork.RunForkContractSelection, overrides []durabledata.ExplicitPin) error {
	if operation == nil {
		return nil
	}
	canonical, _, err := operation.Canonical()
	if err != nil {
		return err
	}
	pins, err := durabledata.CanonicalExplicitPins(overrides)
	if err != nil {
		return err
	}
	if canonical.ResolvedPoint == nil || *canonical.ResolvedPoint != point || canonical.SourceRunID != sourceRunID ||
		(canonical.ForkEventID != "" && canonical.ForkEventID != point.EventID) || canonical.TargetBundleHash != targetBundleHash ||
		canonical.ContractSelection != selection || !slices.Equal(canonical.DataPinOverrides, pins) {
		return fmt.Errorf("fork operation differs from selected materialization request")
	}
	return nil
}

func bindResolvedForkOperation(operation *runfork.ForkOperationRequest, point runfork.RunForkPoint) (*runfork.ForkOperationRequest, error) {
	if operation == nil {
		return nil, nil
	}
	if err := point.Validate(); err != nil {
		return nil, err
	}
	canonical, _, err := operation.Canonical()
	if err != nil {
		return nil, err
	}
	if canonical.ResolvedPoint != nil && *canonical.ResolvedPoint != point {
		return nil, fmt.Errorf("fork operation resolved point differs from selected source revision")
	}
	canonical.ResolvedPoint = &point
	return &canonical, nil
}

// lookupForkOperationTx is the only request-to-child decoder. The API may use
// this before source resolution, but materialization must recheck it under its
// own transaction before writing child state.
func lookupForkOperationTx(ctx context.Context, tx *sql.Tx, actor, key, transportHash string, postgres, lock bool) (runfork.ForkOperationRecord, bool, error) {
	if tx == nil || strings.TrimSpace(actor) == "" || strings.TrimSpace(key) == "" || strings.TrimSpace(transportHash) == "" {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("keyed fork operation lookup requires transaction, actor, key and request hash")
	}
	query := `SELECT operation_id, actor, idempotency_key, transport_hash, semantic_hash,
		request_json, fork_point_kind, fork_revision, COALESCE(CAST(fork_event_id AS TEXT), ''),
		fork_run_id, selected_binding_id, state, result_json, failure_json
		FROM run_fork_operations WHERE actor=$1 AND idempotency_key=$2`
	if postgres && lock {
		query += ` FOR UPDATE`
	}
	record, found, err := decodeForkOperationRow(tx.QueryRowContext(ctx, query, actor, key))
	if err != nil || !found {
		return record, found, err
	}
	if record.Request.TransportHash != transportHash {
		return runfork.ForkOperationRecord{}, false, &runfork.ForkOperationKeyConflictError{
			OriginalRequestHash: record.Request.TransportHash, ConflictingRequestHash: transportHash,
		}
	}
	return record, true, nil
}

func loadForkOperationByIDTx(ctx context.Context, tx *sql.Tx, operationID string, lock bool) (runfork.ForkOperationRecord, bool, error) {
	if tx == nil || strings.TrimSpace(operationID) == "" {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("fork operation lookup requires transaction and operation ID")
	}
	query := `SELECT operation_id, actor, idempotency_key, transport_hash, semantic_hash,
		request_json, fork_point_kind, fork_revision, COALESCE(CAST(fork_event_id AS TEXT), ''),
		fork_run_id, selected_binding_id, state, result_json, failure_json
		FROM run_fork_operations WHERE operation_id=$1`
	if lock {
		query += ` FOR SHARE`
	}
	return decodeForkOperationRow(tx.QueryRowContext(ctx, query, operationID))
}

func decodeForkOperationRow(row *sql.Row) (runfork.ForkOperationRecord, bool, error) {
	var record runfork.ForkOperationRecord
	var requestRaw, resultRaw, failureRaw []byte
	var key sql.NullString
	var pointKind, eventID string
	var revision int64
	err := row.Scan(
		&record.Request.OperationID, &record.Request.Actor, &key,
		&record.Request.TransportHash, &record.SemanticHash, &requestRaw, &pointKind, &revision, &eventID,
		&record.ForkRunID, &record.BindingID, &record.Status, &resultRaw, &failureRaw,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return runfork.ForkOperationRecord{}, false, nil
	}
	if err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	if key.Valid {
		record.Request.IdempotencyKey = key.String
	}
	storedIdentity := record.Request
	if err := canonicaljson.DecodeInto(requestRaw, &record.Request); err != nil {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("decode durable fork request: %w", err)
	}
	if record.Request.OperationID != storedIdentity.OperationID || record.Request.Actor != storedIdentity.Actor ||
		record.Request.IdempotencyKey != storedIdentity.IdempotencyKey || record.Request.TransportHash != storedIdentity.TransportHash {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("durable fork request disagrees with typed identity")
	}
	if record.Request.ResolvedPoint == nil || record.Request.ResolvedPoint.Kind != runfork.RunForkPointKind(pointKind) ||
		record.Request.ResolvedPoint.Revision != revision || record.Request.ResolvedPoint.EventID != eventID {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("durable fork request disagrees with typed revision point")
	}
	if len(resultRaw) != 0 && string(resultRaw) != "null" {
		var result runfork.ForkOperationResult
		if err := canonicaljson.DecodeInto(resultRaw, &result); err != nil {
			return runfork.ForkOperationRecord{}, false, fmt.Errorf("decode durable fork result: %w", err)
		}
		record.Result = &result
	}
	if len(failureRaw) != 0 && string(failureRaw) != "null" {
		var failure runfork.ForkOperationFailure
		if err := canonicaljson.DecodeInto(failureRaw, &failure); err != nil {
			return runfork.ForkOperationRecord{}, false, fmt.Errorf("decode durable fork failure: %w", err)
		}
		record.Failure = &failure
	}
	if err := record.Validate(); err != nil {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("durable fork operation is contradictory: %w", err)
	}
	return record, true, nil
}

func (s *RunForkPostgresOwner) LoadForkOperation(ctx context.Context, actor, key, transportHash string) (runfork.ForkOperationRecord, bool, error) {
	if s == nil || s.backend == nil {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("postgres fork operation store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	defer tx.Rollback()
	return lookupForkOperationTx(ctx, tx, actor, key, transportHash, true, false)
}

func (s *RunForkSQLiteOwner) LoadForkOperation(ctx context.Context, actor, key, transportHash string) (runfork.ForkOperationRecord, bool, error) {
	if s == nil || s.backend == nil {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("sqlite fork operation store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	var record runfork.ForkOperationRecord
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		record, found, err = lookupForkOperationTx(txctx, tx, actor, key, transportHash, false, false)
		return err
	})
	return record, found, err
}

func (s *RunForkPostgresOwner) LoadForkOperationByID(ctx context.Context, operationID string) (runfork.ForkOperationRecord, bool, error) {
	if s == nil || s.backend == nil {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("postgres fork operation store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	defer tx.Rollback()
	return loadForkOperationByIDTx(ctx, tx, operationID, false)
}

func (s *RunForkSQLiteOwner) LoadForkOperationByID(ctx context.Context, operationID string) (runfork.ForkOperationRecord, bool, error) {
	if s == nil || s.backend == nil {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("sqlite fork operation store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	var record runfork.ForkOperationRecord
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		record, found, err = loadForkOperationByIDTx(txctx, tx, operationID, false)
		return err
	})
	return record, found, err
}

// bindForkOperationTx executes inside selected materialization, after child and
// binding insertion but before commit. Concurrent keys cannot own two children.
func bindForkOperationTx(ctx context.Context, tx *sql.Tx, request runfork.ForkOperationRequest, forkRunID, bindingID string, postgres bool) (runfork.ForkOperationRecord, bool, error) {
	canonical, semanticHash, err := request.Canonical()
	if err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	if canonical.IdempotencyKey != "" {
		existing, found, err := lookupForkOperationTx(ctx, tx, canonical.Actor, canonical.IdempotencyKey, canonical.TransportHash, postgres, true)
		if err != nil || found {
			if err == nil && (existing.SemanticHash != semanticHash || existing.ForkRunID != forkRunID || existing.BindingID != bindingID) {
				err = fmt.Errorf("fork operation key already binds another selected request or child")
			}
			return existing, found, err
		}
	}
	if canonical.ResolvedPoint == nil {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("new fork operation requires its selected revision point")
	}
	record := runfork.ForkOperationRecord{Request: canonical, SemanticHash: semanticHash, ForkRunID: forkRunID, BindingID: bindingID, Status: runfork.ForkOperationMaterialized}
	if err := record.Validate(); err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	requestRaw, err := canonicaljson.Bytes(canonical)
	if err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	var key any
	if canonical.IdempotencyKey != "" {
		key = canonical.IdempotencyKey
	}
	query := `INSERT INTO run_fork_operations
		(operation_id, actor, idempotency_key, transport_hash, semantic_hash, request_json,
		 source_run_id, fork_point_kind, fork_revision, fork_event_id, fork_run_id, selected_binding_id, state, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'materialized',$13,$13) ON CONFLICT DO NOTHING`
	if postgres {
		query = strings.Replace(query, "$6,", "$6::jsonb,", 1)
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, query, canonical.OperationID, canonical.Actor, key, canonical.TransportHash,
		semanticHash, string(requestRaw), canonical.SourceRunID, canonical.ResolvedPoint.Kind,
		canonical.ResolvedPoint.Revision, nullableForkEventID(*canonical.ResolvedPoint), forkRunID, bindingID, now)
	if err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	if inserted == 1 {
		return record, false, nil
	}
	if canonical.IdempotencyKey == "" {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("unkeyed fork invocation or child already exists")
	}
	existing, found, err := lookupForkOperationTx(ctx, tx, canonical.Actor, canonical.IdempotencyKey, canonical.TransportHash, postgres, true)
	if err != nil {
		return runfork.ForkOperationRecord{}, false, err
	}
	if !found {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("fork operation conflict without matching durable key")
	}
	if existing.SemanticHash != semanticHash || existing.ForkRunID != forkRunID || existing.BindingID != bindingID {
		return runfork.ForkOperationRecord{}, false, fmt.Errorf("fork operation key already binds another selected request or child")
	}
	return existing, true, nil
}

// Activation consumes the exact resolved request committed at materialization;
// the API request copy can predate revision resolution.
func resolvedForkOperationForActivationTx(ctx context.Context, tx *sql.Tx, caller runfork.ForkOperationRequest, lineage runForkActivationLineage, postgres bool) (runfork.ForkOperationRequest, error) {
	canonical, semanticHash, err := caller.Canonical()
	if err != nil {
		return runfork.ForkOperationRequest{}, err
	}
	record, found, err := loadForkOperationByIDTx(ctx, tx, canonical.OperationID, postgres)
	if err != nil {
		return runfork.ForkOperationRequest{}, err
	}
	if !found || record.Status != runfork.ForkOperationMaterialized || record.ForkRunID != lineage.ForkRunID ||
		record.SemanticHash != semanticHash || record.Request.Actor != canonical.Actor ||
		record.Request.IdempotencyKey != canonical.IdempotencyKey || record.Request.TransportHash != canonical.TransportHash ||
		record.Request.SourceRunID != lineage.SourceRunID || record.Request.TargetBundleHash != lineage.ForkBundleHash {
		return runfork.ForkOperationRequest{}, fmt.Errorf("selected activation differs from materialized fork operation")
	}
	point := record.Request.ResolvedPoint
	if point == nil || point.Kind != lineage.ForkPoint.Kind || point.Revision != lineage.ForkPoint.Revision ||
		point.EventID != lineage.ForkPoint.EventID || (canonical.ResolvedPoint != nil && *canonical.ResolvedPoint != *point) {
		return runfork.ForkOperationRequest{}, fmt.Errorf("selected activation differs from durable fork point")
	}
	var bound bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM run_fork_selected_contract_bindings
		WHERE binding_id=$1 AND fork_run_id=$2 AND source_run_id=$3
		AND fork_point_kind=$4 AND fork_revision=$5 AND COALESCE(CAST(fork_event_id AS TEXT),'')=$6
		AND mode=$7 AND COALESCE(bundle_hash,'')=$8
	)`, record.BindingID, lineage.ForkRunID, lineage.SourceRunID, point.Kind, point.Revision, point.EventID,
		record.Request.ContractSelection.Mode, record.Request.ContractSelection.BundleHash).Scan(&bound)
	if err != nil {
		return runfork.ForkOperationRequest{}, fmt.Errorf("verify selected fork operation binding: %w", err)
	}
	if !bound {
		return runfork.ForkOperationRequest{}, fmt.Errorf("selected activation lacks exact materialized binding")
	}
	return record.Request, nil
}

// completeForkOperationTx is called by the selected activation transaction,
// not by post-execution API response caching.
func completeForkOperationTx(ctx context.Context, tx *sql.Tx, request runfork.ForkOperationRequest, result runfork.ForkOperationResult, postgres bool) error {
	canonical, semanticHash, err := request.Canonical()
	if err != nil {
		return err
	}
	if err := result.Validate(canonical, result.ForkRunID); err != nil {
		return err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	query := `UPDATE run_fork_operations SET state='activated', result_json=$2, updated_at=$3
		WHERE operation_id=$1 AND fork_run_id=$4 AND state='materialized'
		AND actor=$5 AND transport_hash=$6 AND semantic_hash=$7 AND source_run_id=$8
		AND fork_point_kind=$9 AND fork_revision=$10 AND COALESCE(CAST(fork_event_id AS TEXT),'')=$11`
	if postgres {
		query = strings.Replace(query, "result_json=$2", "result_json=$2::jsonb", 1)
	}
	res, err := tx.ExecContext(ctx, query, canonical.OperationID, string(raw), time.Now().UTC(), result.ForkRunID,
		canonical.Actor, canonical.TransportHash, semanticHash, canonical.SourceRunID,
		canonical.ResolvedPoint.Kind, canonical.ResolvedPoint.Revision, canonical.ResolvedPoint.EventID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("fork activation has no matching materialized operation")
	}
	return nil
}

type failedForkOperationPort struct {
	postgres     bool
	runMutation  func(context.Context, func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) error
	loadSnapshot func(context.Context, *sql.Tx, string) (runtimerunlifecycle.Snapshot, error)
	terminalize  func(context.Context, *mutationprotocol.Attempt, string, string) error
	markTerminal func(context.Context, *mutationprotocol.Attempt, runtimerunlifecycle.TerminalRequest) error
}

func failMaterializedForkOperation(ctx context.Context, forkRunID string, failure runfork.ForkOperationFailure, port failedForkOperationPort) error {
	if err := failure.Validate(); err != nil {
		return err
	}
	if port.runMutation == nil || port.loadSnapshot == nil || port.terminalize == nil || port.markTerminal == nil {
		return fmt.Errorf("fork operation failure owner is incomplete")
	}
	failureRaw, err := canonicaljson.Bytes(failure)
	if err != nil {
		return err
	}
	return port.runMutation(ctx, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) error {
		snapshot, err := port.loadSnapshot(txctx, tx, forkRunID)
		if err != nil {
			return err
		}
		if snapshot.State != runtimerunlifecycle.StatePaused {
			return fmt.Errorf("failed selected fork must still be materialized, got %s", snapshot.State)
		}
		if err := port.terminalize(txctx, attempt, forkRunID, failure.Code); err != nil {
			return err
		}
		if err := port.markTerminal(txctx, attempt, runtimerunlifecycle.TerminalRequest{
			RunID: forkRunID, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC(),
		}); err != nil {
			return err
		}
		query := `UPDATE run_fork_operations SET state='failed', failure_json=$2, updated_at=$3
			WHERE fork_run_id=$1 AND state='materialized'`
		if port.postgres {
			query = strings.Replace(query, "failure_json=$2", "failure_json=$2::jsonb", 1)
		}
		result, err := tx.ExecContext(txctx, query, forkRunID, string(failureRaw), time.Now().UTC())
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return fmt.Errorf("failed selected fork has no exact materialized operation")
		}
		return nil
	})
}

func (s *RunForkPostgresOwner) FailMaterializedSelectedContractExecutionFork(ctx context.Context, forkRunID string, failure runfork.ForkOperationFailure) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres fork operation store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return failMaterializedForkOperation(ctx, forkRunID, failure, failedForkOperationPort{
		postgres: true,
		runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) error {
			result := mutationprotocol.RunPostgresWithOptions(ctx, s.backend, &sql.TxOptions{Isolation: sql.LevelSerializable}, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates,
				func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
					return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error { return operation(txctx, tx, attempt) })
				})
			return result.Err()
		},
		loadSnapshot: func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
			return s.RunLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, runID, true)
		},
		terminalize: func(ctx context.Context, attempt *mutationprotocol.Attempt, runID, reason string) error {
			_, err := s.TerminalizeRunDeliveriesTx(ctx, attempt, runID, reason)
			return err
		},
		markTerminal: func(ctx context.Context, attempt *mutationprotocol.Attempt, req runtimerunlifecycle.TerminalRequest) error {
			_, _, err := s.RunLifecyclePostgresOwner.MarkTerminalTx(ctx, attempt, req)
			return err
		},
	})
}

func (s *RunForkSQLiteOwner) FailMaterializedSelectedContractExecutionFork(ctx context.Context, forkRunID string, failure runfork.ForkOperationFailure) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite fork operation store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return failMaterializedForkOperation(ctx, forkRunID, failure, failedForkOperationPort{
		postgres: false,
		runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) error {
			result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite selected fork operation failure", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates,
				func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
					return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error { return operation(txctx, tx, attempt) })
				})
			return result.Err()
		},
		loadSnapshot: func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
			return s.RunLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, runID)
		},
		terminalize: func(ctx context.Context, attempt *mutationprotocol.Attempt, runID, reason string) error {
			_, err := s.TerminalizeRunDeliveriesTx(ctx, attempt, runID, reason)
			return err
		},
		markTerminal: func(ctx context.Context, attempt *mutationprotocol.Attempt, req runtimerunlifecycle.TerminalRequest) error {
			_, _, err := s.RunLifecycleSQLiteOwner.MarkTerminalTx(ctx, attempt, req)
			return err
		},
	})
}
