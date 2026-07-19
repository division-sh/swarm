package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

const directiveOperationDefaultTTL = 24 * time.Hour

var _ runtimeagentcontrol.DirectiveOperationStore = (*PostgresStore)(nil)
var _ runtimeagentcontrol.DirectiveOperationStore = (*SQLiteRuntimeStore)(nil)

func (s *PostgresStore) ReserveDirectiveOperation(ctx context.Context, req runtimeagentcontrol.ReserveDirectiveOperationRequest) (runtimeagentcontrol.DirectiveOperationReservation, error) {
	if s == nil || s.DB == nil {
		return runtimeagentcontrol.DirectiveOperationReservation{}, fmt.Errorf("postgres store is required")
	}
	req.Now = normalizeDirectiveNow(req.Now)
	op, err := validateDirectiveReservation(req)
	if err != nil {
		return runtimeagentcontrol.DirectiveOperationReservation{}, err
	}
	var reservation runtimeagentcontrol.DirectiveOperationReservation
	err = s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if op.IdempotencyKey != "" {
			if _, err := tx.ExecContext(txctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, directiveOperationLockKey(op)); err != nil {
				return fmt.Errorf("lock directive operation key: %w", err)
			}
		}
		if err := purgeExpiredPostgresDirectiveOperations(txctx, tx, req.Now); err != nil {
			return err
		}
		if op.IdempotencyKey != "" {
			existing, ok, err := loadPostgresDirectiveOperationByKey(txctx, tx, op.Method, op.ActorTokenID, op.IdempotencyKey)
			if err != nil {
				return err
			}
			if ok {
				if existing.RequestHash != op.RequestHash {
					return &runtimeagentcontrol.DirectiveIdempotencyConflictError{OriginalRequestHash: existing.RequestHash, ConflictingRequestHash: op.RequestHash, OperationID: existing.OperationID}
				}
				reservation = runtimeagentcontrol.DirectiveOperationReservation{Operation: existing}
				return nil
			}
			// Pre-operation directive completions are not a second replay authority.
			if _, err := tx.ExecContext(txctx, `DELETE FROM api_idempotency WHERE method = $1 AND actor_token_id = $2 AND idempotency_key = $3`, op.Method, op.ActorTokenID, op.IdempotencyKey); err != nil {
				return fmt.Errorf("remove legacy directive idempotency projection: %w", err)
			}
		}
		outcome, err := (sqlPublishCommitter{tx: tx, store: s}).commitNamedEvent(txctx, "reserve directive operation", events.EventAdmissionDiagnosticDirect, events.EventTypePlatformAgentDirective, runtimebus.CommitPublishRequest{
			Event: req.Event, ReplayScope: runtimereplayclaim.CommittedReplayScopeDirect,
		})
		if err != nil {
			return err
		}
		if outcome == runtimebus.EventAppendExactDuplicate {
			return fmt.Errorf("directive event %s already exists without its operation", op.DirectiveEventID)
		}
		if err := insertPostgresDirectiveOperation(txctx, tx, op, req.Now); err != nil {
			return err
		}
		op.CreatedAt = req.Now
		op.UpdatedAt = req.Now
		reservation = runtimeagentcontrol.DirectiveOperationReservation{Operation: op, Created: true}
		return recordDirectiveAuthorActivity(txctx, op, req.Now, nil)
	})
	return reservation, err
}

func (s *SQLiteRuntimeStore) ReserveDirectiveOperation(ctx context.Context, req runtimeagentcontrol.ReserveDirectiveOperationRequest) (runtimeagentcontrol.DirectiveOperationReservation, error) {
	if s == nil || s.DB == nil {
		return runtimeagentcontrol.DirectiveOperationReservation{}, fmt.Errorf("sqlite runtime store is required")
	}
	req.Now = normalizeDirectiveNow(req.Now)
	op, err := validateDirectiveReservation(req)
	if err != nil {
		return runtimeagentcontrol.DirectiveOperationReservation{}, err
	}
	var reservation runtimeagentcontrol.DirectiveOperationReservation
	err = s.runAuthorActivityMutation(ctx, "sqlite reserve directive operation", func(txctx context.Context, tx *sql.Tx) error {
		if err := purgeExpiredSQLiteDirectiveOperationsTx(txctx, tx, req.Now); err != nil {
			return err
		}
		if op.IdempotencyKey != "" {
			existing, ok, err := loadSQLiteDirectiveOperationByKey(txctx, tx, op.Method, op.ActorTokenID, op.IdempotencyKey)
			if err != nil {
				return err
			}
			if ok {
				if existing.RequestHash != op.RequestHash {
					return &runtimeagentcontrol.DirectiveIdempotencyConflictError{OriginalRequestHash: existing.RequestHash, ConflictingRequestHash: op.RequestHash, OperationID: existing.OperationID}
				}
				reservation = runtimeagentcontrol.DirectiveOperationReservation{Operation: existing}
				return nil
			}
			if _, err := tx.ExecContext(txctx, `DELETE FROM api_idempotency WHERE method = ? AND actor_token_id = ? AND idempotency_key = ?`, op.Method, op.ActorTokenID, op.IdempotencyKey); err != nil {
				return fmt.Errorf("remove legacy sqlite directive idempotency projection: %w", err)
			}
		}
		outcome, err := (sqlPublishCommitter{tx: tx, store: s}).commitNamedEvent(txctx, "reserve directive operation", events.EventAdmissionDiagnosticDirect, events.EventTypePlatformAgentDirective, runtimebus.CommitPublishRequest{
			Event: req.Event, ReplayScope: runtimereplayclaim.CommittedReplayScopeDirect,
		})
		if err != nil {
			return err
		}
		if outcome == runtimebus.EventAppendExactDuplicate {
			return fmt.Errorf("directive event %s already exists without its operation", op.DirectiveEventID)
		}
		if err := insertSQLiteDirectiveOperationTx(txctx, tx, op, req.Now); err != nil {
			return err
		}
		op.CreatedAt = req.Now
		op.UpdatedAt = req.Now
		reservation = runtimeagentcontrol.DirectiveOperationReservation{Operation: op, Created: true}
		return recordDirectiveAuthorActivity(txctx, op, req.Now, nil)
	})
	return reservation, err
}

func validateDirectiveReservation(req runtimeagentcontrol.ReserveDirectiveOperationRequest) (runtimeagentcontrol.DirectiveOperation, error) {
	op := req.Operation.Normalized()
	if op.Method == "" {
		op.Method = runtimeagentcontrol.DirectiveOperationMethod
	}
	if op.Method != runtimeagentcontrol.DirectiveOperationMethod {
		return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("unsupported directive operation method %q", op.Method)
	}
	for name, value := range map[string]string{
		"operation_id":       op.OperationID,
		"actor_token_id":     op.ActorTokenID,
		"request_hash":       op.RequestHash,
		"agent_id":           op.AgentID,
		"directive":          op.Directive,
		"resolved_run_id":    op.ResolvedRunID,
		"run_id_resolution":  op.RunIDResolution,
		"source":             op.Source,
		"directive_event_id": op.DirectiveEventID,
	} {
		if strings.TrimSpace(value) == "" {
			return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("%s is required", name)
		}
	}
	for name, value := range map[string]string{"operation_id": op.OperationID, "resolved_run_id": op.ResolvedRunID, "directive_event_id": op.DirectiveEventID} {
		if _, err := uuid.Parse(value); err != nil {
			return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("%s must be a UUID: %w", name, err)
		}
	}
	if op.RequestedRunID != "" {
		if _, err := uuid.Parse(op.RequestedRunID); err != nil {
			return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("requested_run_id must be a UUID: %w", err)
		}
	}
	event := req.Event.Event()
	if event.ID() != op.DirectiveEventID || event.RunID() != op.ResolvedRunID || event.Type() != events.EventTypePlatformAgentDirective || req.Event.Class() != events.EventAdmissionDiagnosticDirect {
		return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("directive operation event identity mismatch")
	}
	if op.State == "" {
		op.State = runtimeagentcontrol.DirectiveOperationPrepared
	}
	if op.State != runtimeagentcontrol.DirectiveOperationPrepared {
		return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("new directive operation state must be prepared")
	}
	if err := runtimeagentcontrol.ValidateDirectiveOperationEvidence(op); err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, err
	}
	return op, nil
}

func normalizeDirectiveNow(now time.Time) time.Time {
	if now.IsZero() {
		return time.Now().UTC()
	}
	return now.UTC()
}

func directiveOperationLockKey(op runtimeagentcontrol.DirectiveOperation) string {
	return "swarm:agent-directive-operation:" + strings.Join([]string{op.Method, op.ActorTokenID, op.IdempotencyKey}, "|")
}

func insertPostgresDirectiveOperation(ctx context.Context, tx *sql.Tx, op runtimeagentcontrol.DirectiveOperation, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO agent_directive_operations (
			operation_id, method, actor_token_id, idempotency_key, request_hash,
			agent_id, directive_text, requested_run_id, resolved_run_id, run_id_resolution,
			source, operator_id, directive_event_id, state, created_at, updated_at
		) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, NULLIF($8, '')::uuid, $9::uuid, $10, $11, $12, $13::uuid, $14, $15, $15)
	`, op.OperationID, op.Method, op.ActorTokenID, nullableString(op.IdempotencyKey), op.RequestHash, op.AgentID, op.Directive, op.RequestedRunID, op.ResolvedRunID, op.RunIDResolution, op.Source, nullableString(op.OperatorID), op.DirectiveEventID, string(op.State), now.UTC())
	if err != nil {
		return fmt.Errorf("insert directive operation: %w", err)
	}
	return nil
}

func insertSQLiteDirectiveOperationTx(ctx context.Context, tx *sql.Tx, op runtimeagentcontrol.DirectiveOperation, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO agent_directive_operations (
			operation_id, method, actor_token_id, idempotency_key, request_hash,
			agent_id, directive_text, requested_run_id, resolved_run_id, run_id_resolution,
			source, operator_id, directive_event_id, state, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, op.OperationID, op.Method, op.ActorTokenID, sqliteNullString(op.IdempotencyKey), op.RequestHash, op.AgentID, op.Directive, sqliteNullUUID(op.RequestedRunID), op.ResolvedRunID, op.RunIDResolution, op.Source, sqliteNullString(op.OperatorID), op.DirectiveEventID, string(op.State), now.UTC(), now.UTC())
	if err != nil {
		return fmt.Errorf("insert sqlite directive operation: %w", err)
	}
	return nil
}

func (s *PostgresStore) AdmitDirectiveExecution(ctx context.Context, operationID, ownerID string, now time.Time, lease time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	return s.transitionPostgresDirectiveOperation(ctx, operationID, func(txctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(txctx, `UPDATE agent_directive_operations SET state = 'executing', execution_owner_id = $2, execution_admitted_at = $3, execution_lease_expires_at = $4, updated_at = $3 WHERE operation_id = $1::uuid AND state = 'prepared'`, operationID, ownerID, now.UTC(), now.Add(normalizeDirectiveLease(lease)).UTC())
		return requireDirectiveTransition(res, err)
	})
}

func (s *SQLiteRuntimeStore) AdmitDirectiveExecution(ctx context.Context, operationID, ownerID string, now time.Time, lease time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	return s.transitionSQLiteDirectiveOperation(ctx, operationID, func(txctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(txctx, `UPDATE agent_directive_operations SET state = 'executing', execution_owner_id = ?, execution_admitted_at = ?, execution_lease_expires_at = ?, updated_at = ? WHERE operation_id = ? AND state = 'prepared'`, ownerID, now.UTC(), now.Add(normalizeDirectiveLease(lease)).UTC(), now.UTC(), operationID)
		return requireDirectiveTransition(res, err)
	})
}

func normalizeDirectiveLease(lease time.Duration) time.Duration {
	if lease <= 0 {
		return 2 * time.Minute
	}
	return lease
}

func requireDirectiveTransition(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return runtimeagentcontrol.ErrDirectiveTransitionConflict
	}
	return nil
}

func (s *PostgresStore) RenewDirectiveExecutionLease(ctx context.Context, operationID, ownerID string, now time.Time, lease time.Duration) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := requireActivePostgresDirectiveOperation(ctx, tx, operationID); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE agent_directive_operations SET execution_lease_expires_at = $4, updated_at = $3 WHERE operation_id = $1::uuid AND execution_owner_id = $2 AND state = 'executing'`, operationID, ownerID, now.UTC(), now.Add(normalizeDirectiveLease(lease)).UTC())
	if err := requireDirectiveTransition(res, err); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteRuntimeStore) RenewDirectiveExecutionLease(ctx context.Context, operationID, ownerID string, now time.Time, lease time.Duration) error {
	return s.runRuntimeMutation(ctx, "sqlite renew directive execution lease", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := requireActiveSQLiteDirectiveOperation(txctx, tx, operationID); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `UPDATE agent_directive_operations SET execution_lease_expires_at = ?, updated_at = ? WHERE operation_id = ? AND execution_owner_id = ? AND state = 'executing'`, now.Add(normalizeDirectiveLease(lease)).UTC(), now.UTC(), operationID, ownerID)
		return requireDirectiveTransition(res, err)
	})
}

func (s *PostgresStore) RecordDirectiveExecuted(ctx context.Context, operationID, ownerID string, response json.RawMessage, now time.Time) (runtimeagentcontrol.DirectiveOperation, error) {
	if !json.Valid(response) {
		return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("directive response must be valid JSON")
	}
	return s.transitionPostgresDirectiveOperation(ctx, operationID, func(txctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(txctx, `UPDATE agent_directive_operations SET state = 'executed', response = $3::jsonb, executed_at = $4, execution_lease_expires_at = NULL, updated_at = $4 WHERE operation_id = $1::uuid AND execution_owner_id = $2 AND state = 'executing'`, operationID, ownerID, string(response), now.UTC())
		return requireDirectiveTransition(res, err)
	})
}

func (s *SQLiteRuntimeStore) RecordDirectiveExecuted(ctx context.Context, operationID, ownerID string, response json.RawMessage, now time.Time) (runtimeagentcontrol.DirectiveOperation, error) {
	if !json.Valid(response) {
		return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("directive response must be valid JSON")
	}
	return s.transitionSQLiteDirectiveOperation(ctx, operationID, func(txctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(txctx, `UPDATE agent_directive_operations SET state = 'executed', response = ?, executed_at = ?, execution_lease_expires_at = NULL, updated_at = ? WHERE operation_id = ? AND execution_owner_id = ? AND state = 'executing'`, string(response), now.UTC(), now.UTC(), operationID, ownerID)
		return requireDirectiveTransition(res, err)
	})
}

func (s *PostgresStore) FinalizeDirectiveSuccess(ctx context.Context, operationID string, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	var out runtimeagentcontrol.DirectiveOperation
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		op, err := requireActivePostgresDirectiveOperation(txctx, tx, operationID)
		if err != nil {
			return err
		}
		if op.State != runtimeagentcontrol.DirectiveOperationExecuted && op.State != runtimeagentcontrol.DirectiveOperationSucceeded {
			return runtimeagentcontrol.ErrorForDirectiveOperation(op)
		}
		if err := s.UpsertPipelineReceiptTx(txctx, tx, op.DirectiveEventID, "processed", nil); err != nil {
			return err
		}
		if err := storePostgresDirectiveProjection(txctx, tx, op, now, ttl); err != nil {
			return err
		}
		if op.State == runtimeagentcontrol.DirectiveOperationExecuted {
			if _, err := tx.ExecContext(txctx, `UPDATE agent_directive_operations SET state = 'succeeded', completed_at = $2, updated_at = $2, expires_at = $3 WHERE operation_id = $1::uuid AND state = 'executed'`, op.OperationID, now.UTC(), now.Add(normalizeDirectiveTTL(ttl)).UTC()); err != nil {
				return err
			}
			op.State = runtimeagentcontrol.DirectiveOperationSucceeded
			op.CompletedAt = now.UTC()
			op.UpdatedAt = now.UTC()
			op.ExpiresAt = now.Add(normalizeDirectiveTTL(ttl)).UTC()
		}
		out = op
		return recordDirectiveAuthorActivity(txctx, op, op.UpdatedAt, nil)
	})
	return out, err
}

func (s *SQLiteRuntimeStore) FinalizeDirectiveSuccess(ctx context.Context, operationID string, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	var out runtimeagentcontrol.DirectiveOperation
	err := s.runAuthorActivityMutation(ctx, "sqlite finalize directive success", func(txctx context.Context, tx *sql.Tx) error {
		op, err := requireActiveSQLiteDirectiveOperation(txctx, tx, operationID)
		if err != nil {
			return err
		}
		if op.State != runtimeagentcontrol.DirectiveOperationExecuted && op.State != runtimeagentcontrol.DirectiveOperationSucceeded {
			return runtimeagentcontrol.ErrorForDirectiveOperation(op)
		}
		if err := s.UpsertPipelineReceiptTx(txctx, tx, op.DirectiveEventID, "processed", nil); err != nil {
			return err
		}
		if err := storeSQLiteDirectiveProjectionTx(txctx, tx, op, now, ttl); err != nil {
			return err
		}
		if op.State == runtimeagentcontrol.DirectiveOperationExecuted {
			if _, err := tx.ExecContext(txctx, `UPDATE agent_directive_operations SET state = 'succeeded', completed_at = ?, updated_at = ?, expires_at = ? WHERE operation_id = ? AND state = 'executed'`, now.UTC(), now.UTC(), now.Add(normalizeDirectiveTTL(ttl)).UTC(), op.OperationID); err != nil {
				return err
			}
			op.State = runtimeagentcontrol.DirectiveOperationSucceeded
			op.CompletedAt = now.UTC()
			op.UpdatedAt = now.UTC()
			op.ExpiresAt = now.Add(normalizeDirectiveTTL(ttl)).UTC()
		}
		out = op
		return recordDirectiveAuthorActivity(txctx, op, op.UpdatedAt, nil)
	})
	return out, err
}

func normalizeDirectiveTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return directiveOperationDefaultTTL
	}
	return ttl
}

func storePostgresDirectiveProjection(ctx context.Context, tx *sql.Tx, op runtimeagentcontrol.DirectiveOperation, now time.Time, ttl time.Duration) error {
	if op.IdempotencyKey == "" {
		return nil
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO api_idempotency (method, actor_token_id, idempotency_key, request_hash, resource_id, response, created_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
		ON CONFLICT (method, actor_token_id, idempotency_key) DO UPDATE SET
			resource_id = EXCLUDED.resource_id, response = EXCLUDED.response, created_at = EXCLUDED.created_at, expires_at = EXCLUDED.expires_at
		WHERE api_idempotency.request_hash = EXCLUDED.request_hash
	`, op.Method, op.ActorTokenID, op.IdempotencyKey, op.RequestHash, op.OperationID, string(op.Response), now.UTC(), now.Add(normalizeDirectiveTTL(ttl)).UTC())
	if err != nil {
		return fmt.Errorf("store directive API completion projection: %w", err)
	}
	return requireDirectiveProjection(res)
}

func storeSQLiteDirectiveProjectionTx(ctx context.Context, tx *sql.Tx, op runtimeagentcontrol.DirectiveOperation, now time.Time, ttl time.Duration) error {
	if op.IdempotencyKey == "" {
		return nil
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO api_idempotency (method, actor_token_id, idempotency_key, request_hash, resource_id, response, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(method, actor_token_id, idempotency_key) DO UPDATE SET
			resource_id = excluded.resource_id, response = excluded.response, created_at = excluded.created_at, expires_at = excluded.expires_at
		WHERE api_idempotency.request_hash = excluded.request_hash
	`, op.Method, op.ActorTokenID, op.IdempotencyKey, op.RequestHash, op.OperationID, string(op.Response), now.UTC(), now.Add(normalizeDirectiveTTL(ttl)).UTC())
	if err != nil {
		return fmt.Errorf("store sqlite directive API completion projection: %w", err)
	}
	return requireDirectiveProjection(res)
}

func requireDirectiveProjection(res sql.Result) error {
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("directive API completion projection request hash conflict")
	}
	return nil
}

func (s *PostgresStore) FinalizeDirectiveFailure(ctx context.Context, operationID, ownerID string, failure runtimefailures.Envelope, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	return s.finalizePostgresDirectiveFailure(ctx, operationID, ownerID, runtimeagentcontrol.DirectiveOperationExecuting, runtimeagentcontrol.DirectiveOperationFailed, failure, now, ttl)
}

func (s *SQLiteRuntimeStore) FinalizeDirectiveFailure(ctx context.Context, operationID, ownerID string, failure runtimefailures.Envelope, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	return s.finalizeSQLiteDirectiveFailure(ctx, operationID, ownerID, runtimeagentcontrol.DirectiveOperationExecuting, runtimeagentcontrol.DirectiveOperationFailed, failure, now, ttl)
}

func (s *PostgresStore) finalizePostgresDirectiveFailure(ctx context.Context, operationID, ownerID string, from, to runtimeagentcontrol.DirectiveOperationState, failure runtimefailures.Envelope, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	failureRaw, err := runtimefailures.MarshalEnvelope(failure)
	if err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("validate directive operation failure: %w", err)
	}
	var out runtimeagentcontrol.DirectiveOperation
	err = s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		op, err := requireActivePostgresDirectiveOperation(txctx, tx, operationID)
		if err != nil {
			return err
		}
		if op.State == to {
			if !sameDirectiveFailure(op.Failure, failureRaw) {
				return fmt.Errorf("directive operation %s terminal failure conflicts with persisted failure", operationID)
			}
			out = op
			return nil
		}
		if op.State != from || (ownerID != "" && op.ExecutionOwnerID != ownerID) {
			return runtimeagentcontrol.ErrorForDirectiveOperation(op)
		}
		if err := s.UpsertPipelineReceiptTx(txctx, tx, op.DirectiveEventID, "error", &failure); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `UPDATE agent_directive_operations SET state = $3, failure = $4::jsonb, execution_lease_expires_at = NULL, completed_at = $5, updated_at = $5, expires_at = $6 WHERE operation_id = $1::uuid AND state = $2`, operationID, string(from), string(to), string(failureRaw), now.UTC(), terminalDirectiveExpiry(to, now, ttl))
		if err := requireDirectiveTransition(res, err); err != nil {
			return err
		}
		op.State, op.Failure = to, runtimefailures.CloneEnvelope(&failure)
		op.ExecutionLeaseExpiresAt = time.Time{}
		op.CompletedAt, op.UpdatedAt = now.UTC(), now.UTC()
		if to == runtimeagentcontrol.DirectiveOperationFailed {
			op.ExpiresAt = now.Add(normalizeDirectiveTTL(ttl)).UTC()
		}
		out = op
		return recordDirectiveAuthorActivity(txctx, op, now, &failure)
	})
	return out, err
}

func (s *SQLiteRuntimeStore) finalizeSQLiteDirectiveFailure(ctx context.Context, operationID, ownerID string, from, to runtimeagentcontrol.DirectiveOperationState, failure runtimefailures.Envelope, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	failureRaw, err := runtimefailures.MarshalEnvelope(failure)
	if err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("validate directive operation failure: %w", err)
	}
	var out runtimeagentcontrol.DirectiveOperation
	err = s.runAuthorActivityMutation(ctx, "sqlite finalize directive failure", func(txctx context.Context, tx *sql.Tx) error {
		op, err := requireActiveSQLiteDirectiveOperation(txctx, tx, operationID)
		if err != nil {
			return err
		}
		if op.State == to {
			if !sameDirectiveFailure(op.Failure, failureRaw) {
				return fmt.Errorf("directive operation %s terminal failure conflicts with persisted failure", operationID)
			}
			out = op
			return nil
		}
		if op.State != from || (ownerID != "" && op.ExecutionOwnerID != ownerID) {
			return runtimeagentcontrol.ErrorForDirectiveOperation(op)
		}
		if err := s.UpsertPipelineReceiptTx(txctx, tx, op.DirectiveEventID, "error", &failure); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `UPDATE agent_directive_operations SET state = ?, failure = ?, execution_lease_expires_at = NULL, completed_at = ?, updated_at = ?, expires_at = ? WHERE operation_id = ? AND state = ?`, string(to), string(failureRaw), now.UTC(), now.UTC(), terminalDirectiveExpiry(to, now, ttl), operationID, string(from))
		if err := requireDirectiveTransition(res, err); err != nil {
			return err
		}
		op.State, op.Failure = to, runtimefailures.CloneEnvelope(&failure)
		op.ExecutionLeaseExpiresAt = time.Time{}
		op.CompletedAt, op.UpdatedAt = now.UTC(), now.UTC()
		if to == runtimeagentcontrol.DirectiveOperationFailed {
			op.ExpiresAt = now.Add(normalizeDirectiveTTL(ttl)).UTC()
		}
		out = op
		return recordDirectiveAuthorActivity(txctx, op, now, &failure)
	})
	return out, err
}

func terminalDirectiveExpiry(state runtimeagentcontrol.DirectiveOperationState, now time.Time, ttl time.Duration) any {
	if state != runtimeagentcontrol.DirectiveOperationFailed && state != runtimeagentcontrol.DirectiveOperationSucceeded {
		return nil
	}
	return now.Add(normalizeDirectiveTTL(ttl)).UTC()
}

func sameDirectiveFailure(existing *runtimefailures.Envelope, expected []byte) bool {
	if existing == nil {
		return false
	}
	raw, err := runtimefailures.MarshalEnvelope(*existing)
	return err == nil && bytes.Equal(raw, expected)
}

func (s *PostgresStore) LoadDirectiveOperation(ctx context.Context, operationID string) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	return loadPostgresDirectiveOperationByID(ctx, s.DB, operationID, false)
}

func (s *SQLiteRuntimeStore) LoadDirectiveOperation(ctx context.Context, operationID string) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	return loadSQLiteDirectiveOperationByID(ctx, s.DB, operationID)
}

func (s *PostgresStore) LoadDirectiveOperationByKey(ctx context.Context, method, actorTokenID, idempotencyKey string) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	return loadPostgresDirectiveOperationByKey(ctx, s.DB, strings.TrimSpace(method), strings.TrimSpace(actorTokenID), strings.TrimSpace(idempotencyKey))
}

func (s *SQLiteRuntimeStore) LoadDirectiveOperationByKey(ctx context.Context, method, actorTokenID, idempotencyKey string) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	return loadSQLiteDirectiveOperationByKey(ctx, s.DB, strings.TrimSpace(method), strings.TrimSpace(actorTokenID), strings.TrimSpace(idempotencyKey))
}

func (s *PostgresStore) transitionPostgresDirectiveOperation(ctx context.Context, operationID string, transition func(context.Context, *sql.Tx) error) (runtimeagentcontrol.DirectiveOperation, error) {
	var out runtimeagentcontrol.DirectiveOperation
	err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if _, err := requireActivePostgresDirectiveOperation(txctx, tx, operationID); err != nil {
			return err
		}
		if err := transition(txctx, tx); err != nil {
			if errors.Is(err, runtimeagentcontrol.ErrDirectiveTransitionConflict) {
				op, ok, loadErr := loadPostgresDirectiveOperationByID(txctx, tx, operationID, false)
				if loadErr != nil {
					return loadErr
				}
				if ok {
					return runtimeagentcontrol.ErrorForDirectiveOperation(op)
				}
			}
			return err
		}
		var ok bool
		var err error
		out, ok, err = loadPostgresDirectiveOperationByID(txctx, tx, operationID, false)
		if err == nil && !ok {
			err = fmt.Errorf("directive operation not found")
		}
		if err != nil {
			return err
		}
		return recordDirectiveAuthorActivity(txctx, out, out.UpdatedAt, out.Failure)
	})
	return out, err
}

func (s *SQLiteRuntimeStore) transitionSQLiteDirectiveOperation(ctx context.Context, operationID string, transition func(context.Context, *sql.Tx) error) (runtimeagentcontrol.DirectiveOperation, error) {
	var out runtimeagentcontrol.DirectiveOperation
	err := s.runAuthorActivityMutation(ctx, "sqlite transition directive operation", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := requireActiveSQLiteDirectiveOperation(txctx, tx, operationID); err != nil {
			return err
		}
		if err := transition(txctx, tx); err != nil {
			if errors.Is(err, runtimeagentcontrol.ErrDirectiveTransitionConflict) {
				op, ok, loadErr := loadSQLiteDirectiveOperationByID(txctx, tx, operationID)
				if loadErr != nil {
					return loadErr
				}
				if ok {
					return runtimeagentcontrol.ErrorForDirectiveOperation(op)
				}
			}
			return err
		}
		var ok bool
		var err error
		out, ok, err = loadSQLiteDirectiveOperationByID(txctx, tx, operationID)
		if err == nil && !ok {
			err = fmt.Errorf("directive operation not found")
		}
		if err != nil {
			return err
		}
		return recordDirectiveAuthorActivity(txctx, out, out.UpdatedAt, out.Failure)
	})
	return out, err
}

func recordDirectiveAuthorActivity(ctx context.Context, op runtimeagentcontrol.DirectiveOperation, occurredAt time.Time, failure *runtimefailures.Envelope) error {
	transition := ""
	switch op.State {
	case runtimeagentcontrol.DirectiveOperationPrepared:
		transition = "received"
	case runtimeagentcontrol.DirectiveOperationExecuting:
		transition = "in_flight"
	case runtimeagentcontrol.DirectiveOperationSucceeded:
		transition = "completed"
	case runtimeagentcontrol.DirectiveOperationFailed:
		transition = "failed"
	case runtimeagentcontrol.DirectiveOperationIndeterminate:
		transition = "outcome_uncertain"
	case runtimeagentcontrol.DirectiveOperationExecuted:
		return nil
	default:
		return fmt.Errorf("directive operation state %q has no author activity disposition", op.State)
	}
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindDirectiveLifecycle, Transition: transition,
		SourceOwner: "agent_directive_operations", SourceIdentity: op.OperationID + ":" + string(op.State),
		DedupKey: "directive:" + op.OperationID + ":" + string(op.State), OccurredAt: occurredAt.UTC(),
		RunID: op.ResolvedRunID, AgentID: op.AgentID, Failure: failure,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "agent", SubjectID: op.AgentID, Method: op.Method, Source: op.Source,
		},
	})
}

func (s *PostgresStore) ReconcileDirectiveOperations(ctx context.Context, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperationReconcileResult, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT o.operation_id::text
		FROM agent_directive_operations o
		JOIN runs run ON run.run_id = o.resolved_run_id
		WHERE run.status IN ('running', 'paused')
		  AND (o.state IN ('executed', 'succeeded') OR (o.state = 'executing' AND o.execution_lease_expires_at <= $1) OR (o.state = 'prepared' AND o.idempotency_key IS NULL) OR (o.state IN ('succeeded', 'failed') AND o.expires_at <= $1))
		ORDER BY o.created_at
	`, now.UTC())
	if err != nil {
		return runtimeagentcontrol.DirectiveOperationReconcileResult{}, err
	}
	ids, err := collectDirectiveOperationIDs(rows)
	if err != nil {
		return runtimeagentcontrol.DirectiveOperationReconcileResult{}, err
	}
	return s.reconcilePostgresDirectiveOperationIDs(ctx, ids, now, ttl)
}

func (s *PostgresStore) ReconcileDirectiveOperation(ctx context.Context, operationID string, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	if _, err := s.reconcilePostgresDirectiveOperationIDs(ctx, []string{strings.TrimSpace(operationID)}, now, ttl); err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, false, err
	}
	return s.LoadDirectiveOperation(ctx, operationID)
}

func (s *SQLiteRuntimeStore) ReconcileDirectiveOperations(ctx context.Context, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperationReconcileResult, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT o.operation_id
		FROM agent_directive_operations o
		JOIN runs run ON run.run_id = o.resolved_run_id
		WHERE run.status IN ('running', 'paused')
		  AND (o.state IN ('executed', 'succeeded') OR (o.state = 'executing' AND o.execution_lease_expires_at <= ?) OR (o.state = 'prepared' AND o.idempotency_key IS NULL) OR (o.state IN ('succeeded', 'failed') AND o.expires_at <= ?))
		ORDER BY o.created_at
	`, now.UTC(), now.UTC())
	if err != nil {
		return runtimeagentcontrol.DirectiveOperationReconcileResult{}, err
	}
	ids, err := collectDirectiveOperationIDs(rows)
	if err != nil {
		return runtimeagentcontrol.DirectiveOperationReconcileResult{}, err
	}
	return s.reconcileSQLiteDirectiveOperationIDs(ctx, ids, now, ttl)
}

func (s *SQLiteRuntimeStore) ReconcileDirectiveOperation(ctx context.Context, operationID string, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	if _, err := s.reconcileSQLiteDirectiveOperationIDs(ctx, []string{strings.TrimSpace(operationID)}, now, ttl); err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, false, err
	}
	return s.LoadDirectiveOperation(ctx, operationID)
}

func collectDirectiveOperationIDs(rows *sql.Rows) ([]string, error) {
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *PostgresStore) reconcilePostgresDirectiveOperationIDs(ctx context.Context, ids []string, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperationReconcileResult, error) {
	var out runtimeagentcontrol.DirectiveOperationReconcileResult
	for _, id := range ids {
		op, ok, err := s.LoadDirectiveOperation(ctx, id)
		if err != nil || !ok {
			if err != nil {
				return out, err
			}
			continue
		}
		switch {
		case (op.State == runtimeagentcontrol.DirectiveOperationSucceeded || op.State == runtimeagentcontrol.DirectiveOperationFailed) && !op.ExpiresAt.IsZero() && !op.ExpiresAt.After(now):
			res, err := s.DB.ExecContext(ctx, `DELETE FROM agent_directive_operations o WHERE o.operation_id = $1::uuid AND o.state IN ('succeeded', 'failed') AND o.expires_at <= $2 AND EXISTS (SELECT 1 FROM runs run WHERE run.run_id = o.resolved_run_id AND run.status IN ('running', 'paused'))`, id, now.UTC())
			if err != nil {
				return out, err
			}
			rows, _ := res.RowsAffected()
			out.Deleted += int(rows)
		case op.State == runtimeagentcontrol.DirectiveOperationExecuted || op.State == runtimeagentcontrol.DirectiveOperationSucceeded:
			if _, err := s.FinalizeDirectiveSuccess(ctx, id, now, ttl); err != nil {
				return out, err
			}
			if op.State == runtimeagentcontrol.DirectiveOperationExecuted {
				out.Finalized++
			} else {
				out.Repaired++
			}
		case op.State == runtimeagentcontrol.DirectiveOperationExecuting && !op.ExecutionLeaseExpiresAt.After(now):
			failure := runtimeagentcontrol.DirectiveExecutionLeaseExpiredFailure()
			if _, err := s.finalizePostgresDirectiveFailure(ctx, id, op.ExecutionOwnerID, runtimeagentcontrol.DirectiveOperationExecuting, runtimeagentcontrol.DirectiveOperationIndeterminate, failure, now, ttl); err != nil {
				return out, err
			}
			out.Indeterminate++
		case op.State == runtimeagentcontrol.DirectiveOperationPrepared && op.IdempotencyKey == "":
			failure := runtimeagentcontrol.DirectiveExecutionNotAdmittedFailure()
			if _, err := s.finalizePostgresDirectiveFailure(ctx, id, "", runtimeagentcontrol.DirectiveOperationPrepared, runtimeagentcontrol.DirectiveOperationFailed, failure, now, ttl); err != nil {
				return out, err
			}
			out.Failed++
		}
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) reconcileSQLiteDirectiveOperationIDs(ctx context.Context, ids []string, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperationReconcileResult, error) {
	var out runtimeagentcontrol.DirectiveOperationReconcileResult
	for _, id := range ids {
		op, ok, err := s.LoadDirectiveOperation(ctx, id)
		if err != nil || !ok {
			if err != nil {
				return out, err
			}
			continue
		}
		switch {
		case (op.State == runtimeagentcontrol.DirectiveOperationSucceeded || op.State == runtimeagentcontrol.DirectiveOperationFailed) && !op.ExpiresAt.IsZero() && !op.ExpiresAt.After(now):
			err := s.runRuntimeMutation(ctx, "sqlite delete expired directive operation", func(txctx context.Context, tx *sql.Tx) error {
				res, err := tx.ExecContext(txctx, `DELETE FROM agent_directive_operations AS o WHERE o.operation_id = ? AND o.state IN ('succeeded', 'failed') AND o.expires_at <= ? AND EXISTS (SELECT 1 FROM runs run WHERE run.run_id = o.resolved_run_id AND run.status IN ('running', 'paused'))`, id, now.UTC())
				if err != nil {
					return err
				}
				rows, _ := res.RowsAffected()
				out.Deleted += int(rows)
				return nil
			})
			if err != nil {
				return out, err
			}
		case op.State == runtimeagentcontrol.DirectiveOperationExecuted || op.State == runtimeagentcontrol.DirectiveOperationSucceeded:
			if _, err := s.FinalizeDirectiveSuccess(ctx, id, now, ttl); err != nil {
				return out, err
			}
			if op.State == runtimeagentcontrol.DirectiveOperationExecuted {
				out.Finalized++
			} else {
				out.Repaired++
			}
		case op.State == runtimeagentcontrol.DirectiveOperationExecuting && !op.ExecutionLeaseExpiresAt.After(now):
			failure := runtimeagentcontrol.DirectiveExecutionLeaseExpiredFailure()
			if _, err := s.finalizeSQLiteDirectiveFailure(ctx, id, op.ExecutionOwnerID, runtimeagentcontrol.DirectiveOperationExecuting, runtimeagentcontrol.DirectiveOperationIndeterminate, failure, now, ttl); err != nil {
				return out, err
			}
			out.Indeterminate++
		case op.State == runtimeagentcontrol.DirectiveOperationPrepared && op.IdempotencyKey == "":
			failure := runtimeagentcontrol.DirectiveExecutionNotAdmittedFailure()
			if _, err := s.finalizeSQLiteDirectiveFailure(ctx, id, "", runtimeagentcontrol.DirectiveOperationPrepared, runtimeagentcontrol.DirectiveOperationFailed, failure, now, ttl); err != nil {
				return out, err
			}
			out.Failed++
		}
	}
	return out, nil
}

func purgeExpiredPostgresDirectiveOperations(ctx context.Context, tx *sql.Tx, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		DELETE FROM agent_directive_operations o
		WHERE o.state IN ('succeeded', 'failed')
		  AND o.expires_at <= $1
		  AND EXISTS (
			SELECT 1 FROM runs run
			WHERE run.run_id = o.resolved_run_id
			  AND run.status IN ('running', 'paused')
		  )
	`, now.UTC())
	return err
}

func purgeExpiredSQLiteDirectiveOperationsTx(ctx context.Context, tx *sql.Tx, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		DELETE FROM agent_directive_operations
		WHERE state IN ('succeeded', 'failed')
		  AND expires_at <= ?
		  AND EXISTS (
			SELECT 1 FROM runs run
			WHERE run.run_id = agent_directive_operations.resolved_run_id
			  AND run.status IN ('running', 'paused')
		  )
	`, now.UTC())
	return err
}

func loadPostgresDirectiveOperationByKey(ctx context.Context, q rowQueryer, method, actor, key string) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	return scanDirectiveOperation(q.QueryRowContext(ctx, postgresDirectiveOperationSelect+` WHERE method = $1 AND actor_token_id = $2 AND idempotency_key = $3`, method, actor, key))
}

func loadPostgresDirectiveOperationByID(ctx context.Context, q rowQueryer, id string, forUpdate bool) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	suffix := ""
	if forUpdate {
		suffix = " FOR UPDATE"
	}
	return scanDirectiveOperation(q.QueryRowContext(ctx, postgresDirectiveOperationSelect+` WHERE operation_id = $1::uuid`+suffix, id))
}

func loadSQLiteDirectiveOperationByKey(ctx context.Context, q rowQueryer, method, actor, key string) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	return scanDirectiveOperation(q.QueryRowContext(ctx, sqliteDirectiveOperationSelect+` WHERE method = ? AND actor_token_id = ? AND idempotency_key = ?`, method, actor, key))
}

func loadSQLiteDirectiveOperationByID(ctx context.Context, q rowQueryer, id string) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	return scanDirectiveOperation(q.QueryRowContext(ctx, sqliteDirectiveOperationSelect+` WHERE operation_id = ?`, id))
}

func requireActivePostgresDirectiveOperation(ctx context.Context, tx *sql.Tx, operationID string) (runtimeagentcontrol.DirectiveOperation, error) {
	op, ok, err := loadPostgresDirectiveOperationByID(ctx, tx, operationID, false)
	if err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, err
	}
	if !ok {
		return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("directive operation not found")
	}
	if err := storerunlifecycle.RequireActive(ctx, tx, op.ResolvedRunID, storerunlifecycle.DialectPostgres); err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, err
	}
	locked, ok, err := loadPostgresDirectiveOperationByID(ctx, tx, operationID, true)
	if err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, err
	}
	if !ok || locked.ResolvedRunID != op.ResolvedRunID {
		return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("directive operation identity changed while acquiring run admission")
	}
	return locked, nil
}

func requireActiveSQLiteDirectiveOperation(ctx context.Context, tx *sql.Tx, operationID string) (runtimeagentcontrol.DirectiveOperation, error) {
	op, ok, err := loadSQLiteDirectiveOperationByID(ctx, tx, operationID)
	if err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, err
	}
	if !ok {
		return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("directive operation not found")
	}
	if err := storerunlifecycle.RequireActive(ctx, tx, op.ResolvedRunID, storerunlifecycle.DialectSQLite); err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, err
	}
	return op, nil
}

const postgresDirectiveOperationSelect = `SELECT operation_id::text, method, actor_token_id, COALESCE(idempotency_key, ''), request_hash, agent_id, directive_text, COALESCE(requested_run_id::text, ''), resolved_run_id::text, run_id_resolution, source, COALESCE(operator_id, ''), directive_event_id::text, state, COALESCE(execution_owner_id, ''), execution_lease_expires_at, response, failure, execution_admitted_at, executed_at, completed_at, created_at, updated_at, expires_at FROM agent_directive_operations`

const sqliteDirectiveOperationSelect = `SELECT operation_id, method, actor_token_id, COALESCE(idempotency_key, ''), request_hash, agent_id, directive_text, COALESCE(requested_run_id, ''), resolved_run_id, run_id_resolution, source, COALESCE(operator_id, ''), directive_event_id, state, COALESCE(execution_owner_id, ''), execution_lease_expires_at, response, failure, execution_admitted_at, executed_at, completed_at, created_at, updated_at, expires_at FROM agent_directive_operations`

type directiveOperationRow interface {
	Scan(...any) error
}

func scanDirectiveOperation(row directiveOperationRow) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	var op runtimeagentcontrol.DirectiveOperation
	var state string
	var leaseRaw, responseRaw, failureRaw, admittedRaw, executedRaw, completedRaw, createdRaw, updatedRaw, expiresRaw any
	err := row.Scan(&op.OperationID, &op.Method, &op.ActorTokenID, &op.IdempotencyKey, &op.RequestHash, &op.AgentID, &op.Directive, &op.RequestedRunID, &op.ResolvedRunID, &op.RunIDResolution, &op.Source, &op.OperatorID, &op.DirectiveEventID, &state, &op.ExecutionOwnerID, &leaseRaw, &responseRaw, &failureRaw, &admittedRaw, &executedRaw, &completedRaw, &createdRaw, &updatedRaw, &expiresRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimeagentcontrol.DirectiveOperation{}, false, nil
	}
	if err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, false, fmt.Errorf("scan directive operation: %w", err)
	}
	op.State = runtimeagentcontrol.DirectiveOperationState(strings.TrimSpace(state))
	op.Response = jsonRawMessageValue(responseRaw)
	op.Failure, err = decodeStoredFailure(failureRaw)
	if err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, false, fmt.Errorf("decode directive operation failure: %w", err)
	}
	timestamps := []struct {
		raw    any
		target *time.Time
	}{
		{leaseRaw, &op.ExecutionLeaseExpiresAt},
		{admittedRaw, &op.ExecutionAdmittedAt},
		{executedRaw, &op.ExecutedAt},
		{completedRaw, &op.CompletedAt},
		{createdRaw, &op.CreatedAt},
		{updatedRaw, &op.UpdatedAt},
		{expiresRaw, &op.ExpiresAt},
	}
	for _, timestamp := range timestamps {
		if at, ok, parseErr := sqliteTimeValue(timestamp.raw); parseErr != nil {
			return runtimeagentcontrol.DirectiveOperation{}, false, parseErr
		} else if ok {
			*timestamp.target = at
		}
	}
	op = op.Normalized()
	if err := runtimeagentcontrol.ValidateDirectiveOperationEvidence(op); err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, false, err
	}
	return op, true, nil
}

func nullableString(value string) any {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return nil
}
