package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
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
	err = s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
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
		if err := s.AppendEventTx(txctx, tx, req.Event); err != nil {
			return err
		}
		if err := s.UpsertCommittedReplayScopeTx(txctx, tx, op.DirectiveEventID, runtimereplayclaim.CommittedReplayScopeDirect); err != nil {
			return err
		}
		if err := insertPostgresDirectiveOperation(txctx, tx, op, req.Now); err != nil {
			return err
		}
		op.CreatedAt = req.Now
		op.UpdatedAt = req.Now
		reservation = runtimeagentcontrol.DirectiveOperationReservation{Operation: op, Created: true}
		return nil
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
	err = s.runRuntimeMutation(ctx, "sqlite reserve directive operation", func(txctx context.Context, tx *sql.Tx) error {
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
		if err := s.AppendEventTx(txctx, tx, req.Event); err != nil {
			return err
		}
		if err := s.UpsertCommittedReplayScopeTx(txctx, tx, op.DirectiveEventID, runtimereplayclaim.CommittedReplayScopeDirect); err != nil {
			return err
		}
		if err := insertSQLiteDirectiveOperationTx(txctx, tx, op, req.Now); err != nil {
			return err
		}
		op.CreatedAt = req.Now
		op.UpdatedAt = req.Now
		reservation = runtimeagentcontrol.DirectiveOperationReservation{Operation: op, Created: true}
		return nil
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
	if req.Event.ID() != op.DirectiveEventID || req.Event.RunID() != op.ResolvedRunID || string(req.Event.Type()) != runtimeagentcontrol.DirectiveEventType {
		return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("directive operation event identity mismatch")
	}
	if op.State == "" {
		op.State = runtimeagentcontrol.DirectiveOperationPrepared
	}
	if op.State != runtimeagentcontrol.DirectiveOperationPrepared {
		return runtimeagentcontrol.DirectiveOperation{}, fmt.Errorf("new directive operation state must be prepared")
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
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_directive_operations SET execution_lease_expires_at = $4, updated_at = $3 WHERE operation_id = $1::uuid AND execution_owner_id = $2 AND state = 'executing'`, operationID, ownerID, now.UTC(), now.Add(normalizeDirectiveLease(lease)).UTC())
	return requireDirectiveTransition(res, err)
}

func (s *SQLiteRuntimeStore) RenewDirectiveExecutionLease(ctx context.Context, operationID, ownerID string, now time.Time, lease time.Duration) error {
	return s.runRuntimeMutation(ctx, "sqlite renew directive execution lease", func(txctx context.Context, tx *sql.Tx) error {
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
	err := s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		op, ok, err := loadPostgresDirectiveOperationByID(txctx, tx, operationID, true)
		if err != nil || !ok {
			if err == nil {
				err = fmt.Errorf("directive operation not found")
			}
			return err
		}
		if op.State != runtimeagentcontrol.DirectiveOperationExecuted && op.State != runtimeagentcontrol.DirectiveOperationSucceeded {
			return runtimeagentcontrol.ErrorForDirectiveOperation(op)
		}
		if err := s.UpsertPipelineReceiptTx(txctx, tx, op.DirectiveEventID, "processed", ""); err != nil {
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
		return nil
	})
	return out, err
}

func (s *SQLiteRuntimeStore) FinalizeDirectiveSuccess(ctx context.Context, operationID string, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	var out runtimeagentcontrol.DirectiveOperation
	err := s.runRuntimeMutation(ctx, "sqlite finalize directive success", func(txctx context.Context, tx *sql.Tx) error {
		op, ok, err := loadSQLiteDirectiveOperationByID(txctx, tx, operationID)
		if err != nil || !ok {
			if err == nil {
				err = fmt.Errorf("directive operation not found")
			}
			return err
		}
		if op.State != runtimeagentcontrol.DirectiveOperationExecuted && op.State != runtimeagentcontrol.DirectiveOperationSucceeded {
			return runtimeagentcontrol.ErrorForDirectiveOperation(op)
		}
		if err := s.UpsertPipelineReceiptTx(txctx, tx, op.DirectiveEventID, "processed", ""); err != nil {
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
		return nil
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

func (s *PostgresStore) FinalizeDirectiveFailure(ctx context.Context, operationID, ownerID, code, message string, details json.RawMessage, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	return s.finalizePostgresDirectiveFailure(ctx, operationID, ownerID, runtimeagentcontrol.DirectiveOperationExecuting, runtimeagentcontrol.DirectiveOperationFailed, code, message, details, now, ttl)
}

func (s *SQLiteRuntimeStore) FinalizeDirectiveFailure(ctx context.Context, operationID, ownerID, code, message string, details json.RawMessage, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	return s.finalizeSQLiteDirectiveFailure(ctx, operationID, ownerID, runtimeagentcontrol.DirectiveOperationExecuting, runtimeagentcontrol.DirectiveOperationFailed, code, message, details, now, ttl)
}

func (s *PostgresStore) finalizePostgresDirectiveFailure(ctx context.Context, operationID, ownerID string, from, to runtimeagentcontrol.DirectiveOperationState, code, message string, details json.RawMessage, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	var out runtimeagentcontrol.DirectiveOperation
	err := s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		op, ok, err := loadPostgresDirectiveOperationByID(txctx, tx, operationID, true)
		if err != nil || !ok {
			if err == nil {
				err = fmt.Errorf("directive operation not found")
			}
			return err
		}
		if op.State == to {
			out = op
			return nil
		}
		if op.State != from || (ownerID != "" && op.ExecutionOwnerID != ownerID) {
			return runtimeagentcontrol.ErrorForDirectiveOperation(op)
		}
		if err := s.UpsertPipelineReceiptTx(txctx, tx, op.DirectiveEventID, "error", message); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `UPDATE agent_directive_operations SET state = $3, error_code = $4, error_message = $5, error_details = $6::jsonb, execution_lease_expires_at = NULL, completed_at = $7, updated_at = $7, expires_at = $8 WHERE operation_id = $1::uuid AND state = $2`, operationID, string(from), string(to), code, message, nullableJSON(details), now.UTC(), terminalDirectiveExpiry(to, now, ttl))
		if err := requireDirectiveTransition(res, err); err != nil {
			return err
		}
		op.State, op.ErrorCode, op.ErrorMessage, op.ErrorDetails = to, code, message, append(json.RawMessage(nil), details...)
		op.ExecutionLeaseExpiresAt = time.Time{}
		op.CompletedAt, op.UpdatedAt = now.UTC(), now.UTC()
		if to == runtimeagentcontrol.DirectiveOperationFailed {
			op.ExpiresAt = now.Add(normalizeDirectiveTTL(ttl)).UTC()
		}
		out = op
		return nil
	})
	return out, err
}

func (s *SQLiteRuntimeStore) finalizeSQLiteDirectiveFailure(ctx context.Context, operationID, ownerID string, from, to runtimeagentcontrol.DirectiveOperationState, code, message string, details json.RawMessage, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	var out runtimeagentcontrol.DirectiveOperation
	err := s.runRuntimeMutation(ctx, "sqlite finalize directive failure", func(txctx context.Context, tx *sql.Tx) error {
		op, ok, err := loadSQLiteDirectiveOperationByID(txctx, tx, operationID)
		if err != nil || !ok {
			if err == nil {
				err = fmt.Errorf("directive operation not found")
			}
			return err
		}
		if op.State == to {
			out = op
			return nil
		}
		if op.State != from || (ownerID != "" && op.ExecutionOwnerID != ownerID) {
			return runtimeagentcontrol.ErrorForDirectiveOperation(op)
		}
		if err := s.UpsertPipelineReceiptTx(txctx, tx, op.DirectiveEventID, "error", message); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `UPDATE agent_directive_operations SET state = ?, error_code = ?, error_message = ?, error_details = ?, execution_lease_expires_at = NULL, completed_at = ?, updated_at = ?, expires_at = ? WHERE operation_id = ? AND state = ?`, string(to), code, message, nullableJSON(details), now.UTC(), now.UTC(), terminalDirectiveExpiry(to, now, ttl), operationID, string(from))
		if err := requireDirectiveTransition(res, err); err != nil {
			return err
		}
		op.State, op.ErrorCode, op.ErrorMessage, op.ErrorDetails = to, code, message, append(json.RawMessage(nil), details...)
		op.ExecutionLeaseExpiresAt = time.Time{}
		op.CompletedAt, op.UpdatedAt = now.UTC(), now.UTC()
		if to == runtimeagentcontrol.DirectiveOperationFailed {
			op.ExpiresAt = now.Add(normalizeDirectiveTTL(ttl)).UTC()
		}
		out = op
		return nil
	})
	return out, err
}

func terminalDirectiveExpiry(state runtimeagentcontrol.DirectiveOperationState, now time.Time, ttl time.Duration) any {
	if state != runtimeagentcontrol.DirectiveOperationFailed && state != runtimeagentcontrol.DirectiveOperationSucceeded {
		return nil
	}
	return now.Add(normalizeDirectiveTTL(ttl)).UTC()
}

func nullableJSON(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
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
	err := s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
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
		return err
	})
	return out, err
}

func (s *SQLiteRuntimeStore) transitionSQLiteDirectiveOperation(ctx context.Context, operationID string, transition func(context.Context, *sql.Tx) error) (runtimeagentcontrol.DirectiveOperation, error) {
	var out runtimeagentcontrol.DirectiveOperation
	err := s.runRuntimeMutation(ctx, "sqlite transition directive operation", func(txctx context.Context, tx *sql.Tx) error {
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
		return err
	})
	return out, err
}

func (s *PostgresStore) ReconcileDirectiveOperations(ctx context.Context, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperationReconcileResult, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT operation_id::text FROM agent_directive_operations WHERE state IN ('executed', 'succeeded') OR (state = 'executing' AND execution_lease_expires_at <= $1) OR (state = 'prepared' AND idempotency_key IS NULL) OR (state IN ('succeeded', 'failed') AND expires_at <= $1) ORDER BY created_at`, now.UTC())
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
	rows, err := s.DB.QueryContext(ctx, `SELECT operation_id FROM agent_directive_operations WHERE state IN ('executed', 'succeeded') OR (state = 'executing' AND execution_lease_expires_at <= ?) OR (state = 'prepared' AND idempotency_key IS NULL) OR (state IN ('succeeded', 'failed') AND expires_at <= ?) ORDER BY created_at`, now.UTC(), now.UTC())
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
			res, err := s.DB.ExecContext(ctx, `DELETE FROM agent_directive_operations WHERE operation_id = $1::uuid AND state IN ('succeeded', 'failed') AND expires_at <= $2`, id, now.UTC())
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
			if _, err := s.finalizePostgresDirectiveFailure(ctx, id, op.ExecutionOwnerID, runtimeagentcontrol.DirectiveOperationExecuting, runtimeagentcontrol.DirectiveOperationIndeterminate, "execution_lease_expired", "directive execution lease expired before a durable outcome", nil, now, ttl); err != nil {
				return out, err
			}
			out.Indeterminate++
		case op.State == runtimeagentcontrol.DirectiveOperationPrepared && op.IdempotencyKey == "":
			if _, err := s.finalizePostgresDirectiveFailure(ctx, id, "", runtimeagentcontrol.DirectiveOperationPrepared, runtimeagentcontrol.DirectiveOperationFailed, "execution_not_admitted", "keyless directive operation was abandoned before execution admission", nil, now, ttl); err != nil {
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
				res, err := tx.ExecContext(txctx, `DELETE FROM agent_directive_operations WHERE operation_id = ? AND state IN ('succeeded', 'failed') AND expires_at <= ?`, id, now.UTC())
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
			if _, err := s.finalizeSQLiteDirectiveFailure(ctx, id, op.ExecutionOwnerID, runtimeagentcontrol.DirectiveOperationExecuting, runtimeagentcontrol.DirectiveOperationIndeterminate, "execution_lease_expired", "directive execution lease expired before a durable outcome", nil, now, ttl); err != nil {
				return out, err
			}
			out.Indeterminate++
		case op.State == runtimeagentcontrol.DirectiveOperationPrepared && op.IdempotencyKey == "":
			if _, err := s.finalizeSQLiteDirectiveFailure(ctx, id, "", runtimeagentcontrol.DirectiveOperationPrepared, runtimeagentcontrol.DirectiveOperationFailed, "execution_not_admitted", "keyless directive operation was abandoned before execution admission", nil, now, ttl); err != nil {
				return out, err
			}
			out.Failed++
		}
	}
	return out, nil
}

func purgeExpiredPostgresDirectiveOperations(ctx context.Context, tx *sql.Tx, now time.Time) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM agent_directive_operations WHERE state IN ('succeeded', 'failed') AND expires_at <= $1`, now.UTC())
	return err
}

func purgeExpiredSQLiteDirectiveOperationsTx(ctx context.Context, tx *sql.Tx, now time.Time) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM agent_directive_operations WHERE state IN ('succeeded', 'failed') AND expires_at <= ?`, now.UTC())
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

const postgresDirectiveOperationSelect = `SELECT operation_id::text, method, actor_token_id, COALESCE(idempotency_key, ''), request_hash, agent_id, directive_text, COALESCE(requested_run_id::text, ''), resolved_run_id::text, run_id_resolution, source, COALESCE(operator_id, ''), directive_event_id::text, state, COALESCE(execution_owner_id, ''), execution_lease_expires_at, response, COALESCE(error_code, ''), COALESCE(error_message, ''), error_details, execution_admitted_at, executed_at, completed_at, created_at, updated_at, expires_at FROM agent_directive_operations`

const sqliteDirectiveOperationSelect = `SELECT operation_id, method, actor_token_id, COALESCE(idempotency_key, ''), request_hash, agent_id, directive_text, COALESCE(requested_run_id, ''), resolved_run_id, run_id_resolution, source, COALESCE(operator_id, ''), directive_event_id, state, COALESCE(execution_owner_id, ''), execution_lease_expires_at, response, COALESCE(error_code, ''), COALESCE(error_message, ''), error_details, execution_admitted_at, executed_at, completed_at, created_at, updated_at, expires_at FROM agent_directive_operations`

type directiveOperationRow interface {
	Scan(...any) error
}

func scanDirectiveOperation(row directiveOperationRow) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	var op runtimeagentcontrol.DirectiveOperation
	var state string
	var leaseRaw, responseRaw, detailsRaw, admittedRaw, executedRaw, completedRaw, createdRaw, updatedRaw, expiresRaw any
	err := row.Scan(&op.OperationID, &op.Method, &op.ActorTokenID, &op.IdempotencyKey, &op.RequestHash, &op.AgentID, &op.Directive, &op.RequestedRunID, &op.ResolvedRunID, &op.RunIDResolution, &op.Source, &op.OperatorID, &op.DirectiveEventID, &state, &op.ExecutionOwnerID, &leaseRaw, &responseRaw, &op.ErrorCode, &op.ErrorMessage, &detailsRaw, &admittedRaw, &executedRaw, &completedRaw, &createdRaw, &updatedRaw, &expiresRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimeagentcontrol.DirectiveOperation{}, false, nil
	}
	if err != nil {
		return runtimeagentcontrol.DirectiveOperation{}, false, fmt.Errorf("scan directive operation: %w", err)
	}
	op.State = runtimeagentcontrol.DirectiveOperationState(strings.TrimSpace(state))
	op.Response = jsonRawMessageValue(responseRaw)
	op.ErrorDetails = jsonRawMessageValue(detailsRaw)
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
	return op.Normalized(), true, nil
}

func nullableString(value string) any {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return nil
}
