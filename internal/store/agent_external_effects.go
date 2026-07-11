package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

var _ runtimeeffects.Store = (*PostgresStore)(nil)
var _ runtimeeffects.Store = (*SQLiteRuntimeStore)(nil)

func (s *PostgresStore) IsLifecycleTokenCurrent(ctx context.Context, token runtimeeffects.LifecycleToken) (bool, error) {
	var epoch int64
	var generation int64
	var phase string
	err := s.DB.QueryRowContext(ctx, `SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase FROM agents WHERE agent_id = $1`, token.AgentID).Scan(&epoch, &generation, &phase)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check external effect lifecycle token: %w", err)
	}
	return epoch == token.RuntimeEpoch && generation == int64(token.Generation) && strings.TrimSpace(phase) == "running", nil
}

func (s *SQLiteRuntimeStore) IsLifecycleTokenCurrent(ctx context.Context, token runtimeeffects.LifecycleToken) (bool, error) {
	var epoch int64
	var generation int64
	var phase string
	err := s.DB.QueryRowContext(ctx, `SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase FROM agents WHERE agent_id = ?`, token.AgentID).Scan(&epoch, &generation, &phase)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check sqlite external effect lifecycle token: %w", err)
	}
	return epoch == token.RuntimeEpoch && generation == int64(token.Generation) && strings.TrimSpace(phase) == "running", nil
}

func (s *PostgresStore) AuthorizeExternalAttempt(ctx context.Context, token runtimeeffects.LifecycleToken, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("authorize external attempt begin: %w", err)
	}
	defer tx.Rollback()
	var epoch int64
	var generation int64
	var phase string
	if err := tx.QueryRowContext(ctx, `SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase FROM agents WHERE agent_id = $1 FOR UPDATE`, token.AgentID).Scan(&epoch, &generation, &phase); err != nil {
		if err == sql.ErrNoRows {
			return runtimeeffects.Attempt{}, supersededExternalAttempt(token, 0, 0, "absent")
		}
		return runtimeeffects.Attempt{}, fmt.Errorf("authorize external attempt lifecycle read: %w", err)
	}
	if epoch != token.RuntimeEpoch || generation != int64(token.Generation) || strings.TrimSpace(phase) != "running" {
		return runtimeeffects.Attempt{}, supersededExternalAttempt(token, epoch, generation, phase)
	}
	attempt, err := insertExternalAttemptPostgres(ctx, tx, token, req)
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	if err := tx.Commit(); err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("authorize external attempt commit: %w", err)
	}
	return attempt, nil
}

func (s *SQLiteRuntimeStore) AuthorizeExternalAttempt(ctx context.Context, token runtimeeffects.LifecycleToken, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	var attempt runtimeeffects.Attempt
	err := s.runRuntimeMutation(ctx, "sqlite authorize external attempt", func(txctx context.Context, tx *sql.Tx) error {
		var epoch int64
		var generation int64
		var phase string
		if err := tx.QueryRowContext(txctx, `SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase FROM agents WHERE agent_id = ?`, token.AgentID).Scan(&epoch, &generation, &phase); err != nil {
			if err == sql.ErrNoRows {
				return supersededExternalAttempt(token, 0, 0, "absent")
			}
			return fmt.Errorf("authorize sqlite external attempt lifecycle read: %w", err)
		}
		if epoch != token.RuntimeEpoch || generation != int64(token.Generation) || strings.TrimSpace(phase) != "running" {
			return supersededExternalAttempt(token, epoch, generation, phase)
		}
		var err error
		attempt, err = insertExternalAttemptSQLiteTx(txctx, tx, token, req)
		return err
	})
	return attempt, err
}

func supersededExternalAttempt(token runtimeeffects.LifecycleToken, currentEpoch, currentGeneration int64, phase string) error {
	return runtimefailures.New(runtimefailures.ClassSupersededGeneration, "superseded_generation", "external-effects", "authorize_attempt", map[string]any{
		"agent_id": token.AgentID, "runtime_epoch": token.RuntimeEpoch, "generation": token.Generation,
		"current_runtime_epoch": currentEpoch, "current_generation": currentGeneration, "current_phase": strings.TrimSpace(phase),
	})
}

func insertExternalAttemptPostgres(ctx context.Context, tx *sql.Tx, token runtimeeffects.LifecycleToken, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	lineage, _ := json.Marshal(req.Lineage)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_external_effect_operations (
			operation_id, effect_kind, effect_class, agent_id, runtime_epoch, generation,
			lineage, request_fingerprint, state, created_at, updated_at
		) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::jsonb, $8, 'authorized', $9, $9)
	`, req.OperationID, string(req.Kind), string(req.Class), token.AgentID, token.RuntimeEpoch, token.Generation, string(lineage), req.RequestFingerprint, req.Now.UTC()); err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("insert external effect operation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_external_effect_attempts (
			attempt_id, operation_id, attempt_ordinal, adapter, transport, runtime_epoch,
			generation, state, authorized_at, updated_at
		) VALUES ($1::uuid, $2::uuid, 1, $3, $4, $5, $6, 'authorized', $7, $7)
	`, req.AttemptID, req.OperationID, req.Adapter, req.Transport, token.RuntimeEpoch, token.Generation, req.Now.UTC()); err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("insert external effect attempt: %w", err)
	}
	return authorizedAttempt(token, req), nil
}

func insertExternalAttemptSQLiteTx(ctx context.Context, tx *sql.Tx, token runtimeeffects.LifecycleToken, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	lineage, _ := json.Marshal(req.Lineage)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_external_effect_operations (
			operation_id, effect_kind, effect_class, agent_id, runtime_epoch, generation,
			lineage, request_fingerprint, state, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'authorized', ?, ?)
	`, req.OperationID, string(req.Kind), string(req.Class), token.AgentID, token.RuntimeEpoch, token.Generation, string(lineage), req.RequestFingerprint, req.Now.UTC(), req.Now.UTC()); err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("insert sqlite external effect operation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_external_effect_attempts (
			attempt_id, operation_id, attempt_ordinal, adapter, transport, runtime_epoch,
			generation, state, authorized_at, updated_at
		) VALUES (?, ?, 1, ?, ?, ?, ?, 'authorized', ?, ?)
	`, req.AttemptID, req.OperationID, req.Adapter, req.Transport, token.RuntimeEpoch, token.Generation, req.Now.UTC(), req.Now.UTC()); err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("insert sqlite external effect attempt: %w", err)
	}
	return authorizedAttempt(token, req), nil
}

func authorizedAttempt(token runtimeeffects.LifecycleToken, req runtimeeffects.AuthorizeRequest) runtimeeffects.Attempt {
	return runtimeeffects.Attempt{
		OperationID: req.OperationID, AttemptID: req.AttemptID, Token: token,
		Kind: req.Kind, Class: req.Class, Adapter: req.Adapter, Transport: req.Transport,
		Ordinal: 1, AuthorizedAt: req.Now.UTC(),
	}
}

func (s *PostgresStore) MarkExternalAttemptLaunched(ctx context.Context, attempt runtimeeffects.Attempt, now time.Time) error {
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_external_effect_attempts SET state = 'launched', launched_at = $2, updated_at = $2 WHERE attempt_id = $1::uuid AND state = 'authorized'`, attempt.AttemptID, now.UTC())
	if err := requireExternalAttemptTransition(res, err); err == nil {
		return nil
	}
	var state string
	if queryErr := s.DB.QueryRowContext(ctx, `SELECT state FROM agent_external_effect_attempts WHERE attempt_id = $1::uuid`, attempt.AttemptID).Scan(&state); queryErr == nil && state == string(runtimeeffects.StateLaunched) {
		return nil
	}
	return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "external-effects", "launch_attempt", map[string]any{"attempt_id": attempt.AttemptID})
}

func (s *SQLiteRuntimeStore) MarkExternalAttemptLaunched(ctx context.Context, attempt runtimeeffects.Attempt, now time.Time) error {
	return s.runRuntimeMutation(ctx, "sqlite mark external attempt launched", func(txctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(txctx, `UPDATE agent_external_effect_attempts SET state = 'launched', launched_at = ?, updated_at = ? WHERE attempt_id = ? AND state = 'authorized'`, now.UTC(), now.UTC(), attempt.AttemptID)
		if err := requireExternalAttemptTransition(res, err); err == nil {
			return nil
		}
		var state string
		if queryErr := tx.QueryRowContext(txctx, `SELECT state FROM agent_external_effect_attempts WHERE attempt_id = ?`, attempt.AttemptID).Scan(&state); queryErr == nil && state == string(runtimeeffects.StateLaunched) {
			return nil
		}
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "external-effects", "launch_attempt", map[string]any{"attempt_id": attempt.AttemptID})
	})
}

func requireExternalAttemptTransition(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "external-effects", "transition_attempt", nil)
	}
	return nil
}

func (s *PostgresStore) SettleExternalAttempt(ctx context.Context, settlement runtimeeffects.Settlement) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := settleExternalAttemptPostgres(ctx, tx, settlement); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteRuntimeStore) SettleExternalAttempt(ctx context.Context, settlement runtimeeffects.Settlement) error {
	return s.runRuntimeMutation(ctx, "sqlite settle external attempt", func(txctx context.Context, tx *sql.Tx) error {
		return settleExternalAttemptSQLiteTx(txctx, tx, settlement)
	})
}

func externalSettlementPayload(settlement runtimeeffects.Settlement) ([]byte, []byte, error) {
	evidence, err := json.Marshal(settlement.Evidence)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal external effect evidence: %w", err)
	}
	var failure []byte
	if settlement.Failure != nil {
		failure, err = json.Marshal(settlement.Failure)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal external effect failure: %w", err)
		}
	}
	return evidence, failure, nil
}

func settleExternalAttemptPostgres(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement) error {
	evidence, failure, err := externalSettlementPayload(settlement)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE agent_external_effect_attempts
		SET state = $3, evidence = $4::jsonb, failure = $5::jsonb,
		    completed_at = $6, updated_at = $6
		WHERE attempt_id = $1::uuid AND operation_id = $2::uuid
		  AND state IN ('authorized', 'launched', 'response_observed')
	`, settlement.AttemptID, settlement.OperationID, string(settlement.State), string(evidence), nullableJSON(failure), settlement.Now.UTC())
	if err := requireExternalAttemptTransition(res, err); err != nil {
		return acceptRepeatedPostgresSettlement(ctx, tx, settlement)
	}
	_, err = tx.ExecContext(ctx, `UPDATE agent_external_effect_operations SET state = $2, completed_at = $3, updated_at = $3 WHERE operation_id = $1::uuid`, settlement.OperationID, string(settlement.State), settlement.Now.UTC())
	return err
}

func settleExternalAttemptSQLiteTx(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement) error {
	evidence, failure, err := externalSettlementPayload(settlement)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE agent_external_effect_attempts
		SET state = ?, evidence = ?, failure = ?, completed_at = ?, updated_at = ?
		WHERE attempt_id = ? AND operation_id = ?
		  AND state IN ('authorized', 'launched', 'response_observed')
	`, string(settlement.State), string(evidence), sqliteNullableJSON(failure), settlement.Now.UTC(), settlement.Now.UTC(), settlement.AttemptID, settlement.OperationID)
	if err := requireExternalAttemptTransition(res, err); err != nil {
		return acceptRepeatedSQLiteSettlement(ctx, tx, settlement)
	}
	_, err = tx.ExecContext(ctx, `UPDATE agent_external_effect_operations SET state = ?, completed_at = ?, updated_at = ? WHERE operation_id = ?`, string(settlement.State), settlement.Now.UTC(), settlement.Now.UTC(), settlement.OperationID)
	return err
}

func acceptRepeatedPostgresSettlement(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement) error {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT state FROM agent_external_effect_attempts WHERE attempt_id = $1::uuid AND operation_id = $2::uuid`, settlement.AttemptID, settlement.OperationID).Scan(&state)
	if err == nil && state == string(settlement.State) {
		return nil
	}
	return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "external-effects", "settle_attempt", map[string]any{"attempt_id": settlement.AttemptID, "current_state": state, "target_state": settlement.State})
}

func acceptRepeatedSQLiteSettlement(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement) error {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT state FROM agent_external_effect_attempts WHERE attempt_id = ? AND operation_id = ?`, settlement.AttemptID, settlement.OperationID).Scan(&state)
	if err == nil && state == string(settlement.State) {
		return nil
	}
	return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "external-effects", "settle_attempt", map[string]any{"attempt_id": settlement.AttemptID, "current_state": state, "target_state": settlement.State})
}

func nullableJSON(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}

func sqliteNullableJSON(raw []byte) any { return nullableJSON(raw) }
