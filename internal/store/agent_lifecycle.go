package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

var _ runtimemanager.AgentLifecyclePersistence = (*PostgresStore)(nil)
var _ runtimemanager.AgentLifecyclePersistence = (*SQLiteRuntimeStore)(nil)
var _ runtimemanager.AgentLifecycleDiagnosticPersistence = (*PostgresStore)(nil)
var _ runtimemanager.AgentLifecycleDiagnosticPersistence = (*SQLiteRuntimeStore)(nil)

func (s *PostgresStore) ListPendingAgentLifecycleDiagnostics(ctx context.Context, limit int) ([]runtimemanager.AgentLifecycleDiagnostic, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT outbox_id::text, operation_id::text, agent_id, event_name, payload, created_at FROM agent_lifecycle_diagnostic_outbox WHERE projected_at IS NULL ORDER BY created_at, outbox_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAgentLifecycleDiagnostics(rows)
}

func (s *SQLiteRuntimeStore) ListPendingAgentLifecycleDiagnostics(ctx context.Context, limit int) ([]runtimemanager.AgentLifecycleDiagnostic, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT outbox_id, operation_id, agent_id, event_name, payload, created_at FROM agent_lifecycle_diagnostic_outbox WHERE projected_at IS NULL ORDER BY created_at, outbox_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAgentLifecycleDiagnostics(rows)
}

func scanAgentLifecycleDiagnostics(rows *sql.Rows) ([]runtimemanager.AgentLifecycleDiagnostic, error) {
	out := make([]runtimemanager.AgentLifecycleDiagnostic, 0)
	for rows.Next() {
		var item runtimemanager.AgentLifecycleDiagnostic
		var raw []byte
		var rawCreatedAt any
		if err := rows.Scan(&item.OutboxID, &item.OperationID, &item.AgentID, &item.EventName, &raw, &rawCreatedAt); err != nil {
			return nil, err
		}
		createdAt, _, err := storeTimeValue(rawCreatedAt)
		if err != nil {
			return nil, fmt.Errorf("decode lifecycle diagnostic created_at: %w", err)
		}
		item.CreatedAt = createdAt
		if err := json.Unmarshal(raw, &item.Payload); err != nil {
			return nil, fmt.Errorf("decode lifecycle diagnostic payload: %w", err)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PostgresStore) MarkAgentLifecycleDiagnosticProjected(ctx context.Context, outboxID string, at time.Time) error {
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_lifecycle_diagnostic_outbox SET projected_at = $2 WHERE outbox_id = $1::uuid AND projected_at IS NULL`, outboxID, at.UTC())
	return requireSingleLifecycleDiagnosticProjection(res, err)
}

func (s *SQLiteRuntimeStore) MarkAgentLifecycleDiagnosticProjected(ctx context.Context, outboxID string, at time.Time) error {
	return s.runRuntimeMutation(ctx, "sqlite mark lifecycle diagnostic projected", func(txctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(txctx, `UPDATE agent_lifecycle_diagnostic_outbox SET projected_at = ? WHERE outbox_id = ? AND projected_at IS NULL`, at.UTC(), outboxID)
		return requireSingleLifecycleDiagnosticProjection(res, err)
	})
}

func requireSingleLifecycleDiagnosticProjection(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("lifecycle diagnostic projection conflict")
	}
	return nil
}

func (s *PostgresStore) CommitAgentLifecycleTransition(ctx context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	var err error
	req, err = normalizeLifecycleTransition(req)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	var result runtimemanager.AgentLifecycleTransitionResult
	err = s.runAuthorActivityMutation(ctx, "postgres commit agent lifecycle transition", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(txctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "swarm:agent-lifecycle:"+req.AgentID); err != nil {
			return err
		}
		var ok bool
		var err error
		if result, ok, err = loadPostgresLifecycleOperationResult(txctx, tx, req); err != nil || ok {
			return err
		}
		previous, exists, err := loadPostgresLifecycleCell(txctx, tx, req.AgentID)
		if err != nil {
			return err
		}
		if err := validateLifecycleExpectation(req, previous, exists); err != nil {
			return err
		}
		result = lifecycleResult(req, previous, exists)
		result.Subordinate, err = applyPostgresLifecycleSubordinate(txctx, tx, req)
		if err != nil {
			return err
		}
		if err := applyPostgresLifecycleCell(txctx, tx, req, result); err != nil {
			return err
		}
		if err := insertPostgresLifecycleEvidence(txctx, tx, req, result); err != nil {
			return err
		}
		return recordAgentLifecycleAuthorActivity(txctx, req, result)
	})
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	return result, nil
}

func (s *SQLiteRuntimeStore) CommitAgentLifecycleTransition(ctx context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	var err error
	req, err = normalizeLifecycleTransition(req)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	var result runtimemanager.AgentLifecycleTransitionResult
	err = s.runAuthorActivityMutation(ctx, "sqlite commit agent lifecycle transition", func(txctx context.Context, tx *sql.Tx) error {
		var ok bool
		var err error
		result, ok, err = loadSQLiteLifecycleOperationResult(txctx, tx, req)
		if err != nil || ok {
			return err
		}
		previous, exists, err := loadSQLiteLifecycleCell(txctx, tx, req.AgentID)
		if err != nil {
			return err
		}
		if err := validateLifecycleExpectation(req, previous, exists); err != nil {
			return err
		}
		result = lifecycleResult(req, previous, exists)
		result.Subordinate, err = applySQLiteLifecycleSubordinate(txctx, tx, req)
		if err != nil {
			return err
		}
		if err := applySQLiteLifecycleCellTx(txctx, tx, req, result); err != nil {
			return err
		}
		if err := insertSQLiteLifecycleEvidenceTx(txctx, tx, req, result); err != nil {
			return err
		}
		return recordAgentLifecycleAuthorActivity(txctx, req, result)
	})
	return result, err
}

func recordAgentLifecycleAuthorActivity(ctx context.Context, req runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	previousGeneration := result.PreviousGeneration
	nextGeneration := result.Generation
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindAgentLifecycle, Transition: string(result.Phase),
		SourceOwner: "agent_lifecycle_transition_facts", SourceIdentity: result.TransitionID,
		DedupKey: "agent-transition:" + result.TransitionID, OccurredAt: req.Now.UTC(), AgentID: result.AgentID,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "agent", SubjectID: result.AgentID, PreviousPhase: string(result.PreviousPhase),
			NextPhase: string(result.Phase), PreviousGeneration: &previousGeneration, NextGeneration: &nextGeneration,
			RunMode: string(result.RunMode),
		},
	})
}

type lifecycleCell struct {
	Epoch      int64
	Generation uint64
	Phase      runtimemanager.AgentLifecyclePhase
}

func normalizeLifecycleTransition(req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransition, error) {
	plan, err := req.Subordinate.Normalize()
	if err != nil {
		return runtimemanager.AgentLifecycleTransition{}, err
	}
	req.Subordinate = plan
	if err := validateLifecycleTransition(req); err != nil {
		return runtimemanager.AgentLifecycleTransition{}, err
	}
	return req, nil
}

func validateLifecycleTransition(req runtimemanager.AgentLifecycleTransition) error {
	for name, value := range map[string]string{
		"operation_id": req.OperationID, "operation_kind": req.OperationKind, "request_hash": req.RequestHash,
		"agent_id": req.AgentID, "trigger": req.Trigger, "config_revision": req.ConfigRevision,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if _, err := uuid.Parse(req.OperationID); err != nil {
		return fmt.Errorf("operation_id must be a UUID: %w", err)
	}
	if req.TargetEpoch <= 0 || req.TargetGeneration == 0 || req.TargetPhase == "" || req.RunMode == "" {
		return fmt.Errorf("complete target lifecycle state is required")
	}
	if req.Now.IsZero() {
		return fmt.Errorf("lifecycle transition time is required")
	}
	return nil
}

func validateLifecycleExpectation(req runtimemanager.AgentLifecycleTransition, previous lifecycleCell, exists bool) error {
	if !exists {
		if req.OperationKind == "spawn" && req.ExpectedGeneration == 0 && req.ExpectedPhase == "" {
			return nil
		}
		return lifecycleConflict(req, previous, false)
	}
	if previous.Epoch != req.ExpectedEpoch || previous.Generation != req.ExpectedGeneration || previous.Phase != req.ExpectedPhase {
		return lifecycleConflict(req, previous, true)
	}
	return nil
}

func lifecycleConflict(req runtimemanager.AgentLifecycleTransition, current lifecycleCell, exists bool) error {
	return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "agent-lifecycle-store", req.OperationKind, map[string]any{
		"agent_id": req.AgentID, "expected_epoch": req.ExpectedEpoch, "expected_generation": req.ExpectedGeneration,
		"expected_phase": req.ExpectedPhase, "current_exists": exists, "current_epoch": current.Epoch,
		"current_generation": current.Generation, "current_phase": current.Phase,
	})
}

func lifecycleResult(req runtimemanager.AgentLifecycleTransition, previous lifecycleCell, exists bool) runtimemanager.AgentLifecycleTransitionResult {
	previousPhase := runtimemanager.AgentLifecyclePhase("absent")
	if exists {
		previousPhase = previous.Phase
	}
	return runtimemanager.AgentLifecycleTransitionResult{
		OperationID: req.OperationID, TransitionID: uuid.NewString(), AgentID: req.AgentID,
		PreviousEpoch: previous.Epoch, RuntimeEpoch: req.TargetEpoch,
		PreviousGeneration: previous.Generation, Generation: req.TargetGeneration,
		PreviousPhase: previousPhase, Phase: req.TargetPhase, ConfigRevision: req.ConfigRevision, RunMode: req.RunMode,
		Subordinate: runtimesessions.LifecycleMutationOutcome{Action: req.Subordinate.Action},
	}
}

type lifecycleSessionRow struct {
	SessionID    string
	RunID        string
	EntityID     string
	FlowInstance string
	ScopeKey     string
	Scope        string
	RuntimeMode  string
	Status       string
}

func applyPostgresLifecycleSubordinate(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition) (runtimesessions.LifecycleMutationOutcome, error) {
	outcome := runtimesessions.LifecycleMutationOutcome{Action: req.Subordinate.Action}
	if req.Subordinate.Action == runtimesessions.LifecycleMutationNone {
		return outcome, nil
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT session_id::text, COALESCE(run_id::text, ''), COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''),
		       scope_key, scope, runtime_mode, status
		FROM agent_sessions
		WHERE agent_id = $1 AND status IN ('active', 'suspended')
		ORDER BY session_id
		FOR UPDATE
	`, req.AgentID)
	if err != nil {
		return outcome, fmt.Errorf("lock lifecycle subordinate session set: %w", err)
	}
	defer rows.Close()
	var selected []lifecycleSessionRow
	for rows.Next() {
		var row lifecycleSessionRow
		if err := rows.Scan(&row.SessionID, &row.RunID, &row.EntityID, &row.FlowInstance, &row.ScopeKey, &row.Scope, &row.RuntimeMode, &row.Status); err != nil {
			return outcome, err
		}
		selected = append(selected, row)
	}
	if err := rows.Err(); err != nil {
		return outcome, err
	}
	if err := rows.Close(); err != nil {
		return outcome, err
	}
	for _, row := range selected {
		mutation, err := applyPostgresLifecycleSessionMutation(ctx, tx, req, row)
		if err != nil {
			return outcome, err
		}
		outcome.Sessions = append(outcome.Sessions, mutation)
	}
	return outcome, nil
}

func applyPostgresLifecycleSessionMutation(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, row lifecycleSessionRow) (runtimesessions.LifecycleSessionMutation, error) {
	mutation := runtimesessions.LifecycleSessionMutation{
		PreviousSessionID: row.SessionID, ScopeKey: row.ScopeKey, RuntimeMode: row.RuntimeMode, PreviousStatus: row.Status,
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions
		SET status = 'terminated', termination_reason = $2, termination_detail = NULLIF($3, ''),
		    successor_session_id = NULL, terminated_at = $4, lease_holder = NULL, lease_expires_at = NULL, updated_at = $4
		WHERE session_id = $1::uuid AND status IN ('active', 'suspended')
	`, row.SessionID, req.Subordinate.TerminationReason.String(), req.Subordinate.TerminationDetail, req.Now.UTC()); err != nil {
		return mutation, fmt.Errorf("terminate lifecycle subordinate session %s: %w", row.SessionID, err)
	}
	if req.Subordinate.Action != runtimesessions.LifecycleMutationRotateCurrentSet {
		return mutation, nil
	}
	mutation.SuccessorSessionID = runtimesessions.LifecycleSuccessorSessionID(req.OperationID, row.SessionID)
	mutation.SuccessorStatus = row.Status
	runtimeState, err := json.Marshal(map[string]any{
		"summary": req.Subordinate.CheckpointSummary, "retries_from_session_id": row.SessionID,
		"rotation_operation_id": req.OperationID,
	})
	if err != nil {
		return mutation, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state, lease_holder, lease_expires_at,
			status, created_at, updated_at
		) VALUES (
			$1::uuid, NULLIF($2, '')::uuid, $3, NULLIF($4, '')::uuid, NULLIF($5, ''), $6, $7,
			'[]'::jsonb, 0, $8, $9::jsonb, NULL, NULL, $10, $11, $11
		)
	`, mutation.SuccessorSessionID, row.RunID, req.AgentID, row.EntityID, row.FlowInstance, row.ScopeKey, row.Scope, row.RuntimeMode, string(runtimeState), row.Status, req.Now.UTC()); err != nil {
		return mutation, fmt.Errorf("insert lifecycle subordinate successor for %s: %w", row.SessionID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET successor_session_id = $2::uuid, updated_at = $3 WHERE session_id = $1::uuid AND status = 'terminated'`, row.SessionID, mutation.SuccessorSessionID, req.Now.UTC()); err != nil {
		return mutation, fmt.Errorf("link lifecycle subordinate successor for %s: %w", row.SessionID, err)
	}
	return mutation, nil
}

func applySQLiteLifecycleSubordinate(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition) (runtimesessions.LifecycleMutationOutcome, error) {
	outcome := runtimesessions.LifecycleMutationOutcome{Action: req.Subordinate.Action}
	if req.Subordinate.Action == runtimesessions.LifecycleMutationNone {
		return outcome, nil
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT session_id, COALESCE(run_id, ''), COALESCE(entity_id, ''), COALESCE(flow_instance, ''),
		       scope_key, scope, runtime_mode, status
		FROM agent_sessions
		WHERE agent_id = ? AND status IN ('active', 'suspended')
		ORDER BY session_id
	`, req.AgentID)
	if err != nil {
		return outcome, fmt.Errorf("lock sqlite lifecycle subordinate session set: %w", err)
	}
	var selected []lifecycleSessionRow
	for rows.Next() {
		var row lifecycleSessionRow
		if err := rows.Scan(&row.SessionID, &row.RunID, &row.EntityID, &row.FlowInstance, &row.ScopeKey, &row.Scope, &row.RuntimeMode, &row.Status); err != nil {
			_ = rows.Close()
			return outcome, err
		}
		selected = append(selected, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return outcome, err
	}
	if err := rows.Close(); err != nil {
		return outcome, err
	}
	for _, row := range selected {
		mutation := runtimesessions.LifecycleSessionMutation{
			PreviousSessionID: row.SessionID, ScopeKey: row.ScopeKey, RuntimeMode: row.RuntimeMode, PreviousStatus: row.Status,
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE agent_sessions
			SET status = 'terminated', termination_reason = ?, termination_detail = ?, successor_session_id = NULL,
			    terminated_at = ?, lease_holder = NULL, lease_expires_at = NULL, updated_at = ?
			WHERE session_id = ? AND status IN ('active', 'suspended')
		`, req.Subordinate.TerminationReason.String(), sqliteNullString(req.Subordinate.TerminationDetail), req.Now.UTC(), req.Now.UTC(), row.SessionID); err != nil {
			return outcome, fmt.Errorf("terminate sqlite lifecycle subordinate session %s: %w", row.SessionID, err)
		}
		if req.Subordinate.Action == runtimesessions.LifecycleMutationRotateCurrentSet {
			mutation.SuccessorSessionID = runtimesessions.LifecycleSuccessorSessionID(req.OperationID, row.SessionID)
			mutation.SuccessorStatus = row.Status
			runtimeState, err := json.Marshal(map[string]any{
				"summary": req.Subordinate.CheckpointSummary, "retries_from_session_id": row.SessionID,
				"rotation_operation_id": req.OperationID,
			})
			if err != nil {
				return outcome, err
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO agent_sessions (
					session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
					conversation, turn_count, runtime_mode, runtime_state, lease_holder, lease_expires_at,
					status, created_at, updated_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, '[]', 0, ?, ?, NULL, NULL, ?, ?, ?)
			`, mutation.SuccessorSessionID, sqliteNullUUID(row.RunID), req.AgentID, sqliteNullUUID(row.EntityID), sqliteNullString(row.FlowInstance),
				row.ScopeKey, row.Scope, row.RuntimeMode, string(runtimeState), row.Status, req.Now.UTC(), req.Now.UTC()); err != nil {
				return outcome, fmt.Errorf("insert sqlite lifecycle subordinate successor for %s: %w", row.SessionID, err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET successor_session_id = ?, updated_at = ? WHERE session_id = ? AND status = 'terminated'`, mutation.SuccessorSessionID, req.Now.UTC(), row.SessionID); err != nil {
				return outcome, fmt.Errorf("link sqlite lifecycle subordinate successor for %s: %w", row.SessionID, err)
			}
		}
		outcome.Sessions = append(outcome.Sessions, mutation)
	}
	return outcome, nil
}

func loadPostgresLifecycleCell(ctx context.Context, tx *sql.Tx, agentID string) (lifecycleCell, bool, error) {
	var cell lifecycleCell
	var generation int64
	err := tx.QueryRowContext(ctx, `SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase FROM agents WHERE agent_id = $1 FOR UPDATE`, agentID).Scan(&cell.Epoch, &generation, &cell.Phase)
	if err == sql.ErrNoRows {
		return lifecycleCell{}, false, nil
	}
	if err != nil {
		return lifecycleCell{}, false, err
	}
	cell.Generation = uint64(generation)
	return cell, true, nil
}

func loadSQLiteLifecycleCell(ctx context.Context, tx *sql.Tx, agentID string) (lifecycleCell, bool, error) {
	var cell lifecycleCell
	var generation int64
	err := tx.QueryRowContext(ctx, `SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase FROM agents WHERE agent_id = ?`, agentID).Scan(&cell.Epoch, &generation, &cell.Phase)
	if err == sql.ErrNoRows {
		return lifecycleCell{}, false, nil
	}
	if err != nil {
		return lifecycleCell{}, false, err
	}
	cell.Generation = uint64(generation)
	return cell, true, nil
}

func loadPostgresLifecycleOperationResult(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, bool, error) {
	var requestHash string
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT request_hash, result FROM agent_lifecycle_operations WHERE operation_id = $1::uuid`, req.OperationID).Scan(&requestHash, &raw)
	return decodeLifecycleOperationResult(req, requestHash, raw, err)
}

func loadSQLiteLifecycleOperationResult(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, bool, error) {
	var requestHash string
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT request_hash, result FROM agent_lifecycle_operations WHERE operation_id = ?`, req.OperationID).Scan(&requestHash, &raw)
	return decodeLifecycleOperationResult(req, requestHash, raw, err)
}

func decodeLifecycleOperationResult(req runtimemanager.AgentLifecycleTransition, requestHash string, raw []byte, err error) (runtimemanager.AgentLifecycleTransitionResult, bool, error) {
	if err == sql.ErrNoRows {
		return runtimemanager.AgentLifecycleTransitionResult{}, false, nil
	}
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, false, err
	}
	if requestHash != req.RequestHash {
		return runtimemanager.AgentLifecycleTransitionResult{}, true, runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "lifecycle_operation_request_conflict", "agent-lifecycle-store", req.OperationKind, map[string]any{"operation_id": req.OperationID})
	}
	var result runtimemanager.AgentLifecycleTransitionResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, true, fmt.Errorf("decode lifecycle operation result: %w", err)
	}
	result.Replayed = true
	return result, true, nil
}

func applyPostgresLifecycleCell(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	if req.Agent != nil {
		projection, err := projectPersistedAgentConfig(req.Agent.Config, req.Agent.ParentAgentID)
		if err != nil {
			return err
		}
		startedAt := req.Agent.StartedAt
		if startedAt.IsZero() {
			startedAt = req.Now
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO agents (agent_id, flow_instance, role, model, llm_backend, conversation_mode, parent_agent_id, entity_id,
				config, subscriptions, emit_events, tools, permissions, runtime_descriptor, status, turn_count, last_active_at, created_at,
				lifecycle_phase, lifecycle_generation, lifecycle_runtime_epoch, lifecycle_config_revision, lifecycle_run_mode, lifecycle_last_transition_id)
			VALUES ($1, NULLIF($2,''), $3, $4, $5, $6, NULLIF($7,''), NULLIF($8,'')::uuid, $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb,
				$13::jsonb, $14::jsonb, $15, 0, $16, $17, $18, $19, $20, $21, $22, $23::uuid)
			ON CONFLICT (agent_id) DO UPDATE SET flow_instance=EXCLUDED.flow_instance, role=EXCLUDED.role, model=EXCLUDED.model,
				llm_backend=EXCLUDED.llm_backend, conversation_mode=EXCLUDED.conversation_mode, parent_agent_id=EXCLUDED.parent_agent_id,
				entity_id=EXCLUDED.entity_id, config=EXCLUDED.config, subscriptions=EXCLUDED.subscriptions, emit_events=EXCLUDED.emit_events,
				tools=EXCLUDED.tools, permissions=EXCLUDED.permissions, runtime_descriptor=EXCLUDED.runtime_descriptor, status=EXCLUDED.status,
				last_active_at=EXCLUDED.last_active_at, lifecycle_phase=EXCLUDED.lifecycle_phase,
				lifecycle_generation=EXCLUDED.lifecycle_generation, lifecycle_runtime_epoch=EXCLUDED.lifecycle_runtime_epoch,
				lifecycle_config_revision=EXCLUDED.lifecycle_config_revision, lifecycle_run_mode=EXCLUDED.lifecycle_run_mode,
				lifecycle_last_transition_id=EXCLUDED.lifecycle_last_transition_id
		`, projection.AgentID, projection.FlowInstance, projection.Role, projection.Model, projection.LLMBackend, projection.ConversationMode,
			projection.ParentAgentID, projection.EntityID, string(projection.ConfigJSON), string(projection.SubscriptionsJSON), string(projection.EmitEventsJSON),
			string(projection.ToolsJSON), string(projection.PermissionsJSON), string(projection.RuntimeDescriptor), lifecycleAgentStatus(req), req.Now.UTC(), startedAt.UTC(),
			string(req.TargetPhase), req.TargetGeneration, req.TargetEpoch, req.ConfigRevision, string(req.RunMode), result.TransitionID)
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE agents SET status=$2, lifecycle_phase=$3, lifecycle_generation=$4, lifecycle_runtime_epoch=$5, lifecycle_config_revision=$6, lifecycle_run_mode=$7, lifecycle_last_transition_id=$8::uuid, last_active_at=$9 WHERE agent_id=$1`, req.AgentID, lifecycleAgentStatus(req), string(req.TargetPhase), req.TargetGeneration, req.TargetEpoch, req.ConfigRevision, string(req.RunMode), result.TransitionID, req.Now.UTC())
	return err
}

func applySQLiteLifecycleCellTx(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	if req.Agent != nil {
		projection, err := projectPersistedAgentConfig(req.Agent.Config, req.Agent.ParentAgentID)
		if err != nil {
			return err
		}
		startedAt := req.Agent.StartedAt
		if startedAt.IsZero() {
			startedAt = req.Now
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO agents (agent_id, flow_instance, role, model, llm_backend, conversation_mode, parent_agent_id, entity_id,
				config, subscriptions, emit_events, tools, permissions, runtime_descriptor, status, turn_count, last_active_at, created_at,
				lifecycle_phase, lifecycle_generation, lifecycle_runtime_epoch, lifecycle_config_revision, lifecycle_run_mode, lifecycle_last_transition_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(agent_id) DO UPDATE SET flow_instance=excluded.flow_instance, role=excluded.role, model=excluded.model,
				llm_backend=excluded.llm_backend, conversation_mode=excluded.conversation_mode, parent_agent_id=excluded.parent_agent_id,
				entity_id=excluded.entity_id, config=excluded.config, subscriptions=excluded.subscriptions, emit_events=excluded.emit_events,
				tools=excluded.tools, permissions=excluded.permissions, runtime_descriptor=excluded.runtime_descriptor, status=excluded.status,
				last_active_at=excluded.last_active_at, lifecycle_phase=excluded.lifecycle_phase,
				lifecycle_generation=excluded.lifecycle_generation, lifecycle_runtime_epoch=excluded.lifecycle_runtime_epoch,
				lifecycle_config_revision=excluded.lifecycle_config_revision, lifecycle_run_mode=excluded.lifecycle_run_mode,
				lifecycle_last_transition_id=excluded.lifecycle_last_transition_id
		`, projection.AgentID, sqliteNullString(projection.FlowInstance), projection.Role, projection.Model, projection.LLMBackend, projection.ConversationMode,
			sqliteNullString(projection.ParentAgentID), sqliteNullUUID(projection.EntityID), string(projection.ConfigJSON), string(projection.SubscriptionsJSON),
			string(projection.EmitEventsJSON), string(projection.ToolsJSON), string(projection.PermissionsJSON), string(projection.RuntimeDescriptor), lifecycleAgentStatus(req),
			req.Now.UTC(), startedAt.UTC(), string(req.TargetPhase), req.TargetGeneration, req.TargetEpoch, req.ConfigRevision, string(req.RunMode), result.TransitionID)
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE agents SET status=?, lifecycle_phase=?, lifecycle_generation=?, lifecycle_runtime_epoch=?, lifecycle_config_revision=?, lifecycle_run_mode=?, lifecycle_last_transition_id=?, last_active_at=? WHERE agent_id=?`, lifecycleAgentStatus(req), string(req.TargetPhase), req.TargetGeneration, req.TargetEpoch, req.ConfigRevision, string(req.RunMode), result.TransitionID, req.Now.UTC(), req.AgentID)
	return err
}

func lifecycleAgentStatus(req runtimemanager.AgentLifecycleTransition) string {
	switch req.TargetPhase {
	case runtimemanager.AgentLifecycleTerminated:
		return "terminated"
	case runtimemanager.AgentLifecycleFailed:
		return "failed"
	default:
		if req.Agent != nil && strings.TrimSpace(req.Agent.Status) != "" {
			return agentPersistedStatus(req.Agent.Status)
		}
		return "active"
	}
}

func insertPostgresLifecycleEvidence(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	raw, _ := json.Marshal(result)
	if _, err := tx.ExecContext(ctx, `INSERT INTO agent_lifecycle_operations (operation_id, agent_id, operation_kind, request_hash, expected_epoch, expected_generation, target_generation, target_phase, config_revision, run_mode, state, result, created_at, updated_at, completed_at) VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,'succeeded',$11::jsonb,$12,$12,$12)`, req.OperationID, req.AgentID, req.OperationKind, req.RequestHash, req.ExpectedEpoch, req.ExpectedGeneration, req.TargetGeneration, string(req.TargetPhase), req.ConfigRevision, string(req.RunMode), string(raw), req.Now.UTC()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO agent_lifecycle_transition_facts (transition_id, operation_id, agent_id, trigger, previous_phase, next_phase, previous_generation, next_generation, runtime_epoch, config_revision, run_mode, created_at) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, result.TransitionID, req.OperationID, req.AgentID, req.Trigger, string(result.PreviousPhase), string(result.Phase), result.PreviousGeneration, result.Generation, result.RuntimeEpoch, result.ConfigRevision, string(result.RunMode), req.Now.UTC()); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO agent_lifecycle_diagnostic_outbox (outbox_id, operation_id, agent_id, event_name, payload, created_at) VALUES ($1::uuid,$2::uuid,$3,'platform.agent_lifecycle_transition',$4::jsonb,$5)`, uuid.NewString(), req.OperationID, req.AgentID, string(raw), req.Now.UTC())
	return err
}

func insertSQLiteLifecycleEvidenceTx(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	raw, _ := json.Marshal(result)
	if _, err := tx.ExecContext(ctx, `INSERT INTO agent_lifecycle_operations (operation_id, agent_id, operation_kind, request_hash, expected_epoch, expected_generation, target_generation, target_phase, config_revision, run_mode, state, result, created_at, updated_at, completed_at) VALUES (?,?,?,?,?,?,?,?,?,?,'succeeded',?,?,?,?)`, req.OperationID, req.AgentID, req.OperationKind, req.RequestHash, req.ExpectedEpoch, req.ExpectedGeneration, req.TargetGeneration, string(req.TargetPhase), req.ConfigRevision, string(req.RunMode), string(raw), req.Now.UTC(), req.Now.UTC(), req.Now.UTC()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO agent_lifecycle_transition_facts (transition_id, operation_id, agent_id, trigger, previous_phase, next_phase, previous_generation, next_generation, runtime_epoch, config_revision, run_mode, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, result.TransitionID, req.OperationID, req.AgentID, req.Trigger, string(result.PreviousPhase), string(result.Phase), result.PreviousGeneration, result.Generation, result.RuntimeEpoch, result.ConfigRevision, string(result.RunMode), req.Now.UTC()); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO agent_lifecycle_diagnostic_outbox (outbox_id, operation_id, agent_id, event_name, payload, created_at) VALUES (?,?,?,'platform.agent_lifecycle_transition',?,?)`, uuid.NewString(), req.OperationID, req.AgentID, string(raw), req.Now.UTC())
	return err
}
