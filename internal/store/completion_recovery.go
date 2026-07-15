package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type completionRecoveryAttempt struct {
	OperationID       string
	AttemptID         string
	AuthorityKind     string
	AuthorityID       string
	AuthorityEvidence string
	OperationMode     string
	AttemptMode       string
	Adapter           string
	Transport         string
	State             string
	TargetKind        string
	TargetID          string
	TargetOrdinal     int
}

type completionRecoveryAuthorityEvidence struct {
	ActorTokenID  string `json:"actor_token_id"`
	ExecutionMode string `json:"execution_mode"`
	UsageTarget   struct {
		Kind          string `json:"kind"`
		ID            string `json:"id"`
		Ordinal       int    `json:"ordinal"`
		RunID         string `json:"run_id"`
		AgentID       string `json:"agent_id"`
		SessionID     string `json:"session_id"`
		MemoryEnabled bool   `json:"memory_enabled"`
		MemorySource  string `json:"memory_source"`
		FlowInstance  string `json:"flow_instance"`
		EntityID      string `json:"entity_id"`
	} `json:"usage_target"`
}

func reconcileCompletionAttemptsPostgres(ctx context.Context, tx *sql.Tx, now time.Time) (runtimeeffects.RecoverySummary, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT o.operation_id::text,a.attempt_id::text,o.authority_kind,o.authority_id,o.authority_evidence::text,
		       o.execution_mode,a.execution_mode,
		       a.adapter,a.transport,a.state,a.usage_target_kind,a.usage_target_id::text,COALESCE(a.target_ordinal,0)
		FROM runtime_external_effect_operations o
		JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
		LEFT JOIN agents g ON o.authority_kind='normal_agent' AND g.agent_id=o.agent_id
		WHERE o.effect_kind='provider_turn' AND a.usage_target_kind IS NOT NULL
		  AND a.state IN ('authorized','launched','response_observed')
		  AND `+postgresExternalEffectActiveOwnerPredicate+`
		  AND (
		    a.lease_expires_at <= $1 OR
		    (o.authority_kind='normal_agent' AND (
		      g.agent_id IS NULL OR g.lifecycle_runtime_epoch IS DISTINCT FROM o.runtime_epoch OR
		      g.lifecycle_generation IS DISTINCT FROM o.generation OR g.lifecycle_phase <> 'running'
		    ))
		  )
		ORDER BY a.authorized_at,a.attempt_id
		FOR UPDATE OF o,a
	`, now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, fmt.Errorf("list completion attempts for recovery: %w", err)
	}
	attempts, err := scanCompletionRecoveryAttempts(rows)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	return reconcileCompletionAttempts(ctx, tx, true, attempts, now)
}

func reconcileCompletionAttemptsSQLite(ctx context.Context, tx *sql.Tx, now time.Time) (runtimeeffects.RecoverySummary, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT o.operation_id,a.attempt_id,o.authority_kind,o.authority_id,o.authority_evidence,
		       o.execution_mode,a.execution_mode,
		       a.adapter,a.transport,a.state,a.usage_target_kind,a.usage_target_id,COALESCE(a.target_ordinal,0)
		FROM runtime_external_effect_operations o
		JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
		LEFT JOIN agents g ON o.authority_kind='normal_agent' AND g.agent_id=o.agent_id
		WHERE o.effect_kind='provider_turn' AND a.usage_target_kind IS NOT NULL
		  AND a.state IN ('authorized','launched','response_observed')
		  AND `+sqliteExternalEffectActiveOwnerPredicate+`
		  AND (
		    a.lease_expires_at <= ? OR
		    (o.authority_kind='normal_agent' AND (
		      g.agent_id IS NULL OR g.lifecycle_runtime_epoch IS NOT o.runtime_epoch OR
		      g.lifecycle_generation IS NOT o.generation OR g.lifecycle_phase <> 'running'
		    ))
		  )
		ORDER BY a.authorized_at,a.attempt_id
	`, now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, fmt.Errorf("list sqlite completion attempts for recovery: %w", err)
	}
	attempts, err := scanCompletionRecoveryAttempts(rows)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	return reconcileCompletionAttempts(ctx, tx, false, attempts, now)
}

func scanCompletionRecoveryAttempts(rows *sql.Rows) ([]completionRecoveryAttempt, error) {
	defer rows.Close()
	var attempts []completionRecoveryAttempt
	for rows.Next() {
		var attempt completionRecoveryAttempt
		if err := rows.Scan(&attempt.OperationID, &attempt.AttemptID, &attempt.AuthorityKind, &attempt.AuthorityID,
			&attempt.AuthorityEvidence, &attempt.OperationMode, &attempt.AttemptMode, &attempt.Adapter, &attempt.Transport, &attempt.State,
			&attempt.TargetKind, &attempt.TargetID, &attempt.TargetOrdinal); err != nil {
			return nil, fmt.Errorf("scan completion attempt for recovery: %w", err)
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read completion attempts for recovery: %w", err)
	}
	return attempts, nil
}

func reconcileCompletionAttempts(ctx context.Context, tx *sql.Tx, postgres bool, attempts []completionRecoveryAttempt, now time.Time) (runtimeeffects.RecoverySummary, error) {
	var summary runtimeeffects.RecoverySummary
	for _, recovered := range attempts {
		state := runtimeeffects.StateTerminalFailure
		failureClass := runtimefailures.ClassLifecycleConflict
		failureCode := "effect_recovery_prelaunch_abandoned"
		if recovered.State == string(runtimeeffects.StateLaunched) || recovered.State == string(runtimeeffects.StateResponseObserved) {
			state = runtimeeffects.StateOutcomeUncertain
			failureClass = runtimefailures.ClassOutcomeUncertain
			failureCode = "effect_recovery_outcome_unconfirmed"
		}
		failureErr := runtimefailures.New(failureClass, failureCode, "external-effects", "startup_reconcile", map[string]any{
			"operation_id": recovered.OperationID, "attempt_id": recovered.AttemptID,
			"recovered_at": now.UTC().Format(time.RFC3339Nano),
		})
		failure := failureErr.(*runtimefailures.Error).Failure
		attempt, settlement, err := completionRecoverySettlement(recovered, state, &failure, now)
		if err != nil {
			return runtimeeffects.RecoverySummary{}, err
		}
		if postgres {
			if err := insertCompletionTargetPostgres(ctx, tx, attempt, settlement); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
			if state != runtimeeffects.StateTerminalFailure {
				if _, err := insertCompletionSpendPostgres(ctx, tx, attempt, settlement); err != nil {
					return runtimeeffects.RecoverySummary{}, err
				}
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM runtime_effect_budget_reservations WHERE attempt_id=$1::uuid`, attempt.AttemptID); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
			if err := settleExternalAttemptPostgres(ctx, tx, settlement.Settlement); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
		} else {
			if err := insertCompletionTargetSQLite(ctx, tx, attempt, settlement); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
			if state != runtimeeffects.StateTerminalFailure {
				if _, err := insertCompletionSpendSQLite(ctx, tx, attempt, settlement); err != nil {
					return runtimeeffects.RecoverySummary{}, err
				}
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM runtime_effect_budget_reservations WHERE attempt_id=?`, attempt.AttemptID); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
			if err := settleExternalAttemptSQLiteTx(ctx, tx, settlement.Settlement); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
		}
		if state == runtimeeffects.StateTerminalFailure {
			summary.PrelaunchTerminal++
		} else {
			summary.OutcomeUncertain++
		}
	}
	return summary, nil
}

func completionRecoverySettlement(recovered completionRecoveryAttempt, state runtimeeffects.State, failure *runtimefailures.Envelope, now time.Time) (runtimeeffects.Attempt, runtimeeffects.CompletionSettlement, error) {
	var evidence completionRecoveryAuthorityEvidence
	if err := json.Unmarshal([]byte(recovered.AuthorityEvidence), &evidence); err != nil {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("decode completion recovery authority evidence for %s: %w", recovered.AttemptID, err)
	}
	mode, ok := executionmode.Parse(recovered.OperationMode)
	if !ok || recovered.AttemptMode != recovered.OperationMode || evidence.ExecutionMode != recovered.OperationMode {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery execution mode conflicts for attempt %s", recovered.AttemptID)
	}
	if evidence.UsageTarget.Kind != recovered.TargetKind || evidence.UsageTarget.ID != recovered.TargetID || evidence.UsageTarget.Ordinal != recovered.TargetOrdinal {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery target evidence conflicts with attempt %s", recovered.AttemptID)
	}
	target := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetKind(recovered.TargetKind), ID: recovered.TargetID, Ordinal: recovered.TargetOrdinal,
		RunID: evidence.UsageTarget.RunID, AgentID: evidence.UsageTarget.AgentID, SessionID: evidence.UsageTarget.SessionID,
		Memory:       agentmemory.Plan{Enabled: evidence.UsageTarget.MemoryEnabled, Source: agentmemory.Source(evidence.UsageTarget.MemorySource)},
		FlowInstance: evidence.UsageTarget.FlowInstance, EntityID: evidence.UsageTarget.EntityID,
	}
	if !target.Valid() {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery target for attempt %s is invalid", recovered.AttemptID)
	}
	authority := runtimeeffects.Authority{Kind: runtimeeffects.AuthorityKind(recovered.AuthorityKind), ID: recovered.AuthorityID, Target: target, ExecutionMode: mode}
	if target.Kind == runtimeeffects.UsageTargetConversationForkCompletion {
		authority.ForkChat.ForkTurnID = target.ID
	}
	attempt := runtimeeffects.Attempt{
		OperationID: recovered.OperationID, AttemptID: recovered.AttemptID, Authority: authority,
		Kind: runtimeeffects.KindProviderTurn, Adapter: recovered.Adapter, Transport: recovered.Transport,
	}
	agentID := strings.TrimSpace(target.AgentID)
	if agentID == "" {
		agentID = strings.TrimSpace(evidence.ActorTokenID)
	}
	if agentID == "" {
		agentID = strings.TrimSpace(recovered.AuthorityID)
	}
	flowInstance := strings.TrimSpace(target.FlowInstance)
	if flowInstance == "" {
		flowInstance = strings.TrimSpace(target.RunID)
	}
	if flowInstance == "" {
		flowInstance = "global"
	}
	settlement := runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{
			OperationID: recovered.OperationID, AttemptID: recovered.AttemptID, Authority: authority,
			State: state, Failure: failure, Evidence: map[string]any{"recovered": true}, Now: now,
		},
		Usage: runtimeeffects.CompletionUsage{ResolvedModel: "unknown", Exactness: runtimeeffects.CompletionUsageUnavailable},
		Spend: runtimeeffects.CompletionSpend{
			EntityID: target.EntityID, FlowInstance: flowInstance, AgentID: agentID, Model: "unknown", ModelAlias: "unknown",
			BackendProfile: "unknown", Provider: recovered.Adapter, Transport: recovered.Transport,
			ResolvedModel: "unknown", InvocationType: coalesce(recovered.Adapter, "completion_recovery"),
		},
		Now: now,
	}
	if target.Kind == runtimeeffects.UsageTargetAgentTurn {
		if _, err := uuid.Parse(strings.TrimSpace(target.SessionID)); err != nil || agentID == "" {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery agent-turn identity for attempt %s is incomplete", recovered.AttemptID)
		}
		settlement.AgentTurn = &runtimeeffects.CompletionAgentTurn{
			TurnID: target.ID, RunID: target.RunID, AgentID: agentID, SessionID: target.SessionID,
			Memory: target.Memory, FlowInstance: target.FlowInstance, EntityID: target.EntityID, Failure: failure,
		}
	}
	return attempt, settlement, nil
}

func reconcileCompletionParentAuthoritiesPostgres(ctx context.Context, tx *sql.Tx, now time.Time) error {
	return reconcileCompletionParentAuthorities(ctx, tx, true, now)
}

func reconcileCompletionParentAuthoritiesSQLite(ctx context.Context, tx *sql.Tx, now time.Time) error {
	return reconcileCompletionParentAuthorities(ctx, tx, false, now)
}

func reconcileCompletionParentAuthorities(ctx context.Context, tx *sql.Tx, postgres bool, now time.Time) error {
	preparedFailure, err := recoveryFailureJSON(runtimefailures.ClassLifecycleConflict, "completion_parent_recovery_prelaunch_abandoned", now)
	if err != nil {
		return err
	}
	uncertainFailure, err := recoveryFailureJSON(runtimefailures.ClassOutcomeUncertain, "completion_parent_recovery_outcome_unconfirmed", now)
	if err != nil {
		return err
	}
	if postgres {
		if _, err := tx.ExecContext(ctx, `UPDATE conversation_fork_turns SET state='abandoned',lease_expires_at=NULL,failure=$1::jsonb,terminal_at=$2,updated_at=$2 WHERE state='prepared' AND lease_expires_at<=$2`, string(preparedFailure), now); err != nil {
			return fmt.Errorf("recover prepared forkchat groups: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE conversation_fork_turns f SET state='outcome_uncertain',lease_expires_at=NULL,failure=$1::jsonb,terminal_at=$2,updated_at=$2 WHERE f.state='executing' AND f.lease_expires_at<=$2 AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.fork_turn_id=f.fork_turn_id AND a.state IN ('authorized','launched','response_observed'))`, string(uncertainFailure), now); err != nil {
			return fmt.Errorf("recover executing forkchat groups: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions e SET state='failed',lease_expires_at=NULL,failure=CASE WHEN e.state='prepared' THEN $1::jsonb ELSE $2::jsonb END,terminal_at=$3,updated_at=$3 WHERE e.state IN ('prepared','running') AND e.lease_expires_at<=$3 AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=e.execution_id AND a.state IN ('authorized','launched','response_observed'))`, string(preparedFailure), string(uncertainFailure), now); err != nil {
			return fmt.Errorf("fail expired selected-contract runtime executions: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='closed',updated_at=$1 WHERE state='failed' AND terminal_at=$1`, now); err != nil {
			return fmt.Errorf("close recovered selected-contract runtime executions: %w", err)
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE conversation_fork_turns SET state='abandoned',lease_expires_at=NULL,failure=?,terminal_at=?,updated_at=? WHERE state='prepared' AND lease_expires_at<=?`, string(preparedFailure), now, now, now); err != nil {
		return fmt.Errorf("recover sqlite prepared forkchat groups: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE conversation_fork_turns AS f SET state='outcome_uncertain',lease_expires_at=NULL,failure=?,terminal_at=?,updated_at=? WHERE f.state='executing' AND f.lease_expires_at<=? AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.fork_turn_id=f.fork_turn_id AND a.state IN ('authorized','launched','response_observed'))`, string(uncertainFailure), now, now, now); err != nil {
		return fmt.Errorf("recover sqlite executing forkchat groups: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions AS e SET state='failed',lease_expires_at=NULL,failure=CASE WHEN e.state='prepared' THEN ? ELSE ? END,terminal_at=?,updated_at=? WHERE e.state IN ('prepared','running') AND e.lease_expires_at<=? AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=e.execution_id AND a.state IN ('authorized','launched','response_observed'))`, string(preparedFailure), string(uncertainFailure), now, now, now); err != nil {
		return fmt.Errorf("fail expired sqlite selected-contract runtime executions: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='closed',updated_at=? WHERE state='failed' AND terminal_at=?`, now, now); err != nil {
		return fmt.Errorf("close recovered sqlite selected-contract runtime executions: %w", err)
	}
	return nil
}

func recoveryFailureJSON(class runtimefailures.Class, code string, now time.Time) ([]byte, error) {
	err := runtimefailures.New(class, code, "completion-authority", "startup_reconcile", map[string]any{"recovered_at": now.UTC().Format(time.RFC3339Nano)})
	failure, ok := runtimefailures.EnvelopeFromError(err)
	if !ok {
		return nil, fmt.Errorf("construct completion parent recovery failure")
	}
	return json.Marshal(failure)
}
