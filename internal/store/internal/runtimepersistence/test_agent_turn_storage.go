package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"

	storerunfork "github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

type ManagedAgentTurnStorageRow struct {
	TurnID, SessionID, TriggerEventID, ExecutionMode          string
	ToolCalls, EmittedEvents, RequestPayload, ResponsePayload json.RawMessage
}

// These are detached physical rows, including failed and non-mock turns. The
// caller retains its original mode/event/cardinality assertions.
func ReadManagedAgentTurnStorageForTest(ctx context.Context, selected any, runID, agentID string) ([]ManagedAgentTurnStorageRow, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return nil, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	query := `SELECT turn_id,session_id,trigger_event_id,execution_mode,tool_calls,emitted_events,request_payload,response_payload
		FROM agent_turns WHERE run_id=$1`
	args := []any{runID}
	if agentID != "" {
		query += ` AND agent_id=$2`
		args = append(args, agentID)
	}
	query += ` ORDER BY created_at,turn_id`
	var out []ManagedAgentTurnStorageRow
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row ManagedAgentTurnStorageRow
			var session, trigger sql.NullString
			var calls, emitted, request, response []byte
			if err := rows.Scan(&row.TurnID, &session, &trigger, &row.ExecutionMode, &calls, &emitted, &request, &response); err != nil {
				return err
			}
			row.TriggerEventID = trigger.String
			row.SessionID = session.String
			row.ToolCalls, row.EmittedEvents = calls, emitted
			row.RequestPayload, row.ResponsePayload = request, response
			out = append(out, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

type ManagedTurnEffectStorage struct {
	Settled, Projected, ResponseConsumed, ProviderAttempts int
	ActiveProviderAttempts, LaunchedCandidateWorkAttempts  int
	Attempts                                               *[]ManagedTurnEffectStorageAttempt
}

type ManagedTurnEffectStorageAttempt struct {
	AttemptID, OperationID, OriginDeliveryID, OriginEventID string
	AttemptState, OperationState, DeliveryState, ReasonCode string
	AuthorizedAt, LaunchedAt, ResponseObservedAt            storerunfork.ConversationForkTimeValue
	CompletedAt, UpdatedAt, OperationCreatedAt              storerunfork.ConversationForkTimeValue
	DeliverySettledAt                                       storerunfork.ConversationForkTimeValue
}

func ReadManagedTurnEffectStorageForTest(ctx context.Context, selected any, runID, agentID string) (ManagedTurnEffectStorage, error) {
	var out ManagedTurnEffectStorage
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return out, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return out, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN agent_turns t ON t.completion_attempt_id=a.attempt_id WHERE t.run_id=$1 AND t.agent_id=$2 AND a.state='settled'),
			(SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN agent_turns t ON t.completion_attempt_id=a.attempt_id WHERE t.run_id=$1 AND a.completion_projection_phase IS NOT NULL),
			(SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN agent_turns t ON t.completion_attempt_id=a.attempt_id WHERE t.run_id=$1 AND t.agent_id=$2 AND a.state='settled' AND a.completion_projection_phase='response_consumed'),
			(SELECT COUNT(*) FROM runtime_external_effect_attempts a
			 JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
			 LEFT JOIN run_fork_selected_contract_runtime_executions x ON x.execution_id=o.selected_execution_id
			 WHERE o.effect_kind='provider_turn' AND
			 ((o.authority_kind='normal_agent' AND o.agent_run_id=$1) OR
			  (o.authority_kind='selected_contract_fork' AND x.fork_run_id=$1))),
			(SELECT COUNT(*) FROM runtime_external_effect_operations o
			 JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
			 WHERE o.agent_run_id=$1 AND o.effect_kind='provider_turn'
			 AND a.state IN ('authorized','launched','response_observed')),
			(SELECT COUNT(*) FROM runtime_external_effect_operations o
			 JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
			 JOIN event_deliveries d ON d.delivery_id=a.origin_delivery_id
			 JOIN events e ON e.event_id=d.event_id AND e.run_id=o.agent_run_id
			 WHERE o.agent_run_id=$1 AND o.effect_kind='provider_turn'
			 AND a.state IN ('launched','response_observed')
			 AND e.event_name LIKE '%/candidate.work.requested')`, runID, agentID).Scan(&out.Settled, &out.Projected, &out.ResponseConsumed, &out.ProviderAttempts,
			&out.ActiveProviderAttempts, &out.LaunchedCandidateWorkAttempts); err != nil {
			return err
		}
		if agentID != "" {
			return nil
		}
		// The run-wide crash observer retains terminal attempts in the same
		// snapshot as the unchanged active/launched selection counts.
		rows, err := tx.QueryContext(ctx, `SELECT
			a.attempt_id,a.operation_id,COALESCE(CAST(a.origin_delivery_id AS TEXT),''),COALESCE(CAST(d.event_id AS TEXT),''),
			a.state,o.state,COALESCE(d.status,''),COALESCE(d.reason_code,''),
			a.authorized_at,a.launched_at,a.response_observed_at,a.completed_at,a.updated_at,o.created_at,d.settled_at
			FROM runtime_external_effect_attempts a
			JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
			LEFT JOIN run_fork_selected_contract_runtime_executions x ON x.execution_id=o.selected_execution_id
			LEFT JOIN event_deliveries d ON d.delivery_id=a.origin_delivery_id AND d.run_id=$1
			WHERE o.effect_kind='provider_turn' AND
			((o.authority_kind='normal_agent' AND o.agent_run_id=$1) OR
			 (o.authority_kind='selected_contract_fork' AND x.fork_run_id=$1))
			ORDER BY a.attempt_id`, runID)
		if err != nil {
			return err
		}
		defer rows.Close()
		var attempts []ManagedTurnEffectStorageAttempt
		for rows.Next() {
			var row ManagedTurnEffectStorageAttempt
			if err := rows.Scan(&row.AttemptID, &row.OperationID, &row.OriginDeliveryID, &row.OriginEventID,
				&row.AttemptState, &row.OperationState, &row.DeliveryState, &row.ReasonCode,
				&row.AuthorizedAt, &row.LaunchedAt, &row.ResponseObservedAt, &row.CompletedAt,
				&row.UpdatedAt, &row.OperationCreatedAt, &row.DeliverySettledAt); err != nil {
				return err
			}
			attempts = append(attempts, row)
		}
		out.Attempts = &attempts
		return rows.Err()
	})
	if err != nil {
		return ManagedTurnEffectStorage{}, err
	}
	return out, nil
}
