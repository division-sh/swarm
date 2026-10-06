package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
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

type ManagedTurnEffectStorage struct{ Settled, Projected, ResponseConsumed, ProviderAttempts int }

func ReadManagedTurnEffectStorageForTest(ctx context.Context, selected any, runID, agentID string) (ManagedTurnEffectStorage, error) {
	var out ManagedTurnEffectStorage
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return out, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return out, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN agent_turns t ON t.completion_attempt_id=a.attempt_id WHERE t.run_id=$1 AND t.agent_id=$2 AND a.state='settled'),
			(SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN agent_turns t ON t.completion_attempt_id=a.attempt_id WHERE t.run_id=$1 AND a.completion_projection_phase IS NOT NULL),
			(SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN agent_turns t ON t.completion_attempt_id=a.attempt_id WHERE t.run_id=$1 AND t.agent_id=$2 AND a.state='settled' AND a.completion_projection_phase='response_consumed'),
			(SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.agent_run_id=$1 AND o.effect_kind='provider_turn')`, runID, agentID).Scan(&out.Settled, &out.Projected, &out.ResponseConsumed, &out.ProviderAttempts)
	})
	if err != nil {
		return ManagedTurnEffectStorage{}, err
	}
	return out, nil
}
