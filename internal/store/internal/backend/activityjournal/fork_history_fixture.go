package activityjournal

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// This is already-recorded fork evidence, not a live claim or a new terminal
// transition. In particular, it must not move the captured source frontier.
func InsertHistoricalActivityForkAttemptTx(ctx context.Context, tx *sql.Tx, postgres bool, record pipeline.ActivityAttemptRecord) error {
	payload, err := json.Marshal(record.ResultPayload)
	if err != nil {
		return err
	}
	generation, err := json.Marshal(record.Generation)
	if err != nil {
		return err
	}
	var failure any
	if record.Failure != nil {
		raw, err := json.Marshal(record.Failure)
		if err != nil {
			return err
		}
		failure = string(raw)
	}
	query := `INSERT INTO activity_attempts (
		request_event_id,run_id,execution_mode,source_event_id,entity_id,flow_instance,node_id,handler_event_key,
		activity_id,tool,attempt,status,success_event,failure_event,result_event_id,result_event_type,
		result_payload,failure,input_hash,loop_generation,loop_stage,started_at,completed_at,updated_at)
		VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24)`
	if postgres {
		query = `INSERT INTO activity_attempts (
			request_event_id,run_id,execution_mode,source_event_id,entity_id,flow_instance,node_id,handler_event_key,
			activity_id,tool,attempt,status,success_event,failure_event,result_event_id,result_event_type,
			result_payload,failure,input_hash,loop_generation,loop_stage,started_at,completed_at,updated_at)
			VALUES ($1::uuid,$2::uuid,$3,$4::uuid,$5::uuid,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::uuid,$16,$17::jsonb,$18::jsonb,$19,$20::jsonb,$21,$22,$23,$24)`
	}
	_, err = tx.ExecContext(ctx, query,
		record.RequestEventID, record.RunID, record.ExecutionMode, record.SourceEventID, record.EntityID, record.FlowInstance, record.NodeID, record.HandlerEventKey,
		record.ActivityID, record.Tool, record.Attempt, record.Status, record.SuccessEvent, record.FailureEvent, record.ResultEventID, record.ResultEventType,
		string(payload), failure, record.InputHash, string(generation), record.LoopStage, record.StartedAt, *record.CompletedAt, record.UpdatedAt)
	return err
}
