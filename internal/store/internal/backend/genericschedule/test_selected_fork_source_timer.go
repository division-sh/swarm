package genericschedule

import (
	"context"
	"database/sql"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
)

// This source-history fixture deliberately proves refusal of unproven timer
// history. It does not admit a live workflow timer or mint declaration authority.
func SeedSelectedForkSourceTimerTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID string, ref timeridentity.WorkflowTimerActivationRef, at time.Time) error {
	query := `INSERT INTO timers (
		timer_id, run_id, timer_name, entity_id, flow_scope_key, flow_instance_id, flow_instance, fire_event,
		fire_payload, routing_source, execution_mode, fire_at, owner_agent, owner_kind, task_type, status, created_at
	) VALUES ($1, $2, $3, $4, 'flow-a', '1', 'flow-a/1', 'timer.check',
		'{}', json_object('kind','flow_owned_control','route',json_object('flow_id','flow-a','flow_instance','flow-a/1','entity_id',$4)),
		'live', $5, 'test-node', 'system', 'workflow_timer', 'active', $6)`
	if postgres {
		query = `INSERT INTO timers (
			timer_id, run_id, timer_name, entity_id, flow_scope_key, flow_instance_id, flow_instance, fire_event,
			fire_payload, routing_source, execution_mode, fire_at, owner_agent, owner_kind, task_type, status, created_at
		) VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'flow-a', '1', 'flow-a/1', 'timer.check',
			'{}'::jsonb, jsonb_build_object('kind','flow_owned_control','route',jsonb_build_object('flow_id','flow-a','flow_instance','flow-a/1','entity_id',$4::text)),
			'live', $5, 'test-node', 'system', 'workflow_timer', 'active', $6)`
	}
	_, err := tx.ExecContext(ctx, query, ref.ActivationID, runID, ref.TaskID(), entityID, at.Add(time.Hour), at)
	return err
}

func ReadSelectedForkSourceTimerStatusTx(ctx context.Context, tx *sql.Tx, timerID, runID string) (string, error) {
	var status string
	err := tx.QueryRowContext(ctx, `SELECT status FROM timers WHERE timer_id=$1 AND run_id=$2`, timerID, runID).Scan(&status)
	return status, err
}
