package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimepipeline "empireai/internal/runtime/pipeline"
)

func (s *PostgresStore) UpsertSchedule(ctx context.Context, sc runtimepipeline.Schedule) error {
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	if strings.TrimSpace(sc.Mode) == "" {
		sc.Mode = "once"
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schedule tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
		UPDATE schedules
		SET active = false,
		    cancelled_at = now()
		WHERE agent_id = $1
		  AND event_type = $2
		  AND active = true
	`, sc.AgentID, sc.EventType); err != nil {
		return fmt.Errorf("deactivate previous schedule: %w", err)
	}

	var atTime any
	var nextFire any
	if !sc.At.IsZero() {
		atTime = sc.At
		nextFire = sc.At
	}
	payload := sc.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO schedules (
			agent_id, vertical_id, event_type, mode, cron_expr,
			at_time, next_fire_at, payload, active, created_at
		)
		VALUES (
			$1, NULLIF($2,'')::uuid, $3, $4, NULLIF($5,''),
			$6, $7, $8::jsonb, true, now()
		)
	`, sc.AgentID, sc.VerticalID, sc.EventType, sc.Mode, sc.Cron, atTime, nextFire, string(payload)); err != nil {
		return fmt.Errorf("insert schedule: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schedule tx: %w", err)
	}
	return nil
}

func (s *PostgresStore) CancelSchedule(ctx context.Context, agentID, eventType string) error {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	_, err := s.DB.ExecContext(ctx, `
		UPDATE schedules
		SET active = false,
		    cancelled_at = now()
		WHERE agent_id = $1
		  AND event_type = $2
		  AND active = true
	`, agentID, eventType)
	if err != nil {
		return fmt.Errorf("cancel schedule: %w", err)
	}
	return nil
}

func (s *PostgresStore) LoadActiveSchedules(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			agent_id,
			event_type,
			mode,
			COALESCE(cron_expr, ''),
			at_time,
			COALESCE(vertical_id::text, ''),
			payload
		FROM schedules
		WHERE active = true
	`)
	if err != nil {
		return nil, fmt.Errorf("query active schedules: %w", err)
	}
	defer rows.Close()

	out := make([]runtimepipeline.Schedule, 0)
	for rows.Next() {
		var sc runtimepipeline.Schedule
		var at sql.NullTime
		if err := rows.Scan(
			&sc.AgentID,
			&sc.EventType,
			&sc.Mode,
			&sc.Cron,
			&at,
			&sc.VerticalID,
			&sc.Payload,
		); err != nil {
			return nil, fmt.Errorf("scan active schedule: %w", err)
		}
		if at.Valid {
			sc.At = at.Time
		}
		out = append(out, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active schedules: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) MarkScheduleFired(ctx context.Context, sc runtimepipeline.Schedule) error {
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return nil
	}
	if sc.Mode == "once" {
		_, err := s.DB.ExecContext(ctx, `
			UPDATE schedules
			SET active = false,
			    last_fired_at = now(),
			    next_fire_at = NULL
			WHERE agent_id = $1
			  AND event_type = $2
			  AND active = true
		`, sc.AgentID, sc.EventType)
		if err != nil {
			return fmt.Errorf("mark once schedule fired: %w", err)
		}
		return nil
	}
	_, err := s.DB.ExecContext(ctx, `
		UPDATE schedules
		SET last_fired_at = now()
		WHERE agent_id = $1
		  AND event_type = $2
		  AND active = true
	`, sc.AgentID, sc.EventType)
	if err != nil {
		return fmt.Errorf("mark recurring schedule fired: %w", err)
	}
	return nil
}
