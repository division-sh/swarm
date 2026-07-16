package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

func (s *PostgresStore) ClaimSchedule(ctx context.Context, sc runtimepipeline.Schedule) (bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return false, err
	}
	if caps.Schedules != SchemaFlavorCanonical {
		return false, unsupportedSchemaCapability("timers/schedules", caps.Schedules)
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return false, fmt.Errorf("agent_id and event_type are required")
	}
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	sc.NormalizeTimerID()
	key := scheduleClaimLockKey(sc)

	s.scheduleClaimMu.Lock()
	defer s.scheduleClaimMu.Unlock()

	if _, ok := s.scheduleClaimKeys[key]; ok {
		conn := s.scheduleClaimConn
		if conn == nil {
			delete(s.scheduleClaimKeys, key)
		} else if strings.TrimSpace(sc.RunID) != "" {
			if err := storerunlifecycle.RequireActive(ctx, conn, sc.RunID, storerunlifecycle.DialectPostgres); err != nil {
				if !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
					return false, err
				}
				if _, unlockErr := conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, key); unlockErr != nil {
					_ = s.closeScheduleClaimConnLocked()
					return false, fmt.Errorf("release terminal-run schedule ownership: %w", unlockErr)
				}
				delete(s.scheduleClaimKeys, key)
				if len(s.scheduleClaimKeys) == 0 {
					if closeErr := s.closeScheduleClaimConnLocked(); closeErr != nil {
						return false, closeErr
					}
				}
				return false, nil
			}
		}
		if conn != nil {
			active, err := scheduleActiveOnConn(ctx, conn, sc)
			if err != nil {
				return false, err
			}
			if active {
				return true, nil
			}
			if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, key); err != nil {
				return false, fmt.Errorf("release inactive schedule ownership: %w", err)
			}
			delete(s.scheduleClaimKeys, key)
			if len(s.scheduleClaimKeys) == 0 {
				if err := s.closeScheduleClaimConnLocked(); err != nil {
					return false, err
				}
			}
		}
	}
	conn, err := s.ensureScheduleClaimConnLocked(ctx)
	if err != nil {
		return false, err
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, key).Scan(&acquired); err != nil {
		return false, fmt.Errorf("claim schedule ownership: %w", err)
	}
	if !acquired {
		return false, nil
	}
	if strings.TrimSpace(sc.RunID) != "" {
		if err := storerunlifecycle.RequireActive(ctx, conn, sc.RunID, storerunlifecycle.DialectPostgres); err != nil {
			_, _ = conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, key)
			if errors.Is(err, storerunlifecycle.ErrRunNotActive) {
				return false, nil
			}
			return false, err
		}
	}
	active, err := scheduleActiveOnConn(ctx, conn, sc)
	if err != nil {
		if _, unlockErr := conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, key); unlockErr != nil {
			_ = s.closeScheduleClaimConnLocked()
		}
		return false, err
	}
	if !active {
		if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, key); err != nil {
			_ = s.closeScheduleClaimConnLocked()
			return false, fmt.Errorf("release inactive schedule claim: %w", err)
		}
		return false, nil
	}
	if s.scheduleClaimKeys == nil {
		s.scheduleClaimKeys = map[string]struct{}{}
	}
	s.scheduleClaimKeys[key] = struct{}{}
	return true, nil
}

func (s *PostgresStore) ReleaseSchedule(ctx context.Context, sc runtimepipeline.Schedule) error {
	if s == nil || s.DB == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	sc.NormalizeTimerID()
	key := scheduleClaimLockKey(sc)

	s.scheduleClaimMu.Lock()
	defer s.scheduleClaimMu.Unlock()

	if _, ok := s.scheduleClaimKeys[key]; !ok {
		return nil
	}
	if s.scheduleClaimConn == nil {
		delete(s.scheduleClaimKeys, key)
		return nil
	}
	if _, err := s.scheduleClaimConn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, key); err != nil {
		return fmt.Errorf("release schedule ownership: %w", err)
	}
	delete(s.scheduleClaimKeys, key)
	if len(s.scheduleClaimKeys) == 0 {
		return s.closeScheduleClaimConnLocked()
	}
	return nil
}

func (s *PostgresStore) CancelScheduleExactTerminal(ctx context.Context, sc runtimepipeline.Schedule) error {
	if sc.EffectiveTimerID() != "" || timeridentity.IsWorkflowTimerActivationTaskID(sc.TaskID) {
		return fmt.Errorf("workflow timer cancellation must be owned by WorkflowTimerLifecycle")
	}
	if _, ok := timeridentity.ParseWorkflowTimerOccurrenceTaskID(sc.TaskID); ok {
		return fmt.Errorf("workflow timer occurrence cancellation must be owned by WorkflowTimerLifecycle")
	}
	return s.applyScheduleTerminalTransition(ctx, sc, s.cancelScheduleExactSpec, true)
}

func (s *PostgresStore) CompleteScheduleFireExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	if sc.EffectiveTimerID() != "" {
		return fmt.Errorf("workflow timer completion must be owned by WorkflowTimerLifecycle")
	}
	recurring, err := s.persistedScheduleRecurring(ctx, sc)
	if err != nil {
		return err
	}
	release := !recurring
	return s.applyScheduleTerminalTransition(ctx, sc, s.MarkScheduleFiredExact, release)
}

func (s *PostgresStore) ReleaseScheduleClaims(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.scheduleClaimMu.Lock()
	defer s.scheduleClaimMu.Unlock()
	return s.closeScheduleClaimConnLocked()
}

func (s *PostgresStore) ensureScheduleClaimConnLocked(ctx context.Context) (*sql.Conn, error) {
	if s.scheduleClaimConn != nil {
		return s.scheduleClaimConn, nil
	}
	conn, err := s.DB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open schedule ownership connection: %w", err)
	}
	s.scheduleClaimConn = conn
	return conn, nil
}

func (s *PostgresStore) closeScheduleClaimConnLocked() error {
	conn := s.scheduleClaimConn
	s.scheduleClaimConn = nil
	s.scheduleClaimKeys = nil
	if conn == nil {
		return nil
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("close schedule ownership connection: %w", err)
	}
	return nil
}

func (s *PostgresStore) applyScheduleTerminalTransition(
	ctx context.Context,
	sc runtimepipeline.Schedule,
	transition func(context.Context, runtimepipeline.Schedule) error,
	release bool,
) error {
	if err := transition(ctx, sc); err != nil {
		return err
	}
	if !release {
		return nil
	}
	if _, activeTx := runtimepipeline.PipelineSQLTxFromContext(ctx); activeTx {
		postCommitCtx := runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(ctx))
		if !runtimepipeline.QueuePipelinePostCommitAction(ctx, func() {
			_ = s.ReleaseSchedule(postCommitCtx, sc)
		}) {
			return fmt.Errorf("schedule claim release requires post-commit ownership")
		}
		return nil
	}
	if err := s.ReleaseSchedule(ctx, sc); err != nil {
		return &runtimepipeline.ScheduleTerminalError{
			Stage:             "release_claim",
			TransitionApplied: true,
			Err:               err,
		}
	}
	return nil
}

func scheduleActiveOnConn(ctx context.Context, conn *sql.Conn, sc runtimepipeline.Schedule) (bool, error) {
	var active bool
	if sc.EffectiveTimerID() != "" {
		occurrence, ok := timeridentity.ParseWorkflowTimerOccurrenceTaskID(sc.TaskID)
		if !ok || occurrence.Activation.ActivationID != sc.EffectiveTimerID() {
			return false, fmt.Errorf("workflow timer claim identity is invalid")
		}
		err := conn.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM timers t
				LEFT JOIN runs run ON run.run_id = t.run_id
				WHERE t.timer_id = $1::uuid
				  AND t.timer_name = $2
				  AND t.run_id = $3::uuid
				  AND t.owner_agent = $4
				  AND t.fire_event = $5
				  AND t.entity_id = $6::uuid
				  AND t.flow_instance = $7
				  AND t.fire_at = $8
				  AND t.status = 'active'
				  AND run.status IN ('running', 'paused')
			)
		`, sc.TimerID, occurrence.Activation.TaskID(), sc.RunID, sc.AgentID, sc.EventType,
			sc.EntityID, sc.FlowInstance, occurrence.DueAt).Scan(&active)
		if err != nil {
			return false, fmt.Errorf("check active workflow timer ownership target: %w", err)
		}
		return active, nil
	}
	err := conn.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM timers t
			LEFT JOIN runs run ON run.run_id = t.run_id
			WHERE t.run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
			  AND t.owner_agent = $2
			  AND t.fire_event = $3
			  AND t.entity_id IS NOT DISTINCT FROM NULLIF($4,'')::uuid
			  AND t.flow_instance IS NOT DISTINCT FROM NULLIF($5,'')
			  AND COALESCE(t.fire_payload->>'__schedule_task_id', '') = $6
			  AND t.timer_name NOT LIKE $7
			  AND t.status = 'active'
			  AND (t.run_id IS NULL OR run.status IN ('running', 'paused'))
		)
	`), sc.RunID, sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID), timeridentity.WorkflowTimerActivationTaskPrefix()+"%").Scan(&active)
	if err != nil {
		return false, fmt.Errorf("check active schedule ownership target: %w", err)
	}
	return active, nil
}

func (s *PostgresStore) persistedScheduleRecurring(ctx context.Context, sc runtimepipeline.Schedule) (bool, error) {
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	queryer := scheduleRecurringQueryer(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		queryer = tx
	}
	var recurring bool
	err := queryer.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT recurring
		FROM timers
		WHERE run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
		  AND owner_agent = $2
		  AND fire_event = $3
		  AND entity_id IS NOT DISTINCT FROM NULLIF($4,'')::uuid
		  AND flow_instance IS NOT DISTINCT FROM NULLIF($5,'')
		  AND %s = $6
		  AND timer_name NOT LIKE $7
		  AND status = 'active'
		ORDER BY created_at DESC
		LIMIT 1
	`, exactScheduleTaskIDSQL()), sc.RunID, sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID), timeridentity.WorkflowTimerActivationTaskPrefix()+"%").Scan(&recurring)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("schedule completion target is missing")
	}
	if err != nil {
		return false, fmt.Errorf("load schedule recurrence: %w", err)
	}
	return recurring, nil
}

type scheduleRecurringQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
