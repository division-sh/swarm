package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimepipeline "swarm/internal/runtime/pipeline"
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
	key := scheduleClaimLockKey(sc)

	s.scheduleClaimMu.Lock()
	defer s.scheduleClaimMu.Unlock()

	if _, ok := s.scheduleClaimKeys[key]; ok {
		return true, nil
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
	return s.applyScheduleTerminalTransition(ctx, sc, s.CancelScheduleExact, true)
}

func (s *PostgresStore) CompleteScheduleFireExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	release := strings.EqualFold(strings.TrimSpace(sc.Mode), "once")
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
	err := conn.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM timers
			WHERE run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
			  AND owner_agent = $2
			  AND fire_event = $3
			  AND entity_id IS NOT DISTINCT FROM NULLIF($4,'')::uuid
			  AND flow_instance IS NOT DISTINCT FROM NULLIF($5,'')
			  AND %s = $6
			  AND status = 'active'
		)
	`, exactScheduleTaskIDSQL()), sc.RunID, sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID)).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("check active schedule ownership target: %w", err)
	}
	return active, nil
}
