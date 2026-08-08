package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

// postgresScheduleClaimOwner owns the dedicated PostgreSQL advisory-lock
// session used by schedule claims. It is separate from the selected-store
// facade and has one terminal release path.
type postgresScheduleClaimOwner struct {
	mu   sync.Mutex
	conn *sql.Conn
	keys map[string]struct{}
}

func (s *PostgresStore) ClaimSchedule(ctx context.Context, sc runtimepipeline.Schedule) (bool, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return false, fmt.Errorf("agent_id and event_type are required")
	}
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	if err := sc.NormalizeOwner(); err != nil {
		return false, err
	}
	var err error
	sc, err = sc.AdmitEventIdentity()
	if err != nil {
		return false, err
	}
	key := scheduleClaimLockKey(sc)

	s.scheduleClaims.mu.Lock()
	defer s.scheduleClaims.mu.Unlock()

	if _, ok := s.scheduleClaims.keys[key]; ok {
		conn := s.scheduleClaims.conn
		if conn == nil {
			delete(s.scheduleClaims.keys, key)
		} else if strings.TrimSpace(sc.RunID) != "" {
			if err := requirePostgresRunActiveQuery(ctx, conn, sc.RunID); err != nil {
				if !errors.Is(err, runtimerunlifecycle.ErrRunNotActive) {
					return false, err
				}
				if _, unlockErr := conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, key); unlockErr != nil {
					_ = s.closeScheduleClaimConnLocked()
					return false, fmt.Errorf("release terminal-run schedule ownership: %w", unlockErr)
				}
				delete(s.scheduleClaims.keys, key)
				if len(s.scheduleClaims.keys) == 0 {
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
			delete(s.scheduleClaims.keys, key)
			if len(s.scheduleClaims.keys) == 0 {
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
		if err := requirePostgresRunActiveQuery(ctx, conn, sc.RunID); err != nil {
			_, _ = conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, key)
			if errors.Is(err, runtimerunlifecycle.ErrRunNotActive) {
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
	if s.scheduleClaims.keys == nil {
		s.scheduleClaims.keys = map[string]struct{}{}
	}
	s.scheduleClaims.keys[key] = struct{}{}
	return true, nil
}

func (s *PostgresStore) ReleaseSchedule(ctx context.Context, sc runtimepipeline.Schedule) error {
	if s == nil || s.backend == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	if err := sc.NormalizeOwner(); err != nil {
		return err
	}
	var err error
	sc, err = sc.AdmitEventIdentity()
	if err != nil {
		return err
	}
	key := scheduleClaimLockKey(sc)

	s.scheduleClaims.mu.Lock()
	defer s.scheduleClaims.mu.Unlock()

	if _, ok := s.scheduleClaims.keys[key]; !ok {
		return nil
	}
	if s.scheduleClaims.conn == nil {
		delete(s.scheduleClaims.keys, key)
		return nil
	}
	if _, err := s.scheduleClaims.conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, key); err != nil {
		return fmt.Errorf("release schedule ownership: %w", err)
	}
	delete(s.scheduleClaims.keys, key)
	if len(s.scheduleClaims.keys) == 0 {
		return s.closeScheduleClaimConnLocked()
	}
	return nil
}

func (s *PostgresStore) CancelScheduleExactTerminal(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.applyScheduleTerminalTransition(ctx, sc, s.cancelScheduleExactSpec, true)
}

func (s *PostgresStore) CompleteScheduleFireExact(ctx context.Context, sc runtimepipeline.Schedule) error {
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
	s.scheduleClaims.mu.Lock()
	defer s.scheduleClaims.mu.Unlock()
	return s.closeScheduleClaimConnLocked()
}

func (s *PostgresStore) ensureScheduleClaimConnLocked(ctx context.Context) (*sql.Conn, error) {
	if s.scheduleClaims.conn != nil {
		return s.scheduleClaims.conn, nil
	}
	conn, err := s.backend.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open schedule ownership connection: %w", err)
	}
	s.scheduleClaims.conn = conn
	return conn, nil
}

func (s *PostgresStore) closeScheduleClaimConnLocked() error {
	conn := s.scheduleClaims.conn
	s.scheduleClaims.conn = nil
	s.scheduleClaims.keys = nil
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
	identityFields, err := scheduleAgentIdentityFields(sc)
	if err != nil {
		return false, err
	}
	var active bool
	err = conn.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM timers t
			LEFT JOIN runs run ON run.run_id = t.run_id
			WHERE t.run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
			  AND t.owner_agent = $2
			  AND t.owner_kind = $7
			  AND t.agent_name_owner IS NOT DISTINCT FROM NULLIF($8, '')
			  AND t.agent_name_source IS NOT DISTINCT FROM NULLIF($9, '')
			  AND t.agent_route_presence IS NOT DISTINCT FROM NULLIF($10, '')
			  AND t.agent_flow_scope_key IS NOT DISTINCT FROM NULLIF($11, '')
			  AND t.agent_flow_instance_id IS NOT DISTINCT FROM NULLIF($12, '')
			  AND t.fire_event = $3
			  AND t.entity_id IS NOT DISTINCT FROM NULLIF($4,'')::uuid
			  AND t.flow_instance IS NOT DISTINCT FROM NULLIF($5,'')
			  AND COALESCE(t.fire_payload->>'__schedule_task_id', '') = $6
			  AND t.task_type IN ('timer', 'scheduled_task', 'deadline', 'global_recurring')
			  AND t.status = 'active'
			  AND (t.run_id IS NULL OR run.status IN (`+runLifecycleActiveStateSQLValues+`))
		)
	`), sc.RunID, sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID),
		sc.OwnerKind, identityFields.NameOwner, identityFields.NameSource, identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID).Scan(&active)
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
	if err := sc.NormalizeOwner(); err != nil {
		return false, err
	}
	var err error
	sc, err = sc.AdmitEventIdentity()
	if err != nil {
		return false, err
	}
	identityFields, err := scheduleAgentIdentityFields(sc)
	if err != nil {
		return false, err
	}
	var recurring bool
	err = s.backend.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT recurring
		FROM timers
		WHERE run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
		  AND owner_agent = $2
		  AND owner_kind = $7
		  AND agent_name_owner IS NOT DISTINCT FROM NULLIF($8, '')
		  AND agent_name_source IS NOT DISTINCT FROM NULLIF($9, '')
		  AND agent_route_presence IS NOT DISTINCT FROM NULLIF($10, '')
		  AND agent_flow_scope_key IS NOT DISTINCT FROM NULLIF($11, '')
		  AND agent_flow_instance_id IS NOT DISTINCT FROM NULLIF($12, '')
		  AND fire_event = $3
		  AND entity_id IS NOT DISTINCT FROM NULLIF($4,'')::uuid
		  AND flow_instance IS NOT DISTINCT FROM NULLIF($5,'')
		  AND %s = $6
		  AND task_type IN ('timer', 'scheduled_task', 'deadline', 'global_recurring')
		  AND status = 'active'
		ORDER BY created_at DESC
		LIMIT 1
	`, exactScheduleTaskIDSQL()), sc.RunID, sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID),
		sc.OwnerKind, identityFields.NameOwner, identityFields.NameSource, identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID).Scan(&recurring)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("schedule completion target is missing")
	}
	if err != nil {
		return false, fmt.Errorf("load schedule recurrence: %w", err)
	}
	return recurring, nil
}
