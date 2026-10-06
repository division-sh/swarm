package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type canceledTurnRecoveryRow struct {
	turnID, admittedAttempt, runID, agentID, flow string
	firstAttempt                                  sql.NullString
	bound                                         sql.NullInt64
	emit, timeoutEvent                            sql.NullString
	launched, requested                           any
	reason, cause                                 string
}

func listCanceledTurnRecoveries(ctx context.Context, tx *sql.Tx, postgres bool, request runtimeeffects.RecoveryRequest) ([]runtimeeffects.TurnExecutionResult, error) {
	// One bounded snapshot includes only canceled origins whose entire physical
	// set is closed. Selected-fork possession remains with its separate owner.
	rows, err := tx.QueryContext(ctx, `SELECT CAST(t.turn_id AS TEXT),CAST(t.admitted_attempt_id AS TEXT),CAST(t.first_attempt_id AS TEXT),CAST(t.run_id AS TEXT),t.agent_id,t.flow_instance,
		t.bound_ns,t.bound_emit,CAST(t.timeout_event_id AS TEXT),t.first_launched_at,t.cancel_reason,CAST(t.cancel_cause_event_id AS TEXT),t.cancel_requested_at
		FROM runtime_agent_turn_lifetimes t
		LEFT JOIN runtime_external_effect_attempts admitted ON admitted.attempt_id=t.admitted_attempt_id
		LEFT JOIN runtime_external_effect_operations o ON o.operation_id=admitted.operation_id
		WHERE t.cancel_reason IS NOT NULL AND t.settled_at IS NULL AND (o.authority_kind='normal_agent' OR o.operation_id IS NULL)
		  AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts a
		    WHERE ((t.origin_kind='delivery' AND a.origin_delivery_id=t.origin_id) OR
		      (t.origin_kind='directive' AND a.origin_directive_operation_id=t.origin_id))
		      AND a.state IN ('authorized','launched','response_observed'))
		ORDER BY t.cancel_requested_at,t.turn_id LIMIT 128`)
	if err != nil {
		return nil, err
	}
	var pending []canceledTurnRecoveryRow
	for rows.Next() {
		var row canceledTurnRecoveryRow
		if err := rows.Scan(&row.turnID, &row.admittedAttempt, &row.firstAttempt, &row.runID, &row.agentID, &row.flow, &row.bound, &row.emit, &row.timeoutEvent, &row.launched, &row.reason, &row.cause, &row.requested); err != nil {
			_ = rows.Close()
			return nil, err
		}
		pending = append(pending, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	turns := make([]runtimeeffects.TurnExecutionResult, 0, len(pending))
	for _, row := range pending {
		admitted, err := loadCanceledFirstAttempt(ctx, tx, postgres, row.admittedAttempt, request.Now())
		if err != nil {
			return nil, err
		}
		attempt := admitted
		if row.firstAttempt.Valid && row.firstAttempt.String != row.admittedAttempt {
			attempt, err = loadCanceledFirstAttempt(ctx, tx, postgres, row.firstAttempt.String, request.Now())
			if err != nil {
				return nil, err
			}
			if !attempt.Origin.Same(admitted.Origin) || attempt.Authority.Target.RunID != admitted.Authority.Target.RunID ||
				attempt.Authority.Target.AgentID != admitted.Authority.Target.AgentID || attempt.Authority.Target.FlowInstance != admitted.Authority.Target.FlowInstance {
				return nil, fmt.Errorf("canceled turn launch and authorization anchors disagree")
			}
		}
		if err := request.Admit(attempt.Authority.ExecutionMode); err != nil {
			return nil, err
		}
		id, _, err := businessTurnIdentity(attempt.Origin)
		if err != nil || id != row.turnID {
			return nil, fmt.Errorf("canceled turn recovery contradicts its exact first origin")
		}
		owner, err := businessTurnOwner(attempt.Authority)
		if err != nil || owner.RunID != row.runID || attempt.Authority.Target.AgentID != row.agentID || owner.Route.InstancePath != row.flow {
			return nil, fmt.Errorf("canceled turn recovery contradicts its exact first origin")
		}
		clock := runtimeeffects.LogicalTurnClock{Origin: attempt.Origin, FirstAttempt: row.firstAttempt.String, TimeoutEvent: row.timeoutEvent.String}
		launched, hasLaunch, err := sqliteTimeValue(row.launched)
		if err != nil || hasLaunch != row.firstAttempt.Valid {
			return nil, fmt.Errorf("canceled turn recovery has contradictory launch evidence")
		}
		clock.LaunchedAt = launched
		if row.bound.Valid != row.emit.Valid || row.bound.Valid != row.timeoutEvent.Valid {
			return nil, fmt.Errorf("canceled turn recovery has incomplete bound evidence")
		}
		if row.bound.Valid {
			clock.Timeout = &timeridentity.TurnTimeout{After: time.Duration(row.bound.Int64), Emit: row.emit.String}
			clock.DeadlineAt = launched.Add(clock.Timeout.After)
		}
		var ownedClock *runtimeeffects.LogicalTurnClock
		if hasLaunch {
			if err := clock.Validate(); err != nil {
				return nil, err
			}
			ownedClock = &clock
		} else if row.reason != string(deliverylifecycle.CancellationTerminate) {
			return nil, fmt.Errorf("unlaunched canceled turn cannot have a timeout intent")
		}
		requested, valid, err := sqliteTimeValue(row.requested)
		if err != nil || !valid {
			return nil, fmt.Errorf("canceled turn recovery lacks its intent timestamp")
		}
		intent := runtimeeffects.TurnCancellation{Committed: true, Requested: true, Origin: attempt.Origin, Reason: deliverylifecycle.CancellationReason(row.reason), CauseEvent: row.cause, RequestedAt: requested}
		if err := intent.ValidateIntent(); err != nil {
			return nil, err
		}
		// The intent owner admitted the exact deadline. Its canonical timestamp
		// is occurrence evidence, not permission to re-round that deadline.
		if intent.Reason == deliverylifecycle.CancellationTurnTimeout && (ownedClock == nil || clock.Timeout == nil || intent.CauseEvent != clock.TimeoutEvent || requested.Before(clock.LaunchedAt)) {
			return nil, fmt.Errorf("canceled turn recovery contradicts its exact timeout")
		}
		turns = append(turns, runtimeeffects.TurnExecutionResult{Attempt: attempt, Cancellation: intent, Clock: ownedClock})
	}
	return turns, nil
}

func loadCanceledFirstAttempt(ctx context.Context, tx *sql.Tx, postgres bool, first string, now time.Time) (runtimeeffects.Attempt, error) {
	attempt, state, err := loadBusinessTurnFirstAttempt(ctx, tx, postgres, first, now)
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	switch state {
	case runtimeeffects.StateSettled, runtimeeffects.StateTerminalFailure, runtimeeffects.StateOutcomeUncertain:
		return attempt, nil
	default:
		return runtimeeffects.Attempt{}, fmt.Errorf("canceled turn recovery has an invalid physical settlement state")
	}
}

func loadBusinessTurnFirstAttempt(ctx context.Context, tx *sql.Tx, postgres bool, first string, now time.Time) (runtimeeffects.Attempt, runtimeeffects.State, error) {
	lineageRun := `COALESCE(json_extract(o.lineage,'$.run_id'),'')`
	if postgres {
		lineageRun = `COALESCE(o.lineage->>'run_id','')`
	}
	// This is the existing recovery decoder's complete authority projection.
	// No current agent token or latest physical attempt can replace these facts.
	query := `SELECT CAST(o.operation_id AS TEXT),CAST(a.attempt_id AS TEXT),o.authority_kind,o.authority_id,CAST(o.authority_evidence AS TEXT),o.agent_frame_bytes,
		o.execution_mode,a.execution_mode,a.adapter,a.transport,a.state,a.usage_target_kind,CAST(a.usage_target_id AS TEXT),COALESCE(a.target_ordinal,0),
		COALESCE(CAST(a.capability_surface_id AS TEXT),''),COALESCE(CAST(s.surface AS TEXT),''),a.execution_owner,a.fence_generation,a.lease_expires_at,
		COALESCE(a.session_grant_id,''),COALESCE(a.session_lock_owner,''),COALESCE(CAST(o.agent_run_id AS TEXT),''),` + lineageRun + `,
		COALESCE(o.agent_id,''),COALESCE(o.agent_name_owner,''),COALESCE(o.agent_name_source,''),COALESCE(o.agent_route_presence,''),
		COALESCE(o.flow_scope_key,''),COALESCE(o.flow_instance_id,''),COALESCE(o.flow_instance,''),COALESCE(o.runtime_epoch,0),COALESCE(o.generation,0),
		COALESCE(a.origin_kind,''),COALESCE(CAST(a.origin_delivery_id AS TEXT),''),COALESCE(CAST(a.origin_run_id AS TEXT),''),COALESCE(a.origin_route_identity,''),
		COALESCE(CAST(a.origin_claim_token AS TEXT),''),COALESCE(a.origin_claim_version,0),COALESCE(a.origin_subscriber_id,''),
		COALESCE(CAST(a.origin_directive_operation_id AS TEXT),''),COALESCE(a.origin_directive_owner_id,'')
		FROM runtime_external_effect_operations o JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
		LEFT JOIN managed_agent_capability_surfaces s ON s.surface_id=a.capability_surface_id
		WHERE a.attempt_id=$1 AND o.effect_kind='provider_turn' AND o.authority_kind='normal_agent'
		  AND ((a.origin_kind='delivery' AND a.origin_subscriber_type='agent') OR a.origin_kind='directive')`
	rows, err := tx.QueryContext(ctx, query, first)
	if err != nil {
		return runtimeeffects.Attempt{}, "", err
	}
	recovered, err := scanCompletionRecoveryAttempts(rows)
	if err != nil {
		return runtimeeffects.Attempt{}, "", err
	}
	if len(recovered) != 1 || recovered[0].TargetKind != string(runtimeeffects.UsageTargetAgentTurn) {
		return runtimeeffects.Attempt{}, "", fmt.Errorf("business turn lacks its exact first provider attempt")
	}
	switch runtimeeffects.State(recovered[0].State) {
	case runtimeeffects.StateAuthorized, runtimeeffects.StateLaunched, runtimeeffects.StateResponseObserved, runtimeeffects.StateSettled, runtimeeffects.StateTerminalFailure, runtimeeffects.StateOutcomeUncertain:
	default:
		return runtimeeffects.Attempt{}, "", fmt.Errorf("business turn has an invalid physical state")
	}
	attempt, _, err := completionRecoverySettlement(recovered[0], runtimeeffects.StateOutcomeUncertain, nil, now)
	if err == nil && (!attempt.Authority.Normal.Valid() || attempt.Authority.ExecutionOwner == "" || attempt.Authority.FenceGeneration == 0) {
		return runtimeeffects.Attempt{}, "", fmt.Errorf("business turn has incomplete original execution authority")
	}
	return attempt, runtimeeffects.State(recovered[0].State), err
}

func (s *EffectPostgresOwner) ListCanceledTurnRecoveries(ctx context.Context, request runtimeeffects.RecoveryRequest) ([]runtimeeffects.TurnExecutionResult, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if err := s.requireCurrent(); err != nil {
		return nil, err
	}
	var turns []runtimeeffects.TurnExecutionResult
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		turns, err = listCanceledTurnRecoveries(ctx, tx, true, request)
		return err
	})
	if err != nil {
		return nil, err
	}
	return turns, nil
}

func (s *EffectSQLiteOwner) ListCanceledTurnRecoveries(ctx context.Context, request runtimeeffects.RecoveryRequest) ([]runtimeeffects.TurnExecutionResult, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if err := s.requireCurrent(); err != nil {
		return nil, err
	}
	var turns []runtimeeffects.TurnExecutionResult
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		turns, err = listCanceledTurnRecoveries(ctx, tx, false, request)
		return err
	})
	if err != nil {
		return nil, err
	}
	return turns, nil
}
