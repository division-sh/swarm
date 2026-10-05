package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

func businessTurnIdentity(origin runtimeeffects.CompletionOrigin) (string, string, error) {
	if err := origin.Validate(); err != nil {
		return "", "", err
	}
	id := origin.Directive.OperationID
	if origin.Kind == runtimeeffects.CompletionOriginDelivery {
		id = origin.Delivery.DeliveryID()
	}
	if _, err := uuid.Parse(id); err != nil {
		return "", "", fmt.Errorf("business turn origin: %w", err)
	}
	turn := uuid.NewSHA1(uuid.NameSpaceOID, []byte("agent-business-turn:"+string(origin.Kind)+":"+id))
	return turn.String(), id, nil
}

func prepareBusinessTurnTx(ctx context.Context, tx *sql.Tx, postgres bool, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest) error {
	if authority.Kind != runtimeeffects.AuthorityNormalAgent || req.Kind != runtimeeffects.KindProviderTurn {
		if req.TurnTimeout != nil {
			return fmt.Errorf("turn_timeout requires an admitted business completion origin")
		}
		return nil
	}
	turnID, originID, err := businessTurnIdentity(req.Origin)
	if err != nil {
		return err
	}
	var bound, emit, event any
	if timeout := req.TurnTimeout; timeout != nil {
		if err := timeout.Validate(); err != nil {
			return err
		}
		bound, emit = int64(timeout.After), timeout.Emit
		turn, _ := uuid.Parse(turnID)
		event = uuid.NewSHA1(turn, []byte("turn-timeout")).String()
	}
	query := `INSERT INTO runtime_agent_turn_lifetimes
		(turn_id,origin_kind,origin_id,run_id,agent_id,flow_instance,bound_ns,bound_emit,timeout_event_id,created_at)
		VALUES ($1::uuid,$2,$3::uuid,$4::uuid,$5,$6,$7,$8,$9::uuid,$10) ON CONFLICT (origin_kind,origin_id) DO NOTHING`
	if !postgres {
		query = `INSERT INTO runtime_agent_turn_lifetimes
			(turn_id,origin_kind,origin_id,run_id,agent_id,flow_instance,bound_ns,bound_emit,timeout_event_id,created_at)
			VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT (origin_kind,origin_id) DO NOTHING`
	}
	if _, err := tx.ExecContext(ctx, query, turnID, string(req.Origin.Kind), originID, authority.Target.RunID,
		authority.Target.AgentID, authority.Target.FlowInstance, bound, emit, event, req.Now.UTC()); err != nil {
		return fmt.Errorf("admit logical provider turn: %w", err)
	}
	query = `SELECT run_id::text,agent_id,flow_instance,bound_ns,bound_emit,cancel_reason,settled_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1::uuid FOR UPDATE`
	if !postgres {
		query = `SELECT run_id,agent_id,flow_instance,bound_ns,bound_emit,cancel_reason,settled_at FROM runtime_agent_turn_lifetimes WHERE turn_id=?`
	}
	var runID, agentID, flow string
	var storedBound sql.NullInt64
	var storedEmit, canceled sql.NullString
	var settled any
	if err := tx.QueryRowContext(ctx, query, turnID).Scan(&runID, &agentID, &flow, &storedBound, &storedEmit, &canceled, &settled); err != nil {
		return fmt.Errorf("lock logical provider turn: %w", err)
	}
	if runID != authority.Target.RunID || agentID != authority.Target.AgentID || flow != authority.Target.FlowInstance {
		return fmt.Errorf("logical provider turn contradicts its exact origin owner")
	}
	if canceled.Valid || settled != nil {
		return fmt.Errorf("logical provider turn no longer admits provider execution")
	}
	if req.TurnTimeout == nil && (storedBound.Valid || storedEmit.Valid) || req.TurnTimeout != nil &&
		(!storedBound.Valid || !storedEmit.Valid || storedBound.Int64 != int64(req.TurnTimeout.After) || storedEmit.String != req.TurnTimeout.Emit) {
		return fmt.Errorf("logical provider turn bound differs from its immutable admission")
	}
	return nil
}

func launchBusinessTurnTx(ctx context.Context, tx *sql.Tx, postgres bool, attempt runtimeeffects.Attempt, launchedAt time.Time) (*runtimeeffects.LogicalTurnClock, error) {
	if attempt.Authority.Kind != runtimeeffects.AuthorityNormalAgent || attempt.Kind != runtimeeffects.KindProviderTurn {
		return nil, nil
	}
	turnID, _, err := businessTurnIdentity(attempt.Origin)
	if err != nil {
		return nil, err
	}
	query := `SELECT run_id::text,agent_id,flow_instance,bound_ns,bound_emit,first_attempt_id::text,first_launched_at,timeout_event_id::text,cancel_reason,settled_at
		FROM runtime_agent_turn_lifetimes WHERE turn_id=$1::uuid FOR UPDATE`
	if !postgres {
		query = `SELECT run_id,agent_id,flow_instance,bound_ns,bound_emit,first_attempt_id,first_launched_at,timeout_event_id,cancel_reason,settled_at
			FROM runtime_agent_turn_lifetimes WHERE turn_id=?`
	}
	var bound sql.NullInt64
	var runID, agentID, flow string
	var emit, first, event, canceled sql.NullString
	var launched, settled any
	if err := tx.QueryRowContext(ctx, query, turnID).Scan(&runID, &agentID, &flow, &bound, &emit, &first, &launched, &event, &canceled, &settled); err != nil {
		return nil, fmt.Errorf("lock logical turn launch: %w", err)
	}
	if runID != attempt.Authority.Target.RunID || agentID != attempt.Authority.Target.AgentID || flow != attempt.Authority.Target.FlowInstance {
		return nil, fmt.Errorf("logical turn launch contradicts its exact origin owner")
	}
	if canceled.Valid || settled != nil {
		return nil, fmt.Errorf("logical provider turn is canceled or settled")
	}
	clock := &runtimeeffects.LogicalTurnClock{Origin: attempt.Origin, FirstAttempt: first.String, TimeoutEvent: event.String}
	if bound.Valid != emit.Valid || bound.Valid != event.Valid {
		return nil, fmt.Errorf("logical provider turn has incomplete bound evidence")
	}
	if bound.Valid {
		clock.Timeout = &timeridentity.TurnTimeout{After: time.Duration(bound.Int64), Emit: emit.String}
	}
	if first.Valid {
		var valid bool
		clock.LaunchedAt, valid, err = sqliteTimeValue(launched)
		if err != nil || !valid {
			return nil, fmt.Errorf("logical provider turn first launch is invalid: %v", err)
		}
		if clock.Timeout != nil {
			clock.DeadlineAt = clock.LaunchedAt.Add(clock.Timeout.After)
		}
		return clock, clock.Validate()
	}
	clock.FirstAttempt, clock.LaunchedAt = attempt.AttemptID, launchedAt.UTC()
	if clock.Timeout != nil {
		clock.DeadlineAt = clock.LaunchedAt.Add(clock.Timeout.After)
	}
	if err := clock.Validate(); err != nil {
		return nil, err
	}
	query = `UPDATE runtime_agent_turn_lifetimes SET first_attempt_id=$1::uuid,first_launched_at=$2
		WHERE turn_id=$3::uuid AND first_attempt_id IS NULL AND cancel_reason IS NULL AND settled_at IS NULL`
	if !postgres {
		query = `UPDATE runtime_agent_turn_lifetimes SET first_attempt_id=?,first_launched_at=?
			WHERE turn_id=? AND first_attempt_id IS NULL AND cancel_reason IS NULL AND settled_at IS NULL`
	}
	result, err := tx.ExecContext(ctx, query, clock.FirstAttempt, clock.LaunchedAt, turnID)
	if err != nil {
		return nil, fmt.Errorf("commit first logical turn launch: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return nil, fmt.Errorf("logical provider turn first launch lost its exact fence")
	}
	return clock, nil
}

func requireExactLaunchAttempt(ctx context.Context, tx *sql.Tx, postgres bool, attempt runtimeeffects.Attempt) error {
	query := `SELECT o.effect_kind,o.authority_kind FROM runtime_external_effect_operations o
		JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
		WHERE a.attempt_id=? AND a.operation_id=? AND a.execution_owner=? AND a.fence_generation=?`
	if postgres {
		query = `SELECT o.effect_kind,o.authority_kind FROM runtime_external_effect_operations o
			JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
			WHERE a.attempt_id=$1::uuid AND a.operation_id=$2::uuid AND a.execution_owner=$3 AND a.fence_generation=$4`
	}
	var kind, authority string
	if err := tx.QueryRowContext(ctx, query, attempt.AttemptID, attempt.OperationID, attempt.Authority.ExecutionOwner, attempt.Authority.FenceGeneration).Scan(&kind, &authority); err != nil {
		return fmt.Errorf("load exact launch attempt: %w", err)
	}
	if kind != string(attempt.Kind) || authority != string(attempt.Authority.Kind) {
		return fmt.Errorf("launch attempt contradicts its admitted effect owner")
	}
	if attempt.Kind == runtimeeffects.KindProviderTurn && attempt.Authority.Kind == runtimeeffects.AuthorityNormalAgent {
		_, err := loadProviderAttemptOrigin(ctx, tx, attempt, postgres)
		return err
	}
	return nil
}
