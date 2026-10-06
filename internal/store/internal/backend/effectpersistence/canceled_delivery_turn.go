package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *EffectPostgresOwner) SettleCanceledDeliveryTurn(ctx context.Context, attempt runtimeeffects.Attempt) (deliverylifecycle.ClaimCommit, error) {
	if err := s.requireCurrent(); err != nil {
		return deliverylifecycle.ClaimCommit{}, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates, func(ctx context.Context, mutation *mutationprotocol.Attempt) (deliverylifecycle.Snapshot, error) {
		return settleCanceledDeliveryTurn(ctx, mutation, true, s.delivery, attempt)
	})
	snapshot, acknowledged := result.Value()
	return deliverylifecycle.ClaimCommit{Snapshot: snapshot, Acknowledged: acknowledged}, result.Err()
}

func (s *EffectSQLiteOwner) SettleCanceledDeliveryTurn(ctx context.Context, attempt runtimeeffects.Attempt) (deliverylifecycle.ClaimCommit, error) {
	if err := s.requireCurrent(); err != nil {
		return deliverylifecycle.ClaimCommit{}, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite settle canceled delivery turn", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates, func(ctx context.Context, mutation *mutationprotocol.Attempt) (deliverylifecycle.Snapshot, error) {
		return settleCanceledDeliveryTurn(ctx, mutation, false, s.delivery, attempt)
	})
	snapshot, acknowledged := result.Value()
	return deliverylifecycle.ClaimCommit{Snapshot: snapshot, Acknowledged: acknowledged}, result.Err()
}

func settleCanceledDeliveryTurn(ctx context.Context, mutation *mutationprotocol.Attempt, postgres bool, delivery providerDrainDeliveryOwner, attempt runtimeeffects.Attempt) (deliverylifecycle.Snapshot, error) {
	if delivery == nil || attempt.Authority.Kind != runtimeeffects.AuthorityNormalAgent ||
		attempt.Kind != runtimeeffects.KindProviderTurn || attempt.Origin.Kind != runtimeeffects.CompletionOriginDelivery || attempt.Origin.Validate() != nil {
		return deliverylifecycle.Snapshot{}, fmt.Errorf("canceled delivery turn requires its exact admitted provider origin")
	}
	var snapshot deliverylifecycle.Snapshot
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		// Lock order matches timeout admission: delivery, physical origin, then
		// logical turn. Completion cannot be overtaken by cancellation.
		if _, err := delivery.ProviderOriginPendingTx(ctx, tx, attempt.Origin.Delivery); err != nil {
			return err
		}
		if err := requireExactLaunchAttempt(ctx, tx, postgres, attempt); err != nil {
			return err
		}
		turnID, _, err := businessTurnIdentity(attempt.Origin)
		if err != nil {
			return err
		}
		query := `SELECT run_id::text,agent_id,flow_instance,cancel_reason,cancel_cause_event_id::text,cancel_requested_at,first_launched_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1::uuid FOR UPDATE`
		if !postgres {
			query = `SELECT run_id,agent_id,flow_instance,cancel_reason,cancel_cause_event_id,cancel_requested_at,first_launched_at FROM runtime_agent_turn_lifetimes WHERE turn_id=?`
		}
		var runID, agentID, flow string
		var reason, cause sql.NullString
		var requested, launched any
		if err := tx.QueryRowContext(ctx, query, turnID).Scan(&runID, &agentID, &flow, &reason, &cause, &requested, &launched); err != nil {
			return err
		}
		if runID != attempt.Authority.Target.RunID || agentID != attempt.Authority.Target.AgentID || flow != attempt.Authority.Target.FlowInstance || !reason.Valid || !cause.Valid || requested == nil {
			return fmt.Errorf("canceled delivery turn lacks exact durable authored intent")
		}
		cancellation, err := deliverylifecycle.ParseCancellationReason(reason.String)
		if err != nil {
			return err
		}
		query = `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE origin_kind='delivery' AND origin_delivery_id=$1::uuid AND state IN ('authorized','launched','response_observed')`
		if !postgres {
			query = `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE origin_kind='delivery' AND origin_delivery_id=? AND state IN ('authorized','launched','response_observed')`
		}
		var pending int
		if err := tx.QueryRowContext(ctx, query, attempt.Origin.Delivery.DeliveryID()).Scan(&pending); err != nil {
			return err
		}
		if pending != 0 {
			return fmt.Errorf("canceled delivery turn still owns %d unsettled provider attempts", pending)
		}
		query = `SELECT clock_timestamp()`
		if !postgres {
			query = `SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')`
		}
		var rawNow any
		if err := tx.QueryRowContext(ctx, query).Scan(&rawNow); err != nil {
			return err
		}
		now, valid, err := sqliteTimeValue(rawNow)
		if err != nil || !valid {
			return fmt.Errorf("canceled delivery settlement has no database time")
		}
		var duration time.Duration
		if first, valid, err := sqliteTimeValue(launched); err != nil {
			return err
		} else if valid && now.After(first) {
			duration = now.Sub(first)
		}
		snapshot, err = delivery.SettleProviderCanceledOriginTx(ctx, mutation, attempt.Origin.Delivery, cancellation, duration)
		if err != nil {
			return err
		}
		query = `UPDATE runtime_agent_turn_lifetimes SET settled_at=$1 WHERE turn_id=$2::uuid AND settled_at IS NULL`
		if !postgres {
			query = `UPDATE runtime_agent_turn_lifetimes SET settled_at=? WHERE turn_id=? AND settled_at IS NULL`
		}
		if _, err := tx.ExecContext(ctx, query, snapshot.SettledAt, turnID); err != nil {
			return err
		}
		// The recorded response remains immutable history, but canceled work
		// cannot remain an executable completion continuation.
		query = `UPDATE runtime_external_effect_attempts SET completion_continuation_active=FALSE WHERE origin_delivery_id=$1::uuid AND completion_continuation_active=TRUE`
		if !postgres {
			query = `UPDATE runtime_external_effect_attempts SET completion_continuation_active=0 WHERE origin_delivery_id=? AND completion_continuation_active=1`
		}
		_, err = tx.ExecContext(ctx, query, attempt.Origin.Delivery.DeliveryID())
		return err
	})
	return snapshot, err
}
