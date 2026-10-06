package delivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

type ClaimedFlowTurn struct {
	Snapshot deliverylifecycle.Snapshot
	Claim    deliverylifecycle.Claim
}

func claimedAgentFlowOrigins(ctx context.Context, tx *sql.Tx, adapter *Adapter, owner flowidentity.RunScopedFlowInstance) ([]ClaimedFlowTurn, error) {
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	query := `SELECT CAST(delivery_id AS TEXT) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND status='in_progress'
		AND agent_route_presence='present' AND agent_flow_scope_key=$2 AND agent_flow_instance_path=$3 ORDER BY delivery_id`
	args := []any{owner.RunID, owner.Route.ScopeKey, owner.Route.InstancePath}
	if owner.Route.ScopeKey == "." {
		if owner.Route.InstanceID != owner.RunID || owner.Route.InstancePath != owner.RunID {
			return nil, fmt.Errorf("root cancellation requires its canonical run-root owner")
		}
		query = `SELECT CAST(delivery_id AS TEXT) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND status='in_progress' AND agent_route_presence='root' ORDER BY delivery_id`
		args = []any{owner.RunID}
	}
	if adapter.dialect == DialectPostgres {
		query += ` FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	result := make([]ClaimedFlowTurn, 0, len(ids))
	for _, id := range ids {
		record, err := adapter.loadByID(ctx, tx, id, true)
		if err != nil {
			return nil, err
		}
		claim, err := deliverylifecycle.AdmitPersistedClaim(id, record.RunID, events.EncodeDeliveryRouteIdentity(record.RouteIdentity), record.claimToken, record.ClaimVersion, record.SubscriberClass, record.SubscriberID)
		if err != nil {
			return nil, err
		}
		result = append(result, ClaimedFlowTurn{Snapshot: record.Snapshot, Claim: claim})
	}
	return result, nil
}

func (s *DeliveryPostgresOwner) ClaimedAgentFlowOriginsTx(ctx context.Context, tx *sql.Tx, owner flowidentity.RunScopedFlowInstance) ([]ClaimedFlowTurn, error) {
	return claimedAgentFlowOrigins(ctx, tx, postgresDeliveryAdapter, owner)
}

func (s *DeliverySQLiteOwner) ClaimedAgentFlowOriginsTx(ctx context.Context, tx *sql.Tx, owner flowidentity.RunScopedFlowInstance) ([]ClaimedFlowTurn, error) {
	return claimedAgentFlowOrigins(ctx, tx, sqliteDeliveryAdapter, owner)
}

func validateUnstartedClaimOwner(ctx context.Context, tx *sql.Tx, adapter *Adapter, claim deliverylifecycle.Claim, owner flowidentity.RunScopedFlowInstance, agentID string) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	record, err := adapter.loadByID(ctx, tx, claim.DeliveryID(), false)
	if err != nil {
		return err
	}
	if record.SubscriberClass != deliverylifecycle.SubscriberAgent || record.SubscriberID != agentID || claim.SubscriberID() != agentID || record.RunID != owner.RunID || !owner.MatchesAgentRoute(record.Route.AgentIdentity) {
		return fmt.Errorf("unstarted origin contradicts its recorded constructed owner")
	}
	_, err = adapter.readProviderOriginRecoveryDisposition(ctx, tx, claim, false)
	return err
}

func (s *DeliveryPostgresOwner) ValidateUnstartedClaimOwnerTx(ctx context.Context, tx *sql.Tx, claim deliverylifecycle.Claim, owner flowidentity.RunScopedFlowInstance, agentID string) error {
	return validateUnstartedClaimOwner(ctx, tx, postgresDeliveryAdapter, claim, owner, agentID)
}

func (s *DeliverySQLiteOwner) ValidateUnstartedClaimOwnerTx(ctx context.Context, tx *sql.Tx, claim deliverylifecycle.Claim, owner flowidentity.RunScopedFlowInstance, agentID string) error {
	return validateUnstartedClaimOwner(ctx, tx, sqliteDeliveryAdapter, claim, owner, agentID)
}

func queuedAgentFlowSnapshots(ctx context.Context, tx *sql.Tx, adapter *Adapter, owner flowidentity.RunScopedFlowInstance) (result []deliverylifecycle.Snapshot, err error) {
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	query := `SELECT CAST(delivery_id AS TEXT) FROM event_deliveries
		WHERE run_id=$1 AND subscriber_type='agent' AND status IN ('pending','failed')
		AND agent_route_presence='present' AND agent_flow_scope_key=$2 AND agent_flow_instance_path=$3
		ORDER BY delivery_id`
	args := []any{owner.RunID, owner.Route.ScopeKey, owner.Route.InstancePath}
	if owner.Route.ScopeKey == "." {
		if owner.Route.InstanceID != owner.RunID || owner.Route.InstancePath != owner.RunID {
			return nil, fmt.Errorf("root cancellation requires its canonical run-root owner")
		}
		query = `SELECT CAST(delivery_id AS TEXT) FROM event_deliveries
			WHERE run_id=$1 AND subscriber_type='agent' AND status IN ('pending','failed')
			AND agent_route_presence='root' ORDER BY delivery_id`
		args = []any{owner.RunID}
	}
	if adapter.dialect == DialectPostgres {
		query += ` FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	for _, id := range ids {
		record, err := adapter.loadByID(ctx, tx, id, true)
		if err != nil {
			return nil, err
		}
		result = append(result, record.Snapshot)
	}
	return result, nil
}

func cancelQueuedAgent(ctx context.Context, mutation *mutationprotocol.Attempt, adapter *Adapter, expected deliverylifecycle.Snapshot) (deliverylifecycle.Snapshot, error) {
	return withDeliverySQL(ctx, mutation, func(ctx context.Context, tx *sql.Tx) (deliverylifecycle.Snapshot, error) {
		record, err := adapter.loadByID(ctx, tx, expected.DeliveryID, true)
		if err != nil {
			return deliverylifecycle.Snapshot{}, err
		}
		if record.SubscriberClass != deliverylifecycle.SubscriberAgent || record.RunID != expected.RunID || record.RouteIdentity != expected.RouteIdentity || record.ClaimVersion != expected.ClaimVersion || record.Status != expected.Status || (record.Status != deliverylifecycle.StatusPending && record.Status != deliverylifecycle.StatusFailed) {
			return deliverylifecycle.Snapshot{}, fmt.Errorf("%w: queued cancellation lost its exact unclaimed origin", deliverylifecycle.ErrConflict)
		}
		now, err := adapter.databaseNow(ctx, tx)
		if err != nil {
			return deliverylifecycle.Snapshot{}, err
		}
		updated, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET status='canceled',reason_code='terminate',failure=NULL,
			next_eligible_at=NULL,settled_at=$1,updated_at=$1
			WHERE delivery_id=$2 AND claim_version=$3 AND status=$4 AND current_attempt_version IS NULL AND current_attempt_open IS NULL`,
			now, record.DeliveryID, record.ClaimVersion, string(record.Status))
		if err != nil {
			return deliverylifecycle.Snapshot{}, err
		}
		count, err := updated.RowsAffected()
		if err != nil || count != 1 {
			return deliverylifecycle.Snapshot{}, errors.Join(err, fmt.Errorf("%w: queued cancellation lost admission race", deliverylifecycle.ErrConflict))
		}
		if err := mutation.AddFact(record.RunID, privaterunforkrevision.FamilyEventDeliveries, record.DeliveryID); err != nil {
			return deliverylifecycle.Snapshot{}, err
		}
		record, err = adapter.loadByID(ctx, tx, record.DeliveryID, false)
		if err != nil {
			return deliverylifecycle.Snapshot{}, err
		}
		if err := adapter.recordTransition(ctx, mutation, record, "canceled", nil, now); err != nil {
			return deliverylifecycle.Snapshot{}, err
		}
		return snapshotAt(record, now), nil
	})
}

func (s *DeliveryPostgresOwner) QueuedAgentFlowSnapshotsTx(ctx context.Context, tx *sql.Tx, owner flowidentity.RunScopedFlowInstance) ([]deliverylifecycle.Snapshot, error) {
	return queuedAgentFlowSnapshots(ctx, tx, postgresDeliveryAdapter, owner)
}

func (s *DeliverySQLiteOwner) QueuedAgentFlowSnapshotsTx(ctx context.Context, tx *sql.Tx, owner flowidentity.RunScopedFlowInstance) ([]deliverylifecycle.Snapshot, error) {
	return queuedAgentFlowSnapshots(ctx, tx, sqliteDeliveryAdapter, owner)
}

func (s *DeliveryPostgresOwner) CancelQueuedAgentTx(ctx context.Context, mutation *mutationprotocol.Attempt, expected deliverylifecycle.Snapshot) (deliverylifecycle.Snapshot, error) {
	return cancelQueuedAgent(ctx, mutation, postgresDeliveryAdapter, expected)
}

func (s *DeliverySQLiteOwner) CancelQueuedAgentTx(ctx context.Context, mutation *mutationprotocol.Attempt, expected deliverylifecycle.Snapshot) (deliverylifecycle.Snapshot, error) {
	return cancelQueuedAgent(ctx, mutation, sqliteDeliveryAdapter, expected)
}
