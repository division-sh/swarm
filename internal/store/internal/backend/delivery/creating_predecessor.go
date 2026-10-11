package delivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// The constructor's first authored turn precedes later turns from its sender
// to that concrete receiver. ACK and source_event_id are not turn settlement.
func (a *Adapter) creatingPredecessorReady(ctx context.Context, q queryer, record deliveryRecord) (bool, error) {
	if !record.Route.Target.ExistingEntity() || record.Status != deliverylifecycle.StatusPending || record.ClaimVersion != 0 {
		// This is first-claim ordering, not materialization or a lease/retry fence.
		return true, nil
	}
	if !record.Route.Initialization.Empty() && record.Route.Initialization.CreatingEventID() == "" {
		// The initialization owner validates explicit no-creating-publication
		// evidence, including historical fixed cuts. Never invent or look up a
		// native creating event for that dependency.
		return true, nil
	}
	tx, ok := q.(*sql.Tx)
	if !ok || a.receiverTarget == nil {
		return false, fmt.Errorf("creating-turn admission requires canonical transactional construction ownership")
	}
	var creatingEventID string
	var err error
	if record.Authority.Kind() == deliverylifecycle.ExecutionAuthoritySelectedContractFork {
		current, fenceErr := a.selectedExecutionCurrentRead(ctx, tx, record.Authority)
		if fenceErr != nil {
			return false, fenceErr
		}
		if !current || record.Authority.ForkRunID() != record.RunID {
			return false, fmt.Errorf("selected creating-turn admission requires its current exact execution fence")
		}
		creatingEventID, err = a.receiverTarget.SelectedReceiverCreatingEventTx(ctx, tx, record.Authority, record.Route)
	} else {
		creatingEventID, err = a.receiverTarget.ReceiverCreatingEventTx(ctx, tx, record.RunID, record.Route)
	}
	if err != nil {
		return false, err
	}
	if creatingEventID == "" || creatingEventID == record.EventID {
		return true, nil
	}
	ids, err := a.unsettledCreatingTurns(ctx, q, record.RunID, creatingEventID)
	if err != nil {
		return false, err
	}
	for _, id := range ids {
		predecessor, err := a.loadByID(ctx, q, id, false)
		if err != nil {
			return false, err
		}
		if predecessor.Status.Terminal() || !events.SameRouteIdentity(predecessor.Route.Target.Route(), record.Route.Target.Route()) {
			continue
		}
		creating, err := a.materializationEvent(ctx, q, creatingEventID)
		if err != nil {
			return false, err
		}
		incoming, err := a.materializationEvent(ctx, q, record.EventID)
		if err != nil {
			return false, err
		}
		return !creating.Producer().Equal(incoming.Producer()), nil
	}
	// Static/no-input construction and historical recovery without a creating
	// delivery have no unfinished turn. Terminal failure/cancellation releases it.
	return true, nil
}

func (a *Adapter) unsettledCreatingTurns(ctx context.Context, q queryer, runID, eventID string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT CAST(delivery_id AS TEXT) FROM event_deliveries
		WHERE run_id=$1 AND event_id=$2
		AND status NOT IN ('delivered','dead_letter','canceled') ORDER BY delivery_id`, runID, eventID)
	if err != nil {
		return nil, fmt.Errorf("read durable creating-turn outcomes: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	readErr, closeErr := rows.Err(), rows.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return ids, nil
}
