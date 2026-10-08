package manager

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func (am *AgentManager) attachLogicalTurn(ctx context.Context, turn *effects.TurnExecution) error {
	lease, _ := ctx.Value(agentExecutionLeaseContextKey{}).(*agentExecutionLease)
	if lease == nil {
		return nil
	}
	am.lifecycle.mu.Lock()
	defer am.lifecycle.mu.Unlock()
	if lease.turn != nil {
		return fmt.Errorf("execution lease already owns a logical turn")
	}
	lease.turn = turn
	return nil
}

func (am *AgentManager) ApplyCommittedQueuedCancellations(ctx context.Context, snapshots []deliverylifecycle.Snapshot) error {
	for _, snapshot := range snapshots {
		if snapshot.Status != deliverylifecycle.StatusCanceled || snapshot.SubscriberClass != deliverylifecycle.SubscriberAgent || snapshot.ReasonCode != "terminate" {
			return fmt.Errorf("queued cancellation requires exact terminal agent evidence")
		}
		if err := deliverylifecycle.ValidateCanceledSnapshot(snapshot); err != nil {
			return err
		}
	}
	if len(snapshots) > 0 && am.roles.DeliveryRuntime == nil {
		return fmt.Errorf("queued cancellation requires its exact continuation owner")
	}
	var result error
	for _, snapshot := range snapshots {
		result = errors.Join(result, am.roles.DeliveryRuntime.ReleaseDeliveryContinuation(snapshot.DeliveryID))
		am.logDeliveryLifecycle(ctx, snapshot)
	}
	return result
}

func (am *AgentManager) ApplyCommittedTurnCancellations(_ context.Context, intents []effects.TurnCancellation) error {
	for _, intent := range intents {
		if err := intent.ValidateIntent(); err != nil {
			return err
		}
	}
	am.lifecycle.mu.Lock()
	owned := map[*effects.TurnExecution]struct{}{}
	collect := func(execution *agentExecutionProjection) {
		if execution != nil {
			for lease := range execution.leases {
				if lease.turn != nil {
					owned[lease.turn] = struct{}{}
				}
			}
		}
	}
	for _, cell := range am.lifecycle.cells {
		collect(cell.execution)
		if cell.retirement != nil {
			collect(cell.retirement.execution)
		}
		if cell.terminalSet != nil {
			for _, retirement := range cell.terminalSet.retirements {
				collect(retirement.execution)
			}
		}
	}
	turns := make([]*effects.TurnExecution, 0, len(owned))
	for turn := range owned {
		turns = append(turns, turn)
	}
	am.lifecycle.mu.Unlock()
	var result error
	for _, intent := range intents {
		for _, turn := range turns {
			_, err := turn.RequestCancellation(intent)
			result = errors.Join(result, err)
		}
	}
	return result
}
