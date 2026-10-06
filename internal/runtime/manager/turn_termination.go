package manager

import (
	"context"
	"errors"
	"fmt"

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
