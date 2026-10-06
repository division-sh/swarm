package pipeline

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/effects"
)

func (pc *PipelineCoordinator) BindTurnCancellationDispatcher(owner effects.TurnCancellationDispatcher) error {
	if pc == nil || owner == nil {
		return fmt.Errorf("turn cancellation dispatch requires its owned runtime")
	}
	pc.turnCancellationMu.Lock()
	defer pc.turnCancellationMu.Unlock()
	if pc.turnCancellations != nil {
		return fmt.Errorf("turn cancellation dispatcher is already bound")
	}
	pc.turnCancellations = owner
	return nil
}

func (pc *PipelineCoordinator) dispatchCommittedTurnCancellations(ctx context.Context, intents []effects.TurnCancellation) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("dispatch committed turn cancellation panic: %v", recovered)
		}
	}()
	if len(intents) == 0 {
		return nil
	}
	pc.turnCancellationMu.Lock()
	owner := pc.turnCancellations
	pc.turnCancellationMu.Unlock()
	if owner == nil {
		return fmt.Errorf("committed turn termination lacks its runtime dispatcher")
	}
	return owner.ApplyCommittedTurnCancellations(ctx, intents)
}
