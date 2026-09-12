package manager

import (
	"context"
	"errors"
	"fmt"
)

// RunExecutionOwnership is an observation, not permission retained across an
// enqueue or mutation. Consumers must recheck through their generation owner.
type RunExecutionOwnership uint8

const (
	RunExecutionOwned RunExecutionOwnership = iota + 1
	RunExecutionForeign
	// RunExecutionOtherNormalSource is not execution permission. Only the
	// complete-source-set owner may consider its existing terminal reconciliation.
	RunExecutionOtherNormalSource
)

type RunExecutionOwner interface {
	InspectRunExecutionOwnership(context.Context, string) (RunExecutionOwnership, error)
}

var ErrRunExecutionNotOwned = errors.New("run execution is not owned by this runtime generation")

func (am *AgentManager) inspectRunExecutionOwnership(ctx context.Context, runID string) (RunExecutionOwnership, error) {
	if am == nil || am.lifecycle == nil {
		return 0, errors.New("run execution admission requires a lifecycle owner")
	}
	return inspectRunExecutionOwnership(ctx, am.lifecycle.persistence(), runID)
}

func inspectRunExecutionOwnership(ctx context.Context, persistence AgentLifecyclePersistence, runID string) (RunExecutionOwnership, error) {
	owner, ok := persistence.(RunExecutionOwner)
	if !ok {
		return 0, errors.New("run execution admission requires a generation grant")
	}
	result, err := owner.InspectRunExecutionOwnership(ctx, runID)
	if err != nil {
		return 0, err
	}
	if result != RunExecutionOwned && result != RunExecutionForeign && result != RunExecutionOtherNormalSource {
		return 0, errors.New("run execution ownership returned an invalid disposition")
	}
	return result, nil
}

func (am *AgentManager) requireRunExecutionOwnership(ctx context.Context, runID string) error {
	result, err := am.inspectRunExecutionOwnership(ctx, runID)
	if err != nil {
		return err
	}
	if result != RunExecutionOwned {
		return fmt.Errorf("%w: %s", ErrRunExecutionNotOwned, runID)
	}
	return nil
}

func (c *agentLifecycleCoordinator) requireRunExecutionOwnership(ctx context.Context, runID string) error {
	result, err := inspectRunExecutionOwnership(ctx, c.persistence(), runID)
	if err != nil {
		return err
	}
	if result != RunExecutionOwned {
		return fmt.Errorf("%w: %s", ErrRunExecutionNotOwned, runID)
	}
	return nil
}
