package destructivereset

import (
	"context"
	"errors"
	"time"
)

type Coordinator struct {
	Planner         Planner
	Locks           LockManager
	Quiescer        QuiescenceApplier
	Cleaner         CleanupApplier
	Containers      ContainerStopper
	RuntimeContexts RuntimeContextQuiescer
	Now             func() time.Time
}

func (c *Coordinator) Execute(ctx context.Context, req Request) (out ExecutionResult, retErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.Planner == nil {
		return ExecutionResult{}, ErrPlannerNotConfigured
	}
	now := c.now()
	req, err := req.normalize(now)
	if err != nil {
		return ExecutionResult{}, err
	}
	if c.Locks == nil {
		return ExecutionResult{}, ErrLockNotConfigured
	}
	if c.Quiescer == nil {
		return ExecutionResult{}, errors.New("destructive reset quiescer is required")
	}
	if c.Cleaner == nil {
		return ExecutionResult{}, errors.New("destructive reset cleaner is required")
	}
	if c.Containers == nil {
		return ExecutionResult{}, errors.New("destructive reset container stopper is required")
	}

	lease, acquired, err := c.Locks.AcquireDestructiveReset(ctx)
	if err != nil {
		return ExecutionResult{}, err
	}
	if !acquired {
		return ExecutionResult{}, ErrOperationInProgress
	}
	if lease == nil {
		return ExecutionResult{}, ErrLockLeaseMissing
	}
	defer func() {
		retErr = errors.Join(retErr, lease.Release(context.WithoutCancel(ctx)))
	}()

	plan, err := c.Planner.BuildPlan(ctx, req)
	if err != nil {
		return ExecutionResult{}, err
	}
	result := Result{
		OperationName:  DefaultOperationName,
		DryRun:         req.DryRun,
		IncludeBundles: req.IncludeBundles,
		PlannedAt:      req.RequestedAt,
		Plan:           plan,
	}
	if !req.DryRun && c.RuntimeContexts != nil {
		if err := c.RuntimeContexts.QuiesceAllRuntimeContexts(ctx); err != nil {
			return ExecutionResult{}, err
		}
	}
	quiescence, err := c.Quiescer.Apply(ctx, QuiescenceRequest{
		Result:       result,
		ActorTokenID: req.ActorTokenID,
		RequestedAt:  req.RequestedAt,
	})
	if err != nil {
		return ExecutionResult{}, err
	}
	cleanup, err := c.Cleaner.Apply(ctx, CleanupRequest{
		OperationID:  req.OperationID,
		Result:       result,
		Quiescence:   quiescence,
		ActorTokenID: req.ActorTokenID,
		RequestedAt:  req.RequestedAt,
	})
	if err != nil {
		return ExecutionResult{}, err
	}
	containers, err := c.Containers.Apply(ctx, ContainerResetRequest{
		Result:       result,
		Cleanup:      cleanup,
		ActorTokenID: req.ActorTokenID,
		RequestedAt:  req.RequestedAt,
	})
	if err != nil {
		return ExecutionResult{}, err
	}
	return ExecutionResult{
		Plan:       result,
		Quiescence: quiescence,
		Cleanup:    cleanup,
		Containers: containers,
	}, nil
}

func (c *Coordinator) now() time.Time {
	if c != nil && c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}
