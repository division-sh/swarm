package destructivereset

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Coordinator struct {
	Planner         Planner
	Locks           LockManager
	Quiescer        QuiescenceApplier
	Cleaner         CleanupApplier
	Containers      ContainerStopper
	RuntimeContexts RuntimeContextLifecycle
	Operations      OperationStore
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
	if !req.DryRun && (c.Operations == nil || c.RuntimeContexts == nil) {
		return ExecutionResult{}, errors.New("destructive reset requires durable operation and runtime lifecycle owners")
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

	var operation Operation
	var runtimeReset RuntimeReset
	if !req.DryRun {
		operation, err = c.Operations.AdmitResetOperation(ctx, req)
		if err != nil {
			return ExecutionResult{}, err
		}
		if operation.Response != nil {
			return operation.Outcome()
		}
		if operation.Phase != PhaseAdmitted {
			return ExecutionResult{}, fmt.Errorf("%w: %s requires recovery from %s", ErrOperationInProgress, operation.Request.OperationID, operation.Phase)
		}
		req = operation.Request
		runtimeReset, err = c.RuntimeContexts.BeginDestructiveReset(ctx)
		if err != nil {
			return ExecutionResult{}, err
		}
		if runtimeReset == nil {
			return ExecutionResult{}, errors.New("reset lifecycle returned no operation")
		}
		defer runtimeReset.Release()
	}
	// Inventory must include persistence and container creation performed by
	// already-admitted work while the predecessor set is draining.
	plan, err := c.Planner.BuildPlan(ctx, req)
	if err != nil {
		if runtimeReset != nil {
			err = errors.Join(err, runtimeReset.Complete(context.WithoutCancel(ctx), true))
		}
		return ExecutionResult{}, err
	}
	result := Result{
		OperationName:          DefaultOperationName,
		DryRun:                 req.DryRun,
		IncludeSourceArtifacts: req.IncludeSourceArtifacts,
		PlannedAt:              req.RequestedAt,
		Plan:                   plan,
	}
	if !req.DryRun {
		next := operation
		next.Phase, next.Revision, next.Plan = PhasePlanned, operation.Revision+1, &result
		if err := c.Operations.AdvanceResetOperation(ctx, operation, next); err != nil {
			return ExecutionResult{}, err
		}
		operation = next
	}
	quiescence, err := c.Quiescer.Apply(ctx, QuiescenceRequest{
		OperationID:  req.OperationID,
		Result:       result,
		ActorTokenID: req.ActorTokenID,
		RequestedAt:  req.RequestedAt,
	})
	if err != nil {
		if runtimeReset != nil {
			err = errors.Join(err, runtimeReset.Complete(context.WithoutCancel(ctx), true))
		}
		return ExecutionResult{}, err
	}
	if !req.DryRun {
		operation, err = c.readEffectReceipt(ctx, req.OperationID, PhaseQuiesced)
		if err != nil {
			return ExecutionResult{}, err
		}
		quiescence = *operation.Quiescence
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
	if !req.DryRun {
		operation, err = c.readEffectReceipt(ctx, req.OperationID, PhaseCleanupCommitted)
		if err != nil {
			return ExecutionResult{}, err
		}
		cleanup = *operation.Cleanup
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
	out = ExecutionResult{Plan: result, Quiescence: quiescence, Cleanup: cleanup, Containers: containers}
	if !req.DryRun && len(containers.Failed) != 0 {
		next := operation
		next.Revision++
		next.Response = &out
		if err := c.Operations.AdvanceResetOperation(ctx, operation, next); err != nil {
			return ExecutionResult{}, err
		}
	}
	if runtimeReset != nil && len(containers.Failed) == 0 {
		next := operation
		next.Phase, next.Revision, next.Containers = PhaseContainersSettled, operation.Revision+1, &containers
		if err := c.Operations.AdvanceResetOperation(ctx, operation, next); err != nil {
			return ExecutionResult{}, err
		}
		operation = next
		if err := runtimeReset.Complete(ctx, !req.IncludeSourceArtifacts); err != nil {
			return ExecutionResult{}, err
		}
		next = operation
		next.Phase, next.Revision = PhaseCompleted, operation.Revision+1
		next.Response = &out
		if err := c.Operations.AdvanceResetOperation(ctx, operation, next); err != nil {
			return ExecutionResult{}, err
		}
	}
	return out, nil
}

func (c *Coordinator) readEffectReceipt(ctx context.Context, id string, phase OperationPhase) (Operation, error) {
	op, err := c.Operations.ReadResetOperation(ctx, id)
	if err != nil {
		return Operation{}, err
	}
	if err := op.Validate(); err != nil {
		return Operation{}, err
	}
	if op.Phase != phase || op.Request.OperationID != id {
		return Operation{}, fmt.Errorf("reset %s has no atomic %s receipt", id, phase)
	}
	return op, nil
}

func (c *Coordinator) now() time.Time {
	if c != nil && c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}
