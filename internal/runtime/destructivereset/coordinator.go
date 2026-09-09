package destructivereset

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
	req, err := req.normalize(c.now())
	if err != nil {
		return ExecutionResult{}, err
	}
	if err := c.validate(!req.DryRun); err != nil {
		return ExecutionResult{}, err
	}
	lease, err := c.acquire(ctx)
	if err != nil {
		return ExecutionResult{}, err
	}
	defer func() { retErr = errors.Join(retErr, lease.Release(context.WithoutCancel(ctx))) }()

	var operation Operation
	if !req.DryRun {
		previous, err := c.Operations.LookupResetOperation(ctx, req)
		if err != nil {
			return ExecutionResult{}, err
		}
		if previous != nil {
			operation = *previous
		} else {
			req.SourceProjections, err = c.RuntimeContexts.ResetSourceProjections(ctx)
			if err != nil {
				return ExecutionResult{}, err
			}
			operation, err = c.Operations.AdmitResetOperation(ctx, req)
			if err != nil {
				return ExecutionResult{}, err
			}
		}
		if err := operation.Validate(); err != nil {
			return ExecutionResult{}, err
		}
		if operation.Response != nil {
			return operation.Outcome()
		}
		req = operation.Request
	}
	return c.continueOperation(ctx, req, operation)
}

// RecoverPending settles unfinished reset obligations independently of historical
// response replay. The startup caller must hold retained process authority and
// invoke this before source ingestion, topology installation, or execution admission.
func (c *Coordinator) RecoverPending(ctx context.Context) (recovery *PendingRecovery, retErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.validate(true); err != nil {
		return nil, err
	}
	lease, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if recovery == nil {
			retErr = errors.Join(retErr, lease.Release(context.WithoutCancel(ctx)))
		}
	}()
	pending, err := c.Operations.PendingResetOperations(ctx)
	if err != nil {
		return nil, err
	}
	if len(pending) > 1 {
		return nil, errors.New("multiple active destructive reset operations")
	}
	for _, operation := range pending {
		if err := operation.Validate(); err != nil {
			return nil, err
		}
		if operation.Phase == PhaseCompleted {
			return nil, errors.New("pending reset enumeration returned a completed operation")
		}
		runtimeReset, err := c.RuntimeContexts.BeginDestructiveReset(ctx, operation.Request.OperationID)
		if err != nil {
			return nil, err
		}
		if runtimeReset == nil {
			return nil, errors.New("reset lifecycle returned no operation")
		}
		current, out, err := c.continueEffects(ctx, operation.Request, operation)
		if err != nil {
			runtimeReset.Release()
			return nil, err
		}
		if current.Phase != PhaseContainersSettled {
			runtimeReset.Release()
			return nil, fmt.Errorf("%w: reset %s remains %s", ErrOperationInProgress, current.Request.OperationID, current.Phase)
		}
		if err := runtimeReset.SettleResources(ctx); err != nil {
			runtimeReset.Release()
			return nil, err
		}
		return &PendingRecovery{coordinator: c, operation: current, outcome: out, runtime: runtimeReset, lease: lease}, nil
	}
	return nil, nil
}

func (c *Coordinator) validate(apply bool) error {
	if c == nil || c.Planner == nil {
		return ErrPlannerNotConfigured
	}
	if c.Locks == nil {
		return ErrLockNotConfigured
	}
	if c.Quiescer == nil {
		return errors.New("destructive reset quiescer is required")
	}
	if c.Cleaner == nil {
		return errors.New("destructive reset cleaner is required")
	}
	if c.Containers == nil {
		return errors.New("destructive reset container stopper is required")
	}
	if apply && (c.Operations == nil || c.RuntimeContexts == nil) {
		return errors.New("destructive reset requires durable operation and runtime lifecycle owners")
	}
	return nil
}

func (c *Coordinator) acquire(ctx context.Context) (LockLease, error) {
	lease, acquired, err := c.Locks.AcquireDestructiveReset(ctx)
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, ErrOperationInProgress
	}
	if lease == nil {
		return nil, ErrLockLeaseMissing
	}
	return lease, nil
}

func (c *Coordinator) continueOperation(ctx context.Context, req Request, operation Operation) (ExecutionResult, error) {
	var runtimeReset RuntimeReset
	if !req.DryRun {
		var err error
		runtimeReset, err = c.RuntimeContexts.BeginDestructiveReset(ctx, req.OperationID)
		if err != nil {
			return ExecutionResult{}, err
		}
		if runtimeReset == nil {
			return ExecutionResult{}, errors.New("reset lifecycle returned no operation")
		}
		defer runtimeReset.Release()
	}
	operation, out, err := c.continueEffects(ctx, req, operation)
	if err != nil || req.DryRun {
		return out, err
	}
	if operation.Phase != PhaseContainersSettled {
		return operation.Outcome()
	}
	if err := runtimeReset.SettleResources(ctx); err != nil {
		return ExecutionResult{}, err
	}
	return c.completeOperation(ctx, operation, out, runtimeReset)
}

func (c *Coordinator) continueEffects(ctx context.Context, req Request, operation Operation) (Operation, ExecutionResult, error) {
	var result Result
	if operation.Plan != nil {
		result = *operation.Plan
	} else {
		// Include work persisted or containers created while predecessors drain.
		plan, err := c.Planner.BuildPlan(ctx, req)
		if err != nil {
			return operation, ExecutionResult{}, err
		}
		result = Result{
			OperationName: DefaultOperationName, DryRun: req.DryRun,
			IncludeSourceArtifacts: req.IncludeSourceArtifacts,
			PlannedAt:              req.RequestedAt, Plan: plan,
		}
		if !req.DryRun {
			next := operation
			next.Phase, next.Revision, next.Plan = PhasePlanned, operation.Revision+1, &result
			if err := c.advanceOperation(ctx, operation, next); err != nil {
				return operation, ExecutionResult{}, err
			}
			operation = next
		}
	}

	var quiescence QuiescenceResult
	if operation.Quiescence != nil {
		quiescence = *operation.Quiescence
	} else {
		var err error
		quiescence, err = c.Quiescer.Apply(ctx, QuiescenceRequest{
			OperationID: req.OperationID, Result: result,
			ActorTokenID: req.ActorTokenID, RequestedAt: req.RequestedAt,
		})
		if !req.DryRun {
			operation, err = c.readEffectReceipt(ctx, operation, PhaseQuiesced, err)
			if err != nil {
				return operation, ExecutionResult{}, err
			}
			quiescence = *operation.Quiescence
		} else if err != nil {
			return operation, ExecutionResult{}, err
		}
	}
	var cleanup CleanupResult
	if operation.Cleanup != nil {
		cleanup = *operation.Cleanup
	} else {
		var err error
		cleanup, err = c.Cleaner.Apply(ctx, CleanupRequest{
			OperationID: req.OperationID, Result: result, Quiescence: quiescence,
			ActorTokenID: req.ActorTokenID, RequestedAt: c.now(),
		})
		if !req.DryRun {
			operation, err = c.readEffectReceipt(ctx, operation, PhaseCleanupCommitted, err)
			if err != nil {
				return operation, ExecutionResult{}, err
			}
			cleanup = *operation.Cleanup
		} else if err != nil {
			return operation, ExecutionResult{}, err
		}
	}

	var containers ContainerResetResult
	if operation.Containers != nil {
		containers = *operation.Containers
	} else {
		var err error
		containers, err = c.Containers.Apply(ctx, ContainerResetRequest{
			Result: result, Cleanup: cleanup, ActorTokenID: req.ActorTokenID, RequestedAt: req.RequestedAt,
		})
		if err != nil {
			return operation, ExecutionResult{}, err
		}
	}
	out := ExecutionResult{Plan: result, Quiescence: quiescence, Cleanup: cleanup, Containers: containers}
	if req.DryRun {
		return operation, out, nil
	}
	if len(containers.Failed) != 0 {
		if operation.Response == nil {
			next := operation
			next.Revision++
			next.Response = &out
			if err := c.advanceOperation(ctx, operation, next); err != nil {
				return operation, ExecutionResult{}, err
			}
			operation = next
		}
		out, err := operation.Outcome()
		return operation, out, err
	}
	if operation.Containers == nil {
		next := operation
		next.Phase, next.Revision, next.Containers = PhaseContainersSettled, operation.Revision+1, &containers
		if err := c.advanceOperation(ctx, operation, next); err != nil {
			return operation, ExecutionResult{}, err
		}
		operation = next
	}
	return operation, out, nil
}

func (c *Coordinator) completeOperation(ctx context.Context, operation Operation, out ExecutionResult, runtimeReset RuntimeReset) (ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, err
	}
	if err := runtimeReset.Complete(ctx, !operation.Request.IncludeSourceArtifacts); err != nil {
		return ExecutionResult{}, err
	}
	// Reconstruction appends durable allocation intents without changing the
	// immutable request or effect receipts. Finalize that exact current revision.
	current, err := c.Operations.ReadResetOperation(ctx, operation.Request.OperationID)
	if err != nil {
		return ExecutionResult{}, err
	}
	if err := validateReconstructionEvidence(operation, current); err != nil {
		return ExecutionResult{}, err
	}
	operation = current
	next := operation
	next.Phase, next.Revision = PhaseCompleted, operation.Revision+1
	if next.Response == nil {
		next.Response = &out
	}
	if err := c.advanceOperation(ctx, operation, next); err != nil {
		return ExecutionResult{}, err
	}
	return next.Outcome()
}

// A returned commit error is not a rollback witness. Only the exact committed
// receipt permits progress; missing or unreadable evidence leaves execution fenced.
func (c *Coordinator) readEffectReceipt(ctx context.Context, before Operation, phase OperationPhase, applyErr error) (Operation, error) {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	op, err := c.Operations.ReadResetOperation(readCtx, before.Request.OperationID)
	if err != nil {
		return Operation{}, errors.Join(applyErr, err)
	}
	if err := ValidateOperationTransition(before, op); err != nil {
		return Operation{}, errors.Join(applyErr, fmt.Errorf("reset %s has no exact atomic %s receipt: %w", before.Request.OperationID, phase, err))
	}
	if op.Phase != phase {
		return Operation{}, errors.Join(applyErr, fmt.Errorf("reset %s has no atomic %s receipt", before.Request.OperationID, phase))
	}
	return op, nil
}

func (c *Coordinator) advanceOperation(ctx context.Context, before, after Operation) error {
	if err := ValidateOperationTransition(before, after); err != nil {
		return err
	}
	if err := c.Operations.AdvanceResetOperation(ctx, before, after); err != nil {
		readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		current, readErr := c.Operations.ReadResetOperation(readCtx, before.Request.OperationID)
		if readErr != nil || !reflect.DeepEqual(current, after) {
			return errors.Join(err, readErr)
		}
	}
	return nil
}

func (c *Coordinator) now() time.Time {
	if c != nil && c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}
