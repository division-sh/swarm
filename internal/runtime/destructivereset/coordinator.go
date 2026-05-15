package destructivereset

import (
	"context"
	"fmt"
	"time"
)

type Coordinator struct {
	Planner     Planner
	Locks       LockManager
	Idempotency IdempotencyStore
	Now         func() time.Time
	Operation   string
	LockKey     string
}

func (c *Coordinator) BuildPlan(ctx context.Context, req Request) (Result, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	now := c.now()
	req, err := req.normalize(now)
	if err != nil {
		return Result{}, false, err
	}
	if c == nil {
		return Result{}, false, ErrPlannerNotConfigured
	}
	if c.Planner == nil {
		return Result{}, false, ErrPlannerNotConfigured
	}

	operation := c.operationName()
	idemKey := IdempotencyKey{
		OperationName:  operation,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: req.IdempotencyKey,
	}.normalized()
	if idemKey.IdempotencyKey != "" {
		if c.Idempotency == nil {
			return Result{}, false, fmt.Errorf("destructive reset idempotency store is required")
		}
		stored, ok, err := c.Idempotency.LoadResetResult(ctx, idemKey)
		if err != nil {
			return Result{}, false, err
		}
		if ok {
			if stored.RequestHash != req.RequestHash {
				return Result{}, false, &IdempotencyConflictError{
					Key:                    idemKey,
					OriginalRequestHash:    stored.RequestHash,
					ConflictingRequestHash: req.RequestHash,
				}
			}
			return stored.Result, true, nil
		}
	}

	if c.Locks == nil {
		return Result{}, false, ErrLockNotConfigured
	}
	lease, acquired, err := c.Locks.TryAcquire(ctx, c.lockKey())
	if err != nil {
		return Result{}, false, err
	}
	if !acquired {
		return Result{}, false, ErrOperationInProgress
	}
	if lease == nil {
		return Result{}, false, ErrLockLeaseMissing
	}
	defer func() {
		_ = lease.Release(context.Background())
	}()

	plan, err := c.Planner.BuildPlan(ctx, req)
	if err != nil {
		return Result{}, false, err
	}
	result := Result{
		OperationName: operation,
		DryRun:        req.DryRun,
		PlannedAt:     req.RequestedAt,
		Plan:          plan,
	}
	if idemKey.IdempotencyKey != "" {
		if err := c.Idempotency.StoreResetResult(ctx, StoredResult{
			Key:         idemKey,
			RequestHash: req.RequestHash,
			Result:      result,
			StoredAt:    now,
		}); err != nil {
			return Result{}, false, err
		}
	}
	return result, false, nil
}

func (c *Coordinator) now() time.Time {
	if c != nil && c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}

func (c *Coordinator) operationName() string {
	if c == nil || c.Operation == "" {
		return DefaultOperationName
	}
	return c.Operation
}

func (c *Coordinator) lockKey() string {
	if c == nil || c.LockKey == "" {
		return defaultLockKey
	}
	return c.LockKey
}
