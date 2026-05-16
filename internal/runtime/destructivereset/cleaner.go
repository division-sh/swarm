package destructivereset

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type Cleaner struct {
	Store CleanupStore
	Now   func() time.Time
}

func (c Cleaner) Apply(ctx context.Context, req CleanupRequest) (CleanupResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req.ActorTokenID = strings.TrimSpace(req.ActorTokenID)
	if req.ActorTokenID == "" {
		return CleanupResult{}, fmt.Errorf("%w: actor token id is required", ErrInvalidRequest)
	}
	if strings.TrimSpace(req.Result.OperationName) == "" {
		req.Result.OperationName = DefaultOperationName
	}
	if req.Result.PlannedAt.IsZero() {
		return CleanupResult{}, fmt.Errorf("%w: destructive reset plan result is required", ErrInvalidRequest)
	}
	if !req.Result.DryRun {
		if req.Quiescence.AppliedAt.IsZero() {
			return CleanupResult{}, fmt.Errorf("%w: destructive reset quiescence result is required", ErrInvalidRequest)
		}
		if req.Quiescence.DryRun {
			return CleanupResult{}, fmt.Errorf("%w: destructive reset cleanup requires applied quiescence", ErrInvalidRequest)
		}
	}
	if req.RequestedAt.IsZero() {
		req.RequestedAt = c.now()
	}
	req.RequestedAt = req.RequestedAt.UTC()
	if c.Store == nil {
		return CleanupResult{}, fmt.Errorf("destructive reset cleanup store is not configured")
	}
	result, err := c.Store.ApplyDestructiveResetCleanup(ctx, req)
	if err != nil {
		return CleanupResult{}, err
	}
	return copyCleanupResult(result), nil
}

func (c Cleaner) now() time.Time {
	if c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}

func copyCleanupResult(result CleanupResult) CleanupResult {
	result.RunIDs = append([]string(nil), result.RunIDs...)
	result.Tables = append([]CleanupTableResult(nil), result.Tables...)
	return result
}
