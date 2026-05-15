package destructivereset

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type Quiescer struct {
	Store QuiescenceStore
	Now   func() time.Time
}

func (q Quiescer) Apply(ctx context.Context, req QuiescenceRequest) (QuiescenceResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req.ActorTokenID = strings.TrimSpace(req.ActorTokenID)
	if req.ActorTokenID == "" {
		return QuiescenceResult{}, fmt.Errorf("%w: actor token id is required", ErrInvalidRequest)
	}
	if strings.TrimSpace(req.Result.OperationName) == "" {
		req.Result.OperationName = DefaultOperationName
	}
	if req.Result.PlannedAt.IsZero() {
		return QuiescenceResult{}, fmt.Errorf("%w: destructive reset plan result is required", ErrInvalidRequest)
	}
	if req.RequestedAt.IsZero() {
		req.RequestedAt = q.now()
	}
	req.RequestedAt = req.RequestedAt.UTC()
	if q.Store == nil {
		return QuiescenceResult{}, fmt.Errorf("destructive reset quiescence store is not configured")
	}
	result, err := q.Store.ApplyDestructiveResetQuiescence(ctx, req)
	if err != nil {
		return QuiescenceResult{}, err
	}
	return copyQuiescenceResult(result), nil
}

func (q Quiescer) now() time.Time {
	if q.Now != nil {
		return q.Now().UTC()
	}
	return time.Now().UTC()
}

func copyQuiescenceResult(result QuiescenceResult) QuiescenceResult {
	result.Runs = append([]QuiescedRun(nil), result.Runs...)
	result.Deliveries = append([]QuiescedDelivery(nil), result.Deliveries...)
	return result
}
