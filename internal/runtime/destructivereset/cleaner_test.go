package destructivereset

import (
	"context"
	"testing"
	"time"
)

func TestCleanerRequiresAppliedQuiescenceForMutation(t *testing.T) {
	now := time.Date(2026, 5, 16, 18, 40, 0, 0, time.UTC)
	cleaner := Cleaner{
		Store: cleanupStoreFunc(func(context.Context, CleanupRequest) (CleanupResult, error) {
			t.Fatal("store should not be called without applied quiescence")
			return CleanupResult{}, nil
		}),
		Now: func() time.Time { return now },
	}
	_, err := cleaner.Apply(context.Background(), CleanupRequest{
		ActorTokenID: "operator-token",
		Result: Result{
			OperationName: DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
		},
		Quiescence: QuiescenceResult{DryRun: true, AppliedAt: now.Add(-30 * time.Second)},
	})
	if err == nil {
		t.Fatal("Cleaner.Apply error = nil, want applied quiescence failure")
	}
}

type cleanupStoreFunc func(context.Context, CleanupRequest) (CleanupResult, error)

func (f cleanupStoreFunc) ApplyDestructiveResetCleanup(ctx context.Context, req CleanupRequest) (CleanupResult, error) {
	return f(ctx, req)
}
