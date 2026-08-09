package runlifecycle

import (
	"context"

	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type CandidateHandoff = storerunhandoff.CandidateHandoff

func ReserveCandidateHandoff(ctx context.Context) (*CandidateHandoff, error) {
	return storerunhandoff.ReserveCandidateHandoff(ctx)
}

func WithCandidateHandoff(ctx context.Context, fn func(*CandidateHandoff) error) error {
	return storerunhandoff.WithCandidateHandoff(ctx, fn)
}

func WithCandidateHandoffResult[T any](ctx context.Context, fn func(*CandidateHandoff) (T, error)) (T, error) {
	return storerunhandoff.WithCandidateHandoffResult(ctx, fn)
}
