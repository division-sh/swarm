package runlifecycle

import (
	"context"

	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type CandidateHandoff = storerunhandoff.CandidateHandoff

func ReserveCandidateHandoff(ctx context.Context) (*CandidateHandoff, error) {
	return storerunhandoff.ReserveCandidateHandoff(ctx)
}

func WithCandidateHandoffOutcomeResult[T any](ctx context.Context, fn func(*CandidateHandoff) (T, bool, error)) (T, error) {
	return storerunhandoff.WithCandidateHandoffOutcomeResult(ctx, fn)
}
