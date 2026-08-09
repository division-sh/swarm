package runtimepersistence

import (
	"context"

	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type runLifecycleCandidateHandoffReservation = storerunhandoff.CandidateHandoff

func reserveRunLifecycleCandidateHandoff(ctx context.Context) (*runLifecycleCandidateHandoffReservation, error) {
	return storerunhandoff.ReserveCandidateHandoff(ctx)
}

func withRunLifecycleCandidateHandoff(
	ctx context.Context,
	fn func(*runLifecycleCandidateHandoffReservation) error,
) error {
	return storerunhandoff.WithCandidateHandoff(ctx, fn)
}

func withRunLifecycleCandidateHandoffResult[T any](
	ctx context.Context,
	fn func(*runLifecycleCandidateHandoffReservation) (T, error),
) (T, error) {
	return storerunhandoff.WithCandidateHandoffResult(ctx, fn)
}
