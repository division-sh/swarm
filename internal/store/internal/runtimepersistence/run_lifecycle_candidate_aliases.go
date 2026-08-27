package runtimepersistence

import (
	"context"

	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type runLifecycleCandidateHandoffReservation = storerunhandoff.CandidateHandoff

func reserveRunLifecycleCandidateHandoff(ctx context.Context) (*runLifecycleCandidateHandoffReservation, error) {
	return storerunhandoff.ReserveCandidateHandoff(ctx)
}
