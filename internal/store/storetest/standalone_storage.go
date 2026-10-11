package storetest

import (
	"context"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type StandaloneRunStorage = private.StandaloneRunStorage
type StandaloneAgentDeliveryStorage = private.StandaloneAgentDeliveryStorage

func ReadStandaloneRunStorage(ctx context.Context, selected any, eventID string) (StandaloneRunStorage, error) {
	return private.ReadStandaloneRunStorageForTest(ctx, selected, eventID)
}

func ReadStandaloneAgentDeliveryStorage(ctx context.Context, selected any, eventID, agentID string) (StandaloneAgentDeliveryStorage, error) {
	return private.ReadStandaloneAgentDeliveryStorageForTest(ctx, selected, eventID, agentID)
}

func ReadStandaloneCompletionCandidate(ctx context.Context, selected any, eventID string) (runtimerunlifecycle.Candidate, error) {
	return private.ReadStandaloneCompletionCandidateForTest(ctx, selected, eventID)
}
func ReadExactAgentDeliveryStatus(ctx context.Context, selected any, eventID, agentID string) (string, error) {
	return private.ReadExactAgentDeliveryStatusForTest(ctx, selected, eventID, agentID)
}
