package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type AgentClaimProfileFault = private.AgentClaimProfileFault

const (
	AgentClaimProfileMissing = private.AgentClaimProfileMissing
	AgentClaimProfileCorrupt = private.AgentClaimProfileCorrupt
	AgentClaimProfileForeign = private.AgentClaimProfileForeign
)

func ApplyAgentClaimProfileFault(ctx context.Context, selected any, claim deliverylifecycle.Claim, expected deliverylifecycle.AgentClaimProfile, fault AgentClaimProfileFault, foreign deliverylifecycle.Claim) error {
	return private.ApplyAgentClaimProfileFaultForTest(ctx, selected, claim, expected, fault, foreign)
}

func ReadFirstAgentClaimProfile(ctx context.Context, selected any, expected deliverylifecycle.Snapshot) (deliverylifecycle.AgentClaimProfile, error) {
	return private.ReadFirstAgentClaimProfileForTest(ctx, selected, expected)
}
