package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func ReadFirstAgentClaimProfileForTest(ctx context.Context, selected any, expected deliverylifecycle.Snapshot) (deliverylifecycle.AgentClaimProfile, error) {
	var profile deliverylifecycle.AgentClaimProfile
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil {
			return profile, fmt.Errorf("agent profile evidence requires its original postgres owner")
		}
		return owner.ReadFirstAgentClaimProfile(ctx, expected)
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil {
			return profile, fmt.Errorf("agent profile evidence requires its original sqlite owner")
		}
		return owner.ReadFirstAgentClaimProfile(ctx, expected)
	default:
		return profile, fmt.Errorf("agent profile evidence requires the original selected owner, got %T", selected)
	}
}
