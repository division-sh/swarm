package storetest

import (
	"context"
	"time"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func InsertEntityToolSiblingAliasFault(ctx context.Context, selected any, runID, ownerID, siblingID string, fields map[string]any, at time.Time) error {
	return private.InsertEntityToolSiblingAliasFaultForTest(ctx, selected, runID, ownerID, siblingID, fields, at)
}

func SetEntityToolPrivateBookkeepingFault(ctx context.Context, selected any, entityID string) error {
	return private.SetEntityToolPrivateBookkeepingFaultForTest(ctx, selected, entityID)
}

func SetEntityToolUndeclaredFieldFault(ctx context.Context, selected any, entityID string) error {
	return private.SetEntityToolUndeclaredFieldFaultForTest(ctx, selected, entityID)
}

func SetEntityToolMarginalStateFault(ctx context.Context, selected any, entityID string) error {
	return private.SetEntityToolMarginalStateFaultForTest(ctx, selected, entityID)
}

func AdvanceEntityToolForkSourceFault(ctx context.Context, selected any, runID, entityID string, at time.Time) error {
	return private.AdvanceEntityToolForkSourceFaultForTest(ctx, selected, runID, entityID, at)
}
