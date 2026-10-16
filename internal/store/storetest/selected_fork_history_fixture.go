package storetest

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SeedSelectedForkSourceSession(ctx context.Context, selected any, sessionID string, identity agentidentity.Identity, at time.Time) error {
	return private.SeedSelectedForkSourceSessionForTest(ctx, selected, sessionID, identity, at)
}

func SeedSelectedForkSourceConversationAudit(ctx context.Context, selected any, auditID, entityID string, identity agentidentity.Identity, at time.Time) error {
	return private.SeedSelectedForkSourceConversationAuditForTest(ctx, selected, auditID, entityID, identity, at)
}

func CountForkOrSourceSessionStorage(ctx context.Context, selected any, forkRunID, sourceSessionID string) (int, error) {
	return private.CountForkOrSourceSessionStorageForTest(ctx, selected, forkRunID, sourceSessionID)
}

func CountForkOrSourceConversationAuditStorage(ctx context.Context, selected any, forkRunID, sourceAuditID string) (int, error) {
	return private.CountForkOrSourceConversationAuditStorageForTest(ctx, selected, forkRunID, sourceAuditID)
}

func CountForkOrSourceTurnStorage(ctx context.Context, selected any, forkRunID, sourceTurnID string) (int, error) {
	return private.CountForkOrSourceTurnStorageForTest(ctx, selected, forkRunID, sourceTurnID)
}
