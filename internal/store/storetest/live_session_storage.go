package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type PostgresAgentOneDeliverySessionStorage = private.PostgresAgentOneDeliverySessionStorage
type PostgresSessionFlowStorage = private.PostgresSessionFlowStorage

func ReadPostgresAgentOneDeliverySessionStorage(ctx context.Context, selected any, eventID string) (PostgresAgentOneDeliverySessionStorage, error) {
	return private.ReadPostgresAgentOneDeliverySessionStorageForTest(ctx, selected, eventID)
}

func ReadPostgresSessionFlowStorage(ctx context.Context, selected any, sessionID string) (PostgresSessionFlowStorage, error) {
	return private.ReadPostgresSessionFlowStorageForTest(ctx, selected, sessionID)
}

func CountPostgresAgentOneSupportLiveSessions(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountPostgresAgentOneSupportLiveSessionsForTest(ctx, selected, runID)
}

func CountPostgresAgentOneSessions(ctx context.Context, selected any) (int, error) {
	return private.CountPostgresAgentOneSessionsForTest(ctx, selected)
}
