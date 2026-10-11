package storetest

import (
	"context"
	"encoding/json"
	"time"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type CausalEventStorageRow = private.CausalEventStorageRow

type GlobalEventChronologyRow = private.GlobalEventChronologyRow

func ReadGlobalEventChronology(ctx context.Context, selected any) ([]GlobalEventChronologyRow, error) {
	return private.ReadGlobalEventChronologyForTest(ctx, selected)
}

func CountRunEventNameStorage(ctx context.Context, selected any, run, name string) (int, error) {
	return private.CountRunEventNameStorageForTest(ctx, selected, run, name)
}

type SourceRouteSettlementStorage = private.SourceRouteSettlementStorage

func ReadSourceRouteSettlementStorage(ctx context.Context, selected any, runID, eventID string) (SourceRouteSettlementStorage, error) {
	return private.ReadSourceRouteSettlementStorageForTest(ctx, selected, runID, eventID)
}

func CountChainDepthDiagnosticStorage(ctx context.Context, selected any, runID, entityID, handlerNode string) (int, error) {
	return private.CountChainDepthDiagnosticStorageForTest(ctx, selected, runID, entityID, handlerNode)
}

func ReadLatestHandlerErrorLog(ctx context.Context, selected any) (json.RawMessage, error) {
	return private.ReadLatestHandlerErrorLogForTest(ctx, selected)
}

func ReadCausalEventStorageSince(ctx context.Context, selected any, since time.Time) ([]CausalEventStorageRow, error) {
	return private.ReadCausalEventStorageSinceForTest(ctx, selected, since)
}
func ReadAgentSettledAttemptCount(ctx context.Context, selected any, eventID, agentID string) (int, error) {
	return private.ReadAgentSettledAttemptCountForTest(ctx, selected, eventID, agentID)
}
