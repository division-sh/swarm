package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type PostgresCompletionCandidateClockStorage = private.PostgresCompletionCandidateClockStorage
type PostgresSystemNodeDeliveryStorage = private.PostgresSystemNodeDeliveryStorage
type PostgresSystemNodeOutcomeStorage = private.PostgresSystemNodeOutcomeStorage
type PostgresFlowEntityTerminalStorage = private.PostgresFlowEntityTerminalStorage

func ReadPostgresCompletionCandidateClockStorage(ctx context.Context, selected any, runID string) (PostgresCompletionCandidateClockStorage, error) {
	return private.ReadPostgresCompletionCandidateClockStorageForTest(ctx, selected, runID)
}

func ReadPostgresRunTriggerEventIdentityStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadPostgresRunTriggerEventIdentityStorageForTest(ctx, selected, runID)
}

func ReadPostgresSystemNodeDeliveryStorage(ctx context.Context, selected any, eventID, nodeID string) (PostgresSystemNodeDeliveryStorage, error) {
	return private.ReadPostgresSystemNodeDeliveryStorageForTest(ctx, selected, eventID, nodeID)
}

func ReadPostgresSystemNodeOutcomeStorage(ctx context.Context, selected any, eventID, nodeID string) (PostgresSystemNodeOutcomeStorage, error) {
	return private.ReadPostgresSystemNodeOutcomeStorageForTest(ctx, selected, eventID, nodeID)
}

func ReadPostgresExactPipelineReceiptOutcomeStorage(ctx context.Context, selected any, eventID string) (string, error) {
	return private.ReadPostgresExactPipelineReceiptOutcomeStorageForTest(ctx, selected, eventID)
}

func ReadPostgresFlowEntityTerminalStorage(ctx context.Context, selected any, runID, entityID, flowInstance string) (PostgresFlowEntityTerminalStorage, error) {
	return private.ReadPostgresFlowEntityTerminalStorageForTest(ctx, selected, runID, entityID, flowInstance)
}
