package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	decision "github.com/division-sh/swarm/internal/store/internal/backend/decisioncard"
	"github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type GateCompletionCardStorage = decision.GateCompletionCardStorage
type GateCompletionFlowStorage = pipelinepersistence.GateCompletionFlowStorage
type GateCompletionAPIStorage = apiidempotency.GateCompletionAPIStorage

func ReadGateCompletionPrincipalStorageForTest(ctx context.Context, selected any) (string, error) {
	return readServedReadRole(ctx, selected, operatorchannel.ObserveFixturePrincipalTx)
}

func ReadGateCompletionCardStorageForTest(ctx context.Context, selected any, cardID string) (GateCompletionCardStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (GateCompletionCardStorage, error) {
		return decision.ReadGateCompletionCardStorageTx(ctx, tx, cardID)
	})
}

func ReadGateCompletionFlowStorageForTest(ctx context.Context, selected any, runID string) (GateCompletionFlowStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (GateCompletionFlowStorage, error) {
		return pipelinepersistence.ReadGateCompletionFlowStorageTx(ctx, tx, runID)
	})
}

func ReadGateCompletionChangesStorageForTest(ctx context.Context, selected any, cardID string) ([][3]string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([][3]string, error) {
		return decision.ReadGateCompletionChangesStorageTx(ctx, tx, cardID)
	})
}

func ReadGateCompletionReceiptsStorageForTest(ctx context.Context, selected any, runID string) ([][5]string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([][5]string, error) {
		return pipelinepersistence.ReadGateCompletionReceiptsStorageTx(ctx, tx, runID)
	})
}

func ReadGateCompletionAPIStorageForTest(ctx context.Context, selected any, cardID, method, key string) ([]GateCompletionAPIStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]GateCompletionAPIStorage, error) {
		return apiidempotency.ReadGateCompletionAPIStorageTx(ctx, tx, cardID, method, key)
	})
}
