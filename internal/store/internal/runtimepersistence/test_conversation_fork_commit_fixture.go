package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

type ConversationForkCompletionExpiryStorage = apiidempotency.ConversationForkCompletionExpiryStorage

func CountConversationForkStorageForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, runforkpersistence.CountConversationForkStorageTx)
}

func CountConversationForkCompletionStorageForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, apiidempotency.CountConversationForkCompletionStorageTx)
}

func ReadConversationForkCompletionExpiryStorageForTest(ctx context.Context, selected any) (ConversationForkCompletionExpiryStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (ConversationForkCompletionExpiryStorage, error) {
		return apiidempotency.ReadConversationForkCompletionExpiryStorageTx(ctx, tx, postgres)
	})
}

func InstallConversationForkCompletionInsertFailureForTest(ctx context.Context, selected any) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return apiidempotency.InstallConversationForkCompletionInsertFailureTx(ctx, tx, postgres)
	})
}

func RemoveConversationForkCompletionInsertFailureForTest(ctx context.Context, selected any) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return apiidempotency.RemoveConversationForkCompletionInsertFailureTx(ctx, tx, postgres)
	})
}
