package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	authoractivityadapter "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity/readadapter"
	decisioncard "github.com/division-sh/swarm/internal/store/internal/backend/decisioncard"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	budgetspend "github.com/division-sh/swarm/internal/store/internal/budgetspend"
)

type MockConversationTurnDiagnosticsStorage = llmpersistence.MockConversationTurnDiagnosticsStorage
type MockAgentDeliveryDiagnosticsStorage = delivery.MockAgentDeliveryDiagnosticsStorage
type MockDecisionCardStorage = decisioncard.MockDecisionCardStorage

func CountMockConversationTurnsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return llmpersistence.CountMockConversationTurnsStorageTx(ctx, tx)
	})
}

func CountMockPhraseTurnsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return llmpersistence.CountMockPhraseTurnsStorageTx(ctx, tx)
	})
}

func ReadMockConversationTurnDiagnosticsStorageForTest(ctx context.Context, selected any) (MockConversationTurnDiagnosticsStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (MockConversationTurnDiagnosticsStorage, error) {
		return llmpersistence.ReadMockConversationTurnDiagnosticsStorageTx(ctx, tx)
	})
}

func ReadLatestMockPhraseRequestStorageForTest(ctx context.Context, selected any) (string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return llmpersistence.ReadLatestMockPhraseRequestStorageTx(ctx, tx)
	})
}

func CountUnfinishedMockAgentDeliveriesStorageForTest(ctx context.Context, selected any) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountUnfinishedMockAgentDeliveriesStorageTx(ctx, tx)
	})
}

func ReadMockAgentDeliveryDiagnosticsStorageForTest(ctx context.Context, selected any) (MockAgentDeliveryDiagnosticsStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (MockAgentDeliveryDiagnosticsStorage, error) {
		return delivery.ReadMockAgentDeliveryDiagnosticsStorageTx(ctx, tx)
	})
}

func ReadMockConnectorDeliveryDiagnosticsStorageForTest(ctx context.Context, selected any) ([]string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return delivery.ReadMockConnectorDeliveryDiagnosticsStorageTx(ctx, tx)
	})
}

func ReadMockDecisionCardsStorageForTest(ctx context.Context, selected any) ([]MockDecisionCardStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]MockDecisionCardStorage, error) {
		return decisioncard.ReadMockDecisionCardsStorageTx(ctx, tx)
	})
}

func CountMockTelegramDecisionCardsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return decisioncard.CountMockTelegramDecisionCardsStorageTx(ctx, tx)
	})
}

func CountMockPythonSettledAttemptsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return effectpersistence.CountMockPythonSettledAttemptsStorageTx(ctx, tx)
	})
}

func CountNonMockExternalAttemptsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return effectpersistence.CountNonMockExternalAttemptsStorageTx(ctx, tx)
	})
}

func CountLiveTelegramAttemptsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return activityjournal.CountLiveTelegramAttemptsStorageTx(ctx, tx)
	})
}

func ReadMockConnectorAttemptDiagnosticsStorageForTest(ctx context.Context, selected any) ([]string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return activityjournal.ReadMockConnectorAttemptDiagnosticsStorageTx(ctx, tx)
	})
}

func CountExecutionModeSpendStorageForTest(ctx context.Context, selected any, mode string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return budgetspend.CountExecutionModeSpendStorageTx(ctx, tx, mode)
	})
}

func CountMockAuthorActivityStorageForTest(ctx context.Context, selected any) (int, error) {
	_, postgres := selected.(*PostgresStore)
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return authoractivityadapter.CountMockAuthorActivityStorageTx(ctx, tx, postgres)
	})
}

func ReadMockEntityIdentityStorageForTest(ctx context.Context, selected any, entityID string) (string, error) {
	_, postgres := selected.(*PostgresStore)
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadMockEntityIdentityStorageTx(ctx, tx, postgres, entityID)
	})
}
