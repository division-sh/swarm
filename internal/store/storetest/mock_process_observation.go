package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type MockConversationTurnDiagnosticsStorage = private.MockConversationTurnDiagnosticsStorage
type MockAgentDeliveryDiagnosticsStorage = private.MockAgentDeliveryDiagnosticsStorage
type MockDecisionCardStorage = private.MockDecisionCardStorage

func CountMockConversationTurnsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountMockConversationTurnsStorageForTest(ctx, selected)
}

func CountMockPhraseTurnsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountMockPhraseTurnsStorageForTest(ctx, selected)
}

func ReadMockConversationTurnDiagnosticsStorage(ctx context.Context, selected any) (MockConversationTurnDiagnosticsStorage, error) {
	return private.ReadMockConversationTurnDiagnosticsStorageForTest(ctx, selected)
}

func ReadLatestMockPhraseRequestStorage(ctx context.Context, selected any) (string, error) {
	return private.ReadLatestMockPhraseRequestStorageForTest(ctx, selected)
}

func CountUnfinishedMockAgentDeliveriesStorage(ctx context.Context, selected any) (int, error) {
	return private.CountUnfinishedMockAgentDeliveriesStorageForTest(ctx, selected)
}

func ReadMockAgentDeliveryDiagnosticsStorage(ctx context.Context, selected any) (MockAgentDeliveryDiagnosticsStorage, error) {
	return private.ReadMockAgentDeliveryDiagnosticsStorageForTest(ctx, selected)
}

func ReadMockConnectorDeliveryDiagnosticsStorage(ctx context.Context, selected any) ([]string, error) {
	return private.ReadMockConnectorDeliveryDiagnosticsStorageForTest(ctx, selected)
}

func ReadMockDecisionCardsStorage(ctx context.Context, selected any) ([]MockDecisionCardStorage, error) {
	return private.ReadMockDecisionCardsStorageForTest(ctx, selected)
}

func CountMockTelegramDecisionCardsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountMockTelegramDecisionCardsStorageForTest(ctx, selected)
}

func CountMockPythonSettledAttemptsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountMockPythonSettledAttemptsStorageForTest(ctx, selected)
}

func CountNonMockExternalAttemptsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountNonMockExternalAttemptsStorageForTest(ctx, selected)
}

func CountLiveTelegramAttemptsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountLiveTelegramAttemptsStorageForTest(ctx, selected)
}

func ReadMockConnectorAttemptDiagnosticsStorage(ctx context.Context, selected any) ([]string, error) {
	return private.ReadMockConnectorAttemptDiagnosticsStorageForTest(ctx, selected)
}

func CountExecutionModeSpendStorage(ctx context.Context, selected any, mode string) (int, error) {
	return private.CountExecutionModeSpendStorageForTest(ctx, selected, mode)
}

func CountMockAuthorActivityStorage(ctx context.Context, selected any) (int, error) {
	return private.CountMockAuthorActivityStorageForTest(ctx, selected)
}

func ReadMockEntityIdentityStorage(ctx context.Context, selected any, entityID string) (string, error) {
	return private.ReadMockEntityIdentityStorageForTest(ctx, selected, entityID)
}

func CountMockTelegramReplyStorage(ctx context.Context, selected any) (int, error) {
	return private.CountMockTelegramReplyStorageForTest(ctx, selected)
}

func ReadMockConnectorEventDiagnosticsStorage(ctx context.Context, selected any) ([]string, error) {
	return private.ReadMockConnectorEventDiagnosticsStorageForTest(ctx, selected)
}
