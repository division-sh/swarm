package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type SelectedCompletionCapabilityStorage = private.SelectedCompletionCapabilityStorage
type SelectedClaudeStartupCounts = private.SelectedClaudeStartupCounts
type SelectedClaudeTurnCapabilityStorage = private.SelectedClaudeTurnCapabilityStorage
type SelectedClaudeAttemptDiagnosticStorage = private.SelectedClaudeAttemptDiagnosticStorage
type SelectedPreparationStorage = private.SelectedPreparationStorage
type SelectedCompletionModelAliasStorage = private.SelectedCompletionModelAliasStorage

func CountSelectedProviderOperationsStorage(ctx context.Context, selected any, executionID string, generation uint64) (int, error) {
	return private.CountSelectedProviderOperationsStorageForTest(ctx, selected, executionID, generation)
}

func ReadSelectedCompletionCapabilitiesStorage(ctx context.Context, selected any, executionID, adapter string, generation uint64, executionOwner, runID string) ([]SelectedCompletionCapabilityStorage, error) {
	return private.ReadSelectedCompletionCapabilitiesStorageForTest(ctx, selected, executionID, adapter, generation, executionOwner, runID)
}

func ReadSelectedPreparationStorage(ctx context.Context, selected any, executionID string) (SelectedPreparationStorage, error) {
	return private.ReadSelectedPreparationStorageForTest(ctx, selected, executionID)
}

func ReadSelectedClaudeStartupCountsStorage(ctx context.Context, selected any, preparationID, processAuthorityID string) (SelectedClaudeStartupCounts, error) {
	return private.ReadSelectedClaudeStartupCountsStorageForTest(ctx, selected, preparationID, processAuthorityID)
}

func ReadSelectedClaudeStartupSurfaceStorage(ctx context.Context, selected any, preparationID, processAuthorityID string) (string, error) {
	return private.ReadSelectedClaudeStartupSurfaceStorageForTest(ctx, selected, preparationID, processAuthorityID)
}

func ReadSelectedClaudeTurnCapabilityStorage(ctx context.Context, selected any, executionID, runID string) (SelectedClaudeTurnCapabilityStorage, error) {
	return private.ReadSelectedClaudeTurnCapabilityStorageForTest(ctx, selected, executionID, runID)
}

func ReadSelectedClaudeAttemptDiagnosticsStorage(ctx context.Context, selected any, executionID string) ([]SelectedClaudeAttemptDiagnosticStorage, error) {
	return private.ReadSelectedClaudeAttemptDiagnosticsStorageForTest(ctx, selected, executionID)
}

func ReadSelectedCompletionModelAliasStorage(ctx context.Context, selected any, executionID, adapter string) (SelectedCompletionModelAliasStorage, error) {
	return private.ReadSelectedCompletionModelAliasStorageForTest(ctx, selected, executionID, adapter)
}
