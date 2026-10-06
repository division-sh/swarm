package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/effects"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ManagedAgentTurnStorageRow = private.ManagedAgentTurnStorageRow
type ManagedTurnEffectStorage = private.ManagedTurnEffectStorage
type ManagedDeliveryStorage = private.ManagedDeliveryStorage
type SelectedExecutionStorage = private.SelectedExecutionStorage
type ConversationForkStorage = private.ConversationForkStorage
type ConversationForkTurnStorage = private.ConversationForkTurnStorage
type ConversationForkDomainStorage = private.ConversationForkDomainStorage
type ConversationForkTurnDiagnostic = private.ConversationForkTurnDiagnostic
type AuthoredHTTPToolEffectStorage = private.AuthoredHTTPToolEffectStorage
type SelectedSourceOutcomeFixture = private.SelectedSourceOutcomeFixture
type WorkspaceMockInvocationStorage = private.WorkspaceMockInvocationStorage
type WorkspaceInvocationPhases = private.WorkspaceInvocationPhases
type WorkspaceEffectFailureStorage = private.WorkspaceEffectFailureStorage

func ReadWorkspaceEffectFailures(ctx context.Context, selected any) ([]WorkspaceEffectFailureStorage, error) {
	return private.ReadWorkspaceEffectFailuresForTest(ctx, selected)
}

func ReadWorkspaceMockInvocationStorage(ctx context.Context, selected any) (WorkspaceMockInvocationStorage, error) {
	return private.ReadWorkspaceMockInvocationStorageForTest(ctx, selected)
}

func ReadWorkspaceInvocationPhases(ctx context.Context, selected any) (WorkspaceInvocationPhases, error) {
	return private.ReadWorkspaceInvocationPhasesForTest(ctx, selected)
}

func ReadManagedAgentTurnStorage(ctx context.Context, selected any, runID, agentID string) ([]ManagedAgentTurnStorageRow, error) {
	return private.ReadManagedAgentTurnStorageForTest(ctx, selected, runID, agentID)
}
func ReadManagedTurnEffectStorage(ctx context.Context, selected any, runID, agentID string) (ManagedTurnEffectStorage, error) {
	return private.ReadManagedTurnEffectStorageForTest(ctx, selected, runID, agentID)
}
func ReadManagedDeliveryStorage(ctx context.Context, selected any, runID, agentID string) (ManagedDeliveryStorage, error) {
	return private.ReadManagedDeliveryStorageForTest(ctx, selected, runID, agentID)
}
func ReadStartFailedCompensationCount(ctx context.Context, selected any, token effects.LifecycleToken) (int, error) {
	return private.ReadStartFailedCompensationCountForTest(ctx, selected, token)
}
func ReadSelectedExecutionStorage(ctx context.Context, selected any, runID string) (SelectedExecutionStorage, error) {
	return private.ReadSelectedExecutionStorageForTest(ctx, selected, runID)
}
func ReadConversationForkStorage(ctx context.Context, selected any, forkID string) (ConversationForkStorage, error) {
	return private.ReadConversationForkStorageForTest(ctx, selected, forkID)
}
func ReadConversationForkTurnStorage(ctx context.Context, selected any, forkID, key string) (ConversationForkTurnStorage, error) {
	return private.ReadConversationForkTurnStorageForTest(ctx, selected, forkID, key)
}
func ReadConversationForkTurnDiagnostics(ctx context.Context, selected any, forkID string) ([]ConversationForkTurnDiagnostic, error) {
	return private.ReadConversationForkTurnDiagnosticsForTest(ctx, selected, forkID)
}
func ReadConversationForkDomainStorage(ctx context.Context, selected any, runID string) (ConversationForkDomainStorage, error) {
	return private.ReadConversationForkDomainStorageForTest(ctx, selected, runID)
}
func ReadAuthoredHTTPToolEffectStorage(ctx context.Context, selected any) ([]AuthoredHTTPToolEffectStorage, error) {
	return private.ReadAuthoredHTTPToolEffectStorageForTest(ctx, selected)
}
func ReadSelectedForkSourceDomain(ctx context.Context, selected any, runID string) (map[string]SelectedForkStorageTableSnapshot, error) {
	return private.ReadSelectedForkSourceDomainForTest(ctx, selected, runID)
}
func SeedSelectedSourceOutcome(ctx context.Context, selected any, fixture SelectedSourceOutcomeFixture) error {
	return private.SeedSelectedSourceOutcomeForTest(ctx, selected, fixture)
}
