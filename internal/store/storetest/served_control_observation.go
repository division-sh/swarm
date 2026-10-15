package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type DirectiveIdentityStorage = private.DirectiveIdentityStorage
type DirectiveFailureStorage = private.DirectiveFailureStorage
type DirectiveReplayStorage = private.DirectiveReplayStorage
type ServedSetupRunStorage = private.ServedSetupRunStorage
type ServedSetupEntityStorage = private.ServedSetupEntityStorage
type ServedSetupMutationAuthorityStorage = private.ServedSetupMutationAuthorityStorage
type ServedSetupConstructorMutationStorage = private.ServedSetupConstructorMutationStorage

func ReadDirectiveIdentityStorage(ctx context.Context, selected any, key string) (DirectiveIdentityStorage, error) {
	return private.ReadDirectiveIdentityStorageForTest(ctx, selected, key)
}
func CountKeyedDirectiveOperationsStorage(ctx context.Context, selected any, key string) (int, error) {
	return private.CountKeyedDirectiveOperationsStorageForTest(ctx, selected, key)
}
func ReadDirectiveFailureStorage(ctx context.Context, selected any, key string) (DirectiveFailureStorage, error) {
	return private.ReadDirectiveFailureStorageForTest(ctx, selected, key)
}
func ReadDirectiveReplayStorage(ctx context.Context, selected any, key string) (DirectiveReplayStorage, error) {
	return private.ReadDirectiveReplayStorageForTest(ctx, selected, key)
}
func ReadDirectivePipelineFailureStorage(ctx context.Context, selected any, key string) ([]byte, error) {
	return private.ReadDirectivePipelineFailureStorageForTest(ctx, selected, key)
}
func CountSuccessfulDirectivePipelineReceiptsStorage(ctx context.Context, selected any, eventID string) (int, error) {
	return private.CountSuccessfulDirectivePipelineReceiptsStorageForTest(ctx, selected, eventID)
}
func ReadDirectiveProjectionResourceStorage(ctx context.Context, selected any, key string) (string, error) {
	return private.ReadDirectiveProjectionResourceStorageForTest(ctx, selected, key)
}
func ReadDirectivePayloadOperationIDStorage(ctx context.Context, selected any, eventID string) (string, error) {
	return private.ReadDirectivePayloadOperationIDStorageForTest(ctx, selected, eventID)
}
func ReadServedSetupRunStorage(ctx context.Context, selected any, runID string) (ServedSetupRunStorage, error) {
	return private.ReadServedSetupRunStorageForTest(ctx, selected, runID)
}
func ReadServedSetupEntityStorage(ctx context.Context, selected any, runID, entityID string) (ServedSetupEntityStorage, error) {
	return private.ReadServedSetupEntityStorageForTest(ctx, selected, runID, entityID)
}
func CountAllRunEntityMutationsStorage(ctx context.Context, selected any, runID, entityID string) (int, error) {
	return private.CountAllRunEntityMutationsStorageForTest(ctx, selected, runID, entityID)
}
func ReadServedSetupMutationAuthoritiesStorage(ctx context.Context, selected any, runID, entityID string) ([]ServedSetupMutationAuthorityStorage, error) {
	return private.ReadServedSetupMutationAuthoritiesStorageForTest(ctx, selected, runID, entityID)
}
func ReadServedSetupConstructorMutationsStorage(ctx context.Context, selected any, runID, entityID string) ([]ServedSetupConstructorMutationStorage, error) {
	return private.ReadServedSetupConstructorMutationsStorageForTest(ctx, selected, runID, entityID)
}
