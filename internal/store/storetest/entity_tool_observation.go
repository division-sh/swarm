package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountRunEntityStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunEntityStorageForTest(ctx, selected, runID)
}

type EntityToolMutationAuthorityStorage = private.EntityToolMutationAuthorityStorage
type EntityToolStatusRevisionStorage = private.EntityToolStatusRevisionStorage

func ReadEntityToolStatus(ctx context.Context, selected any, runID, entityID string) (string, error) {
	return private.ReadEntityToolStatusForTest(ctx, selected, runID, entityID)
}

func ReadEntityToolStatusRevision(ctx context.Context, selected any, runID, entityID string) (EntityToolStatusRevisionStorage, error) {
	return private.ReadEntityToolStatusRevisionForTest(ctx, selected, runID, entityID)
}

func CountEntityToolAgentStatusMutations(ctx context.Context, selected any, runID, entityID string) (int, error) {
	return private.CountEntityToolAgentStatusMutationsForTest(ctx, selected, runID, entityID)
}

func ReadEntityToolFieldWire(ctx context.Context, selected any, runID, entityID, field string) ([]byte, error) {
	return private.ReadEntityToolFieldWireForTest(ctx, selected, runID, entityID, field)
}

func CountEntityToolAuthoredPathMutations(ctx context.Context, selected any, entityID, path string) (int, error) {
	return private.CountEntityToolAuthoredPathMutationsForTest(ctx, selected, entityID, path)
}

func ReadEntityToolInitialMutationAuthorities(ctx context.Context, selected any, entityID string) ([]EntityToolMutationAuthorityStorage, error) {
	return private.ReadEntityToolInitialMutationAuthoritiesForTest(ctx, selected, entityID)
}
