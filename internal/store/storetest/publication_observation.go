package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type PublicationRunStorage = private.PublicationRunStorage
type PublicationRunTriggerStorage = private.PublicationRunTriggerStorage
type PublicationEventProjection = private.PublicationEventProjection
type PublicationEventIdentityStorage = private.PublicationEventIdentityStorage
type OperatorReferenceStorage = private.OperatorReferenceStorage
type RunStartStorage = private.RunStartStorage
type RootTemplatePublicationStorage = private.RootTemplatePublicationStorage

func ReadPostgresRootTemplatePublication(ctx context.Context, selected any, eventID string) (RootTemplatePublicationStorage, error) {
	return private.ReadPostgresRootTemplatePublicationForTest(ctx, selected, eventID)
}

func SeedPostgresPublicationIdentityLookalikes(ctx context.Context, selected any, runID, entityID string) error {
	return private.SeedPostgresPublicationIdentityLookalikesForTest(ctx, selected, runID, entityID)
}

func SeedPostgresPublicationUnroutableEntity(ctx context.Context, selected any, runID, entityID, flowInstance string) error {
	return private.SeedPostgresPublicationUnroutableEntityForTest(ctx, selected, runID, entityID, flowInstance)
}

func ReadEventPublicationStoredResponse(ctx context.Context, selected any, key string) ([]byte, error) {
	return private.ReadEventPublicationStoredResponseForTest(ctx, selected, key)
}

func ReadPublicationRunTriggerStorage(ctx context.Context, selected any, runID string) (PublicationRunTriggerStorage, error) {
	return private.ReadPublicationRunTriggerStorageForTest(ctx, selected, runID)
}

func ReadPostgresRunStartStorage(ctx context.Context, selected any, runID, eventName string) (RunStartStorage, error) {
	return private.ReadPostgresRunStartStorageForTest(ctx, selected, runID, eventName)
}

func ReadCreatedPublicationRunStorage(ctx context.Context, selected any, runID string) (PublicationRunStorage, error) {
	return private.ReadCreatedPublicationRunStorageForTest(ctx, selected, runID)
}

func ReadExistingPublicationRunStorage(ctx context.Context, selected any, runID string) (PublicationRunStorage, error) {
	return private.ReadExistingPublicationRunStorageForTest(ctx, selected, runID)
}

func ReadPublicationEventProjection(ctx context.Context, selected any, eventID string) (PublicationEventProjection, error) {
	return private.ReadPublicationEventProjectionForTest(ctx, selected, eventID)
}

func ReadPublicationEventIdentityStorage(ctx context.Context, selected any, eventID string) (PublicationEventIdentityStorage, error) {
	return private.ReadPublicationEventIdentityStorageForTest(ctx, selected, eventID)
}

func ReadServedEventRunIDStorage(ctx context.Context, selected any, eventID string) (string, error) {
	return private.ReadServedEventRunIDStorageForTest(ctx, selected, eventID)
}

func ReadOperatorReferenceStorage(ctx context.Context, selected any, eventID string) (OperatorReferenceStorage, error) {
	return private.ReadOperatorReferenceStorageForTest(ctx, selected, eventID)
}

func ReadPublicationDeliveryTargetEncodings(ctx context.Context, selected any, eventID string) ([]string, error) {
	return private.ReadPublicationDeliveryTargetEncodingsForTest(ctx, selected, eventID)
}

func ReadPhysicalPublicationEntityIdentityStorage(ctx context.Context, selected any, eventID string) (string, error) {
	return private.ReadPhysicalPublicationEntityIdentityStorageForTest(ctx, selected, eventID)
}
