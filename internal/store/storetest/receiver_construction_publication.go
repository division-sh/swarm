package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadReceiverConstructionPublicationFields(ctx context.Context, selected any, owner flowidentity.RunScopedFlowInstance, entityID, eventID string) (map[string]any, error) {
	return private.ReadReceiverConstructionPublicationFieldsForTest(ctx, selected, owner, entityID, eventID)
}
