package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadReceiverConstructionPublication(ctx context.Context, selected any, owner flowidentity.RunScopedFlowInstance, entityID string) (pipeline.FlowConstructionPublicationEvidence, error) {
	return private.ReadReceiverConstructionPublicationForTest(ctx, selected, owner, entityID)
}

func ReadReceiverConstructionPublicationFields(ctx context.Context, selected any, owner flowidentity.RunScopedFlowInstance, entityID, eventID string) (map[string]any, error) {
	return private.ReadReceiverConstructionPublicationFieldsForTest(ctx, selected, owner, entityID, eventID)
}
