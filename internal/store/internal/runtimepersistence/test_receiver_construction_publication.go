package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func ReadReceiverConstructionPublicationFieldsForTest(ctx context.Context, selected any, owner flowidentity.RunScopedFlowInstance, entityID, eventID string) (map[string]any, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var fields map[string]any
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		fields, err = pipelinepersistence.ReadFlowConstructionPublicationFieldsTx(ctx, tx, owner, entityID, eventID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return fields, nil
}
