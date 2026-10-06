package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func ReadReceiverConstructionPublicationForTest(ctx context.Context, selected any, owner flowidentity.RunScopedFlowInstance, entityID string) (pipeline.FlowConstructionPublicationEvidence, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return pipeline.FlowConstructionPublicationEvidence{}, err
	}
	var evidence pipeline.FlowConstructionPublicationEvidence
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		evidence, err = pipelinepersistence.ReadFlowConstructionPublicationTx(ctx, tx, owner, entityID)
		return err
	})
	if err != nil {
		return pipeline.FlowConstructionPublicationEvidence{}, err
	}
	return evidence, nil
}
