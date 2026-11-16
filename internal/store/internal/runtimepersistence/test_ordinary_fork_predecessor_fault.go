package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func CopyReceiverConstructionReceiptForTest(ctx context.Context, selected any, source, destination flowidentity.RunScopedFlowInstance, sourceEntity, destinationEntity string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.CopyReceiverConstructionReceiptForTest(ctx, source, destination, sourceEntity, destinationEntity)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.CopyReceiverConstructionReceiptForTest(ctx, source, destination, sourceEntity, destinationEntity)
	default:
		return fmt.Errorf("receiver observation fault requires the original native owner")
	}
}

func CorruptReceiverConstructionHistoryCutForTest(ctx context.Context, selected any, owner flowidentity.RunScopedFlowInstance, entityID string, missingRevision int64) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.runForkPostgresOwner.CorruptReceiverConstructionHistoryCutForTest(ctx, owner, entityID, missingRevision)
	case *SQLiteRuntimeStore:
		return store.runForkSQLiteOwner.CorruptReceiverConstructionHistoryCutForTest(ctx, owner, entityID, missingRevision)
	default:
		return fmt.Errorf("receiver observation fault requires the original native owner")
	}
}

func ReadProjectedReceiverConstructionReceiptForTest(ctx context.Context, selected any, owner flowidentity.RunScopedFlowInstance, entityID string) (pipeline.FlowConstructionReceipt, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return pipeline.FlowConstructionReceipt{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.runForkPostgresOwner.ReadProjectedReceiverConstructionReceiptForTest(ctx, owner, entityID)
	case *SQLiteRuntimeStore:
		return store.runForkSQLiteOwner.ReadProjectedReceiverConstructionReceiptForTest(ctx, owner, entityID)
	default:
		return pipeline.FlowConstructionReceipt{}, fmt.Errorf("receiver observation fault requires the original native owner")
	}
}
