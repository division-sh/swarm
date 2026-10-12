package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

func CopyReceiverConstructionReceiptForTest(ctx context.Context, selected any, source, destination flowidentity.RunScopedFlowInstance, sourceEntity, destinationEntity string) error {
	return runReceiverConstructionObservationFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if store, ok := selected.(*PostgresStore); ok {
			return store.pipelinePostgresOwner.CopyReceiverConstructionReceiptForTest(ctx, tx, source, destination, sourceEntity, destinationEntity)
		}
		return selected.(*SQLiteRuntimeStore).pipelineSQLiteOwner.CopyReceiverConstructionReceiptForTest(ctx, tx, source, destination, sourceEntity, destinationEntity)
	})
}

func CorruptReceiverConstructionHistoryCutForTest(ctx context.Context, selected any, owner flowidentity.RunScopedFlowInstance, entityID string, missingRevision int64) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	return runReceiverConstructionObservationFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		header, err := pipelinepersistence.ReadReceiverConstructionHeaderForTest(ctx, tx, owner, entityID, !postgres)
		if err != nil {
			return err
		}
		if store, ok := selected.(*PostgresStore); ok {
			run, err := store.runLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, owner.RunID, false)
			if err != nil {
				return err
			}
			return runforkpersistence.CorruptReceiverConstructionHistoryCutForTest(ctx, tx, run, header, missingRevision)
		}
		run, err := selected.(*SQLiteRuntimeStore).runLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, owner.RunID)
		if err != nil {
			return err
		}
		return runforkpersistence.CorruptReceiverConstructionHistoryCutForTest(ctx, tx, run, header, missingRevision)
	})
}
