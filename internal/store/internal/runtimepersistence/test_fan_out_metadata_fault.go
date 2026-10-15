package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type FanOutRunClaimMetadataStorage = pipelinepersistence.FanOutRunClaimMetadataStorage

func ReadFanOutRunCapsuleWireForTest(ctx context.Context, selected any, runID string) ([]byte, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) ([]byte, error) {
		return pipelinepersistence.ReadFanOutRunCapsuleWireTx(ctx, tx, runID)
	})
}

func ReadFanOutRunClaimMetadataStorageForTest(ctx context.Context, selected any, runID string) (FanOutRunClaimMetadataStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (FanOutRunClaimMetadataStorage, error) {
		return pipelinepersistence.ReadFanOutRunClaimMetadataStorageTx(ctx, tx, runID)
	})
}

func SetRetainedFanOutCapsuleWireForTest(ctx context.Context, selected any, runID string, raw []byte) (int64, error) {
	if err := validateWorkflowProjectionFaultRun(runID); err != nil {
		return 0, err
	}
	var changed int64
	err := runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = pipelinepersistence.SetRetainedFanOutCapsuleWireForTest(ctx, tx, runID, raw)
		return err
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}

func SetRetainedFanOutCardinalityForTest(ctx context.Context, selected any, runID string, cardinality int) (int64, error) {
	if err := validateWorkflowProjectionFaultRun(runID); err != nil {
		return 0, err
	}
	var changed int64
	err := runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = pipelinepersistence.SetRetainedFanOutCardinalityForTest(ctx, tx, runID, cardinality)
		return err
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}
