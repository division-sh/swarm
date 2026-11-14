package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type ExactPipelineReceiptOutcomeReason = pipelinepersistence.ExactPipelineReceiptOutcomeReason

func ReadExactFanOutBarrierForTest(ctx context.Context, selected any, key fanoutobligation.IntentKey) (fanoutbarrier.Barrier, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return fanoutbarrier.Barrier{}, err
	}
	if err := key.Validate(); err != nil {
		return fanoutbarrier.Barrier{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadExactFanOutBarrierForTest(ctx, key)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadExactFanOutBarrierForTest(ctx, key)
	default:
		return fanoutbarrier.Barrier{}, fmt.Errorf("fan-out barrier observation requires its original native owner")
	}
}

func DeleteCommittedReplayScopeForTest(ctx context.Context, selected any, eventID string) error {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.DeleteCommittedReplayScopeForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.DeleteCommittedReplayScopeForTest(ctx, eventID)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func ReadExactPipelineReceiptOutcomeReasonForTest(ctx context.Context, selected any, eventID string) (ExactPipelineReceiptOutcomeReason, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return ExactPipelineReceiptOutcomeReason{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadExactPipelineReceiptOutcomeReasonForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadExactPipelineReceiptOutcomeReasonForTest(ctx, eventID)
	default:
		return ExactPipelineReceiptOutcomeReason{}, fmt.Errorf("pipeline receipt observation requires its original native owner")
	}
}
