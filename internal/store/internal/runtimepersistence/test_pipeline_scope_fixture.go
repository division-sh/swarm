package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type ExactPipelineReceiptOutcomeReason = pipelinepersistence.ExactPipelineReceiptOutcomeReason

func DeleteCommittedReplayScopeForTest(ctx context.Context, selected any, eventID string) error {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.DeleteCommittedReplayScopeFixtureTx(ctx, tx, postgres, eventID)
	})
}

func ReadExactPipelineReceiptOutcomeReasonForTest(ctx context.Context, selected any, eventID string) (ExactPipelineReceiptOutcomeReason, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return ExactPipelineReceiptOutcomeReason{}, err
	}
	_, postgres := selected.(*PostgresStore)
	var out ExactPipelineReceiptOutcomeReason
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadExactPipelineReceiptOutcomeReasonTx(ctx, tx, postgres, eventID)
		return err
	})
	if err != nil {
		return ExactPipelineReceiptOutcomeReason{}, err
	}
	return out, nil
}
