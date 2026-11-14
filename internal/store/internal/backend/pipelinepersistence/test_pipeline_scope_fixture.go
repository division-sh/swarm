package pipelinepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func (s *PipelinePostgresOwner) ReadExactFanOutBarrierForTest(ctx context.Context, key fanoutobligation.IntentKey) (fanoutbarrier.Barrier, error) {
	var out fanoutbarrier.Barrier
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadFanOutBarrierContinuationTx(ctx, tx, true, key)
		return err
	})
	return out, err
}

func (s *PipelineSQLiteOwner) ReadExactFanOutBarrierForTest(ctx context.Context, key fanoutobligation.IntentKey) (fanoutbarrier.Barrier, error) {
	var out fanoutbarrier.Barrier
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadFanOutBarrierContinuationTx(ctx, tx, false, key)
		return err
	})
	return out, err
}

func (s *PipelinePostgresOwner) DeleteCommittedReplayScopeForTest(ctx context.Context, eventID string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return DeleteCommittedReplayScopeFixtureTx(ctx, tx, true, eventID)
	})
}

func (s *PipelineSQLiteOwner) DeleteCommittedReplayScopeForTest(ctx context.Context, eventID string) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return DeleteCommittedReplayScopeFixtureTx(ctx, tx, false, eventID)
	})
}

func (s *PipelinePostgresOwner) ReadExactPipelineReceiptOutcomeReasonForTest(ctx context.Context, eventID string) (ExactPipelineReceiptOutcomeReason, error) {
	var out ExactPipelineReceiptOutcomeReason
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadExactPipelineReceiptOutcomeReasonTx(ctx, tx, true, eventID)
		return err
	})
	if err != nil {
		return ExactPipelineReceiptOutcomeReason{}, err
	}
	return out, err
}

func (s *PipelineSQLiteOwner) ReadExactPipelineReceiptOutcomeReasonForTest(ctx context.Context, eventID string) (ExactPipelineReceiptOutcomeReason, error) {
	var out ExactPipelineReceiptOutcomeReason
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadExactPipelineReceiptOutcomeReasonTx(ctx, tx, false, eventID)
		return err
	})
	if err != nil {
		return ExactPipelineReceiptOutcomeReason{}, err
	}
	return out, err
}
