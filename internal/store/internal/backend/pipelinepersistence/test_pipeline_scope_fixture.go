package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

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
		return deleteCommittedReplayScopeFixtureTx(ctx, tx, true, eventID)
	})
}

func (s *PipelineSQLiteOwner) DeleteCommittedReplayScopeForTest(ctx context.Context, eventID string) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return deleteCommittedReplayScopeFixtureTx(ctx, tx, false, eventID)
	})
}

func (s *PipelinePostgresOwner) ReadExactPipelineReceiptOutcomeReasonForTest(ctx context.Context, eventID string) (ExactPipelineReceiptOutcomeReason, error) {
	var out ExactPipelineReceiptOutcomeReason
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readExactPipelineReceiptOutcomeReasonTx(ctx, tx, true, eventID)
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
		out, err = readExactPipelineReceiptOutcomeReasonTx(ctx, tx, false, eventID)
		return err
	})
	if err != nil {
		return ExactPipelineReceiptOutcomeReason{}, err
	}
	return out, err
}

func readExactPipelineReceiptOutcomeReasonTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) (ExactPipelineReceiptOutcomeReason, error) {
	query := `SELECT outcome, COALESCE(reason_code, '') FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if postgres {
		query = `SELECT outcome, COALESCE(reason_code, '') FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	var out ExactPipelineReceiptOutcomeReason
	if err := tx.QueryRowContext(ctx, query, eventID).Scan(&out.Outcome, &out.Reason); err != nil {
		return ExactPipelineReceiptOutcomeReason{}, err
	}
	return out, nil
}

func deleteCommittedReplayScopeFixtureTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) error {
	query := `DELETE FROM committed_replay_scopes WHERE event_id = ?`
	if postgres {
		query = `DELETE FROM committed_replay_scopes WHERE event_id = $1::uuid`
	}
	result, err := tx.ExecContext(ctx, query, eventID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("committed replay scope fault removed %d rows, want one exact event", changed)
	}
	return nil
}

func (s *PipelinePostgresOwner) ReadDecisionRouteStatusForTest(ctx context.Context, eventID string) (string, error) {
	var status string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		status, err = readDecisionRouteStatusTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return "", err
	}
	return status, nil
}

func (s *PipelineSQLiteOwner) ReadDecisionRouteStatusForTest(ctx context.Context, eventID string) (string, error) {
	var status string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		status, err = readDecisionRouteStatusTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return "", err
	}
	return status, nil
}

func readDecisionRouteStatusTx(ctx context.Context, tx *sql.Tx, eventID string) (string, error) {
	if tx == nil {
		return "", fmt.Errorf("decision route evidence requires a selected read transaction")
	}
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM decision_card_route_obligations WHERE event_id=$1`, eventID).Scan(&status); err != nil {
		return "", err
	}
	return status, nil
}
