package pipelinepersistence

import (
	"context"
	"database/sql"
)

func (s *PipelinePostgresOwner) CountClosedSourceFanOutIssuanceForTest(ctx context.Context, runID, eventID string, cardinality int) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = CountClosedSourceFanOutIssuance(ctx, tx, runID, eventID, cardinality)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PipelineSQLiteOwner) CountClosedSourceFanOutIssuanceForTest(ctx context.Context, runID, eventID string, cardinality int) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = CountClosedSourceFanOutIssuance(ctx, tx, runID, eventID, cardinality)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PipelinePostgresOwner) ReadSourceFanOutIntentDiagnosticRowsForTest(ctx context.Context, runID, eventID string) ([]SourceFanOutIntentDiagnosticRow, error) {
	var rows []SourceFanOutIntentDiagnosticRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		rows, err = ReadSourceFanOutIntentDiagnosticRows(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *PipelineSQLiteOwner) ReadSourceFanOutIntentDiagnosticRowsForTest(ctx context.Context, runID, eventID string) ([]SourceFanOutIntentDiagnosticRow, error) {
	var rows []SourceFanOutIntentDiagnosticRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		rows, err = ReadSourceFanOutIntentDiagnosticRows(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *PipelinePostgresOwner) ReadEarliestEventPipelineReceiptRowsForTest(ctx context.Context) ([]EventPipelineReceiptObservationRow, error) {
	var rows []EventPipelineReceiptObservationRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		rows, err = ReadEarliestEventPipelineReceiptRows(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *PipelineSQLiteOwner) ReadEarliestEventPipelineReceiptRowsForTest(ctx context.Context) ([]EventPipelineReceiptObservationRow, error) {
	var rows []EventPipelineReceiptObservationRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		rows, err = ReadEarliestEventPipelineReceiptRows(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *PipelinePostgresOwner) ReadLatestPlatformPipelineReceiptStorageForTest(ctx context.Context, eventID string) (LatestPipelineReceiptStorage, error) {
	var receipt LatestPipelineReceiptStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		receipt, err = ReadLatestPlatformPipelineReceiptStorage(ctx, tx, true, eventID)
		return err
	})
	if err != nil {
		return LatestPipelineReceiptStorage{}, err
	}
	return receipt, nil
}

func (s *PipelineSQLiteOwner) ReadLatestPlatformPipelineReceiptStorageForTest(ctx context.Context, eventID string) (LatestPipelineReceiptStorage, error) {
	var receipt LatestPipelineReceiptStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		receipt, err = ReadLatestPlatformPipelineReceiptStorage(ctx, tx, false, eventID)
		return err
	})
	if err != nil {
		return LatestPipelineReceiptStorage{}, err
	}
	return receipt, nil
}

func (s *PipelinePostgresOwner) ReadServedIncompletePipelineHandoffCountForTest(ctx context.Context, runID string) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = ReadIncompletePipelineHandoffCount(ctx, tx, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PipelineSQLiteOwner) ReadServedIncompletePipelineHandoffCountForTest(ctx context.Context, runID string) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = ReadIncompletePipelineHandoffCount(ctx, tx, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

type SelectedFanOutOutcomeCorrespondence struct {
	Outcomes, ExactContinued int
}

func (s *PipelinePostgresOwner) ReadSelectedFanOutOutcomeCorrespondenceForTest(ctx context.Context, runID, kind, sourceEventID, eventID, sourceRunID string) (SelectedFanOutOutcomeCorrespondence, error) {
	var evidence SelectedFanOutOutcomeCorrespondence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		evidence, err = readSelectedFanOutOutcomeCorrespondenceTx(ctx, tx, runID, kind, sourceEventID, eventID, sourceRunID)
		return err
	})
	if err != nil {
		return SelectedFanOutOutcomeCorrespondence{}, err
	}
	return evidence, nil
}

func (s *PipelineSQLiteOwner) ReadSelectedFanOutOutcomeCorrespondenceForTest(ctx context.Context, runID, kind, sourceEventID, eventID, sourceRunID string) (SelectedFanOutOutcomeCorrespondence, error) {
	var evidence SelectedFanOutOutcomeCorrespondence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		evidence, err = readSelectedFanOutOutcomeCorrespondenceTx(ctx, tx, runID, kind, sourceEventID, eventID, sourceRunID)
		return err
	})
	if err != nil {
		return SelectedFanOutOutcomeCorrespondence{}, err
	}
	return evidence, nil
}

func readSelectedFanOutOutcomeCorrespondenceTx(ctx context.Context, tx *sql.Tx, runID, kind, sourceEventID, eventID, sourceRunID string) (SelectedFanOutOutcomeCorrespondence, error) {
	var evidence SelectedFanOutOutcomeCorrespondence
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(CASE WHEN o.outcome_kind='continued'
			AND CAST(o.source_event_id AS TEXT)=$3 AND CAST(o.event_id AS TEXT)=$4
			AND o.inherited_disposition IS NULL AND o.failure IS NULL
			AND EXISTS(SELECT 1 FROM run_fork_selected_contract_executions x WHERE x.fork_run_id=o.run_id
				AND x.fork_event_id=o.event_id AND x.source_event_id=o.source_event_id AND x.source_run_id=$5)
			AND EXISTS(SELECT 1 FROM events e WHERE e.event_id=o.event_id AND e.run_id=o.run_id AND e.created_at=o.created_at)
			AND EXISTS(SELECT 1 FROM event_deliveries d WHERE d.event_id=o.event_id AND d.run_id=o.run_id AND d.status='delivered')
			THEN 1 END) FROM fan_out_outcomes o WHERE o.run_id=$1 AND o.outcome_kind=$2`,
		runID, kind, sourceEventID, eventID, sourceRunID).Scan(&evidence.Outcomes, &evidence.ExactContinued)
	if err != nil {
		return SelectedFanOutOutcomeCorrespondence{}, err
	}
	return evidence, nil
}
