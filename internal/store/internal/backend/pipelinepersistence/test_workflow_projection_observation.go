package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

func (s *PipelinePostgresOwner) CountWorkflowHeadersCreatedSinceForTest(ctx context.Context, since time.Time) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = CountWorkflowHeadersCreatedSince(ctx, tx, since)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PipelineSQLiteOwner) CountWorkflowHeadersCreatedSinceForTest(ctx context.Context, since time.Time) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = CountWorkflowHeadersCreatedSince(ctx, tx, since)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PipelinePostgresOwner) CountWorkflowHeadersForPathCreatedSinceForTest(ctx context.Context, path string, since time.Time) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = CountWorkflowHeadersForPathCreatedSince(ctx, tx, path, since)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PipelineSQLiteOwner) CountWorkflowHeadersForPathCreatedSinceForTest(ctx context.Context, path string, since time.Time) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = CountWorkflowHeadersForPathCreatedSince(ctx, tx, path, since)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PipelinePostgresOwner) ReadLatestWorkflowFieldsForPathForTest(ctx context.Context, path string) (json.RawMessage, bool, error) {
	var fields json.RawMessage
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		fields, found, err = ReadLatestWorkflowFieldsForPath(ctx, tx, path)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return fields, found, nil
}

func (s *PipelineSQLiteOwner) ReadLatestWorkflowFieldsForPathForTest(ctx context.Context, path string) (json.RawMessage, bool, error) {
	var fields json.RawMessage
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		fields, found, err = ReadLatestWorkflowFieldsForPath(ctx, tx, path)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return fields, found, nil
}

func (s *PipelinePostgresOwner) ReadWorkflowStateObservationRowsForTest(ctx context.Context) ([]WorkflowStateObservationRow, error) {
	var rows []WorkflowStateObservationRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		rows, err = ReadWorkflowStateObservationRows(ctx, tx, true)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *PipelineSQLiteOwner) ReadWorkflowStateObservationRowsForTest(ctx context.Context) ([]WorkflowStateObservationRow, error) {
	var rows []WorkflowStateObservationRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		rows, err = ReadWorkflowStateObservationRows(ctx, tx, false)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *PipelinePostgresOwner) ReadWorkflowControlProjectionStorageForTest(ctx context.Context, run, entity string) (WorkflowControlProjectionStorage, error) {
	var out WorkflowControlProjectionStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadWorkflowControlProjectionStorageForTest(ctx, tx, true, run, entity)
		return err
	})
	if err != nil {
		return WorkflowControlProjectionStorage{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ReadWorkflowControlProjectionStorageForTest(ctx context.Context, run, entity string) (WorkflowControlProjectionStorage, error) {
	var out WorkflowControlProjectionStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadWorkflowControlProjectionStorageForTest(ctx, tx, false, run, entity)
		return err
	})
	if err != nil {
		return WorkflowControlProjectionStorage{}, err
	}
	return out, nil
}

func (s *PipelinePostgresOwner) ReadWorkflowTransitionEvidenceWireForTest(ctx context.Context, run, path string) (json.RawMessage, error) {
	var wire json.RawMessage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		wire, err = ReadWorkflowTransitionEvidenceWireForTest(ctx, tx, run, path)
		return err
	})
	if err != nil {
		return nil, err
	}
	return wire, nil
}

func (s *PipelineSQLiteOwner) ReadWorkflowTransitionEvidenceWireForTest(ctx context.Context, run, path string) (json.RawMessage, error) {
	var wire json.RawMessage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		wire, err = ReadWorkflowTransitionEvidenceWireForTest(ctx, tx, run, path)
		return err
	})
	if err != nil {
		return nil, err
	}
	return wire, nil
}

func (s *PipelinePostgresOwner) ReadWorkflowDuplicateProjectionStorageForTest(ctx context.Context, run, entity string) (WorkflowDuplicateProjectionStorage, error) {
	var out WorkflowDuplicateProjectionStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadWorkflowDuplicateProjectionStorageForTest(ctx, tx, true, run, entity)
		return err
	})
	if err != nil {
		return WorkflowDuplicateProjectionStorage{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ReadWorkflowDuplicateProjectionStorageForTest(ctx context.Context, run, entity string) (WorkflowDuplicateProjectionStorage, error) {
	var out WorkflowDuplicateProjectionStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadWorkflowDuplicateProjectionStorageForTest(ctx, tx, false, run, entity)
		return err
	})
	if err != nil {
		return WorkflowDuplicateProjectionStorage{}, err
	}
	return out, nil
}

func (s *PipelinePostgresOwner) CountWorkflowInstanceHeadersForTest(ctx context.Context) (int64, error) {
	var count int64
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = CountWorkflowInstanceHeadersForTest(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PipelineSQLiteOwner) CountWorkflowInstanceHeadersForTest(ctx context.Context) (int64, error) {
	var count int64
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = CountWorkflowInstanceHeadersForTest(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PipelinePostgresOwner) ReadWorkflowEnginePhysicalCountsForTest(ctx context.Context) (WorkflowEnginePhysicalCounts, error) {
	var out WorkflowEnginePhysicalCounts
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadWorkflowEnginePhysicalCountsForTest(ctx, tx)
		return err
	})
	if err != nil {
		return WorkflowEnginePhysicalCounts{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ReadWorkflowEnginePhysicalCountsForTest(ctx context.Context) (WorkflowEnginePhysicalCounts, error) {
	var out WorkflowEnginePhysicalCounts
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadWorkflowEnginePhysicalCountsForTest(ctx, tx)
		return err
	})
	if err != nil {
		return WorkflowEnginePhysicalCounts{}, err
	}
	return out, nil
}
