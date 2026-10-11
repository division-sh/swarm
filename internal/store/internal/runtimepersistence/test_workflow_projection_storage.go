package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

type WorkflowControlProjectionStorage = pipelinepersistence.WorkflowControlProjectionStorage
type WorkflowDuplicateProjectionStorage = pipelinepersistence.WorkflowDuplicateProjectionStorage
type WorkflowEnginePhysicalCounts = pipelinepersistence.WorkflowEnginePhysicalCounts
type WorkflowStateObservationRow = pipelinepersistence.WorkflowStateObservationRow

func CountWorkflowHeadersCreatedSinceForTest(ctx context.Context, selected any, since time.Time) (int, error) {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = pipelinepersistence.CountWorkflowHeadersCreatedSince(ctx, tx, since)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func CountWorkflowHeadersForPathCreatedSinceForTest(ctx context.Context, selected any, path string, since time.Time) (int, error) {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = pipelinepersistence.CountWorkflowHeadersForPathCreatedSince(ctx, tx, path, since)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadLatestWorkflowFieldsForPathForTest(ctx context.Context, selected any, path string) (json.RawMessage, bool, error) {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return nil, false, err
	}
	var fields json.RawMessage
	var found bool
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		fields, found, err = pipelinepersistence.ReadLatestWorkflowFieldsForPath(ctx, tx, path)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return fields, found, nil
}

func ReadWorkflowStateObservationRowsForTest(ctx context.Context, selected any) ([]WorkflowStateObservationRow, error) {
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return nil, err
	}
	var out []WorkflowStateObservationRow
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadWorkflowStateObservationRows(ctx, tx, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func workflowProjectionObservationPostgres(selected any, run, entity string) (bool, error) {
	for _, key := range []string{run, entity} {
		id, err := uuid.Parse(key)
		if err != nil || id == uuid.Nil || id.String() != key {
			return false, fmt.Errorf("workflow projection requires exact canonical run and entity")
		}
	}
	return workflowProjectionNativePostgres(selected)
}

func workflowProjectionNativePostgres(selected any) (bool, error) {
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || owner.pipelinePostgresOwner == nil || !owner.backend.Valid() {
			return false, fmt.Errorf("projection observation requires an initialized postgres owner")
		}
		return true, owner.requireCurrentSchema()
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || owner.pipelineSQLiteOwner == nil || !owner.backend.Valid() {
			return false, fmt.Errorf("projection observation requires an initialized sqlite owner")
		}
		return false, owner.requireCurrentSchema()
	default:
		return false, fmt.Errorf("projection observation requires the original native owner, got %T", selected)
	}
}

func ReadWorkflowControlProjectionStorageForTest(ctx context.Context, selected any, run, entity string) (WorkflowControlProjectionStorage, error) {
	postgres, err := workflowProjectionObservationPostgres(selected, run, entity)
	if err != nil {
		return WorkflowControlProjectionStorage{}, err
	}
	var out WorkflowControlProjectionStorage
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadWorkflowControlProjectionStorageForTest(ctx, tx, postgres, run, entity)
		return err
	})
	if err != nil {
		return WorkflowControlProjectionStorage{}, err
	}
	return out, nil
}

func ReadWorkflowTransitionEvidenceWireForTest(ctx context.Context, selected any, run, path string) (json.RawMessage, error) {
	if err := validateWorkflowProjectionFaultKey(run, path, false); err != nil {
		return nil, err
	}
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return nil, err
	}
	var wire json.RawMessage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		wire, err = pipelinepersistence.ReadWorkflowTransitionEvidenceWireForTest(ctx, tx, run, path)
		return err
	})
	if err != nil {
		return nil, err
	}
	return wire, nil
}

func ReadWorkflowDuplicateProjectionStorageForTest(ctx context.Context, selected any, run, entity string) (WorkflowDuplicateProjectionStorage, error) {
	postgres, err := workflowProjectionObservationPostgres(selected, run, entity)
	if err != nil {
		return WorkflowDuplicateProjectionStorage{}, err
	}
	var out WorkflowDuplicateProjectionStorage
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadWorkflowDuplicateProjectionStorageForTest(ctx, tx, postgres, run, entity)
		return err
	})
	if err != nil {
		return WorkflowDuplicateProjectionStorage{}, err
	}
	return out, nil
}

func CountWorkflowInstanceHeadersForTest(ctx context.Context, selected any) (int64, error) {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return 0, err
	}
	var count int64
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = pipelinepersistence.CountWorkflowInstanceHeadersForTest(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadWorkflowEnginePhysicalCountsForTest(ctx context.Context, selected any) (WorkflowEnginePhysicalCounts, error) {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return WorkflowEnginePhysicalCounts{}, err
	}
	var out WorkflowEnginePhysicalCounts
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadWorkflowEnginePhysicalCountsForTest(ctx, tx)
		return err
	})
	if err != nil {
		return WorkflowEnginePhysicalCounts{}, err
	}
	return out, nil
}
