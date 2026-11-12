package runtimepersistence

import (
	"context"
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
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.CountWorkflowHeadersCreatedSinceForTest(ctx, since)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.CountWorkflowHeadersCreatedSinceForTest(ctx, since)
	default:
		return 0, fmt.Errorf("projection observation requires the original native owner")
	}
}

func CountWorkflowHeadersForPathCreatedSinceForTest(ctx context.Context, selected any, path string, since time.Time) (int, error) {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return 0, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.CountWorkflowHeadersForPathCreatedSinceForTest(ctx, path, since)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.CountWorkflowHeadersForPathCreatedSinceForTest(ctx, path, since)
	default:
		return 0, fmt.Errorf("projection observation requires the original native owner")
	}
}

func ReadLatestWorkflowFieldsForPathForTest(ctx context.Context, selected any, path string) (json.RawMessage, bool, error) {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return nil, false, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadLatestWorkflowFieldsForPathForTest(ctx, path)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadLatestWorkflowFieldsForPathForTest(ctx, path)
	default:
		return nil, false, fmt.Errorf("projection observation requires the original native owner")
	}
}

func ReadWorkflowStateObservationRowsForTest(ctx context.Context, selected any) ([]WorkflowStateObservationRow, error) {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return nil, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadWorkflowStateObservationRowsForTest(ctx)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadWorkflowStateObservationRowsForTest(ctx)
	default:
		return nil, fmt.Errorf("projection observation requires the original native owner")
	}
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
	if _, err := workflowProjectionObservationPostgres(selected, run, entity); err != nil {
		return WorkflowControlProjectionStorage{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadWorkflowControlProjectionStorageForTest(ctx, run, entity)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadWorkflowControlProjectionStorageForTest(ctx, run, entity)
	default:
		return WorkflowControlProjectionStorage{}, fmt.Errorf("projection observation requires the original native owner")
	}
}

func ReadWorkflowTransitionEvidenceWireForTest(ctx context.Context, selected any, run, path string) (json.RawMessage, error) {
	if err := validateWorkflowProjectionFaultKey(run, path, false); err != nil {
		return nil, err
	}
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return nil, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadWorkflowTransitionEvidenceWireForTest(ctx, run, path)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadWorkflowTransitionEvidenceWireForTest(ctx, run, path)
	default:
		return nil, fmt.Errorf("projection observation requires the original native owner")
	}
}

func ReadWorkflowDuplicateProjectionStorageForTest(ctx context.Context, selected any, run, entity string) (WorkflowDuplicateProjectionStorage, error) {
	if _, err := workflowProjectionObservationPostgres(selected, run, entity); err != nil {
		return WorkflowDuplicateProjectionStorage{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadWorkflowDuplicateProjectionStorageForTest(ctx, run, entity)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadWorkflowDuplicateProjectionStorageForTest(ctx, run, entity)
	default:
		return WorkflowDuplicateProjectionStorage{}, fmt.Errorf("projection observation requires the original native owner")
	}
}

func CountWorkflowInstanceHeadersForTest(ctx context.Context, selected any) (int64, error) {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return 0, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.CountWorkflowInstanceHeadersForTest(ctx)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.CountWorkflowInstanceHeadersForTest(ctx)
	default:
		return 0, fmt.Errorf("projection observation requires the original native owner")
	}
}

func ReadWorkflowEnginePhysicalCountsForTest(ctx context.Context, selected any) (WorkflowEnginePhysicalCounts, error) {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return WorkflowEnginePhysicalCounts{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadWorkflowEnginePhysicalCountsForTest(ctx)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadWorkflowEnginePhysicalCountsForTest(ctx)
	default:
		return WorkflowEnginePhysicalCounts{}, fmt.Errorf("projection observation requires the original native owner")
	}
}
