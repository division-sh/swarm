package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

func SetWorkflowProjectionFieldsArrayForTest(ctx context.Context, selected any, run, key string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, key, true, pipelinepersistence.SetWorkflowProjectionFieldsArrayForTest)
}

func SetWorkflowProjectionNumericGateForTest(ctx context.Context, selected any, run, key string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, key, false, pipelinepersistence.SetWorkflowProjectionNumericGateForTest)
}

func SetWorkflowProjectionAccumulatorArrayForTest(ctx context.Context, selected any, run, key string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, key, false, pipelinepersistence.SetWorkflowProjectionAccumulatorArrayForTest)
}

func SetWorkflowProjectionMalformedTransitionHistoryForTest(ctx context.Context, selected any, run, key string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, key, false, pipelinepersistence.SetWorkflowProjectionMalformedTransitionHistoryForTest)
}

func SetWorkflowProjectionConflictingInstanceIDForTest(ctx context.Context, selected any, run, key string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, key, false, pipelinepersistence.SetWorkflowProjectionConflictingInstanceIDForTest)
}

func SetWorkflowProjectionSlashOnlyFlowPathForTest(ctx context.Context, selected any, run, key string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, key, false, pipelinepersistence.SetWorkflowProjectionSlashOnlyFlowPathForTest)
}

func applyWorkflowProjectionShapeFault(ctx context.Context, selected any, run, key string, entity bool, fault func(context.Context, *sql.Tx, string, string) (int64, error)) (int64, error) {
	if err := validateWorkflowProjectionFaultKey(run, key, entity); err != nil {
		return 0, err
	}
	var changed int64
	write := func(ctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = fault(ctx, tx, run, key)
		return err
	}
	err := runWorkflowProjectionFault(ctx, selected, write)
	if err != nil {
		return 0, err
	}
	return changed, nil
}

func validateWorkflowProjectionFaultKey(run, key string, entity bool) error {
	if err := validateWorkflowProjectionFaultRun(run); err != nil {
		return err
	}
	if entity {
		id, err := uuid.Parse(key)
		if err != nil || id == uuid.Nil || id.String() != key {
			return fmt.Errorf("workflow projection field fault requires an exact entity")
		}
	} else if key == "" || strings.TrimSpace(key) != key {
		return fmt.Errorf("workflow projection header fault requires an exact storage path")
	}
	return nil
}

func validateWorkflowProjectionFaultRun(run string) error {
	id, err := uuid.Parse(run)
	if err != nil || id == uuid.Nil || id.String() != run {
		return fmt.Errorf("workflow projection fault requires an exact canonical run")
	}
	return nil
}

func runWorkflowProjectionFault(ctx context.Context, selected any, write func(context.Context, *sql.Tx) error) error {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.backend.RunTransaction(ctx, write)
	case *SQLiteRuntimeStore:
		return owner.backend.RunTransaction(ctx, "workflow projection shape fault fixture", write)
	default:
		return fmt.Errorf("workflow projection fault requires the original native owner")
	}
}

func SetWorkflowProjectionObsoleteFieldRowsForTest(ctx context.Context, selected any, run string) (int64, error) {
	if err := validateWorkflowProjectionFaultRun(run); err != nil {
		return 0, err
	}
	var changed int64
	err := runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = pipelinepersistence.SetWorkflowProjectionObsoleteFieldRowsForTest(ctx, tx, run)
		return err
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}

func SetWorkflowProjectionPlatformBookkeepingForTest(ctx context.Context, selected any, run, path string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, path, false, pipelinepersistence.SetWorkflowProjectionPlatformBookkeepingForTest)
}

func SetEntityProjectionPrivateBookkeepingForTest(ctx context.Context, selected any, run, entity string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, entity, true, pipelinepersistence.SetEntityProjectionPrivateBookkeepingForTest)
}

func SetWorkflowProjectionConflictingEntityTypeForTest(ctx context.Context, selected any, run, entity string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, entity, true, pipelinepersistence.SetWorkflowProjectionConflictingEntityTypeForTest)
}

func RemoveWorkflowProjectionHeaderForTest(ctx context.Context, selected any, run, path string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, path, false, pipelinepersistence.RemoveWorkflowProjectionHeaderForTest)
}

func RemoveWorkflowProjectionFieldsForTest(ctx context.Context, selected any, run, entity string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, entity, true, pipelinepersistence.RemoveWorkflowProjectionFieldsForTest)
}

func SetWorkflowProjectionDrainingForTest(ctx context.Context, selected any, run, path string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, path, false, pipelinepersistence.SetWorkflowProjectionDrainingForTest)
}

func SetWorkflowProjectionTerminatedForTest(ctx context.Context, selected any, run, path string, at time.Time) (int64, error) {
	if at.IsZero() {
		return 0, fmt.Errorf("terminated projection fault requires an exact time")
	}
	return applyWorkflowProjectionShapeFault(ctx, selected, run, path, false, func(ctx context.Context, tx *sql.Tx, run, path string) (int64, error) {
		return pipelinepersistence.SetWorkflowProjectionTerminatedForTest(ctx, tx, run, path, at)
	})
}

func SetWorkflowProjectionActiveTerminatedTimestampForTest(ctx context.Context, selected any, run, path string, at time.Time) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, path, false, func(ctx context.Context, tx *sql.Tx, run, path string) (int64, error) {
		return pipelinepersistence.SetWorkflowProjectionActiveTerminatedTimestampForTest(ctx, tx, run, path, at)
	})
}

func SetWorkflowProjectionNumericFlowPathForTest(ctx context.Context, selected any, run, path string) (int64, error) {
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return 0, err
	}
	return applyWorkflowProjectionShapeFault(ctx, selected, run, path, false, func(ctx context.Context, tx *sql.Tx, run, path string) (int64, error) {
		return pipelinepersistence.SetWorkflowProjectionNumericFlowPathForTest(ctx, tx, postgres, run, path)
	})
}

func AddAmbiguousWorkflowProjectionFieldsForTest(ctx context.Context, selected any, run, entity string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, entity, true, pipelinepersistence.AddAmbiguousWorkflowProjectionFieldsForTest)
}

func SetWorkflowTransitionEvidenceWireForTest(ctx context.Context, selected any, run, path string, wire json.RawMessage) (int64, error) {
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return 0, err
	}
	return applyWorkflowProjectionShapeFault(ctx, selected, run, path, false, func(ctx context.Context, tx *sql.Tx, run, path string) (int64, error) {
		return pipelinepersistence.SetWorkflowTransitionEvidenceWireForTest(ctx, tx, postgres, run, path, wire)
	})
}
