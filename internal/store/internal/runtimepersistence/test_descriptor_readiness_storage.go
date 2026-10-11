package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

type DescriptorReadinessStampStorage = pipelinepersistence.DescriptorReadinessStampStorage

func ReadDescriptorReadinessStampStorageForTest(ctx context.Context, selected any, run, path string) (DescriptorReadinessStampStorage, error) {
	if err := validateWorkflowProjectionFaultKey(run, path, false); err != nil {
		return DescriptorReadinessStampStorage{}, err
	}
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return DescriptorReadinessStampStorage{}, err
	}
	var out DescriptorReadinessStampStorage
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadDescriptorReadinessStampStorage(ctx, tx, postgres, run, path)
		return err
	})
	if err != nil {
		return DescriptorReadinessStampStorage{}, err
	}
	return out, nil
}

func SetDescriptorReadinessMalformedPlanForTest(ctx context.Context, selected any, run, path string, wire json.RawMessage) (int64, error) {
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return 0, err
	}
	return applyWorkflowProjectionShapeFault(ctx, selected, run, path, false, func(ctx context.Context, tx *sql.Tx, run, path string) (int64, error) {
		return pipelinepersistence.SetDescriptorReadinessMalformedPlan(ctx, tx, postgres, run, path, wire)
	})
}

func SetDescriptorReadinessConcurrentPlanForTest(ctx context.Context, selected any, plan pipeline.DynamicFlowRuntimeReadinessPlan) (int64, error) {
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return 0, err
	}
	return applyWorkflowProjectionShapeFault(ctx, selected, plan.RunID, plan.Identity.InstancePath, false, func(ctx context.Context, tx *sql.Tx, _, _ string) (int64, error) {
		return pipelinepersistence.SetDescriptorReadinessConcurrentPlan(ctx, tx, postgres, plan)
	})
}

func RemoveDescriptorReadinessForTest(ctx context.Context, selected any, run, path string) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, path, false, pipelinepersistence.RemoveDescriptorReadiness)
}

func SetDescriptorReadinessCreationClockForTest(ctx context.Context, selected any, run, path string, at time.Time) (int64, error) {
	return applyWorkflowProjectionShapeFault(ctx, selected, run, path, false, func(ctx context.Context, tx *sql.Tx, run, path string) (int64, error) {
		return pipelinepersistence.SetDescriptorReadinessCreationClock(ctx, tx, run, path, at)
	})
}

func InstallDescriptorReadinessBatchUpdateFailureForTest(ctx context.Context, selected any, run, path string) (func(context.Context) error, error) {
	if err := validateWorkflowProjectionFaultKey(run, path, false); err != nil {
		return nil, err
	}
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return nil, err
	}
	id := uuid.NewString()
	err = runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.InstallDescriptorReadinessBatchUpdateFailure(ctx, tx, postgres, run, path, id)
	})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			return pipelinepersistence.RemoveDescriptorReadinessBatchUpdateFailure(ctx, tx, postgres, id)
		})
	}, nil
}

func InsertDescriptorEntityStateWithoutTypeForTest(ctx context.Context, selected any, run string) error {
	if err := validateWorkflowProjectionFaultRun(run); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.InsertDescriptorEntityStateWithoutType(ctx, tx, run)
	})
}

func InsertDescriptorEntityStateWithBlankTypeForTest(ctx context.Context, selected any, run string) error {
	if err := validateWorkflowProjectionFaultRun(run); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.InsertDescriptorEntityStateWithBlankType(ctx, tx, run)
	})
}
