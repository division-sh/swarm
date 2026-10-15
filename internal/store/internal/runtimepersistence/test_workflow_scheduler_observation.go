package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/google/uuid"
)

type WorkflowTimerActivationStorage = genericschedule.WorkflowTimerActivationStorage
type WorkflowTimerOccurrenceStorage = genericschedule.WorkflowTimerOccurrenceStorage

func ReadWorkflowTimerActivationStorageForTest(ctx context.Context, selected any, runID, entityID string) (WorkflowTimerActivationStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID, entityID}, func(ctx context.Context, tx *sql.Tx) (WorkflowTimerActivationStorage, error) {
		return genericschedule.ReadWorkflowTimerActivationStorageTx(ctx, tx, postgres, runID, entityID)
	})
}

func ReadWorkflowTimerOccurrenceStorageForTest(ctx context.Context, selected any, runID, entityID string) (WorkflowTimerOccurrenceStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID, entityID}, func(ctx context.Context, tx *sql.Tx) (WorkflowTimerOccurrenceStorage, error) {
		return genericschedule.ReadWorkflowTimerOccurrenceStorageTx(ctx, tx, postgres, runID, entityID)
	})
}

func CountWorkflowTimerStatusRowsForTest(ctx context.Context, selected any, runID, entityID, status string) (int, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID, entityID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return genericschedule.CountWorkflowTimerStatusRowsTx(ctx, tx, postgres, runID, entityID, status)
	})
}

func CountRunTimerStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return genericschedule.CountTimerRowsForRun(ctx, tx, runID)
	})
}

func CountRunEventTimerStorageForTest(ctx context.Context, selected any, runID, eventType string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return genericschedule.CountRunEventTimerStorageTx(ctx, tx, runID, eventType)
	})
}

func SetA2ActivationJoinScheduleFailureForTest(ctx context.Context, selected any, enabled bool) error {
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return genericschedule.SetA2ActivationJoinScheduleFailureForTest(ctx, tx, postgres, enabled)
	})
}

func SetA2JoinCloseScheduleFailureForTest(ctx context.Context, selected any, enabled bool) error {
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return genericschedule.SetA2JoinCloseScheduleFailureForTest(ctx, tx, postgres, enabled)
	})
}

func CountWorkflowSchedulerTimersForTest(ctx context.Context, selected any, entity, path string) (int64, error) {
	id, err := uuid.Parse(entity)
	if err != nil || id == uuid.Nil || id.String() != entity || strings.Trim(strings.TrimSpace(path), "/") != path || path == "" {
		return 0, fmt.Errorf("scheduler observation requires exact entity and instance path")
	}
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return 0, err
	}
	var count int64
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = genericschedule.CountWorkflowSchedulerTimersForTest(ctx, tx, postgres, entity, path)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
