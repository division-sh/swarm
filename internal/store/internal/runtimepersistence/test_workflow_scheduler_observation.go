package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/google/uuid"
)

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
