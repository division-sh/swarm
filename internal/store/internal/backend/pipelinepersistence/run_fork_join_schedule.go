package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *PipelinePostgresOwner) MaterializeRunForkArrivalJoinScheduleTx(ctx context.Context, attempt *mutationprotocol.Attempt, request storegenericschedule.ForkJoinRequest) (genericschedule.Activation, error) {
	if s == nil || s.RunLifecyclePostgresOwner == nil {
		return genericschedule.Activation{}, fmt.Errorf("fork arrival schedule requires the postgres lifecycle owner")
	}
	if err := requireRunForkArrivalScheduleSource(ctx, attempt, s.RunLifecyclePostgresOwner, request.Child.RunID); err != nil {
		return genericschedule.Activation{}, err
	}
	return storegenericschedule.RestoreForkJoinTx(ctx, attempt, true, request)
}

func (s *PipelineSQLiteOwner) MaterializeRunForkArrivalJoinScheduleTx(ctx context.Context, attempt *mutationprotocol.Attempt, request storegenericschedule.ForkJoinRequest) (genericschedule.Activation, error) {
	if s == nil || s.RunLifecycleSQLiteOwner == nil {
		return genericschedule.Activation{}, fmt.Errorf("fork arrival schedule requires the sqlite lifecycle owner")
	}
	if err := requireRunForkArrivalScheduleSource(ctx, attempt, s.RunLifecycleSQLiteOwner, request.Child.RunID); err != nil {
		return genericschedule.Activation{}, err
	}
	return storegenericschedule.RestoreForkJoinTx(ctx, attempt, false, request)
}

func (s *PipelinePostgresOwner) ReadRunForkArrivalJoinScheduleInventoryTx(ctx context.Context, attempt *mutationprotocol.Attempt, childRunID string) ([]genericschedule.Activation, error) {
	if s == nil || s.RunLifecyclePostgresOwner == nil {
		return nil, fmt.Errorf("fork arrival readback requires the postgres lifecycle owner")
	}
	if err := requireRunForkArrivalScheduleSource(ctx, attempt, s.RunLifecyclePostgresOwner, childRunID); err != nil {
		return nil, err
	}
	return storegenericschedule.ReadForkJoinInventoryTx(ctx, attempt, true, childRunID)
}

func (s *PipelineSQLiteOwner) ReadRunForkArrivalJoinScheduleInventoryTx(ctx context.Context, attempt *mutationprotocol.Attempt, childRunID string) ([]genericschedule.Activation, error) {
	if s == nil || s.RunLifecycleSQLiteOwner == nil {
		return nil, fmt.Errorf("fork arrival readback requires the sqlite lifecycle owner")
	}
	if err := requireRunForkArrivalScheduleSource(ctx, attempt, s.RunLifecycleSQLiteOwner, childRunID); err != nil {
		return nil, err
	}
	return storegenericschedule.ReadForkJoinInventoryTx(ctx, attempt, false, childRunID)
}

func requireRunForkArrivalScheduleSource(ctx context.Context, attempt *mutationprotocol.Attempt, owner runForkWorkflowTimerSourceOwner, childRunID string) error {
	if attempt == nil {
		return fmt.Errorf("fork arrival schedule requires its native mutation attempt")
	}
	if err := attempt.RequireExistingSQLFrame(ctx); err != nil {
		return err
	}
	if childRunID == "" || correlation.RunIDFromContext(ctx) != childRunID {
		return fmt.Errorf("fork arrival schedule source frame belongs to another child run")
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return admitRunForkWorkflowTimerSource(ctx, tx, owner, childRunID)
	})
}
