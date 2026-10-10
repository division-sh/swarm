package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

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

func (s *PipelinePostgresOwner) RequireRunForkArrivalJoinScheduleTx(ctx context.Context, attempt *mutationprotocol.Attempt, request storegenericschedule.ForkJoinRequest) (genericschedule.Activation, error) {
	if s == nil || s.RunLifecyclePostgresOwner == nil {
		return genericschedule.Activation{}, fmt.Errorf("fork arrival readback requires the postgres lifecycle owner")
	}
	if err := requireRunForkArrivalScheduleSource(ctx, attempt, s.RunLifecyclePostgresOwner, request.Child.RunID); err != nil {
		return genericschedule.Activation{}, err
	}
	return storegenericschedule.RequireForkJoinTx(ctx, attempt, true, request)
}

func (s *PipelineSQLiteOwner) RequireRunForkArrivalJoinScheduleTx(ctx context.Context, attempt *mutationprotocol.Attempt, request storegenericschedule.ForkJoinRequest) (genericschedule.Activation, error) {
	if s == nil || s.RunLifecycleSQLiteOwner == nil {
		return genericschedule.Activation{}, fmt.Errorf("fork arrival readback requires the sqlite lifecycle owner")
	}
	if err := requireRunForkArrivalScheduleSource(ctx, attempt, s.RunLifecycleSQLiteOwner, request.Child.RunID); err != nil {
		return genericschedule.Activation{}, err
	}
	return storegenericschedule.RequireForkJoinTx(ctx, attempt, false, request)
}

func requireRunForkArrivalScheduleSource(ctx context.Context, attempt *mutationprotocol.Attempt, owner runForkWorkflowTimerSourceOwner, childRunID string) error {
	if attempt == nil {
		return fmt.Errorf("fork arrival schedule requires its native mutation attempt")
	}
	if err := attempt.RequireExistingSQLFrame(ctx); err != nil {
		return err
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return admitRunForkWorkflowTimerSource(ctx, tx, owner, childRunID)
	})
}
