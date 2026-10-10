package runforkpersistence

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type runForkArrivalJoinMaterializationOwner interface {
	MaterializeRunForkArrivalJoinScheduleTx(context.Context, *mutationprotocol.Attempt, storegenericschedule.ForkJoinRequest) (genericschedule.Activation, error)
	RequireRunForkArrivalJoinScheduleTx(context.Context, *mutationprotocol.Attempt, storegenericschedule.ForkJoinRequest) (genericschedule.Activation, error)
}

func prepareRunForkArrivalJoinRequests(plan runfork.RunForkPlan, childRunID string, bornAt time.Time) ([]storegenericschedule.ForkJoinRequest, error) {
	projected, err := prepareRunForkArrivalJoinSchedules(plan, childRunID)
	if err != nil {
		return nil, err
	}
	requests := make([]storegenericschedule.ForkJoinRequest, 0, len(projected))
	for _, row := range projected {
		request := storegenericschedule.ForkJoinRequest{
			Source: row.source, Child: row.command, BornAt: bornAt,
			PointKind: forkpoint.Kind(plan.ForkPoint.Kind), PointRevision: plan.ForkPoint.Revision, PointEventID: plan.ForkPoint.EventID,
		}
		if err := request.Validate(); err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	return requests, nil
}

func materializeRunForkArrivalJoinSchedules(ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan, childRunID string, bornAt time.Time, owner runForkArrivalJoinMaterializationOwner) error {
	requests, err := prepareRunForkArrivalJoinRequests(plan, childRunID, bornAt)
	if err != nil {
		return err
	}
	if len(requests) != 0 && owner == nil {
		return fmt.Errorf("arrival join restoration requires its canonical native schedule owner")
	}
	for _, request := range requests {
		if _, err := owner.MaterializeRunForkArrivalJoinScheduleTx(ctx, attempt, request); err != nil {
			return err
		}
	}
	return nil
}

// This is require-only staged readback. Continuing lifecycle progress and the
// complete inherited inventory must also be proven before execution admission.
func requireMaterializedRunForkArrivalJoinSchedules(ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan, childRunID string, bornAt time.Time, owner runForkArrivalJoinMaterializationOwner) error {
	requests, err := prepareRunForkArrivalJoinRequests(plan, childRunID, bornAt)
	if err != nil {
		return err
	}
	if len(requests) != 0 && owner == nil {
		return fmt.Errorf("arrival join readback requires its canonical native schedule owner")
	}
	for _, request := range requests {
		if _, err := owner.RequireRunForkArrivalJoinScheduleTx(ctx, attempt, request); err != nil {
			return err
		}
	}
	return nil
}
