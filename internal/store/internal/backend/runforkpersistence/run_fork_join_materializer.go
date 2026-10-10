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
	ReadRunForkArrivalJoinScheduleInventoryTx(context.Context, *mutationprotocol.Attempt, string) ([]genericschedule.Activation, error)
}

type runForkArrivalScheduleReadbackPhase uint8

const (
	runForkArrivalScheduleAtCut runForkArrivalScheduleReadbackPhase = iota + 1
	runForkArrivalScheduleContinuing
)

type runForkArrivalScheduleKey struct{ scope, key string }

func prepareRunForkArrivalJoinRequests(plan runfork.RunForkPlan, childRunID string, bornAt time.Time) ([]storegenericschedule.ForkJoinRequest, error) {
	projected, err := prepareRunForkArrivalJoinSchedules(plan, childRunID)
	if err != nil {
		return nil, err
	}
	requests := make([]storegenericschedule.ForkJoinRequest, 0, len(projected))
	for _, row := range projected {
		if row.source.Status == genericschedule.StatusFired {
			publication, found := plan.HistoricalArrivalPublication(row.source.CurrentEventID)
			if !found {
				return nil, fmt.Errorf("published arrival requires its fixed-cut historical publication")
			}
			if _, err := row.source.ValidatePublishedOccurrence(publication.Event()); err != nil {
				return nil, err
			}
			// Its event and existing delivery are continued by selected publication;
			// there is deliberately no child schedule to rearm or claim.
			continue
		}
		request := storegenericschedule.ForkJoinRequest{
			Source: row.source, Child: row.command, BornAt: bornAt,
			Disposition: genericschedule.ForkJoinRetained,
			PointKind:   forkpoint.Kind(plan.ForkPoint.Kind), PointRevision: plan.ForkPoint.Revision, PointEventID: plan.ForkPoint.EventID,
		}
		if err := request.Validate(); err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	return requests, nil
}

func selectRunForkArrivalJoinRequests(plan runfork.RunForkPlan, childRunID string, bornAt time.Time, selection runfork.InheritedScheduleSelection) ([]storegenericschedule.ForkJoinRequest, error) {
	requests, err := prepareRunForkArrivalJoinRequests(plan, childRunID, bornAt)
	if err != nil {
		return nil, err
	}
	if len(requests) != 0 && selection == nil {
		return nil, fmt.Errorf("arrival join restoration requires sealed selected admission")
	}
	for index := range requests {
		requests[index].Disposition, err = selection.SelectInheritedArrivalJoin(requests[index].Source)
		if err != nil {
			return nil, err
		}
		if err := requests[index].Validate(); err != nil {
			return nil, err
		}
	}
	return requests, nil
}

func materializeRunForkArrivalJoinSchedules(ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan, childRunID string, bornAt time.Time, selection runfork.InheritedScheduleSelection, owner runForkArrivalJoinMaterializationOwner) error {
	requests, err := selectRunForkArrivalJoinRequests(plan, childRunID, bornAt, selection)
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

// Complete readback creates no work and does not discharge source admission or
// grant execution. Ordinary child schedules are not inherited inventory.
func readRunForkArrivalJoinInventory(ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan, childRunID string, bornAt time.Time, selection runfork.InheritedScheduleSelection, owner runForkArrivalJoinMaterializationOwner, phase runForkArrivalScheduleReadbackPhase) ([]genericschedule.Activation, error) {
	if phase != runForkArrivalScheduleAtCut && phase != runForkArrivalScheduleContinuing {
		return nil, fmt.Errorf("arrival schedule readback requires an exact lifecycle phase")
	}
	if err := requireRunForkWorkflowTimerReadbackFrame(ctx, attempt, plan, childRunID, bornAt); err != nil {
		return nil, err
	}
	requests, err := selectRunForkArrivalJoinRequests(plan, childRunID, bornAt, selection)
	if err != nil {
		return nil, err
	}
	if owner == nil {
		return nil, fmt.Errorf("arrival join readback requires its canonical native schedule owner")
	}
	actual, err := owner.ReadRunForkArrivalJoinScheduleInventoryTx(ctx, attempt, childRunID)
	if err != nil {
		return nil, err
	}
	if err := requireExactRunForkArrivalJoinInventory(requests, actual, phase); err != nil {
		return nil, err
	}
	if err := requireRunForkRemovedArrivalDependents(ctx, attempt, plan, childRunID, requests); err != nil {
		return nil, err
	}
	byKey, err := indexRunForkArrivalJoinInventory(actual)
	if err != nil {
		return nil, err
	}
	expected := make([]genericschedule.Activation, 0, len(requests))
	for _, request := range requests {
		scope, err := request.Child.ScopeKey()
		if err != nil {
			return nil, err
		}
		row, err := request.Expected(byKey[runForkArrivalScheduleKey{scope, request.Child.ScheduleKey}].ID)
		if err != nil {
			return nil, err
		}
		expected = append(expected, row)
	}
	return expected, attempt.RequireExistingSQLFrame(ctx)
}

func requireExactRunForkArrivalJoinInventory(requests []storegenericschedule.ForkJoinRequest, actual []genericschedule.Activation, phase runForkArrivalScheduleReadbackPhase) error {
	if phase != runForkArrivalScheduleAtCut && phase != runForkArrivalScheduleContinuing {
		return fmt.Errorf("arrival schedule inventory requires an exact lifecycle phase")
	}
	if len(requests) != len(actual) {
		return fmt.Errorf("inherited arrival schedule inventory differs from the complete fixed-cut projection")
	}
	byKey, err := indexRunForkArrivalJoinInventory(actual)
	if err != nil {
		return err
	}
	for _, request := range requests {
		scope, err := request.Child.ScopeKey()
		if err != nil {
			return err
		}
		key := runForkArrivalScheduleKey{scope, request.Child.ScheduleKey}
		row, found := byKey[key]
		if !found {
			return fmt.Errorf("expected inherited arrival schedule is absent")
		}
		delete(byKey, key)
		expected, err := request.Expected(row.ID)
		if err != nil {
			return err
		}
		if err := row.ValidateForkJoinReplay(expected); err != nil {
			return err
		}
		if phase == runForkArrivalScheduleAtCut {
			want, err := expected.EvidenceDigest()
			if err != nil {
				return err
			}
			got, err := row.EvidenceDigest()
			if err != nil || want != got {
				return fmt.Errorf("unactivated inherited arrival schedule progressed beyond its cut")
			}
		}
	}
	if len(byKey) != 0 {
		return fmt.Errorf("arrival schedule readback omitted an inherited child row")
	}
	return nil
}

func indexRunForkArrivalJoinInventory(actual []genericschedule.Activation) (map[runForkArrivalScheduleKey]genericschedule.Activation, error) {
	byKey := make(map[runForkArrivalScheduleKey]genericschedule.Activation, len(actual))
	ids := make(map[string]struct{}, len(actual))
	for _, row := range actual {
		if err := row.Validate(); err != nil {
			return nil, err
		}
		scope, err := row.Command.ScopeKey()
		if err != nil {
			return nil, err
		}
		key := runForkArrivalScheduleKey{scope, row.Command.ScheduleKey}
		if _, duplicate := byKey[key]; duplicate {
			return nil, fmt.Errorf("inherited arrival schedule inventory repeats a scoped key")
		}
		if _, duplicate := ids[row.ID]; duplicate {
			return nil, fmt.Errorf("inherited arrival schedule inventory repeats an activation")
		}
		ids[row.ID], byKey[key] = struct{}{}, row
	}
	return byKey, nil
}
