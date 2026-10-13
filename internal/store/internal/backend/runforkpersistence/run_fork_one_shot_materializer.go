package runforkpersistence

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type runForkOneShotMaterializationOwner interface {
	MaterializeRunForkOneShotScheduleTx(context.Context, *mutationprotocol.Attempt, storegenericschedule.ForkOneShotRequest) (genericschedule.Activation, error)
}

func prepareRunForkOneShotRequests(plan runfork.RunForkPlan, childRunID string, bornAt time.Time) ([]storegenericschedule.ForkOneShotRequest, error) {
	requests := make([]storegenericschedule.ForkOneShotRequest, 0, len(plan.OneShotSchedules))
	seen := make(map[string]bool, len(plan.OneShotSchedules))
	for _, source := range plan.OneShotSchedules {
		if seen[source.ID] {
			return nil, fmt.Errorf("one-shot restoration repeats a captured activation")
		}
		seen[source.ID] = true
		if source.Command.RunID != plan.SourceRunID {
			return nil, fmt.Errorf("one-shot restoration source contradicts the fixed-cut run")
		}
		if err := source.ValidateForkOneShotRestorationSource(); err != nil {
			return nil, err
		}
		child := source.Command
		identity, err := agentidentity.New(childRunID, child.AgentIdentity.Name, child.AgentIdentity.Route)
		if err != nil {
			return nil, err
		}
		routing, err := runfork.ProjectProducerOwnership(plan.SourceRunID, childRunID, child.RoutingSource)
		if err != nil {
			return nil, err
		}
		if child.EntityID == plan.SourceRunID {
			child.EntityID = childRunID
		}
		child.RunID, child.AgentIdentity, child.RoutingSource = childRunID, identity, routing
		child.FlowInstance, child.OwnerID = identity.FlowInstance(), identity.AgentID()
		child.ReplyContext, err = projectRunForkReplyReference(plan.SourceRunID, childRunID, plan.ReplyContexts, source.Command.ReplyContext)
		if err != nil {
			return nil, err
		}
		request := storegenericschedule.ForkOneShotRequest{
			Source: source, Child: child, BornAt: bornAt,
			PointKind: forkpoint.Kind(plan.ForkPoint.Kind), PointRevision: plan.ForkPoint.Revision, PointEventID: plan.ForkPoint.EventID,
		}
		if err := request.Validate(); err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	return requests, nil
}

func materializeRunForkOneShotSchedules(ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan, childRunID string, bornAt time.Time, owner runForkArrivalJoinMaterializationOwner) error {
	requests, err := prepareRunForkOneShotRequests(plan, childRunID, bornAt)
	if err != nil || len(requests) == 0 {
		return err
	}
	native, supported := owner.(runForkOneShotMaterializationOwner)
	if !supported || native == nil {
		return fmt.Errorf("one-shot restoration requires the existing native generic-schedule owner")
	}
	for _, request := range requests {
		if _, err := native.MaterializeRunForkOneShotScheduleTx(ctx, attempt, request); err != nil {
			return err
		}
	}
	return nil
}

func readRunForkOneShotInventory(ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan, childRunID string, bornAt time.Time, owner runForkArrivalJoinMaterializationOwner, phase runForkWorkflowTimerReadbackPhase) ([]genericschedule.Activation, error) {
	requests, err := prepareRunForkOneShotRequests(plan, childRunID, bornAt)
	if err != nil {
		return nil, err
	}
	if owner == nil {
		return nil, fmt.Errorf("one-shot readback requires its native schedule owner")
	}
	all, err := owner.ReadRunForkArrivalJoinScheduleInventoryTx(ctx, attempt, childRunID)
	if err != nil {
		return nil, err
	}
	actual := make([]genericschedule.Activation, 0, len(requests))
	for _, row := range all {
		if row.ForkOneShotOrigin != nil {
			actual = append(actual, row)
		}
	}
	if len(actual) != len(requests) {
		return nil, fmt.Errorf("inherited one-shot inventory differs from complete fixed-cut projection")
	}
	byKey, err := indexRunForkArrivalJoinInventory(actual)
	if err != nil {
		return nil, err
	}
	var expectedRows []genericschedule.Activation
	for _, request := range requests {
		scope, err := request.Child.ScopeKey()
		if err != nil {
			return nil, err
		}
		key := runForkArrivalScheduleKey{scope, request.Child.ScheduleKey}
		actual, found := byKey[key]
		if !found {
			return nil, fmt.Errorf("inherited one-shot row is absent")
		}
		delete(byKey, key)
		expected, err := request.Expected(actual.ID)
		if err != nil {
			return nil, err
		}
		if err := actual.ValidateForkOneShotReplay(expected); err != nil {
			return nil, err
		}
		if phase == runForkWorkflowTimerAtCut {
			want, err := expected.EvidenceDigest()
			if err != nil {
				return nil, err
			}
			got, err := actual.EvidenceDigest()
			if err != nil || got != want {
				return nil, fmt.Errorf("unactivated inherited one-shot progressed beyond its cut")
			}
		}
		expectedRows = append(expectedRows, expected)
	}
	return expectedRows, attempt.RequireExistingSQLFrame(ctx)
}
