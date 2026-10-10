package runforkpersistence

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func requireMaterializedRunForkTimerHistory(ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan, childRunID string,
	selection runfork.InheritedScheduleSelection, workflow runForkWorkflowTimerMaterializationOwner, arrival runForkArrivalJoinMaterializationOwner,
	bornAt time.Time, admission runfork.RunForkReplayResumeAdmission) (runfork.RunForkReplayResumeAdmission, error) {
	return requireRunForkTimerHistory(ctx, attempt, plan, childRunID, selection, workflow, arrival, bornAt, admission, runForkWorkflowTimerAtCut)
}

func requireContinuingRunForkTimerHistory(ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan, childRunID string,
	selection runfork.InheritedScheduleSelection, workflow runForkWorkflowTimerMaterializationOwner, arrival runForkArrivalJoinMaterializationOwner,
	bornAt time.Time, admission runfork.RunForkReplayResumeAdmission) (runfork.RunForkReplayResumeAdmission, error) {
	return requireRunForkTimerHistory(ctx, attempt, plan, childRunID, selection, workflow, arrival, bornAt, admission, runForkWorkflowTimerContinuing)
}

// Neither family can discharge source history alone. All errors retain the
// input admission; these require-only reads never repair missing child work.
func requireRunForkTimerHistory(ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan, childRunID string,
	selection runfork.InheritedScheduleSelection, workflow runForkWorkflowTimerMaterializationOwner, arrival runForkArrivalJoinMaterializationOwner,
	bornAt time.Time, admission runfork.RunForkReplayResumeAdmission, phase runForkWorkflowTimerReadbackPhase) (runfork.RunForkReplayResumeAdmission, error) {
	if phase != runForkWorkflowTimerAtCut && phase != runForkWorkflowTimerContinuing {
		return admission, fmt.Errorf("timer history readback requires an exact lifecycle phase")
	}
	if err := requireRunForkWorkflowTimerReadbackFrame(ctx, attempt, plan, childRunID, bornAt); err != nil {
		return admission, err
	}
	materializable, err := runForkTimerHistoryMaterializable(plan)
	if err != nil {
		return admission, err
	}
	if !materializable && hasRunForkTimerHistory(plan, admission) {
		return admission, fmt.Errorf("timer history readback requires complete exact source inventory admission")
	}
	projected, err := readRunForkWorkflowTimerInventory(ctx, attempt, plan, childRunID, selection, workflow, bornAt, phase)
	if err != nil {
		return admission, err
	}
	arrivalPhase := runForkArrivalScheduleAtCut
	if phase == runForkWorkflowTimerContinuing {
		arrivalPhase = runForkArrivalScheduleContinuing
	}
	arrivals, err := readRunForkArrivalJoinInventory(ctx, attempt, plan, childRunID, bornAt, selection, arrival, arrivalPhase)
	if err != nil {
		return admission, err
	}
	if !materializable {
		return admission, nil
	}
	inventory, err := runForkTimerRecordInventory(plan.SourceRunID, plan.WorkflowTimers, plan.JoinSchedules, plan.TransferredJoins...)
	if err != nil {
		return admission, err
	}
	inventory.Complete, inventory.Point = true, plan.ForkPoint
	pending, err := inventory.pendingCertificate()
	if err != nil {
		return admission, err
	}
	published := plan.HistoricalArrivalCoordinates()
	transferred, err := readRunForkTransferredJoinInventory(ctx, attempt, plan, childRunID)
	if err != nil {
		return admission, err
	}
	if len(projected) != len(inventory.ActiveTimerIDs) || len(arrivals)+len(transferred) != len(inventory.ArrivalScheduleIDs)+len(inventory.TransferredPublicationIDs) {
		return admission, fmt.Errorf("timer history readback omitted an inherited family")
	}
	applied, err := runForkTimerAppliedCertificate(pending, childRunID, bornAt, projected, arrivals, published, transferred...)
	if err != nil {
		return admission, err
	}
	if err := attempt.RequireExistingSQLFrame(ctx); err != nil {
		return admission, err
	}
	return dischargeMaterializedRunForkTimerAdmission(admission, pending, applied, len(projected)+len(arrivals)+len(transferred))
}

func hasRunForkTimerHistory(plan runfork.RunForkPlan, admission runfork.RunForkReplayResumeAdmission) bool {
	if len(plan.WorkflowTimers)+len(plan.JoinSchedules)+len(plan.TransferredJoins) != 0 {
		return true
	}
	for _, blocker := range admission.UnsupportedBlockers {
		if blocker.Code == runfork.RunForkBlockerTimerHistoryUnproven {
			return true
		}
	}
	for _, fact := range admission.Dispositions {
		if fact.Fact == runfork.RunForkReplayResumeFactTimerHistory {
			return true
		}
	}
	return false
}
