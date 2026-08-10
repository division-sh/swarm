package pipeline

import (
	"context"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
)

type GenericScheduleWakeupOwner = runtimetimercancellation.GenericScheduleWakeupOwner

func (pc *PipelineCoordinator) reconcileTimerCancellations(ctx context.Context, refs []runtimetimercancellation.Ref) error {
	return pc.TimerCancellationReconciler().Reconcile(ctx, refs)
}

func (pc *PipelineCoordinator) TimerCancellationReconciler() *runtimetimercancellation.Reconciler {
	if pc == nil {
		return runtimetimercancellation.NewReconciler(nil, nil)
	}
	if pc.timerCancellations != nil {
		return pc.timerCancellations
	}
	return runtimetimercancellation.NewReconciler(pc.genericSchedules, pc.workflowTimers)
}
