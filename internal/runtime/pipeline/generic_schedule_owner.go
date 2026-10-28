package pipeline

import runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"

type GenericScheduleWakeupOwner = runtimetimercancellation.GenericScheduleWakeupOwner

func (pc *PipelineCoordinator) TimerCancellationReconciler() *runtimetimercancellation.Reconciler {
	if pc == nil {
		return runtimetimercancellation.NewReconciler(nil, nil)
	}
	if pc.timerCancellations != nil {
		return pc.timerCancellations
	}
	return runtimetimercancellation.NewReconciler(pc.genericSchedules, pc.workflowTimers)
}
