package pipeline

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
)

type GenericScheduleWakeupOwner interface {
	ReconcileWakeup(context.Context, string) error
}

func (pc *PipelineCoordinator) reconcileTimerCancellations(ctx context.Context, refs []runtimetimercancellation.Ref) error {
	for _, cancellation := range refs {
		cancellation = cancellation.Canonical()
		if err := cancellation.Validate(); err != nil {
			return err
		}
		switch cancellation.Family {
		case runtimetimercancellation.FamilyGenericSchedule:
			if pc.genericSchedules == nil {
				return fmt.Errorf("generic schedule cancellation reconciliation owner is required")
			}
			if err := pc.genericSchedules.ReconcileWakeup(ctx, cancellation.ActivationID); err != nil {
				return err
			}
		case runtimetimercancellation.FamilyWorkflowTimer:
			if pc.workflowTimers == nil {
				return fmt.Errorf("workflow timer cancellation reconciliation owner is required")
			}
			ref, ok := timeridentity.ParseWorkflowTimerActivationTaskID(cancellation.TaskID)
			if !ok || ref.ActivationID != cancellation.ActivationID {
				return fmt.Errorf("workflow timer cancellation evidence is inconsistent")
			}
			if err := pc.workflowTimers.ReconcileWakeup(ctx, ref); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported timer cancellation family %q", cancellation.Family)
		}
	}
	return nil
}
