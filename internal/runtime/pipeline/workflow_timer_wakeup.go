package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
)

type workflowTimerWakeupFamily uint8

const workflowTimerActivationWakeup workflowTimerWakeupFamily = 1
const workflowTimerTaskFamily = "workflow_timer"

// WorkflowTimerWakeup is the complete process-local projection of one durable
// workflow timer occurrence. All execution facts are reloaded from storage.
type WorkflowTimerWakeup struct {
	family     workflowTimerWakeupFamily
	occurrence timeridentity.WorkflowTimerOccurrenceRef
	dueAt      time.Time
}

func newWorkflowTimerWakeup(activation WorkflowTimerActivation) (WorkflowTimerWakeup, error) {
	occurrence := activation.occurrence()
	wakeup := WorkflowTimerWakeup{
		family:     workflowTimerActivationWakeup,
		occurrence: occurrence,
		dueAt:      canonicalWorkflowTimerTime(occurrence.DueAt),
	}
	if err := wakeup.validate(); err != nil {
		return WorkflowTimerWakeup{}, err
	}
	return wakeup, nil
}

func (w WorkflowTimerWakeup) Family() string {
	if w.family == workflowTimerActivationWakeup {
		return workflowTimerTaskFamily
	}
	return ""
}

func (w WorkflowTimerWakeup) Occurrence() timeridentity.WorkflowTimerOccurrenceRef {
	return w.occurrence.Normalize()
}

func (w WorkflowTimerWakeup) DueAt() time.Time {
	return canonicalWorkflowTimerTime(w.dueAt)
}

func (w WorkflowTimerWakeup) validate() error {
	occurrence := w.Occurrence()
	if w.family != workflowTimerActivationWakeup {
		return fmt.Errorf("workflow timer wakeup family is invalid")
	}
	if !occurrence.Valid() || occurrence.Activation.ActivationID == "" {
		return fmt.Errorf("workflow timer wakeup occurrence is invalid")
	}
	if w.DueAt().IsZero() || !w.DueAt().Equal(occurrence.DueAt) {
		return fmt.Errorf("workflow timer wakeup due time is invalid")
	}
	return nil
}

func (l *WorkflowTimerLifecycle) registerWakeup(ctx context.Context, wakeup WorkflowTimerWakeup) error {
	if l == nil || l.scheduler == nil {
		return nil
	}
	if err := wakeup.validate(); err != nil {
		return err
	}
	return l.scheduler.registerWorkflowTimerWakeup(ctx, wakeup)
}

func (l *WorkflowTimerLifecycle) handleWakeup(ctx context.Context, wakeup WorkflowTimerWakeup) {
	outcome, recurrenceCommitted, err := l.fireWakeup(ctx, wakeup)
	if err != nil {
		l.logFailure(ctx, "workflow_timer_fire_failed", wakeup.Occurrence().Activation, err)
	}
	if outcome == WorkflowTimerFireRetry {
		l.startWakeupRecovery(wakeup.Occurrence().Activation)
		return
	}
	if outcome == WorkflowTimerFireCommitted && recurrenceCommitted {
		ref := wakeup.Occurrence().Activation
		if err := l.ReconcileWakeup(ownerActionAdmissionContext(ctx), ref); err != nil {
			l.logFailure(ctx, "workflow_timer_recurrence_reconcile_failed", ref, err)
			l.startWakeupRecovery(ref)
		}
	}
}

func (l *WorkflowTimerLifecycle) retireWakeup(ref timeridentity.WorkflowTimerActivationRef) error {
	if l == nil || l.scheduler == nil {
		return nil
	}
	return l.scheduler.cancelWorkflowTimerWakeup(ref)
}

func (l *WorkflowTimerLifecycle) stopWakeups(ctx context.Context) error {
	if l == nil || l.scheduler == nil {
		return nil
	}
	return l.scheduler.stopWorkflowTimerWakeups(ctx)
}
