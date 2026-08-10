package timercancellation

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
)

type GenericScheduleWakeupOwner interface {
	ReconcileWakeup(context.Context, string) error
}

type WorkflowTimerWakeupOwner interface {
	ReconcileWakeup(context.Context, timeridentity.WorkflowTimerActivationRef) error
}

// Reconciler is the sole post-commit interpreter of family-typed timer
// cancellation evidence.
type Reconciler struct {
	generic  GenericScheduleWakeupOwner
	workflow WorkflowTimerWakeupOwner
}

func NewReconciler(generic GenericScheduleWakeupOwner, workflow WorkflowTimerWakeupOwner) *Reconciler {
	return &Reconciler{generic: generic, workflow: workflow}
}

func (r *Reconciler) Reconcile(ctx context.Context, refs []Ref) error {
	var failures []error
	for _, cancellation := range refs {
		cancellation = cancellation.Canonical()
		if err := cancellation.Validate(); err != nil {
			failures = append(failures, err)
			continue
		}
		switch cancellation.Family {
		case FamilyGenericSchedule:
			if r == nil || r.generic == nil {
				failures = append(failures, fmt.Errorf("generic schedule cancellation reconciliation owner is required"))
				continue
			}
			if err := r.generic.ReconcileWakeup(ctx, cancellation.ActivationID); err != nil {
				failures = append(failures, fmt.Errorf("reconcile generic schedule %s: %w", cancellation.ActivationID, err))
			}
		case FamilyWorkflowTimer:
			if r == nil || r.workflow == nil {
				failures = append(failures, fmt.Errorf("workflow timer cancellation reconciliation owner is required"))
				continue
			}
			ref, ok := timeridentity.ParseWorkflowTimerActivationTaskID(cancellation.TaskID)
			if !ok || ref.ActivationID != cancellation.ActivationID {
				failures = append(failures, fmt.Errorf("workflow timer cancellation evidence is inconsistent"))
				continue
			}
			if err := r.workflow.ReconcileWakeup(ctx, ref); err != nil {
				failures = append(failures, fmt.Errorf("reconcile workflow timer %s: %w", cancellation.ActivationID, err))
			}
		default:
			failures = append(failures, fmt.Errorf("unsupported timer cancellation family %q", cancellation.Family))
		}
	}
	return errors.Join(failures...)
}
