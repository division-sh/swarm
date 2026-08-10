package timercancellation

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
)

type GenericScheduleWakeupOwner interface {
	ReconcileWakeupWithRecovery(context.Context, string) (bool, error)
}

type WorkflowTimerWakeupOwner interface {
	ReconcileWakeupWithRecovery(context.Context, timeridentity.WorkflowTimerActivationRef) (bool, error)
}

type Failure struct {
	Ref Ref
	Err error
}

// ReconciliationError distinguishes committed cancellations whose process
// projection is queued for recovery from failures that could not be queued.
type ReconciliationError struct {
	Pending []Failure
	Failed  []Failure
}

func (e *ReconciliationError) Error() string {
	if e == nil {
		return ""
	}
	parts := make([]string, 0, len(e.Pending)+len(e.Failed))
	for _, failure := range e.Pending {
		parts = append(parts, fmt.Sprintf("pending %s/%s: %v", failure.Ref.Family, failure.Ref.ActivationID, failure.Err))
	}
	for _, failure := range e.Failed {
		parts = append(parts, fmt.Sprintf("failed %s/%s: %v", failure.Ref.Family, failure.Ref.ActivationID, failure.Err))
	}
	return fmt.Sprintf("timer cancellation reconciliation pending=%d failed=%d: %s", len(e.Pending), len(e.Failed), strings.Join(parts, "; "))
}

func (e *ReconciliationError) Unwrap() []error {
	if e == nil {
		return nil
	}
	errs := make([]error, 0, len(e.Pending)+len(e.Failed))
	for _, failure := range append(append([]Failure(nil), e.Pending...), e.Failed...) {
		if failure.Err != nil {
			errs = append(errs, failure.Err)
		}
	}
	return errs
}

func (e *ReconciliationError) RecoveryPendingOnly() bool {
	return e != nil && len(e.Pending) > 0 && len(e.Failed) == 0
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
	outcome := &ReconciliationError{}
	failed := func(ref Ref, err error) {
		outcome.Failed = append(outcome.Failed, Failure{Ref: ref.Canonical(), Err: err})
	}
	pending := func(ref Ref, err error) {
		outcome.Pending = append(outcome.Pending, Failure{Ref: ref.Canonical(), Err: err})
	}
	for _, cancellation := range refs {
		cancellation = cancellation.Canonical()
		if err := cancellation.Validate(); err != nil {
			failed(cancellation, err)
			continue
		}
		switch cancellation.Family {
		case FamilyGenericSchedule:
			if r == nil || r.generic == nil {
				failed(cancellation, fmt.Errorf("generic schedule cancellation reconciliation owner is required"))
				continue
			}
			queued, err := r.generic.ReconcileWakeupWithRecovery(ctx, cancellation.ActivationID)
			if err != nil {
				err = fmt.Errorf("reconcile generic schedule %s: %w", cancellation.ActivationID, err)
				if queued {
					pending(cancellation, err)
				} else {
					failed(cancellation, err)
				}
			}
		case FamilyWorkflowTimer:
			if r == nil || r.workflow == nil {
				failed(cancellation, fmt.Errorf("workflow timer cancellation reconciliation owner is required"))
				continue
			}
			ref, ok := timeridentity.ParseWorkflowTimerActivationTaskID(cancellation.TaskID)
			if !ok || ref.ActivationID != cancellation.ActivationID {
				failed(cancellation, fmt.Errorf("workflow timer cancellation evidence is inconsistent"))
				continue
			}
			queued, err := r.workflow.ReconcileWakeupWithRecovery(ctx, ref)
			if err != nil {
				err = fmt.Errorf("reconcile workflow timer %s: %w", cancellation.ActivationID, err)
				if queued {
					pending(cancellation, err)
				} else {
					failed(cancellation, err)
				}
			}
		default:
			failed(cancellation, fmt.Errorf("unsupported timer cancellation family %q", cancellation.Family))
		}
	}
	if len(outcome.Pending) == 0 && len(outcome.Failed) == 0 {
		return nil
	}
	return outcome
}
