package timercancellation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/google/uuid"
)

type genericWakeupProbe struct {
	ids    []string
	err    error
	queued bool
}

func (p *genericWakeupProbe) ReconcileWakeupWithRecovery(_ context.Context, id string) (bool, error) {
	p.ids = append(p.ids, id)
	return p.queued, p.err
}

func TestReconcilerAttemptsEveryCommittedCancellationAfterOneOwnerFails(t *testing.T) {
	genericErr := errors.New("generic retirement failed")
	generic := &genericWakeupProbe{err: genericErr}
	workflow := &workflowWakeupProbe{}
	workflowRef := timeridentity.WorkflowTimerActivationRef{
		ActivationID: uuid.NewString(), DeclarationKey: "review_timeout", DeclarationRevision: "revision-1",
		Cause: timeridentity.WorkflowTimerActivationCauseInitial,
	}
	err := NewReconciler(generic, workflow).Reconcile(context.Background(), []Ref{
		{Family: FamilyGenericSchedule, ActivationID: "generic-1", DueAt: time.Now()},
		{Family: FamilyWorkflowTimer, ActivationID: workflowRef.ActivationID, TaskID: workflowRef.TaskID(), DueAt: time.Now()},
	})
	if !errors.Is(err, genericErr) || len(generic.ids) != 1 || len(workflow.refs) != 1 {
		t.Fatalf("joined reconciliation error=%v generic=%#v workflow=%#v", err, generic.ids, workflow.refs)
	}
}

type workflowWakeupProbe struct {
	refs []timeridentity.WorkflowTimerActivationRef
}

func (p *workflowWakeupProbe) ReconcileWakeupWithRecovery(_ context.Context, ref timeridentity.WorkflowTimerActivationRef) (bool, error) {
	p.refs = append(p.refs, ref)
	return false, nil
}

func TestReconcilerClassifiesQueuedRecoverySeparatelyFromPermanentFailure(t *testing.T) {
	sentinel := errors.New("retirement failed")
	err := NewReconciler(&genericWakeupProbe{err: sentinel, queued: true}, nil).Reconcile(context.Background(), []Ref{{
		Family: FamilyGenericSchedule, ActivationID: "generic-1", DueAt: time.Now(),
	}})
	var reconciliation *ReconciliationError
	if !errors.As(err, &reconciliation) || !reconciliation.RecoveryPendingOnly() || !errors.Is(err, sentinel) {
		t.Fatalf("queued reconciliation error = %#v, want pending-only sentinel", err)
	}
}

func TestReconcilerDispatchesExactFamilyTypedEvidence(t *testing.T) {
	generic := &genericWakeupProbe{}
	workflow := &workflowWakeupProbe{}
	workflowRef := timeridentity.WorkflowTimerActivationRef{
		ActivationID: uuid.NewString(), DeclarationKey: "review_timeout", DeclarationRevision: "revision-1",
		Cause: timeridentity.WorkflowTimerActivationCauseInitial,
	}
	due := time.Now().UTC().Add(time.Hour)
	reconciler := NewReconciler(generic, workflow)
	if err := reconciler.Reconcile(context.Background(), []Ref{
		{Family: FamilyGenericSchedule, ActivationID: "generic-1", DueAt: due},
		{Family: FamilyWorkflowTimer, ActivationID: workflowRef.ActivationID, TaskID: workflowRef.TaskID(), DueAt: due},
	}); err != nil {
		t.Fatal(err)
	}
	if len(generic.ids) != 1 || generic.ids[0] != "generic-1" || len(workflow.refs) != 1 || workflow.refs[0] != workflowRef.Normalize() {
		t.Fatalf("family dispatch generic=%#v workflow=%#v", generic.ids, workflow.refs)
	}
}

func TestReconcilerRejectsInconsistentWorkflowEvidenceBeforeDispatch(t *testing.T) {
	workflow := &workflowWakeupProbe{}
	ref := timeridentity.WorkflowTimerActivationRef{
		ActivationID: uuid.NewString(), DeclarationKey: "review_timeout", DeclarationRevision: "revision-1",
		Cause: timeridentity.WorkflowTimerActivationCauseInitial,
	}
	err := NewReconciler(nil, workflow).Reconcile(context.Background(), []Ref{{
		Family: FamilyWorkflowTimer, ActivationID: uuid.NewString(), TaskID: ref.TaskID(), DueAt: time.Now(),
	}})
	if err == nil || len(workflow.refs) != 0 {
		t.Fatalf("inconsistent evidence error=%v dispatches=%#v", err, workflow.refs)
	}
}
