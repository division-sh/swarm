package pipeline

import (
	"reflect"
	"testing"
	"time"
)

func TestWorkflowTimerRuleRemovalCancellationValidation(t *testing.T) {
	initial := inheritedWorkflowTimerForTest(t)
	removed := initial
	removed.Status, removed.CancelCause, removed.CancelledAt = workflowTimerStatusCancelled, WorkflowTimerCancelCauseRuleRemoved, initial.CreatedAt
	if err := removed.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := removed.ValidateCauseReplay(initial); err != nil {
		t.Fatalf("terminal cancellation was mistaken for a changed immutable arm: %v", err)
	}
	ordinary := initial
	ordinary.Status = workflowTimerStatusCancelled
	if err := ordinary.Validate(); err != nil {
		t.Fatalf("ordinary status-only cancellation semantics changed: %v", err)
	}
	for _, test := range []struct {
		name   string
		change func(*WorkflowTimerActivation)
	}{
		{"missing_time", func(a *WorkflowTimerActivation) { a.CancelledAt = time.Time{} }},
		{"missing_cause", func(a *WorkflowTimerActivation) { a.CancelCause = "" }},
		{"unknown_cause", func(a *WorkflowTimerActivation) { a.CancelCause = "guessed_removal" }},
		{"active_with_disposition", func(a *WorkflowTimerActivation) { a.Status = workflowTimerStatusActive }},
		{"before_child_birth", func(a *WorkflowTimerActivation) { a.CancelledAt = a.CreatedAt.Add(-time.Microsecond) }},
		{"ordinary_row", func(a *WorkflowTimerActivation) {
			a.SourceTimerID, a.ForkedFromRunID, a.ForkedFromPointKind, a.ForkedFromEventID, a.ReconstructionOwner = "", "", "", "", ""
			a.ForkedFromPointRevision, a.SourceArmedAt = 0, time.Time{}
			a.FireAt = a.CreatedAt.Add(time.Hour)
		}},
		{"before_accepted_recurrence", func(a *WorkflowTimerActivation) {
			a.Recurring, a.RecurrenceInterval = true, time.Hour
			a.FireAt = a.SourceArmedAt.Add(5 * time.Hour)
			a.FiredAt = a.SourceArmedAt.Add(4 * time.Hour)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := removed
			test.change(&invalid)
			if err := invalid.Validate(); err == nil {
				t.Fatal("invalid or unproven rule-removal disposition accepted")
			}
		})
	}
}

func TestWorkflowTimerCancellationMutationRequiresExactInstructions(t *testing.T) {
	activation := inheritedWorkflowTimerForTest(t)
	for _, mutation := range []WorkflowTimerMutation{
		{Kind: WorkflowTimerMutationCancel, Activation: activation},
		{Kind: WorkflowTimerMutationCancel, Activation: activation, CancelCause: WorkflowTimerCancelCauseRuleRemoved, CancelledAt: activation.CreatedAt},
		{Kind: WorkflowTimerMutationInsert, Activation: activation},
	} {
		if err := mutation.Validate(activation.RunID, activation.Route, activation.EntityID); err != nil {
			t.Fatal(err)
		}
	}
	for _, mutation := range []WorkflowTimerMutation{
		{Kind: WorkflowTimerMutationCancel, Activation: activation, CancelCause: WorkflowTimerCancelCauseRuleRemoved},
		{Kind: WorkflowTimerMutationCancel, Activation: activation, CancelledAt: activation.CreatedAt},
		{Kind: WorkflowTimerMutationCancel, Activation: activation, CancelCause: "unknown", CancelledAt: activation.CreatedAt},
		{Kind: WorkflowTimerMutationCancel, Activation: activation, CancelCause: WorkflowTimerCancelCauseRuleRemoved, CancelledAt: activation.CreatedAt.Add(-time.Microsecond)},
		{Kind: WorkflowTimerMutationInsert, Activation: activation, CancelCause: WorkflowTimerCancelCauseRuleRemoved, CancelledAt: activation.CreatedAt},
	} {
		if err := mutation.Validate(activation.RunID, activation.Route, activation.EntityID); err == nil {
			t.Fatalf("invalid cancellation instructions accepted: %+v", mutation)
		}
	}
}

func TestWorkflowTimerCancellationPersistenceRecordRoundTrip(t *testing.T) {
	activation := inheritedWorkflowTimerForTest(t)
	activation.Status, activation.CancelCause, activation.CancelledAt = workflowTimerStatusCancelled, WorkflowTimerCancelCauseRuleRemoved, activation.CreatedAt
	record := activation.PersistenceRecord()
	decoded, err := DecodeWorkflowTimerActivationPersistenceRecord(record)
	if err != nil || !reflect.DeepEqual(decoded, activation.Canonical()) {
		t.Fatalf("primitive carrier lost cancellation history: %+v: %v", decoded, err)
	}
	record.CancelledAt = time.Time{}
	if _, err := DecodeWorkflowTimerActivationPersistenceRecord(record); err == nil {
		t.Fatal("primitive decoder accepted rule removal without its timestamp")
	}
}
