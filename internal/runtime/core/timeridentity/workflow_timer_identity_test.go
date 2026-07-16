package timeridentity

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
)

func TestWorkflowTimerActivationAndOccurrenceIdentityAreCanonical(t *testing.T) {
	activationID := WorkflowTimerActivationID("run-1", "entity-1", "review/one", "review.expiry", "", "initial", "", "", "", "", "waiting")
	ref := WorkflowTimerActivationRef{
		ActivationID: activationID,
		Declaration:  "review.expiry",
		Generation: attemptgeneration.Generation{
			LoopID: "revision", RevisionField: "revision_id", RevisionID: "rev-1", Attempt: 2,
		},
	}
	taskID := ref.TaskID()
	parsed, ok := ParseWorkflowTimerActivationTaskID(taskID)
	if !ok || parsed != ref.Normalize() || parsed.TaskID() != taskID {
		t.Fatalf("activation round trip = %#v ok=%v", parsed, ok)
	}

	dueAt := time.Date(2026, time.July, 1, 12, 0, 0, 123456000, time.UTC)
	occurrence := WorkflowTimerOccurrenceRef{Activation: ref, DueAt: dueAt}
	occurrenceTaskID := occurrence.TaskID()
	parsedOccurrence, ok := ParseWorkflowTimerOccurrenceTaskID(occurrenceTaskID)
	if !ok || parsedOccurrence != occurrence.Normalize() || parsedOccurrence.TaskID() != occurrenceTaskID {
		t.Fatalf("occurrence round trip = %#v ok=%v", parsedOccurrence, ok)
	}
	if got, want := WorkflowTimerOccurrenceEventID(parsedOccurrence), WorkflowTimerOccurrenceEventID(occurrence); got == "" || got != want {
		t.Fatalf("occurrence event id = %q, want deterministic %q", got, want)
	}
	next := occurrence
	next.DueAt = next.DueAt.Add(time.Hour)
	if WorkflowTimerOccurrenceEventID(next) == WorkflowTimerOccurrenceEventID(occurrence) {
		t.Fatal("distinct recurring coordinates produced the same event id")
	}
}

func TestWorkflowTimerIdentityRejectsNonCanonicalOrUnknownFields(t *testing.T) {
	activationID := WorkflowTimerActivationID("run-1", "entity-1")
	valid := WorkflowTimerActivationRef{ActivationID: activationID, Declaration: "timer"}.TaskID()
	if _, ok := ParseWorkflowTimerActivationTaskID(" " + valid); ok {
		t.Fatal("non-canonical whitespace-wrapped activation identity was accepted")
	}
	if _, ok := ParseWorkflowTimerActivationTaskID(valid + "junk"); ok {
		t.Fatal("activation identity with trailing data was accepted")
	}
	if _, ok := ParseWorkflowTimerActivationTaskID(WorkflowTimerActivationTaskPrefix() + strings.TrimPrefix(valid, WorkflowTimerActivationTaskPrefix()) + "="); ok {
		t.Fatal("non-canonical base64 activation identity was accepted")
	}
}
