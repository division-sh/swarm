package genericschedule

import (
	"testing"
	"time"
)

func TestReviewer642JoinReplayRejectsTerminalBeforeOccurrence(t *testing.T) {
	for _, state := range []Status{StatusCancelled, StatusFailed, StatusFired} {
		t.Run(string(state), func(t *testing.T) {
			expected := forkJoinOriginTestActivation(t)
			base := expected.AdmittedAt
			if expected.CurrentDueAt.After(base) {
				base = expected.CurrentDueAt
			}
			actual := expected.Canonical()
			actual.CurrentEventID = OccurrenceEventID(actual.ID, actual.CurrentDueAt)
			actual.CurrentEventAdmittedAt = base.Add(2 * time.Minute)
			actual.Status = state
			switch state {
			case StatusCancelled:
				actual.CancelCause, actual.CancelledAt = "join_stage_exit", base.Add(time.Minute)
			case StatusFailed:
				actual.Failure, actual.FailedAt = Failure{Code: "publication_failed"}, base.Add(time.Minute)
			case StatusFired:
				actual.FiredAt, actual.AcceptedAt = base.Add(time.Minute), base.Add(3*time.Minute)
			}
			if err := actual.ValidateForkJoinReplay(expected); err == nil {
				t.Fatalf("continuing readback accepted %s before retained occurrence admission", state)
			}
		})
	}
}
