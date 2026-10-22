package genericschedule

import (
	"testing"
	"time"
)

func TestTerminalOccurrenceChronologyBeforeEqualAfter(t *testing.T) {
	for _, inherited := range []bool{false, true} {
		for _, status := range []Status{StatusCancelled, StatusFailed, StatusFired} {
			for _, offset := range []time.Duration{-time.Microsecond, 0, time.Microsecond} {
				t.Run(map[bool]string{false: "ordinary/", true: "inherited/"}[inherited]+string(status)+"/"+offset.String(), func(t *testing.T) {
					expected := forkJoinOriginTestActivation(t)
					if !inherited {
						expected.ForkJoinOrigin = nil
					}
					actual := expected.Canonical()
					admission := actual.CurrentDueAt.Add(time.Minute)
					actual.CurrentEventID, actual.CurrentEventAdmittedAt = OccurrenceEventID(actual.ID, actual.CurrentDueAt), admission
					terminal := admission.Add(offset)
					actual.Status = status
					switch status {
					case StatusCancelled:
						actual.CancelCause, actual.CancelledAt = "join_stage_exit", terminal
					case StatusFailed:
						actual.Failure, actual.FailedAt = Failure{Code: "publication_failed"}, terminal
					case StatusFired:
						actual.FiredAt, actual.AcceptedAt = terminal, admission.Add(time.Minute)
					}
					if err := actual.Validate(); (err == nil) != (offset >= 0) {
						t.Fatalf("canonical lifecycle status=%s offset=%s err=%v", status, offset, err)
					}
					if inherited {
						if err := actual.ValidateForkJoinReplay(expected); (err == nil) != (offset >= 0) {
							t.Fatalf("fork replay status=%s offset=%s err=%v", status, offset, err)
						}
					}
				})
			}
		}
	}
}

func TestTerminalWithoutOccurrenceMayPrecedeDue(t *testing.T) {
	for _, status := range []Status{StatusCancelled, StatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			expected := forkJoinOriginTestActivation(t)
			actual := expected.Canonical()
			actual.Status = status
			at := actual.AdmittedAt.Add(time.Minute)
			if !at.Before(actual.CurrentDueAt) {
				t.Fatal("fixture does not precede due")
			}
			if status == StatusCancelled {
				actual.CancelCause, actual.CancelledAt = "join_stage_exit", at
			} else {
				actual.Failure, actual.FailedAt = Failure{Code: "publication_failed"}, at
			}
			if err := actual.Validate(); err != nil {
				t.Fatal(err)
			}
			if err := actual.ValidateForkJoinReplay(expected); err != nil {
				t.Fatal("unpublished terminal disposition refused", err)
			}
		})
	}
}

func TestRecurringTerminalKeepsPriorAcceptedOccurrenceHistory(t *testing.T) {
	arm := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	for _, status := range []Status{StatusActive, StatusCancelled, StatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			actual := testGlobalActivation(t, EveryDue(time.Minute), arm, arm.Add(2*time.Minute))
			actual.FiredAt, actual.AcceptedAt = arm.Add(time.Minute), arm.Add(time.Minute)
			admission := actual.CurrentDueAt.Add(time.Second)
			actual.CurrentEventID, actual.CurrentEventAdmittedAt = OccurrenceEventID(actual.ID, actual.CurrentDueAt), admission
			actual.Status = status
			if status == StatusCancelled {
				actual.CancelCause, actual.CancelledAt = "operator_cancelled", admission
			} else if status == StatusFailed {
				actual.Failure, actual.FailedAt = Failure{Code: "publication_failed"}, admission
			}
			if err := actual.Validate(); err != nil {
				t.Fatal("preceding recurrence history was mistaken for current terminalization", err)
			}
		})
	}
}
