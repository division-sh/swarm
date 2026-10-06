package genericschedule

import (
	"reflect"
	"testing"
	"time"
)

func TestClockSuspensionPreservesLifetimeWithoutResumeCatchup(t *testing.T) {
	for _, cadence := range []DueBasis{EveryDue(5 * time.Minute), CronDue("*/5 * * * *")} {
		t.Run(string(cadence.Kind), func(t *testing.T) {
			original := instanceRecoveryActivation(t)
			original.Command.Due = cadence
			original.AdmittedAt = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
			original.InitialDueAt, _ = cadence.FirstDue(original.AdmittedAt)
			original.CurrentDueAt = original.InitialDueAt
			original.ImmutableHash, _ = original.Command.ImmutableHash()
			parkedAt := original.AdmittedAt.Add(time.Minute)
			parked, err := ParkClock(original, parkedAt)
			if err != nil {
				t.Fatal(err)
			}
			if parked.Status != StatusParked || parked.CancelCause != "" || !parked.CancelledAt.IsZero() {
				t.Fatalf("suspension became terminal cancellation: %+v", parked)
			}
			if _, err := parked.Wakeup(); err == nil {
				t.Fatal("parked clock retained an executable wakeup")
			}
			resumedAt := parkedAt.Add(time.Hour + 30*time.Second)
			resumed, err := ResumeClock(parked, resumedAt)
			if err != nil {
				t.Fatal(err)
			}
			if resumed.ID != original.ID || resumed.ImmutableHash != original.ImmutableHash || !reflect.DeepEqual(resumed.Command, original.Command) || !resumed.InitialDueAt.Equal(original.InitialDueAt) {
				t.Fatal("resume replaced the clock lifetime or immutable admission")
			}
			wantDue, _ := cadence.FirstDue(resumedAt)
			if resumed.Status != StatusActive || !resumed.CurrentDueAt.Equal(wantDue) || !resumed.CurrentDueAt.After(resumedAt) {
				t.Fatalf("resume retained an elapsed coordinate: got=%v want=%v", resumed.CurrentDueAt, wantDue)
			}
			s := resumed.ClockSuspension
			if !s.SuspendedFrom.Equal(parkedAt) || !s.ResumedAt.Equal(resumedAt) || s.SkippedOccurrences != 12 || !s.ParkedAt.IsZero() {
				t.Fatalf("incorrect skipped interval: %+v", s)
			}
			if original.ClockSuspension != nil || parked.Status != StatusParked || !parked.ClockSuspension.ParkedAt.Equal(parkedAt) {
				t.Fatal("clock transition mutated its input evidence")
			}
			next, _ := cadence.Next(resumed.CurrentDueAt)
			resumed.CurrentDueAt = next
			if err := resumed.Validate(); err != nil {
				t.Fatalf("recurrence after rearming lost its new cadence: %v", err)
			}
		})
	}
}

func TestClockSuspensionRejectsTerminalAndNonClockRearming(t *testing.T) {
	activation := instanceRecoveryActivation(t)
	activation.Status = StatusCancelled
	activation.CancelCause, activation.CancelledAt = "reset", activation.AdmittedAt.Add(time.Second)
	if _, err := ResumeClock(activation, activation.CancelledAt.Add(time.Second)); err == nil {
		t.Fatal("terminal clock resurrected")
	}
	global := testGlobalActivation(t, EveryDue(time.Minute), activation.AdmittedAt, activation.AdmittedAt.Add(time.Minute))
	if _, err := ParkClock(global, activation.AdmittedAt.Add(time.Second)); err == nil {
		t.Fatal("control schedule acquired clock parking")
	}
}

func TestClockSuspensionCountsOnlyTheSuspendedInterval(t *testing.T) {
	for _, cadence := range []DueBasis{EveryDue(5 * time.Minute), CronDue("*/5 * * * *")} {
		t.Run(string(cadence.Kind), func(t *testing.T) {
			original := instanceRecoveryActivation(t)
			original.Command.Due = cadence
			original.AdmittedAt = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
			original.InitialDueAt, _ = cadence.FirstDue(original.AdmittedAt)
			original.CurrentDueAt = original.InitialDueAt
			original.ImmutableHash, _ = original.Command.ImmutableHash()
			parked, err := ParkClock(original, original.AdmittedAt.Add(21*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			resumed, err := ResumeClock(parked, original.AdmittedAt.Add(61*time.Minute))
			if err != nil || resumed.ClockSuspension.SkippedOccurrences != 8 {
				t.Fatalf("pre-suspension backlog counted as suspended: %+v, %v", resumed.ClockSuspension, err)
			}
		})
	}
}
