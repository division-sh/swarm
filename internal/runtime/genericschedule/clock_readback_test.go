package genericschedule

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestClockReadbackUsesPersistedLifecycleEvidence(t *testing.T) {
	command := instanceScheduleCommand(t, ".")
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	activation := Activation{ID: uuid.NewString(), Command: command, ImmutableHash: hash,
		AdmittedAt: at, InitialDueAt: at.Add(time.Minute), CurrentDueAt: at.Add(5 * time.Minute), Status: StatusActive}
	for _, runActive := range []bool{true, false} {
		view, err := ProjectClockReadback(activation, runActive)
		if err != nil || view.NextDueAt == nil || !view.NextDueAt.Equal(activation.CurrentDueAt) || view.RetainsRun != runActive || view.FlowInstance != command.FlowInstance {
			t.Fatalf("persisted recurrence/readiness lost: %#v, %v", view, err)
		}
		*view.NextDueAt = at
		if !activation.CurrentDueAt.Equal(at.Add(5 * time.Minute)) {
			t.Fatal("readback mutated the admitted activation")
		}
	}
	activation.Status, activation.CancelCause, activation.CancelledAt = StatusCancelled, "clock_removed", at.Add(time.Second)
	view, err := ProjectClockReadback(activation, true)
	if err != nil || view.NextDueAt != nil || view.RetainsRun || view.CancelCause != "clock_removed" || view.CancelledAt == nil {
		t.Fatalf("cancelled clock invented an occurrence: %#v, %v", view, err)
	}
	activation.Status, activation.CancelCause, activation.CancelledAt = StatusFailed, "", time.Time{}
	activation.FailedAt, activation.Failure = at.Add(time.Second), Failure{Code: "publication_refused", Message: "business publication refused"}
	view, err = ProjectClockReadback(activation, true)
	if err != nil || view.NextDueAt != nil || view.RetainsRun || view.Failure == nil || view.Failure.Code != activation.Failure.Code || view.Failure.Message != activation.Failure.Message {
		t.Fatalf("failed clock lost evidence: %#v, %v", view, err)
	}
	activation.ImmutableHash = "corrupt"
	if _, err := ProjectClockReadback(activation, true); err == nil {
		t.Fatal("corrupt activation projected as a clock")
	}
}

func TestClockReadbackKeepsParkingAndSkippedIntervalIsolated(t *testing.T) {
	activation := instanceRecoveryActivation(t)
	parked, err := ParkClock(activation, activation.AdmittedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	view, err := ProjectClockReadback(parked, true)
	if err != nil || view.Status != StatusParked || view.NextDueAt != nil || view.RetainsRun || view.Suspension == nil {
		t.Fatalf("parked projection=%+v error=%v", view, err)
	}
	view.Suspension.ParkedAt = time.Time{}
	if parked.ClockSuspension.ParkedAt.IsZero() {
		t.Fatal("readback mutated durable parking evidence")
	}
	resumed, err := ResumeClock(parked, activation.AdmittedAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	view, err = ProjectClockReadback(resumed, true)
	if err != nil || view.NextDueAt == nil || !view.RetainsRun || view.Suspension.SkippedOccurrences != 60 {
		t.Fatalf("resumed projection=%+v error=%v", view, err)
	}
	view.Suspension.SkippedOccurrences = -1
	if resumed.ClockSuspension.SkippedOccurrences != 60 {
		t.Fatal("readback mutated skipped occurrence evidence")
	}
}
