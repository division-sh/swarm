package deliverylifecycle

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestAuthoredCanceledStatusIsTerminalWithoutFailure(t *testing.T) {
	status, err := ParseStatus("canceled")
	if err != nil {
		t.Fatalf("authored cancellation is not a delivery status: %v", err)
	}
	if !status.Terminal() || string(StateFromStatus(status, "")) != "canceled" {
		t.Fatalf("cancellation classified as retry/failure: terminal=%v state=%q", status.Terminal(), StateFromStatus(status, ""))
	}
	if snapshot := (Snapshot{Status: status}); !snapshot.Terminal() || snapshot.State() != StateFromStatus(status, "") {
		t.Fatalf("snapshot disagrees with cancellation owner: %+v", snapshot)
	}
}

func TestCancellationReasonRejectsOperationalFallbacks(t *testing.T) {
	for _, raw := range []string{"terminate", "turn_timeout"} {
		if reason, err := ParseCancellationReason(raw); err != nil || string(reason) != raw {
			t.Fatalf("authored reason %q: %q %v", raw, reason, err)
		}
	}
	for _, raw := range []string{"", "shutdown", "context_canceled", "panic", "superseded", "stale_authority", " terminate", "turn_timeout ", "TERMINATE"} {
		if _, err := ParseCancellationReason(raw); err == nil {
			t.Errorf("operational or noncanonical reason %q accepted", raw)
		}
	}
	if _, err := ParseStatus(" canceled "); err == nil {
		t.Fatal("noncanonical new canceled status accepted")
	}
}

func TestCanceledSnapshotCannotPresentIntentAsSettlement(t *testing.T) {
	settled := Snapshot{Status: StatusCanceled, ReasonCode: string(CancellationTerminate), SettledAt: time.Now().UTC()}
	if err := ValidateCanceledSnapshot(settled); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*Snapshot)
	}{
		{"missing_reason", func(s *Snapshot) { s.ReasonCode = "" }},
		{"operational_reason", func(s *Snapshot) { s.ReasonCode = "shutdown" }},
		{"unsettled_intent", func(s *Snapshot) { s.SettledAt = time.Time{} }},
		{"failure", func(s *Snapshot) { s.Failure = &runtimefailures.Envelope{} }},
		{"retry_time", func(s *Snapshot) { s.NextEligibleAt = time.Now() }},
		{"retry_scheduled", func(s *Snapshot) { s.RetryScheduled = true }},
		{"reclaimable", func(s *Snapshot) { s.ClaimReclaimable = true }},
		{"open_claim", func(s *Snapshot) { s.ClaimExpiresAt = time.Now() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid := settled
			tc.change(&invalid)
			if err := ValidateCanceledSnapshot(invalid); err == nil {
				t.Fatal("invalid canceled delivery shape accepted")
			}
		})
	}
}

func TestCanceledRunSummaryConservesWithoutSuccessOrFailure(t *testing.T) {
	summary := RunSummary{RunID: "canceled-run", Total: 1, Canceled: 1}
	if err := summary.Validate(); err != nil || !summary.Settled() || summary.Delivered != 0 || summary.DeadLetter != 0 {
		t.Fatalf("cancellation masquerades as success/failure or blocks delivery settlement: %+v err=%v", summary, err)
	}
	summary.Canceled = -1
	if err := summary.Validate(); err == nil {
		t.Fatal("negative canceled count accepted")
	}
	summary.Canceled = 0
	if err := summary.Validate(); err == nil {
		t.Fatal("missing canceled outcome escaped conservation")
	}
}

func TestCanceledSelectionRetainsOnlyRealObservation(t *testing.T) {
	for _, reached := range []bool{false, true} {
		name := "not_reached"
		observed := handlerselection.NotReached()
		if reached {
			name = "observed"
			observed = handlerselection.Resolved(handlerselection.NotApplicable())
		}
		t.Run(name, func(t *testing.T) {
			selection, err := FinalSelection(Status("canceled"), observed)
			if err != nil {
				t.Fatalf("canceled selection: %v", err)
			}
			if selection.Present() != reached {
				t.Fatalf("cancellation fabricated or discarded selection: present=%v reached=%v", selection.Present(), reached)
			}
			if reached {
				fact, err := selection.Fact()
				if err != nil || fact != handlerselection.NotApplicable() {
					t.Fatalf("actual observation changed: %+v err=%v", fact, err)
				}
			}
		})
	}
}
