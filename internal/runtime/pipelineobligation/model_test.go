package pipelineobligation

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/google/uuid"
)

func TestPipelineDispositionContractIsClosed(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name        string
		disposition Disposition
		purpose     Purpose
		wantErr     bool
	}{
		{name: "acknowledged", disposition: Acknowledged("processed"), purpose: PurposeRecovery},
		{name: "publication_deferred", disposition: Deferred("waiting", now, nil), purpose: PurposePublication},
		{name: "decision_route_deferred", disposition: Deferred("waiting", now, nil), purpose: PurposeDecisionRoute},
		{name: "terminal", disposition: Terminal("failed", nil), purpose: PurposeRecovery},
		{name: "dead_letter", disposition: DeadLetter("failed", nil), purpose: PurposeRecovery},
		{name: "quarantined", disposition: Quarantined("invalid", nil), purpose: PurposeRecovery},
		{name: "recovery_cannot_defer", disposition: Deferred("waiting", now, nil), purpose: PurposeRecovery, wantErr: true},
		{name: "deferred_requires_retry", disposition: Deferred("waiting", time.Time{}, nil), purpose: PurposeDecisionRoute, wantErr: true},
		{name: "empty_terminal_requires_reason", disposition: Terminal("", nil), purpose: PurposeRecovery, wantErr: true},
		{name: "zero_value_rejected", disposition: Disposition{}, purpose: PurposeRecovery, wantErr: true},
		{name: "unknown_purpose_rejected", disposition: Acknowledged("processed"), purpose: Purpose("other"), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.disposition.ValidateFor(test.purpose)
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateFor() error = %v, wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestPipelineRetryReleaseIsNotDurableDisposition(t *testing.T) {
	outcome := ReleaseForRetry(" activity_contract_pin_unavailable ", nil)
	if outcome.ContinueDispatch() {
		t.Fatal("retry release continued dispatch")
	}
	if disposition, ok := outcome.Disposition(); ok {
		t.Fatalf("retry release exposed durable disposition %#v", disposition)
	}
	retry, ok := outcome.RetryRelease()
	if !ok {
		t.Fatal("retry release outcome did not expose retry release")
	}
	if got := retry.ReasonCode(); got != "activity_contract_pin_unavailable" {
		t.Fatalf("retry release reason = %q", got)
	}
	if retry.Failure() != nil {
		t.Fatal("retry release unexpectedly carried failure")
	}
}

func TestPipelineClaimIssuerRejectsWrongEventAndForeignIssuer(t *testing.T) {
	eventID := uuid.NewString()
	otherEventID := uuid.NewString()
	issuer := NewClaimIssuer()
	claim, err := issuer.Issue(eventID, PurposeRecovery)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if err := issuer.Verify(claim, otherEventID, PurposeRecovery); !errors.Is(err, ErrWrongClaim) {
		t.Fatalf("wrong-event Verify error = %v, want ErrWrongClaim", err)
	}
	if err := issuer.Verify(claim, eventID, PurposeDecisionRoute); !errors.Is(err, ErrWrongClaim) {
		t.Fatalf("wrong-purpose Verify error = %v, want ErrWrongClaim", err)
	}
	if err := NewClaimIssuer().Verify(claim, eventID, PurposeRecovery); !errors.Is(err, ErrStaleClaim) {
		t.Fatalf("foreign-issuer Verify error = %v, want ErrStaleClaim", err)
	}
}

func TestPipelineClaimQueryIsClosed(t *testing.T) {
	if err := GlobalRecoveryQuery().Validate(); err != nil {
		t.Fatalf("global recovery query: %v", err)
	}
	if err := RunRecoveryQuery(uuid.NewString()).Validate(); err != nil {
		t.Fatalf("run recovery query: %v", err)
	}
	if err := DecisionRouteQuery().Validate(); err != nil {
		t.Fatalf("decision-route query: %v", err)
	}
	for _, query := range []ClaimQuery{
		{Purpose: PurposePublication},
		{Purpose: Purpose("other")},
		{RunID: "not-a-uuid", Purpose: PurposeRecovery},
		{RunID: uuid.NewString(), Purpose: PurposeDecisionRoute},
	} {
		if err := query.Validate(); err == nil {
			t.Fatalf("invalid query %#v was accepted", query)
		}
	}
}

func TestPreclassifiedWorkAcceptsOnlyClaimBoundTerminalNonSuccess(t *testing.T) {
	eventID := uuid.NewString()
	runID := uuid.NewString()
	event := eventtest.PersistedProjection(
		eventID,
		events.EventType("test.event"),
		"runtime",
		"",
		[]byte(`{"ok":true}`),
		0,
		runID,
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	claim, err := NewClaimIssuer().Issue(eventID, PurposeRecovery)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	work := ClaimedWork{Event: event, Scope: ScopeDirect, Claim: claim}
	classified, err := PreclassifiedWork(work, Quarantined("invalid", nil))
	if err != nil {
		t.Fatalf("PreclassifiedWork: %v", err)
	}
	disposition, ok := classified.PreDispatchDisposition()
	if !ok || disposition.Kind() != DispositionQuarantined || disposition.ReasonCode() != "invalid" {
		t.Fatalf("pre-dispatch disposition = %#v ok=%v", disposition, ok)
	}
	if _, err := PreclassifiedWork(work, Acknowledged("processed")); err == nil {
		t.Fatal("PreclassifiedWork accepted successful disposition")
	}
	wrong := work
	wrong.Event = eventtest.PersistedProjection(
		uuid.NewString(),
		events.EventType("test.event"),
		"runtime",
		"",
		[]byte(`{"ok":true}`),
		0,
		runID,
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	if _, err := PreclassifiedWork(wrong, Quarantined("invalid", nil)); !errors.Is(err, ErrWrongClaim) {
		t.Fatalf("wrong-event classification error = %v, want ErrWrongClaim", err)
	}
}
