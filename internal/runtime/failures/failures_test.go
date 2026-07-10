package failures

import (
	"errors"
	"reflect"
	"testing"
)

func TestRegistryIsClosedAndSelectorsArePositiveSets(t *testing.T) {
	if got := len(Classes()); got != 21 {
		t.Fatalf("class count = %d, want 21", got)
	}
	all, ok := SelectorMembers(SelectorAny)
	if !ok || !reflect.DeepEqual(all, Classes()) {
		t.Fatalf("platform.any = %#v, %t", all, ok)
	}
	task, ok := SelectorMembers(SelectorAnyTaskFailure)
	if !ok {
		t.Fatal("platform.any_task_failure is not declared")
	}
	for _, class := range []Class{
		ClassEarlyArrival,
		ClassStaleArrival,
		ClassUnexpectedArrival,
		ClassConflictingDuplicate,
		ClassReplyAlreadyTerminal,
	} {
		if Matches(SelectorAnyTaskFailure, class) {
			t.Fatalf("platform.any_task_failure unexpectedly contains %s", class)
		}
	}
	if len(task) != 16 {
		t.Fatalf("platform.any_task_failure member count = %d, want 16", len(task))
	}
	if _, ok := SelectorMembers("platform.anything"); ok {
		t.Fatal("unknown platform selector was accepted")
	}
}

func TestConstructorOwnsDecisionsAndRendering(t *testing.T) {
	err := New(ClassConnectorFailure, "provider_rate_limited", "llm-provider", "dispatch", map[string]any{"status": 429})
	failure, ok := As(err)
	if !ok {
		t.Fatalf("As() did not return canonical failure: %T", err)
	}
	if failure.Failure.Class != ClassConnectorFailure || !failure.Failure.Retryable || failure.Failure.Deterministic {
		t.Fatalf("failure envelope = %#v", failure.Failure)
	}
	if validationErr := ValidateEnvelope(failure.Failure); validationErr != nil {
		t.Fatalf("ValidateEnvelope() error = %v", validationErr)
	}
}

func TestInvalidConstructionFailsClosedAsInternalFailure(t *testing.T) {
	err := New(Class("platform.not_declared"), "bad-code", "test", "construct", nil)
	failure, ok := As(err)
	if !ok {
		t.Fatalf("As() did not return canonical failure: %T", err)
	}
	if failure.Failure.Class != ClassInternalFailure || failure.Failure.Detail.Code != "invalid_failure_construction" || failure.Failure.Retryable {
		t.Fatalf("failure envelope = %#v", failure.Failure)
	}
}

func TestFromErrorNeverDefaultsRawFailureToRetryable(t *testing.T) {
	failure := FromError(errors.New("temporary"), "engine", "execute")
	if failure.Failure.Class != ClassInternalFailure || failure.Failure.Retryable {
		t.Fatalf("raw failure = %#v", failure.Failure)
	}
}

func TestTypedErrorExtractionRejectsMalformedEnvelopeAuthority(t *testing.T) {
	malformed := func() error {
		return &Error{Failure: Envelope{
			SchemaVersion: EnvelopeSchemaVersion,
			Class:         ClassConnectorFailure,
			Detail:        Detail{Code: "provider_rate_limited"},
			Retryable:     true,
			Deterministic: false,
			Message:       "forged presentation",
			Remediation:   "forged remediation",
			Component:     "forged",
			Operation:     "classify",
		}}
	}
	assertCanonical := func(t *testing.T, failure Envelope) {
		t.Helper()
		if err := ValidateEnvelope(failure); err != nil {
			t.Fatalf("normalized failure is invalid: %v", err)
		}
		if failure.Class != ClassInternalFailure || failure.Detail.Code != "invalid_failure_construction" {
			t.Fatalf("normalized failure = %#v, want invalid construction", failure)
		}
	}

	t.Run("As", func(t *testing.T) {
		failure, ok := As(malformed())
		if !ok {
			t.Fatal("As() did not recognize typed error")
		}
		assertCanonical(t, failure.Failure)
	})
	t.Run("FromError", func(t *testing.T) {
		assertCanonical(t, FromError(malformed(), "test", "from_error").Failure)
	})
	t.Run("EnvelopeFromError", func(t *testing.T) {
		failure, ok := EnvelopeFromError(malformed())
		if !ok {
			t.Fatal("EnvelopeFromError() did not recognize typed error")
		}
		assertCanonical(t, failure)
	})
	t.Run("Normalize", func(t *testing.T) {
		assertCanonical(t, Normalize(malformed(), "test", "normalize"))
	})
}

func TestCloneEnvelopePreservesMalformedEvidence(t *testing.T) {
	malformed := &Envelope{SchemaVersion: "forged", Class: ClassConnectorFailure}
	cloned := CloneEnvelope(malformed)
	if cloned == nil {
		t.Fatal("CloneEnvelope() erased malformed evidence")
	}
	if err := ValidateEnvelope(*cloned); err == nil {
		t.Fatalf("CloneEnvelope() unexpectedly repaired malformed evidence: %#v", cloned)
	}

	valid := Normalize(New(ClassConnectorFailure, "provider_rate_limited", "test", "clone", map[string]any{
		"nested": map[string]any{"value": "original"},
	}), "test", "clone")
	validClone := CloneEnvelope(&valid)
	validClone.Detail.Attributes["nested"].(map[string]any)["value"] = "changed"
	if got := valid.Detail.Attributes["nested"].(map[string]any)["value"]; got != "original" {
		t.Fatalf("CloneEnvelope() aliased nested attributes: %v", got)
	}
}

func TestClassSpecificDetailValidation(t *testing.T) {
	tests := []struct {
		name       string
		class      Class
		detailCode string
		attributes map[string]any
	}{
		{name: "budget kind", class: ClassBudgetExhausted, detailCode: "limit", attributes: map[string]any{"budget_kind": "unknown"}},
		{name: "data limit", class: ClassDataLimitExceeded, detailCode: "limit", attributes: map[string]any{"limit_kind": "bytes", "limit": 1}},
		{name: "authentication kind", class: ClassAuthenticationNeeded, detailCode: "missing", attributes: nil},
		{name: "authorization action", class: ClassAuthorizationDenied, detailCode: "denied", attributes: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure, ok := As(New(test.class, test.detailCode, "test", "validate", test.attributes))
			if !ok || failure.Failure.Class != ClassInternalFailure || failure.Failure.Detail.Code != "invalid_failure_construction" {
				t.Fatalf("failure = %#v, %t", failure, ok)
			}
		})
	}
}

func TestRunTerminalPersistenceUnconfirmedDetailIsClosed(t *testing.T) {
	for _, status := range []string{"failed", "completed"} {
		err := New(ClassOutcomeUncertain, "run_terminal_persistence_unconfirmed", "builder.run_hub", "mark_run_terminal", map[string]any{"attempted_status": status})
		failure, ok := As(err)
		if !ok || failure.Failure.Class != ClassOutcomeUncertain {
			t.Fatalf("status %s failure = %#v, want outcome_uncertain", status, failure)
		}
	}
	for _, attributes := range []map[string]any{
		nil,
		{"attempted_status": "cancelled"},
		{"attempted_status": "failed", "cause": "raw"},
	} {
		err := New(ClassOutcomeUncertain, "run_terminal_persistence_unconfirmed", "builder.run_hub", "mark_run_terminal", attributes)
		failure, ok := As(err)
		if !ok || failure.Failure.Class != ClassInternalFailure || failure.Failure.Detail.Code != "invalid_failure_construction" {
			t.Fatalf("attributes %#v failure = %#v, want invalid construction", attributes, failure)
		}
	}
}

func TestSemanticFingerprintExcludesPresentationAndIncludesTypedDetail(t *testing.T) {
	failure, ok := EnvelopeFromError(New(ClassOutcomeUncertain, "run_terminal_persistence_unconfirmed", "builder.run_hub", "mark_run_terminal", map[string]any{"attempted_status": "failed"}))
	if !ok {
		t.Fatal("expected canonical failure")
	}
	want, err := SemanticFingerprint(failure)
	if err != nil {
		t.Fatalf("SemanticFingerprint: %v", err)
	}
	presentationChanged := failure
	presentationChanged.Message = "different presentation"
	presentationChanged.Remediation = "different remediation"
	got, err := SemanticFingerprint(presentationChanged)
	if err != nil {
		t.Fatalf("SemanticFingerprint(presentation changed): %v", err)
	}
	if got != want {
		t.Fatalf("presentation fingerprint = %q, want %q", got, want)
	}
	detailChanged := failure
	detailChanged.Detail.Attributes = map[string]any{"attempted_status": "completed"}
	got, err = SemanticFingerprint(detailChanged)
	if err != nil {
		t.Fatalf("SemanticFingerprint(detail changed): %v", err)
	}
	if got == want {
		t.Fatalf("typed detail fingerprint = %q, want change from %q", got, want)
	}
}
