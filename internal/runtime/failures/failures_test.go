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
