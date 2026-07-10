package agentcontrol

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestDirectiveFailureMappingsAreClosed(t *testing.T) {
	tests := []struct {
		name          string
		failure       runtimefailures.Envelope
		class         runtimefailures.Class
		detail        string
		component     string
		operation     string
		deterministic bool
	}{
		{name: "untyped board step", failure: DirectiveBoardStepFailure(errors.New("provider prose")), class: runtimefailures.ClassInternalFailure, detail: DirectiveBoardStepFailedDetail, component: directiveManagerComponent, operation: "execute_directive"},
		{name: "heartbeat shutdown", failure: DirectiveHeartbeatShutdownUnconfirmedFailure(), class: runtimefailures.ClassOutcomeUncertain, detail: DirectiveHeartbeatShutdownUnconfirmedDetail, component: directiveManagerComponent, operation: "stop_directive_heartbeat"},
		{name: "failure persistence", failure: DirectiveFailurePersistenceUnconfirmedFailure(), class: runtimefailures.ClassOutcomeUncertain, detail: DirectiveFailurePersistenceUnconfirmedDetail, component: directiveManagerComponent, operation: "finalize_directive_failure"},
		{name: "result persistence", failure: DirectiveResultPersistenceUnconfirmedFailure(), class: runtimefailures.ClassOutcomeUncertain, detail: DirectiveResultPersistenceUnconfirmedDetail, component: directiveManagerComponent, operation: "record_directive_result"},
		{name: "lease expired", failure: DirectiveExecutionLeaseExpiredFailure(), class: runtimefailures.ClassOutcomeUncertain, detail: DirectiveExecutionLeaseExpiredDetail, component: directiveOperationRecoveryComponent, operation: "reconcile_execution_lease"},
		{name: "not admitted", failure: DirectiveExecutionNotAdmittedFailure(), class: runtimefailures.ClassInternalFailure, detail: DirectiveExecutionNotAdmittedDetail, component: directiveOperationRecoveryComponent, operation: "reconcile_prepared_operation", deterministic: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := runtimefailures.ValidateEnvelope(test.failure); err != nil {
				t.Fatalf("ValidateEnvelope: %v", err)
			}
			if test.failure.Class != test.class || test.failure.Detail.Code != test.detail || test.failure.Component != test.component || test.failure.Operation != test.operation {
				t.Fatalf("failure = %#v", test.failure)
			}
			if test.failure.Retryable || test.failure.Deterministic != test.deterministic || len(test.failure.Detail.Attributes) != 0 {
				t.Fatalf("failure decisions/detail = %#v", test.failure)
			}
			encoded, err := json.Marshal(test.failure)
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded) == "" || strings.Contains(string(encoded), "provider prose") || strings.Contains(string(encoded), "storage prose") {
				t.Fatalf("failure leaked raw prose: %s", encoded)
			}
		})
	}
}

func TestDirectiveBoardStepFailurePreservesTypedEnvelope(t *testing.T) {
	want, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.ClassAuthenticationNeeded, "provider_unauthorized", "llm-provider", "dispatch", map[string]any{"auth_kind": "provider_credential"}))
	if !ok {
		t.Fatal("construct typed failure")
	}
	got := DirectiveBoardStepFailure(runtimefailures.FromEnvelope(want))
	wantRaw, _ := runtimefailures.MarshalEnvelope(want)
	gotRaw, _ := runtimefailures.MarshalEnvelope(got)
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("preserved failure = %s, want %s", gotRaw, wantRaw)
	}
}

func TestValidateDirectiveOperationEvidenceMatrix(t *testing.T) {
	response := json.RawMessage(`{"ok":true}`)
	failure := DirectiveExecutionNotAdmittedFailure()
	valid := []DirectiveOperation{
		{State: DirectiveOperationPrepared},
		{State: DirectiveOperationExecuting},
		{State: DirectiveOperationExecuted, Response: response},
		{State: DirectiveOperationSucceeded, Response: response},
		{State: DirectiveOperationFailed, Failure: &failure},
		{State: DirectiveOperationIndeterminate, Failure: &failure},
	}
	for _, op := range valid {
		if err := ValidateDirectiveOperationEvidence(op); err != nil {
			t.Fatalf("state %s valid evidence: %v", op.State, err)
		}
	}
	for _, op := range []DirectiveOperation{
		{State: DirectiveOperationPrepared, Response: response},
		{State: DirectiveOperationExecuting, Failure: &failure},
		{State: DirectiveOperationExecuted},
		{State: DirectiveOperationSucceeded, Failure: &failure},
		{State: DirectiveOperationFailed},
		{State: DirectiveOperationIndeterminate, Response: response, Failure: &failure},
	} {
		if err := ValidateDirectiveOperationEvidence(op); err == nil {
			t.Fatalf("state %s accepted invalid evidence %#v", op.State, op)
		}
	}
}
