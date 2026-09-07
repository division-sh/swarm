package engine

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/failures"
)

func TestNormalizeFailureAndDisposition(t *testing.T) {
	if got := NormalizeFailure(nil, "test", "nil"); got != nil {
		t.Fatalf("nil failure = %#v", got)
	}
	tests := []struct {
		name        string
		err         error
		class       failures.Class
		disposition FailureDisposition
	}{
		{name: "chain depth", err: ErrChainDepthExceeded, class: failures.ClassChainDepthExceeded, disposition: FailureDispositionDeadLetter},
		{name: "missing state repo", err: ErrMissingStateRepo, class: failures.ClassDependencyUnavailable, disposition: FailureDispositionRetry},
		{name: "fan out bound", err: ErrFanOutBoundExceeded, class: failures.ClassFanOutBoundExceeded, disposition: FailureDispositionTerminal},
		{name: "raw error", err: errors.New("temporary"), class: failures.ClassInternalFailure, disposition: FailureDispositionTerminal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure := NormalizeFailure(test.err, "test", test.name)
			if failure.Failure.Class != test.class {
				t.Fatalf("class = %s, want %s", failure.Failure.Class, test.class)
			}
			if got := FailureDispositionFor(test.err); got != test.disposition {
				t.Fatalf("disposition = %s, want %s", got, test.disposition)
			}
		})
	}
}

func TestNormalizeFailureRoutesTypedErrorsThroughCausePreservingOwner(t *testing.T) {
	innerCause := errors.New("provider socket closed")
	inner := failures.Wrap(
		failures.ClassConnectorFailure,
		"provider_rate_limited",
		"provider",
		"call",
		map[string]any{"status": 429},
		innerCause,
	).(*failures.Error)
	outer := fmt.Errorf("manager receipt: %w", inner)

	normalized := NormalizeFailure(outer, "agent-manager", "process_event.on_event")
	if normalized == inner {
		t.Fatal("NormalizeFailure returned the extracted inner failure")
	}
	if !reflect.DeepEqual(normalized.Failure, inner.Failure) {
		t.Fatalf("normalized envelope = %#v, want %#v", normalized.Failure, inner.Failure)
	}
	if !errors.Is(normalized, outer) || !errors.Is(normalized, inner) || !errors.Is(normalized, innerCause) {
		t.Fatalf("normalized chain does not retain caller, typed, and root causes: %v", normalized)
	}
}

func TestNormalizeFailurePreservesTypedEmitContractEvidence(t *testing.T) {
	cause := errors.New("$.rows[3].gem_score must be number")
	err := &EmitPayloadContractError{
		Event: "company.registered", Kind: EmitPayloadSchemaMismatch,
		Path: "$.rows[3].gem_score", Constraint: "type", Expected: "number", Actual: "string",
		Detail: "payload violates schema: $.rows[3].gem_score must be number", Cause: cause,
	}

	failure := NormalizeFailure(err, "runtime.engine", "fan_out.emit")
	if failure.Failure.Class != failures.ClassSchemaInvalid || failure.Failure.Detail.Code != "emit_payload_contract_violation" {
		t.Fatalf("normalized failure = %#v", failure.Failure)
	}
	want := err.Attributes()
	if !reflect.DeepEqual(failure.Failure.Detail.Attributes, want) {
		t.Fatalf("normalized attributes = %#v, want %#v", failure.Failure.Detail.Attributes, want)
	}
	if !errors.Is(failure, err) || !errors.Is(failure, cause) {
		t.Fatalf("normalized failure lost typed cause chain: %v", failure)
	}
}

func TestNormalizeFailureAdmitsPresentEmptySchemaMismatchActual(t *testing.T) {
	err := &EmitPayloadContractError{
		Event: "company.registered", Kind: EmitPayloadSchemaMismatch,
		Path: "$.external_id", Constraint: "format", Expected: "uuid", Actual: "",
		Detail: "payload violates schema: $.external_id must be uuid",
	}

	failure := NormalizeFailure(err, "runtime.engine", "fan_out.emit")
	if !IsEmitPayloadContractFailure(err) || !IsEmitPayloadContractFailure(failure) {
		t.Fatalf("present empty actual was not admitted as exact typed evidence: %#v", failure.Failure)
	}
	actual, present := failure.Failure.Detail.Attributes["actual"].(string)
	if !present || actual != "" {
		t.Fatalf("normalized actual = %#v, want present empty string", failure.Failure.Detail.Attributes["actual"])
	}
}

func TestNormalizeFailureRejectsBareEmitContractSentinelAsSemanticEvidence(t *testing.T) {
	failure := NormalizeFailure(ErrEmitPayloadContractViolation, "runtime.engine", "fan_out.emit")
	if failure.Failure.Class != failures.ClassInternalFailure || failure.Failure.Detail.Code == "emit_payload_contract_violation" {
		t.Fatalf("bare sentinel normalized as semantic evidence: %#v", failure.Failure)
	}
}
