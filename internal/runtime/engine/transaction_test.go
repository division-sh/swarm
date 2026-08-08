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
