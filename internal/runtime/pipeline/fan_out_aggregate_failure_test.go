package pipeline

import (
	"errors"
	"fmt"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestFanOutSafeAggregateFailureReturnsMatchedEvidence(t *testing.T) {
	cause := errors.New("native publication rejection")
	envelope := fanOutFailureEnvelope(t, runtimefailures.ClassSchemaInvalid, "fan_out_test_aggregate_invalid")
	err := NewFanOutSafeAggregateError(envelope, cause)
	want, ok := err.(*FanOutSafeAggregateError)
	if !ok {
		t.Fatalf("construct aggregate: %v", err)
	}
	for _, tc := range []struct {
		name string
		err  error
		want *FanOutSafeAggregateError
	}{
		{"direct", err, want},
		{"wrapped", fmt.Errorf("publication: %w", err), want},
		{"joined", errors.Join(errors.New("other failure"), err), want},
		{"nil", nil, nil},
		{"unrelated", cause, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, matched := FanOutSafeAggregateFailure(tc.err)
			if got != tc.want || matched != (tc.want != nil) {
				t.Fatalf("extracted evidence = (%p, %v), want (%p, %v)", got, matched, tc.want, tc.want != nil)
			}
			if matched && (got.Failure.Detail.Code != envelope.Detail.Code || !errors.Is(got, cause)) {
				t.Fatalf("extraction lost canonical failure or cause: %#v", got)
			}
		})
	}
}
