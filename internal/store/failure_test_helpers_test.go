package store

import (
	"encoding/json"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func testRetryableFailure() *runtimefailures.Envelope {
	failure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "test_connector_failure", nil)
	return &failure
}

func testFailureEnvelope(class runtimefailures.Class, detailCode string, attributes map[string]any) runtimefailures.Envelope {
	return runtimefailures.Normalize(runtimefailures.New(
		class,
		detailCode,
		"store-test",
		"exercise_failure_path",
		attributes,
	), "store-test", "exercise_failure_path")
}

func mustMarshalTestFailure(t testing.TB, failure runtimefailures.Envelope) string {
	t.Helper()
	raw, err := json.Marshal(failure)
	if err != nil {
		t.Fatalf("marshal test failure: %v", err)
	}
	return string(raw)
}

func failureDetailCode(failure *runtimefailures.Envelope) string {
	if failure == nil {
		return ""
	}
	return failure.Detail.Code
}
