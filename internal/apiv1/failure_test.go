package apiv1

import (
	"encoding/json"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func testFailure(detailCode string) *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(
		runtimefailures.New(runtimefailures.ClassConnectorFailure, detailCode, "api-test", "read", nil),
		"api-test",
		"read",
	)
	return &failure
}

func mustMarshalTestFailure(t testing.TB, failure *runtimefailures.Envelope) string {
	t.Helper()
	raw, err := json.Marshal(failure)
	if err != nil {
		t.Fatalf("marshal test failure: %v", err)
	}
	return string(raw)
}
