package server

import (
	"encoding/json"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func testFailure(detailCode string) *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(
		runtimefailures.New(runtimefailures.ClassConnectorFailure, detailCode, "dashboard-test", "read", nil),
		"dashboard-test",
		"read",
	)
	return &failure
}

func mustMarshalFailure(t testing.TB, failure *runtimefailures.Envelope) string {
	t.Helper()
	raw, err := json.Marshal(failure)
	if err != nil {
		t.Fatalf("marshal failure: %v", err)
	}
	return string(raw)
}
