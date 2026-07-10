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

func requireRPCFailure(t testing.TB, rpcErr *rpcError, class runtimefailures.Class, detailCode string) runtimefailures.Envelope {
	t.Helper()
	if rpcErr == nil {
		t.Fatal("RPC failure is required")
	}
	raw, err := json.Marshal(rpcErr.Data)
	if err != nil {
		t.Fatalf("marshal RPC error data: %v", err)
	}
	var data struct {
		Details struct {
			Failure json.RawMessage `json:"failure"`
		} `json:"details"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatalf("decode RPC error data: %v", err)
	}
	failure, err := runtimefailures.UnmarshalEnvelope(data.Details.Failure)
	if err != nil {
		t.Fatalf("decode RPC failure: %v data=%s", err, raw)
	}
	if failure.Class != class || failure.Detail.Code != detailCode {
		t.Fatalf("RPC failure = %#v, want %s/%s", failure, class, detailCode)
	}
	return failure
}
