package tools_test

import (
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func requireToolFailure(t testing.TB, err error, class runtimefailures.Class, detailCode string) runtimefailures.Envelope {
	t.Helper()
	failure, ok := runtimefailures.As(err)
	if !ok {
		t.Fatalf("error = %T %v, want canonical failure", err, err)
	}
	if failure.Failure.Class != class || failure.Failure.Detail.Code != detailCode {
		t.Fatalf("failure = %s/%s, want %s/%s", failure.Failure.Class, failure.Failure.Detail.Code, class, detailCode)
	}
	return failure.Failure
}
