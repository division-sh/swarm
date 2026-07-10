package main

import runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"

func testRuntimeFailure(detailCode string) *runtimefailures.Envelope {
	return testRuntimeFailureClass(runtimefailures.ClassConnectorFailure, detailCode)
}

func testRuntimeFailureClass(class runtimefailures.Class, detailCode string) *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(
		runtimefailures.New(class, detailCode, "cli-test", "read", nil),
		"cli-test",
		"read",
	)
	return &failure
}
