package conformance

import runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"

func testFailure(detailCode string) *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(
		runtimefailures.New(runtimefailures.ClassConnectorFailure, detailCode, "conformance", "delivery", nil),
		"conformance",
		"delivery",
	)
	return &failure
}
