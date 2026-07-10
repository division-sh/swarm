package store_test

import runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"

func testRetryableFailure() *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(
		runtimefailures.New(runtimefailures.ClassConnectorFailure, "test_connector_failure", "store-test", "delivery", nil),
		"store-test",
		"delivery",
	)
	return &failure
}
