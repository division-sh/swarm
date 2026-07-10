package pipeline

import (
	"net/http"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func testPipelineFailure(class runtimefailures.Class, detailCode string) *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(
		runtimefailures.New(class, detailCode, "pipeline-test", "execute", nil),
		"pipeline-test",
		"execute",
	)
	return &failure
}

func TestActivityHTTPStatusFailureUsesCanonicalProviderMapping(t *testing.T) {
	tests := []struct {
		status        int
		class         runtimefailures.Class
		detailCode    string
		retryable     bool
		deterministic bool
	}{
		{status: http.StatusUnauthorized, class: runtimefailures.ClassAuthenticationNeeded, detailCode: "provider_unauthorized", deterministic: true},
		{status: http.StatusForbidden, class: runtimefailures.ClassAuthorizationDenied, detailCode: "provider_forbidden", deterministic: true},
		{status: http.StatusPaymentRequired, class: runtimefailures.ClassConnectorFailure, detailCode: "provider_credit_exhausted", deterministic: true},
		{status: http.StatusRequestTimeout, class: runtimefailures.ClassTimeout, detailCode: "provider_request_timeout", retryable: true},
		{status: http.StatusTooManyRequests, class: runtimefailures.ClassConnectorFailure, detailCode: "provider_http_status", retryable: true},
		{status: http.StatusServiceUnavailable, class: runtimefailures.ClassConnectorFailure, detailCode: "provider_http_status", retryable: true},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			failure, ok := runtimefailures.EnvelopeFromError(activityHTTPStatusFailure("provider_call", tt.status))
			if !ok {
				t.Fatal("activityHTTPStatusFailure did not return a canonical failure")
			}
			if failure.Class != tt.class || failure.Detail.Code != tt.detailCode {
				t.Fatalf("failure = %s/%s, want %s/%s", failure.Class, failure.Detail.Code, tt.class, tt.detailCode)
			}
			if failure.Retryable != tt.retryable || failure.Deterministic != tt.deterministic {
				t.Fatalf("decisions = retryable:%t deterministic:%t, want retryable:%t deterministic:%t", failure.Retryable, failure.Deterministic, tt.retryable, tt.deterministic)
			}
		})
	}
}
