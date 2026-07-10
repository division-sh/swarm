package tools

import (
	"context"
	"errors"
	"net/http"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestClassifyHTTPToolFailureUsesCanonicalTaxonomy(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		class         runtimefailures.Class
		detailCode    string
		retryable     bool
		deterministic bool
	}{
		{name: "unauthorized", err: httpToolStatusError{StatusCode: http.StatusUnauthorized}, class: runtimefailures.ClassAuthenticationNeeded, detailCode: "provider_unauthorized", deterministic: true},
		{name: "forbidden", err: httpToolStatusError{StatusCode: http.StatusForbidden}, class: runtimefailures.ClassAuthorizationDenied, detailCode: "provider_forbidden", deterministic: true},
		{name: "credit exhausted", err: httpToolStatusError{StatusCode: http.StatusPaymentRequired}, class: runtimefailures.ClassConnectorFailure, detailCode: "provider_credit_exhausted", deterministic: true},
		{name: "request timeout", err: httpToolStatusError{StatusCode: http.StatusRequestTimeout}, class: runtimefailures.ClassTimeout, detailCode: "provider_request_timeout", retryable: true},
		{name: "rate limited", err: httpToolStatusError{StatusCode: http.StatusTooManyRequests}, class: runtimefailures.ClassConnectorFailure, detailCode: "provider_http_status", retryable: true},
		{name: "provider unavailable", err: httpToolStatusError{StatusCode: http.StatusServiceUnavailable}, class: runtimefailures.ClassConnectorFailure, detailCode: "provider_http_status", retryable: true},
		{name: "transport timeout", err: context.DeadlineExceeded, class: runtimefailures.ClassTimeout, detailCode: "http_tool_timeout", retryable: true},
		{name: "transport failure", err: errors.New("connection refused"), class: runtimefailures.ClassConnectorFailure, detailCode: "http_tool_request_failed", retryable: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyHTTPToolFailure(tt.err, "provider_call")
			failure := requireToolFailure(t, err, tt.class, tt.detailCode)
			if failure.Retryable != tt.retryable || failure.Deterministic != tt.deterministic {
				t.Fatalf("decisions = retryable:%t deterministic:%t, want retryable:%t deterministic:%t", failure.Retryable, failure.Deterministic, tt.retryable, tt.deterministic)
			}
		})
	}
}
