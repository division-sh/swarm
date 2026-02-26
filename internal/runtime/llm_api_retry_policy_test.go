package runtime

import (
	"errors"
	"testing"
)

func TestShouldRetryAnthropicError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "429", err: anthropicHTTPError{StatusCode: 429, Message: "rate"}, want: true},
		{name: "500", err: anthropicHTTPError{StatusCode: 500, Message: "server"}, want: true},
		{name: "400", err: anthropicHTTPError{StatusCode: 400, Message: "bad"}, want: false},
		{name: "401", err: anthropicHTTPError{StatusCode: 401, Message: "unauth"}, want: false},
		{name: "timeout text", err: errors.New("request failed: timeout"), want: true},
		{name: "context canceled", err: errors.New("context canceled"), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldRetryAnthropicError(tc.err)
			if got != tc.want {
				t.Fatalf("shouldRetryAnthropicError(%v)=%v want=%v", tc.err, got, tc.want)
			}
		})
	}
}
