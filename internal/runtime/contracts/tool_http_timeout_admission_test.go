package contracts

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func TestToolHTTPTimeoutDurationAdmission(t *testing.T) {
	const largestSeconds = int(math.MaxInt64 / int64(time.Second))
	for _, row := range []struct {
		name    string
		seconds int
		want    time.Duration
	}{
		{"omitted", 0, 30 * time.Second},
		{"authored_10s", 10, 10 * time.Second},
		{"largest", largestSeconds, time.Duration(largestSeconds) * time.Second},
	} {
		t.Run(row.name, func(t *testing.T) {
			execution, err := AdmitToolHTTPExecution(HTTPToolSpec{Method: "GET", URL: "https://example.invalid", TimeoutSeconds: row.seconds})
			if err != nil {
				t.Fatal(err)
			}
			request, err := execution.Prepare(nil, nil)
			if err != nil || request.Timeout() != row.want {
				t.Fatalf("prepared timeout = %v, want %v: %v", request.Timeout(), row.want, err)
			}
		})
	}
	for _, seconds := range []int{-1, largestSeconds + 1, math.MaxInt64} {
		t.Run(fmt.Sprint(seconds), func(t *testing.T) {
			execution, err := AdmitToolHTTPExecution(HTTPToolSpec{Method: "GET", URL: "https://example.invalid", TimeoutSeconds: seconds})
			if err == nil {
				request, prepareErr := execution.Prepare(nil, nil)
				t.Fatalf("unrepresentable timeout admitted: seconds=%d prepared=%v err=%v", seconds, request.Timeout(), prepareErr)
			}
			if !strings.Contains(err.Error(), "http.timeout_seconds") {
				t.Fatalf("timeout refusal lost field: %v", err)
			}
		})
	}
}

func TestAuthoredToolHTTPTimeoutDurationAdmission(t *testing.T) {
	for _, row := range []struct {
		value string
		valid bool
	}{
		{"", true}, {"10", true}, {"9223372036", true},
		{"9223372037", false}, {"9223372036854775807", false},
		{"0", false}, {"-1", false}, {"null", false}, {"1.5", false},
	} {
		t.Run("seconds_"+row.value, func(t *testing.T) {
			timeout := ""
			if row.value != "" {
				timeout = ", timeout_seconds: " + row.value
			}
			entries, err := admitW5Tools(t, "worker:\n  handler_type: http\n  http: {method: GET, url: 'https://example.invalid'"+timeout+"}\n")
			if !row.valid {
				if err == nil {
					t.Fatalf("authored timeout %q admitted", row.value)
				}
				if !strings.Contains(err.Error(), "tools.yaml:3:") {
					t.Fatalf("refusal lost authored source coordinate: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			execution, ok := entries["worker"].HTTPExecution()
			if !ok {
				t.Fatal("admitted HTTP execution missing")
			}
			request, err := execution.Prepare(nil, nil)
			if err != nil || request.Timeout() <= 0 {
				t.Fatalf("authored timeout was not honored: %v: %v", request.Timeout(), err)
			}
		})
	}
}

func TestAuthoredToolHTTPTimeoutClosedFields(t *testing.T) {
	for _, fields := range []string{
		"TimeoutSeconds: 7",
		"timeout_seconds: 1, TimeoutSeconds: 7",
		"timeout_seconds: '10'",
		"timeout_seconds: true",
	} {
		t.Run(fields, func(t *testing.T) {
			_, err := admitW5Tools(t, "worker:\n  handler_type: http\n  http: {method: GET, url: 'https://example.invalid', "+fields+"}\n")
			if err == nil || !strings.Contains(err.Error(), "tools.yaml:3:") {
				t.Fatalf("invalid HTTP field accepted or source lost: fields=%s err=%v", fields, err)
			}
		})
	}
}
