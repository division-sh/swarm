package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestCLIAPIErrorExitCodeClassifiesSharedCategories(t *testing.T) {
	rpcError := func(code string) error {
		data, err := json.Marshal(map[string]any{"code": code})
		if err != nil {
			t.Fatalf("marshal rpc error data: %v", err)
		}
		return &jsonRPCError{Code: -32010, Message: "Application error: " + code, Data: data}
	}
	classifier := cliAPIErrorClassifier{
		notFoundCodes: []string{"RUN_NOT_FOUND"},
		conflictCodes: []string{"IDEMPOTENCY_CONFLICT", "RUN_ALREADY_TERMINAL"},
	}
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{name: "missing token", err: errCLIAPITokenRequired, want: cliExitAuth},
		{name: "http unauthorized", err: &cliAPIHTTPError{surface: "v1 RPC", statusCode: 401, message: "unauthorized"}, want: cliExitAuth},
		{name: "http forbidden", err: &cliAPIHTTPError{surface: "v1 RPC", statusCode: 403, message: "forbidden"}, want: cliExitAuth},
		{name: "http runtime", err: &cliAPIHTTPError{surface: "v1 RPC", statusCode: 502, message: "bad gateway"}, want: cliExitRuntime},
		{name: "rpc unauthorized", err: rpcError("UNAUTHORIZED"), want: cliExitAuth},
		{name: "rpc not found", err: rpcError("RUN_NOT_FOUND"), want: cliExitNotFound},
		{name: "rpc conflict", err: rpcError("IDEMPOTENCY_CONFLICT"), want: cliExitConflict},
		{name: "rpc state conflict", err: rpcError("RUN_ALREADY_TERMINAL"), want: cliExitConflict},
		{name: "unknown rpc", err: rpcError("UNKNOWN_CODE"), want: cliExitRuntime},
		{name: "transport", err: &cliAPITransportError{surface: "runtime API", endpoint: "http://127.0.0.1:1/v1/rpc", operation: "request", err: errors.New("connection refused")}, want: cliExitRuntime},
		{name: "malformed response", err: errors.New("malformed JSON-RPC response: jsonrpc=\"1.0\""), want: cliExitRuntime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := cliAPIErrorExitCode(tc.err, classifier); got != tc.want {
				t.Fatalf("exit = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCLIAPITransportDiagnosticRendersUserFacingRuntimeTarget(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want []string
	}{
		{
			name: "transport",
			err:  &cliAPITransportError{surface: "runtime API", endpoint: "http://127.0.0.1:1/v1/rpc", operation: "request", err: errors.New("connection refused")},
			want: []string{
				"ERROR: cannot reach the Swarm runtime at 127.0.0.1:1.",
				"Is the runtime running?",
				"Check the selected target with `swarm context current`; override with --api-server.",
			},
		},
		{
			name: "http auth",
			err:  &cliAPIHTTPError{surface: "runtime API", endpoint: "http://127.0.0.1:8081/proxy/v1/rpc", statusCode: 401, message: "unauthorized"},
			want: []string{
				"ERROR: the Swarm runtime at 127.0.0.1:8081/proxy rejected the request with status 401.",
				"Check API credentials",
			},
		},
		{
			name: "http runtime",
			err:  &cliAPIHTTPError{surface: "runtime API", endpoint: "http://127.0.0.1:8081/v1/rpc", statusCode: 503, message: "unavailable"},
			want: []string{
				"ERROR: the Swarm runtime at 127.0.0.1:8081 returned status 503.",
				"Check the runtime with `swarm health`",
			},
		},
		{
			name: "protocol",
			err:  &cliAPIProtocolError{surface: "runtime event stream", endpoint: "ws://127.0.0.1:8081/v1/ws", operation: "subscription response", err: errors.New("bad jsonrpc")},
			want: []string{
				"ERROR: the Swarm runtime at 127.0.0.1:8081 returned an invalid API response.",
				"Check the selected target with `swarm context current`; override with --api-server.",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := formatCLIAPIError(tc.err)
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Fatalf("diagnostic = %q, want substring %q", got, want)
				}
			}
			for _, forbidden := range []string{"v1 RPC", "v1 WS", "/v1/rpc", "/v1/ws", "Post ", "dial tcp", "connection refused"} {
				if strings.Contains(got, forbidden) {
					t.Fatalf("diagnostic = %q, must not leak %q", got, forbidden)
				}
			}
		})
	}
}

func TestCLIAPITransportDiagnosticPreservesWrapperContext(t *testing.T) {
	err := fmt.Errorf("scenario.yaml: step 2: %w", &cliAPITransportError{
		surface:   "runtime API",
		endpoint:  "http://127.0.0.1:1/v1/rpc",
		operation: "request",
		err:       errors.New("connection refused"),
	})

	got := formatCLIAPIError(err)
	for _, want := range []string{
		"ERROR: scenario.yaml: step 2: cannot reach the Swarm runtime at 127.0.0.1:1.",
		"Is the runtime running?",
		"Check the selected target with `swarm context current`; override with --api-server.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("diagnostic = %q, want substring %q", got, want)
		}
	}
	for _, forbidden := range []string{"runtime API request failed", "connection refused", "/v1/rpc"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("diagnostic = %q, must not leak %q", got, forbidden)
		}
	}

	detail := formatCLIAPITransportDetail(err)
	if want := "scenario.yaml: step 2: cannot reach the Swarm runtime at 127.0.0.1:1."; detail != want {
		t.Fatalf("detail = %q, want %q", detail, want)
	}
}

func TestCLIAPIErrorDoesNotClassifyGenericYAMLAsContractLoaderDiagnostic(t *testing.T) {
	err := errors.New("load runtime config: yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `oops` into config.Config")

	got := formatCLIAPIError(err)
	if got != err.Error() {
		t.Fatalf("diagnostic = %q, want original error %q", got, err.Error())
	}
	if strings.Contains(got, "contract YAML has a value with the wrong shape") {
		t.Fatalf("diagnostic misclassified generic YAML as contract loader error: %q", got)
	}
}

func TestJSONRPCErrorRendersStandardErrorDiagnostics(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"correlation_id": "corr-event-publish",
		"details": map[string]any{
			"error":  "postgres store is required",
			"run_id": "run-1",
		},
	})
	if err != nil {
		t.Fatalf("marshal rpc error data: %v", err)
	}
	got := (&jsonRPCError{Code: -32603, Message: "internal error", Data: data}).Error()
	for _, want := range []string{
		"JSON-RPC -32603: internal error",
		"correlation_id=corr-event-publish",
		"details: ",
		"error=postgres store is required",
		"run_id=run-1",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("error string = %q, want substring %q", got, want)
		}
	}
	if strings.TrimSpace(got) == "internal error" {
		t.Fatalf("error string = %q, want diagnostic context", got)
	}
}
