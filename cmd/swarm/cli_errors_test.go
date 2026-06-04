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
		{name: "transport", err: fmt.Errorf("v1 RPC request failed: %w", errors.New("connection refused")), want: cliExitRuntime},
		{name: "malformed response", err: errors.New("malformed JSON-RPC response: jsonrpc=\"1.0\""), want: cliExitRuntime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := cliAPIErrorExitCode(tc.err, classifier); got != tc.want {
				t.Fatalf("exit = %d, want %d", got, tc.want)
			}
		})
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
