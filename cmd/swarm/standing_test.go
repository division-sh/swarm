package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestStandingActionsConsumeCanonicalAPI(t *testing.T) {
	const serviceID = "11111111-1111-1111-1111-111111111111"
	const runID = "22222222-2222-2222-2222-222222222222"

	for _, action := range []string{"suspend", "resume", "reset"} {
		t.Run(action, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			var captured jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/rpc" {
					t.Errorf("path = %q, want /v1/rpc", r.URL.Path)
				}
				if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
					t.Errorf("Authorization = %q, want bearer token", got)
				}
				if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
					t.Errorf("decode request: %v", err)
				}
				writeJSONRPCResult(t, w, captured.ID, map[string]any{
					"service_id":      serviceID,
					"run_id":          runID,
					"generation":      2,
					"effective_state": action + "ed",
					"transition":      action,
				})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
				"standing", action, serviceID,
				"--reason", "operator request",
				"--idempotency-key", "idem-1",
			}, &stdout, &stderr, testRootCommandOptions(server))
			if code != cliExitOK {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if captured.JSONRPC != "2.0" || captured.Method != "standing."+action {
				t.Fatalf("request jsonrpc/method = %s/%s", captured.JSONRPC, captured.Method)
			}
			wantParams := map[string]any{
				"service_id":      serviceID,
				"reason":          "operator request",
				"idempotency_key": "idem-1",
			}
			if !reflect.DeepEqual(captured.Params, wantParams) {
				t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
			}
			for _, want := range []string{serviceID, runID, "generation=2", "transition=" + action} {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("stdout = %q, want %q", stdout.String(), want)
				}
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestStandingActionRejectsInvalidServiceIDBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"standing", "suspend", "not-a-uuid",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != controlCommandExitCodeValidation {
		t.Fatalf("code = %d, want %d", code, controlCommandExitCodeValidation)
	}
	if !strings.Contains(stderr.String(), "service id must be a UUID") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("RPC calls = %d, want 0", calls.Load())
	}
}
