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

func TestForkCommandUsesRunForkRPCAndRenders(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	sourceRunID := "11111111-1111-1111-1111-111111111111"
	forkEventID := "22222222-2222-2222-2222-222222222222"
	bundleHash := validBundleHash("d")
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
		writeJSONRPCResult(t, w, captured.ID, validRunForkResult(sourceRunID, bundleHash))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"fork", sourceRunID,
		"--bundle-hash", bundleHash,
		"--at-event", forkEventID,
		"--idempotency-key", "idem-fork-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertForkRequest(t, captured, map[string]any{
		"source_run_id":   sourceRunID,
		"bundle_hash":     bundleHash,
		"fork_event_id":   forkEventID,
		"idempotency_key": "idem-fork-1",
	})
	for _, want := range []string{"Fork created", "source_run_id=" + sourceRunID, "fork_run_id=33333333-3333-3333-3333-333333333333", "bundle_hash=" + bundleHash, "owner=run.fork.selected_contracts.v1"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestForkCommandJSONPreservesAPIShape(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	sourceRunID := "55555555-5555-5555-5555-555555555555"
	bundleHash := validBundleHash("e")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validRunForkResult(sourceRunID, bundleHash))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"fork", sourceRunID, "--json"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertForkRequest(t, captured, map[string]any{"source_run_id": sourceRunID})
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("decode stdout json: %v\n%s", err, stdout.String())
	}
	if decoded["source_run_id"] != sourceRunID || decoded["fork_run_id"] != "33333333-3333-3333-3333-333333333333" {
		t.Fatalf("json run fork result = %#v", decoded)
	}
	for _, wrapper := range []string{"fork", "run"} {
		if _, ok := decoded[wrapper]; ok {
			t.Fatalf("json output contains CLI wrapper %q: %#v", wrapper, decoded)
		}
	}
}

func TestForkCommandRejectsInvalidInputBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "missing source", args: []string{"fork"}, wantStderr: "accepts 1 arg(s)"},
		{name: "blank source", args: []string{"fork", " "}, wantStderr: "source run id is required"},
		{name: "invalid source", args: []string{"fork", "bad id!"}, wantStderr: "source run id must be a UUID"},
		{name: "opaque non uuid source", args: []string{"fork", "run_opaque-1"}, wantStderr: "source run id must be a UUID"},
		{name: "invalid bundle hash", args: []string{"fork", "11111111-1111-1111-1111-111111111111", "--bundle-hash", "sha256:abc"}, wantStderr: "--bundle-hash must match bundle-v1:sha256:<64 lowercase hex>"},
		{name: "blank bundle hash", args: []string{"fork", "11111111-1111-1111-1111-111111111111", "--bundle-hash", ""}, wantStderr: "--bundle-hash must be non-empty"},
		{name: "invalid at event", args: []string{"fork", "11111111-1111-1111-1111-111111111111", "--at-event", "bad id!"}, wantStderr: "--at-event must be a UUID"},
		{name: "opaque non uuid at event", args: []string{"fork", "11111111-1111-1111-1111-111111111111", "--at-event", "event_opaque-1"}, wantStderr: "--at-event must be a UUID"},
		{name: "blank at event", args: []string{"fork", "11111111-1111-1111-1111-111111111111", "--at-event", ""}, wantStderr: "--at-event must be non-empty"},
		{name: "blank idempotency", args: []string{"fork", "11111111-1111-1111-1111-111111111111", "--idempotency-key", ""}, wantStderr: "--idempotency-key must be non-empty"},
		{name: "legacy dry run flag", args: []string{"fork", "run-1", "--dry-run"}, wantStderr: "unknown flag"},
		{name: "legacy materialize flag", args: []string{"fork", "run-1", "--materialize-only"}, wantStderr: "unknown flag"},
		{name: "legacy activate flag", args: []string{"fork", "run-1", "--activate"}, wantStderr: "unknown flag"},
		{name: "legacy contracts flag", args: []string{"fork", "run-1", "--contracts", "."}, wantStderr: "unknown flag"},
		{name: "draft bundle flag", args: []string{"fork", "run-1", "--bundle", validBundleHash("a")}, wantStderr: "unknown flag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != cliExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
			if calls.Load() != 0 {
				t.Fatalf("RPC calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestForkCommandFailClosedOnRPCAndMalformedResponses(t *testing.T) {
	sourceRunID := "11111111-1111-1111-1111-111111111111"
	bundleHash := validBundleHash("f")
	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "run not found",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeRunForkJSONRPCError(t, w, req.ID, "RUN_NOT_FOUND")
			},
			wantCode:   cliExitNotFound,
			wantStderr: "RUN_NOT_FOUND: Application error: RUN_NOT_FOUND",
		},
		{
			name: "event not found",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeRunForkJSONRPCError(t, w, req.ID, "EVENT_NOT_FOUND")
			},
			wantCode:   cliExitNotFound,
			wantStderr: "EVENT_NOT_FOUND: Application error: EVENT_NOT_FOUND",
		},
		{
			name: "idempotency conflict",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeRunForkJSONRPCError(t, w, req.ID, "IDEMPOTENCY_CONFLICT")
			},
			wantCode:   cliExitConflict,
			wantStderr: "IDEMPOTENCY_CONFLICT: Application error: IDEMPOTENCY_CONFLICT",
		},
		{
			name: "cross bundle split failure",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeRunForkJSONRPCError(t, w, req.ID, "UNSUPPORTED_BUNDLE_HASH_FORK")
			},
			wantCode:   cliExitRuntime,
			wantStderr: "UNSUPPORTED_BUNDLE_HASH_FORK: Application error: UNSUPPORTED_BUNDLE_HASH_FORK",
		},
		{
			name: "malformed missing owner",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validRunForkResult(sourceRunID, bundleHash)
				delete(result, "owner")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed run.fork result: owner is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"fork", sourceRunID}, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func assertForkRequest(t *testing.T, req jsonRPCRequest, wantParams map[string]any) {
	t.Helper()
	if req.JSONRPC != "2.0" || req.Method != runForkMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", req.JSONRPC, req.Method, runForkMethod)
	}
	if !reflect.DeepEqual(req.Params, wantParams) {
		t.Fatalf("%s params = %#v, want %#v", runForkMethod, req.Params, wantParams)
	}
}

func validRunForkResult(sourceRunID, bundleHash string) map[string]any {
	return map[string]any{
		"owner":                "run.fork.selected_contracts.v1",
		"source_run_id":        sourceRunID,
		"fork_run_id":          "33333333-3333-3333-3333-333333333333",
		"fork_event_id":        "44444444-4444-4444-4444-444444444444",
		"fork_run_status":      "running",
		"bundle_hash":          bundleHash,
		"executed_event_count": 1,
	}
}

func writeRunForkJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
	t.Helper()
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32010,
			"message": "Application error: " + code,
			"data": map[string]any{
				"code":           code,
				"details":        map[string]any{"source_run_id": "missing"},
				"retryable":      false,
				"correlation_id": "corr-run-fork",
			},
		},
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
