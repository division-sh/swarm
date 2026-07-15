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
	setCLIAPITestToken(t, "test-token")
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
		"run", "fork", sourceRunID,
		"--confirm-source-freeze",
		"--bundle-hash", bundleHash,
		"--at-event", forkEventID,
		"--idempotency-key", "idem-fork-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertForkRequest(t, captured, map[string]any{
		"source_run_id":         sourceRunID,
		"confirm_source_freeze": true,
		"bundle_hash":           bundleHash,
		"fork_event_id":         forkEventID,
		"idempotency_key":       "idem-fork-1",
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
	setCLIAPITestToken(t, "test-token")
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
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "fork", sourceRunID, "--confirm-source-freeze", "--json"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertForkRequest(t, captured, map[string]any{"source_run_id": sourceRunID, "confirm_source_freeze": true})
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("decode stdout json: %v\n%s", err, stdout.String())
	}
	if decoded["source_run_id"] != sourceRunID || decoded["fork_run_id"] != "33333333-3333-3333-3333-333333333333" {
		t.Fatalf("json run fork result = %#v", decoded)
	}
	for _, wrapper := range []string{"run", "fork", "run"} {
		if _, ok := decoded[wrapper]; ok {
			t.Fatalf("json output contains CLI wrapper %q: %#v", wrapper, decoded)
		}
	}
}

func TestForkCommandRejectsInvalidInputBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
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
		{name: "missing source", args: []string{"run", "fork"}, wantStderr: "requires <source-run-id>"},
		{name: "blank source", args: []string{"run", "fork", " "}, wantStderr: "source run id is required"},
		{name: "invalid source", args: []string{"run", "fork", "bad id!"}, wantStderr: "source run id must be a UUID"},
		{name: "opaque non uuid source", args: []string{"run", "fork", "run_opaque-1"}, wantStderr: "source run id must be a UUID"},
		{name: "invalid bundle hash", args: []string{"run", "fork", "11111111-1111-1111-1111-111111111111", "--bundle-hash", "sha256:abc"}, wantStderr: "--bundle-hash must match bundle-v1:sha256:<64 lowercase hex>"},
		{name: "blank bundle hash", args: []string{"run", "fork", "11111111-1111-1111-1111-111111111111", "--bundle-hash", ""}, wantStderr: "--bundle-hash must be non-empty"},
		{name: "invalid at event", args: []string{"run", "fork", "11111111-1111-1111-1111-111111111111", "--at-event", "bad id!"}, wantStderr: "--at-event must be a UUID"},
		{name: "opaque non uuid at event", args: []string{"run", "fork", "11111111-1111-1111-1111-111111111111", "--at-event", "event_opaque-1"}, wantStderr: "--at-event must be a UUID"},
		{name: "blank at event", args: []string{"run", "fork", "11111111-1111-1111-1111-111111111111", "--at-event", ""}, wantStderr: "--at-event must be non-empty"},
		{name: "blank idempotency", args: []string{"run", "fork", "11111111-1111-1111-1111-111111111111", "--idempotency-key", ""}, wantStderr: "--idempotency-key must be non-empty"},
		{name: "legacy dry run flag", args: []string{"run", "fork", "run-1", "--dry-run"}, wantStderr: "unknown flag"},
		{name: "legacy materialize flag", args: []string{"run", "fork", "run-1", "--materialize-only"}, wantStderr: "unknown flag"},
		{name: "legacy activate flag", args: []string{"run", "fork", "run-1", "--activate"}, wantStderr: "unknown flag"},
		{name: "legacy contracts flag", args: []string{"run", "fork", "run-1", "--contracts", "."}, wantStderr: "unknown flag"},
		{name: "draft bundle flag", args: []string{"run", "fork", "run-1", "--bundle", validBundleHash("a")}, wantStderr: "unknown flag"},
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
			name: "target bundle unavailable",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeRunForkJSONRPCError(t, w, req.ID, "BUNDLE_UNAVAILABLE")
			},
			wantCode:   cliExitRuntime,
			wantStderr: "BUNDLE_UNAVAILABLE: Application error: BUNDLE_UNAVAILABLE",
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
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "fork", sourceRunID, "--confirm-source-freeze"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestForkCommandConfirmsOnlyActiveSourceFreeze(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	const sourceRunID = "11111111-1111-1111-1111-111111111111"
	bundleHash := validBundleHash("a")

	for _, tc := range []struct {
		name          string
		status        string
		args          []string
		input         string
		stdinTerminal bool
		wantCode      int
		wantCalls     int
		wantConfirmed bool
		wantStderr    string
	}{
		{name: "active non tty requires flag", status: "running", args: []string{"run", "fork", sourceRunID}, wantCode: cliExitValidation, wantCalls: 1, wantStderr: "pass --confirm-source-freeze"},
		{name: "active tty refusal aborts", status: "paused", args: []string{"run", "fork", sourceRunID}, input: "n\n", stdinTerminal: true, wantCode: cliExitValidation, wantCalls: 1, wantStderr: "source run was not frozen"},
		{name: "active tty confirmation proceeds", status: "running", args: []string{"run", "fork", sourceRunID}, input: "yes\n", stdinTerminal: true, wantCalls: 2, wantConfirmed: true, wantStderr: "Continue? [y/N]"},
		{name: "terminal source needs no ceremony", status: "completed", args: []string{"run", "fork", sourceRunID}, wantCalls: 2},
		{name: "explicit flag bypasses preflight", status: "running", args: []string{"run", "fork", sourceRunID, "--confirm-source-freeze"}, wantCalls: 1, wantConfirmed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls []jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("decode request: %v", err)
				}
				calls = append(calls, req)
				switch req.Method {
				case runCommandMethodGet:
					writeJSONRPCResult(t, w, req.ID, map[string]any{"run": validDiagnosticRunHeaderWithStatus(sourceRunID, tc.status)})
				case runForkMethod:
					writeJSONRPCResult(t, w, req.ID, validRunForkResult(sourceRunID, bundleHash))
				default:
					t.Errorf("unexpected method %q", req.Method)
				}
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			opts := testRootCommandOptions(server)
			opts.input = strings.NewReader(tc.input)
			opts.stdinIsTerminal = func() bool { return tc.stdinTerminal }
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, opts)
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if len(calls) != tc.wantCalls {
				t.Fatalf("RPC calls = %d, want %d", len(calls), tc.wantCalls)
			}
			if tc.wantStderr != "" && !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
			if tc.wantCalls > 0 && calls[len(calls)-1].Method == runForkMethod {
				confirmed, _ := calls[len(calls)-1].Params["confirm_source_freeze"].(bool)
				if confirmed != tc.wantConfirmed {
					t.Fatalf("confirm_source_freeze = %v, want %v", confirmed, tc.wantConfirmed)
				}
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
