package cliapp

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

func TestControlRunSingleCommandsSendV1RPCRequests(t *testing.T) {
	for _, tc := range []struct {
		action     string
		wantMethod string
		wantOutput string
	}{
		{action: "pause", wantMethod: controlCommandRunPauseMethod, wantOutput: "control pause ok: scope=run run_id=run-1"},
		{action: "continue", wantMethod: controlCommandRunContinueMethod, wantOutput: "control continue ok: scope=run run_id=run-1"},
		{action: "stop", wantMethod: controlCommandRunStopMethod, wantOutput: "control stop ok: scope=run run_id=run-1"},
	} {
		t.Run(tc.action, func(t *testing.T) {
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
				writeJSONRPCResult(t, w, captured.ID, map[string]any{"ok": true})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", tc.action, "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if captured.JSONRPC != "2.0" || captured.Method != tc.wantMethod {
				t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", captured.JSONRPC, captured.Method, tc.wantMethod)
			}
			if !reflect.DeepEqual(captured.Params, map[string]any{"run_id": "run-1"}) {
				t.Fatalf("params = %#v, want run_id", captured.Params)
			}
			if !strings.Contains(stdout.String(), tc.wantOutput) {
				t.Fatalf("stdout = %q, want substring %q", stdout.String(), tc.wantOutput)
			}
			if strings.TrimSpace(stderr.String()) != "" {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestControlRunSingleCommandsSendIdempotencyKeys(t *testing.T) {
	for _, tc := range []struct {
		action     string
		wantMethod string
	}{
		{action: "pause", wantMethod: controlCommandRunPauseMethod},
		{action: "continue", wantMethod: controlCommandRunContinueMethod},
		{action: "stop", wantMethod: controlCommandRunStopMethod},
	} {
		t.Run(tc.action, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			var captured jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
					t.Errorf("decode request: %v", err)
				}
				writeJSONRPCResult(t, w, captured.ID, map[string]any{"ok": true})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", tc.action, "run-1", "--idempotency-key", "idem-1"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			assertControlRequest(t, captured, tc.wantMethod, map[string]any{"run_id": "run-1", "idempotency_key": "idem-1"})
			if strings.TrimSpace(stderr.String()) != "" {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestControlRunAllPauseContinueSendRuntimeRPCRequests(t *testing.T) {
	for _, tc := range []struct {
		action     string
		wantMethod string
		wantOutput string
	}{
		{action: "pause", wantMethod: controlCommandRuntimePauseMethod, wantOutput: "control pause ok: scope=runtime"},
		{action: "continue", wantMethod: controlCommandRuntimeResumeMethod, wantOutput: "control continue ok: scope=runtime"},
	} {
		t.Run(tc.action, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			var captured jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
					t.Errorf("decode request: %v", err)
				}
				writeJSONRPCResult(t, w, captured.ID, map[string]any{"ok": true})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", tc.action, "--all"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if captured.JSONRPC != "2.0" || captured.Method != tc.wantMethod {
				t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", captured.JSONRPC, captured.Method, tc.wantMethod)
			}
			if !reflect.DeepEqual(captured.Params, map[string]any{}) {
				t.Fatalf("params = %#v, want empty map", captured.Params)
			}
			if !strings.Contains(stdout.String(), tc.wantOutput) {
				t.Fatalf("stdout = %q, want substring %q", stdout.String(), tc.wantOutput)
			}
			if strings.TrimSpace(stderr.String()) != "" {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestControlRunAllPauseContinueSendIdempotencyKeys(t *testing.T) {
	for _, tc := range []struct {
		action     string
		wantMethod string
	}{
		{action: "pause", wantMethod: controlCommandRuntimePauseMethod},
		{action: "continue", wantMethod: controlCommandRuntimeResumeMethod},
	} {
		t.Run(tc.action, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			var captured jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
					t.Errorf("decode request: %v", err)
				}
				writeJSONRPCResult(t, w, captured.ID, map[string]any{"ok": true})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", tc.action, "--all", "--idempotency-key", "idem-1"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			assertControlRequest(t, captured, tc.wantMethod, map[string]any{"idempotency_key": "idem-1"})
			if strings.TrimSpace(stderr.String()) != "" {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestControlStopAllListsActiveRunsAndStopsUniqueTargets(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		captured = append(captured, req)
		switch len(captured) {
		case 1:
			writeJSONRPCResult(t, w, req.ID, map[string]any{
				"runs": []map[string]any{
					controlRunHeaderResult("run-a", "running"),
					controlRunHeaderResult("run-b", "running"),
				},
				"next_cursor": "running-2",
			})
		case 2:
			writeJSONRPCResult(t, w, req.ID, map[string]any{
				"runs": []map[string]any{
					controlRunHeaderResult("run-c", "running"),
				},
			})
		case 3:
			writeJSONRPCResult(t, w, req.ID, map[string]any{
				"runs": []map[string]any{
					controlRunHeaderResult("run-b", "paused"),
					controlRunHeaderResult("run-d", "paused"),
				},
			})
		default:
			writeJSONRPCResult(t, w, req.ID, map[string]any{"ok": true})
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", "stop", "--all", "--yes"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(captured) != 7 {
		t.Fatalf("captured %d requests, want 7: %#v", len(captured), captured)
	}
	assertControlRequest(t, captured[0], controlCommandRunListMethod, map[string]any{"status": "running", "limit": float64(controlCommandStopAllPageLimit)})
	assertControlRequest(t, captured[1], controlCommandRunListMethod, map[string]any{"status": "running", "limit": float64(controlCommandStopAllPageLimit), "cursor": "running-2"})
	assertControlRequest(t, captured[2], controlCommandRunListMethod, map[string]any{"status": "paused", "limit": float64(controlCommandStopAllPageLimit)})
	assertControlRequest(t, captured[3], controlCommandRunStopMethod, map[string]any{"run_id": "run-a"})
	assertControlRequest(t, captured[4], controlCommandRunStopMethod, map[string]any{"run_id": "run-b"})
	assertControlRequest(t, captured[5], controlCommandRunStopMethod, map[string]any{"run_id": "run-c"})
	assertControlRequest(t, captured[6], controlCommandRunStopMethod, map[string]any{"run_id": "run-d"})
	if !strings.Contains(stdout.String(), "control stop ok: scope=all matched=4 stopped=4 failed=0") {
		t.Fatalf("stdout = %q, want stop-all success summary", stdout.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestControlRunRejectsInvalidTargetsBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"ok": true})
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "missing target", args: []string{"control", "pause"}, wantStderr: "requires <run-id>"},
		{name: "all with run id", args: []string{"control", "pause", "run-1", "--all"}, wantStderr: "--all cannot be combined with a run id"},
		{name: "extra target", args: []string{"control", "continue", "run-1", "run-2"}, wantStderr: "accepts at most one argument"},
		{name: "blank run id", args: []string{"control", "stop", "  "}, wantStderr: "run id is required"},
		{name: "blank idempotency key pause run", args: []string{"control", "pause", "run-1", "--idempotency-key", "  "}, wantStderr: "--idempotency-key must be non-empty"},
		{name: "blank idempotency key pause all", args: []string{"control", "pause", "--all", "--idempotency-key", "  "}, wantStderr: "--idempotency-key must be non-empty"},
		{name: "blank idempotency key continue run", args: []string{"control", "continue", "run-1", "--idempotency-key", "  "}, wantStderr: "--idempotency-key must be non-empty"},
		{name: "blank idempotency key continue all", args: []string{"control", "continue", "--all", "--idempotency-key", "  "}, wantStderr: "--idempotency-key must be non-empty"},
		{name: "blank idempotency key stop run", args: []string{"control", "stop", "run-1", "--idempotency-key", "  "}, wantStderr: "--idempotency-key must be non-empty"},
		{name: "stop all rejects idempotency key", args: []string{"control", "stop", "--all", "--idempotency-key", "idem-1"}, wantStderr: "--idempotency-key is not supported with control stop --all"},
		{name: "stop all rejects idempotency key with yes", args: []string{"control", "stop", "--all", "--yes", "--idempotency-key", "idem-1"}, wantStderr: "--idempotency-key is not supported with control stop --all"},
		{name: "yes without all", args: []string{"control", "stop", "run-1", "--yes"}, wantStderr: "--yes is only supported with control stop --all"},
		{name: "unsupported flag", args: []string{"control", "stop", "run-1", "--unknown"}, wantStderr: "unknown flag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls.Load()
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
			if calls.Load() != before {
				t.Fatalf("server calls changed from %d to %d, want no request", before, calls.Load())
			}
		})
	}
}

func TestControlRunRequiresAPITokenBeforeRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"ok": true})
	}))
	defer server.Close()

	for _, args := range [][]string{
		{"control", "pause", "run-1"},
		{"control", "continue", "--all"},
		{"control", "stop", "--all", "--yes"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "")
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 4 {
				t.Fatalf("code = %d, want 4 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if !strings.Contains(stderr.String(), "API token source is required") {
				t.Fatalf("stderr = %q, want missing-token message", stderr.String())
			}
			if calls.Load() != 0 {
				t.Fatalf("server calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestControlRunFailsClosedOnTransportRPCAndMalformedResponses(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "http auth failure",
			args: []string{"control", "pause", "run-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"invalid bearer token"}`))
			},
			wantCode:   4,
			wantStderr: "rejected the request with status 401",
		},
		{
			name: "run not found",
			args: []string{"control", "stop", "missing"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeControlJSONRPCApplicationError(t, w, req.ID, "RUN_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "RUN_NOT_FOUND",
		},
		{
			name: "malformed response",
			args: []string{"control", "continue", "run-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{`))
			},
			wantCode:   3,
			wantStderr: "returned an invalid API response",
		},
		{
			name: "malformed ok result",
			args: []string{"control", "pause", "run-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{"ok": false})
			},
			wantCode:   3,
			wantStderr: "malformed run.pause result: ok must be true",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestControlStopAllConfirmationAndNoCallPaths(t *testing.T) {
	setCLIAPITestToken(t, "test-token")

	for _, tc := range []struct {
		name          string
		args          []string
		input         string
		stdinTerminal bool
		wantCode      int
		wantCalls     int
		wantStderr    []string
	}{
		{
			name:          "tty confirmation proceeds with y",
			args:          []string{"control", "stop", "--all"},
			input:         "y\n",
			stdinTerminal: true,
			wantCalls:     2,
			wantStderr:    []string{"WARNING: `swarm control stop --all` will stop every running or paused run.", "Continue? [y/N]"},
		},
		{
			name:          "tty confirmation proceeds with yes case insensitive",
			args:          []string{"control", "stop", "--all"},
			input:         "YES\n",
			stdinTerminal: true,
			wantCalls:     2,
			wantStderr:    []string{"Continue? [y/N]"},
		},
		{
			name:          "tty abort makes no request",
			args:          []string{"control", "stop", "--all"},
			input:         "n\n",
			stdinTerminal: true,
			wantCode:      2,
			wantStderr:    []string{"Aborted; no runs stopped."},
		},
		{
			name:          "tty empty answer makes no request",
			args:          []string{"control", "stop", "--all"},
			input:         "\n",
			stdinTerminal: true,
			wantCode:      2,
			wantStderr:    []string{"Aborted; no runs stopped."},
		},
		{
			name:       "non tty without yes makes no request",
			args:       []string{"control", "stop", "--all"},
			wantCode:   2,
			wantStderr: []string{"ERROR: `swarm control stop --all` stops every running or paused run; pass --yes for non-TTY invocations."},
		},
		{
			name:      "yes bypasses prompt",
			args:      []string{"control", "stop", "--all", "--yes"},
			wantCalls: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var captured []jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("decode request: %v", err)
				}
				captured = append(captured, req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{"runs": []any{}})
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
			if len(captured) != tc.wantCalls {
				t.Fatalf("server calls = %d, want %d", len(captured), tc.wantCalls)
			}
			for _, want := range tc.wantStderr {
				if !strings.Contains(stderr.String(), want) {
					t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
				}
			}
			if tc.wantCalls > 0 {
				assertControlRequest(t, captured[0], controlCommandRunListMethod, map[string]any{"status": "running", "limit": float64(controlCommandStopAllPageLimit)})
				assertControlRequest(t, captured[1], controlCommandRunListMethod, map[string]any{"status": "paused", "limit": float64(controlCommandStopAllPageLimit)})
				if !strings.Contains(stdout.String(), "control stop ok: scope=all matched=0 stopped=0 failed=0") {
					t.Fatalf("stdout = %q, want empty stop-all success", stdout.String())
				}
			}
			if tc.name == "yes bypasses prompt" && strings.Contains(stderr.String(), "Continue?") {
				t.Fatalf("stderr = %q, want no prompt", stderr.String())
			}
		})
	}
}

func TestControlStopAllConfirmationPrecedesAPIClientConstruction(t *testing.T) {
	for _, tc := range []struct {
		name          string
		input         string
		stdinTerminal bool
		wantCode      int
		wantStderr    string
	}{
		{
			name:       "non tty without yes fails before missing token",
			wantCode:   2,
			wantStderr: "pass --yes for non-TTY",
		},
		{
			name:          "tty abort fails before missing token",
			input:         "n\n",
			stdinTerminal: true,
			wantCode:      2,
			wantStderr:    "Aborted; no runs stopped.",
		},
		{
			name:          "tty confirmation then checks token",
			input:         "y\n",
			stdinTerminal: true,
			wantCode:      4,
			wantStderr:    "API token source is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "")
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				writeJSONRPCResult(t, w, "unexpected", map[string]any{"runs": []any{}})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			opts := testRootCommandOptions(server)
			opts.input = strings.NewReader(tc.input)
			opts.stdinIsTerminal = func() bool { return tc.stdinTerminal }
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", "stop", "--all"}, &stdout, &stderr, opts)
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if calls.Load() != 0 {
				t.Fatalf("server calls = %d, want 0", calls.Load())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestControlStopAllReportsPartialFailures(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		captured = append(captured, req)
		switch len(captured) {
		case 1:
			writeJSONRPCResult(t, w, req.ID, map[string]any{"runs": []map[string]any{
				controlRunHeaderResult("run-a", "running"),
				controlRunHeaderResult("run-b", "running"),
			}})
		case 2:
			writeJSONRPCResult(t, w, req.ID, map[string]any{"runs": []any{}})
		case 3:
			writeJSONRPCResult(t, w, req.ID, map[string]any{"ok": true})
		case 4:
			writeControlJSONRPCApplicationError(t, w, req.ID, "RUN_ALREADY_TERMINAL")
		default:
			t.Fatalf("unexpected request %d: %#v", len(captured), req)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", "stop", "--all", "--yes"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 3 {
		t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if len(captured) != 4 {
		t.Fatalf("captured %d requests, want 4: %#v", len(captured), captured)
	}
	if !strings.Contains(stdout.String(), "control stop partial: scope=all matched=2 stopped=1 failed=1") {
		t.Fatalf("stdout = %q, want partial summary", stdout.String())
	}
	for _, want := range []string{"control stop failed: run_id=run-b", "RUN_ALREADY_TERMINAL"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
		}
	}
}

func TestControlStopAllPerRunHTTPFailureUsesSharedDiagnostic(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		captured = append(captured, req)
		switch len(captured) {
		case 1:
			writeJSONRPCResult(t, w, req.ID, map[string]any{"runs": []map[string]any{
				controlRunHeaderResult("run-a", "running"),
			}})
		case 2:
			writeJSONRPCResult(t, w, req.ID, map[string]any{"runs": []any{}})
		case 3:
			http.Error(w, "runtime unavailable", http.StatusServiceUnavailable)
		default:
			t.Fatalf("unexpected request %d: %#v", len(captured), req)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", "stop", "--all", "--yes"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 3 {
		t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "control stop partial: scope=all matched=1 stopped=0 failed=1") {
		t.Fatalf("stdout = %q, want partial summary", stdout.String())
	}
	for _, want := range []string{
		"ERROR: control stop failed: run_id=run-a: the Swarm runtime at ",
		"returned status 503",
		"Check the runtime with `swarm health`",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
		}
	}
	for _, forbidden := range []string{"runtime API returned status", "/v1/rpc"} {
		if strings.Contains(stderr.String(), forbidden) {
			t.Fatalf("stderr = %q, must not contain %q", stderr.String(), forbidden)
		}
	}
}

func assertControlRequest(t *testing.T, req jsonRPCRequest, wantMethod string, wantParams map[string]any) {
	t.Helper()
	if req.JSONRPC != "2.0" || req.Method != wantMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", req.JSONRPC, req.Method, wantMethod)
	}
	if !reflect.DeepEqual(req.Params, wantParams) {
		t.Fatalf("params for %s = %#v, want %#v", wantMethod, req.Params, wantParams)
	}
}

func controlRunHeaderResult(runID, status string) map[string]any {
	return map[string]any{
		"run_id":             runID,
		"status":             status,
		"trigger_event_type": "manual.test",
		"trigger_event_id":   "event-" + runID,
		"entity_count":       1,
		"event_count":        2,
		"started_at":         "2026-05-18T01:00:00Z",
	}
}

func writeControlJSONRPCApplicationError(t *testing.T, w http.ResponseWriter, id, code string) {
	t.Helper()
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32010,
			"message": "Application error: " + code,
			"data": map[string]any{
				"code": code,
			},
		},
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
