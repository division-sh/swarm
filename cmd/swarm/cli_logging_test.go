package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestCLILoggingPolicyResolver(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error"} {
		t.Run(level, func(t *testing.T) {
			policy, err := (cliLoggingOptions{level: level}).resolve()
			if err != nil {
				t.Fatalf("resolve returned error: %v", err)
			}
			if string(policy.level) != level {
				t.Fatalf("level = %q, want %q", policy.level, level)
			}
		})
	}

	policy, err := defaultCLILoggingOptions().resolve()
	if err != nil {
		t.Fatalf("default resolve returned error: %v", err)
	}
	if policy.level != cliLogLevelInfo {
		t.Fatalf("default level = %q, want %q", policy.level, cliLogLevelInfo)
	}

	for _, level := range []string{"", "fatal", "WARN", "debug "} {
		t.Run("reject "+level, func(t *testing.T) {
			if _, err := (cliLoggingOptions{level: level}).resolve(); err == nil {
				t.Fatalf("resolve(%q) returned nil error, want validation error", level)
			}
		})
	}
}

func TestCLILoggingForSharedOutputConsumers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   func(*testing.T) []string
		repo   func(*testing.T) string
		method string
		result map[string]any
	}{
		{
			name: "version local",
			args: func(*testing.T) []string { return []string{"version"} },
			repo: func(*testing.T) string { return repoRoot() },
		},
		{
			name: "verify",
			args: func(t *testing.T) []string {
				return []string{"verify", "--contracts", outputModeVerifyFixture(t)}
			},
			repo: func(*testing.T) string { return repoRoot() },
		},
		{
			name:   "health",
			args:   func(*testing.T) []string { return []string{"health"} },
			method: "health.check",
			result: validVersionHealthResult(),
		},
		{
			name:   "runs",
			args:   func(*testing.T) []string { return []string{"runs"} },
			method: "run.list",
			result: map[string]any{"runs": []any{validDiagnosticRunHeader("run-1"), validDiagnosticRunHeader("run-2")}},
		},
		{
			name:   "status",
			args:   func(*testing.T) []string { return []string{"status", "run-1"} },
			method: "run.diagnose",
			result: map[string]any{
				"run":               validDiagnosticRunHeader("run-1"),
				"operational_state": "stalled",
				"blocking_layer":    "delivery_lifecycle",
				"blocking_reason":   "no_active_deliveries",
				"heuristics":        []any{"dead letters exist for this run"},
			},
		},
		{
			name:   "conversations list",
			args:   func(*testing.T) []string { return []string{"conversations", "list"} },
			method: conversationListMethod,
			result: map[string]any{"conversations": []map[string]any{validConversationSummary("sess-1")}},
		},
		{
			name:   "conversation view",
			args:   func(*testing.T) []string { return []string{"conversation", "view", "sess-1"} },
			method: conversationGetMethod,
			result: validConversationDetail("sess-1"),
		},
		{
			name:   "conversation turn",
			args:   func(*testing.T) []string { return []string{"conversation", "turn", "sess-1", "2"} },
			method: conversationGetTurnMethod,
			result: validConversationTurnDetail("sess-1", 2),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"", "--json", "--quiet"} {
				t.Run("mode "+mode, func(t *testing.T) {
					setCLIAPITestToken(t, "test-token")

					args := append([]string{}, tc.args(t)...)
					if mode != "" {
						args = append(args, mode)
					}
					args = append(args, "--log-level", "debug")

					repo := t.TempDir()
					if tc.repo != nil {
						repo = tc.repo(t)
					}
					opts := defaultRootCommandOptions()
					var calls *atomic.Int32
					if tc.method != "" {
						server, rpcCalls := newCLILoggingRPCServer(t, tc.method, tc.result)
						defer server.Close()
						opts = testRootCommandOptions(server)
						calls = rpcCalls
					}

					var stdout, stderr bytes.Buffer
					code := executeRootCommandWithOptions(context.Background(), repo, args, &stdout, &stderr, opts)
					if code != 0 {
						t.Fatalf("%s code = %d stdout=%s stderr=%s", strings.Join(args, " "), code, stdout.String(), stderr.String())
					}
					if strings.TrimSpace(stdout.String()) == "" {
						t.Fatalf("%s stdout is empty, want successful output", strings.Join(args, " "))
					}
					assertEmptyStderr(t, stderr.String())
					if mode == "--json" {
						decodeOutputJSON[map[string]any](t, stdout.String())
					}
					if calls != nil && calls.Load() != 1 {
						t.Fatalf("%s RPC calls = %d, want 1", strings.Join(args, " "), calls.Load())
					}
				})
			}
		})
	}
}

func TestCLILoggingAcceptedLevelsOnConsumer(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error"} {
		t.Run(level, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{"version", "--log-level", level}, &stdout, &stderr, defaultRootCommandOptions())
			if code != 0 {
				t.Fatalf("version --log-level %s code = %d stdout=%s stderr=%s", level, code, stdout.String(), stderr.String())
			}
			if strings.TrimSpace(stdout.String()) == "" {
				t.Fatalf("stdout is empty, want version output")
			}
			assertEmptyStderr(t, stderr.String())
		})
	}
}

func TestCLILoggingInvalidLevelFailsBeforeSideEffects(t *testing.T) {
	setCLIAPITestToken(t, "test-token")

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	for _, args := range [][]string{
		{"version", "--server", "--log-level", "fatal"},
		{"health", "--log-level", "fatal"},
		{"runs", "--log-level", "WARN"},
		{"status", "run-1", "--log-level", ""},
		{"conversations", "list", "--log-level", "debug "},
		{"conversation", "view", "sess-1", "--log-level", "fatal"},
		{"conversation", "turn", "sess-1", "2", "--log-level", "fatal"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			before := calls.Load()
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
			if code != cliExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), "--log-level must be one of debug, info, warn, error") {
				t.Fatalf("stderr = %q, want log-level validation", stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if calls.Load() != before {
				t.Fatalf("RPC calls changed from %d to %d", before, calls.Load())
			}
		})
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), repoRoot(), []string{"verify", "--contracts", "missing", "--log-level", "fatal"}, &stdout, &stderr)
	if code != cliExitValidation {
		t.Fatalf("verify invalid log-level code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "--log-level must be one of debug, info, warn, error") {
		t.Fatalf("verify stderr = %q, want log-level validation", stderr.String())
	}
	if strings.Contains(stderr.String(), "resolve contracts") {
		t.Fatalf("verify stderr = %q, local verification path ran before log-level validation", stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify stdout = %q, want empty", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommand(context.Background(), repoRoot(), []string{"verify", "positional", "--log-level", "fatal"}, &stdout, &stderr)
	if code != cliExitValidation {
		t.Fatalf("verify positional invalid log-level code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "--log-level must be one of debug, info, warn, error") {
		t.Fatalf("verify positional stderr = %q, want log-level validation", stderr.String())
	}
	if strings.Contains(stderr.String(), "resolve contracts") || strings.Contains(stderr.String(), "load Swarm contracts") {
		t.Fatalf("verify positional stderr = %q, local verification path ran before log-level validation", stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify positional stdout = %q, want empty", stdout.String())
	}
}

func TestCLILoggingUnsupportedSurfacesFailClosedBeforeSideEffects(t *testing.T) {
	setCLIAPITestToken(t, "test-token")

	var rpcCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcCalls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	var serveCalls atomic.Int32
	opts := testRootCommandOptions(server)
	opts.runServe = func(context.Context, string, serveOptions) int {
		serveCalls.Add(1)
		return 0
	}

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "root", args: []string{"--log-level", "debug", "version"}, wantStderr: "unknown flag"},
		{name: "completion", args: []string{"completion", "bash", "--log-level", "debug"}, wantStderr: "unknown flag"},
		{name: "serve", args: []string{"serve", "--log-level", "debug"}, wantStderr: "unknown flag"},
		{name: "logs", args: []string{"logs", "--log-level", "debug"}, wantStderr: "unknown flag"},
		{name: "incidents", args: []string{"incidents", "--log-level", "debug"}, wantStderr: "unknown flag"},
		{name: "trace", args: []string{"trace", "--log-level", "debug"}, wantStderr: "unknown flag"},
		{name: "run", args: []string{"run", "--log-level", "debug"}, wantStderr: "unknown flag"},
		{name: "retired investigate", args: []string{"investigate", "runs", "--log-level", "debug"}, wantStderr: "retired in CLI v2"},
		{name: "forkchat parent", args: []string{"forkchat", "--log-level", "debug"}, wantStderr: "unknown flag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeRPC := rpcCalls.Load()
			beforeServe := serveCalls.Load()
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, opts)
			if code != cliExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.wantStderr)
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if rpcCalls.Load() != beforeRPC {
				t.Fatalf("RPC calls changed from %d to %d", beforeRPC, rpcCalls.Load())
			}
			if serveCalls.Load() != beforeServe {
				t.Fatalf("serve calls changed from %d to %d", beforeServe, serveCalls.Load())
			}
		})
	}
}

func TestCLILoggingIgnoresRejectedEnvironmentSource(t *testing.T) {
	t.Setenv("SWARM_LOG_LEVEL", "fatal")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{"version"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("version with SWARM_LOG_LEVEL code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) == "" {
		t.Fatalf("stdout is empty, want version output")
	}
	assertEmptyStderr(t, stderr.String())
}

func newCLILoggingRPCServer(t *testing.T, wantMethod string, result map[string]any) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if req.Method != wantMethod {
			t.Errorf("method = %q, want %s", req.Method, wantMethod)
		}
		writeJSONRPCResult(t, w, req.ID, result)
	}))
	return server, &calls
}
