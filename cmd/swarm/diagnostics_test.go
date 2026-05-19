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

func TestRunsUseRunListV1RPC(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
		if req.Method != "run.list" {
			t.Fatalf("method = %q, want run.list", req.Method)
		}
		return map[string]any{
			"runs":        []any{validDiagnosticRunHeader("run-1")},
			"next_cursor": "next-1",
		}
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"runs", "--status", "RUNNING", "--limit", "2", "--cursor", "cur-1", "--since", "2026-05-13T10:00:00Z", "--until", "2026-05-13T11:00:00Z"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(*requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(*requests))
	}
	wantParams := map[string]any{
		"status": "running",
		"limit":  float64(2),
		"cursor": "cur-1",
		"since":  "2026-05-13T10:00:00Z",
		"until":  "2026-05-13T11:00:00Z",
	}
	if !reflect.DeepEqual((*requests)[0].Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", (*requests)[0].Params, wantParams)
	}
	for _, want := range []string{"RUN ID", "run-1", "running", "scan.requested", "next_cursor=next-1"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestInvestigateNamespaceIsRetiredWithoutRequest(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		wantOutput []string
	}{
		{
			name: "bare investigate",
			args: []string{"investigate"},
			wantOutput: []string{
				"ERROR: `swarm investigate` was retired in CLI v2.",
				"Use `swarm runs`",
				"Use `swarm status [run-id]`",
				"Use `swarm trace [run-id] [--follow]`",
				"Use `swarm health`",
			},
		},
		{
			name: "investigate runs",
			args: []string{"investigate", "runs"},
			wantOutput: []string{
				"ERROR: `swarm investigate runs` was retired in CLI v2.",
				"Use `swarm runs`.",
			},
		},
		{
			name: "investigate runs legacy flags",
			args: []string{"investigate", "runs", "--status", "running", "--limit", "1", "--cursor", "cur", "--since", "2026-05-13T10:00:00Z", "--until", "2026-05-13T11:00:00Z"},
			wantOutput: []string{
				"ERROR: `swarm investigate runs` was retired in CLI v2.",
				"Use `swarm runs`.",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				writeJSONRPCResult(t, w, "unexpected", map[string]any{})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			for _, want := range tc.wantOutput {
				if !strings.Contains(stderr.String(), want) {
					t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
				}
			}
			if calls.Load() != 0 {
				t.Fatalf("RPC calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestStatusUsesDiagnoseAndRunGet(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		wantMethod string
		wantOutput []string
		notOutput  string
	}{
		{
			name:       "diagnose default",
			args:       []string{"status", "run-1"},
			wantMethod: "run.diagnose",
			wantOutput: []string{"Run: run-1", "operational_state=stalled", "blocking_layer=delivery_lifecycle", "dead letters exist"},
		},
		{
			name:       "header only",
			args:       []string{"status", "run-1", "--no-diagnose"},
			wantMethod: "run.get",
			wantOutput: []string{"Run: run-1", "status=running", "trigger=scan.requested"},
			notOutput:  "operational_state=",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
				if req.Method != tc.wantMethod {
					t.Fatalf("method = %q, want %s", req.Method, tc.wantMethod)
				}
				if got := req.Params["run_id"]; got != "run-1" {
					t.Fatalf("run_id param = %#v, want run-1", got)
				}
				if tc.wantMethod == "run.get" {
					return map[string]any{"run": validDiagnosticRunHeader("run-1")}
				}
				return map[string]any{
					"run":               validDiagnosticRunHeader("run-1"),
					"operational_state": "stalled",
					"blocking_layer":    "delivery_lifecycle",
					"blocking_reason":   "no_active_deliveries",
					"heuristics":        []any{"dead letters exist for this run"},
				}
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if len(*requests) != 1 {
				t.Fatalf("requests = %d, want 1", len(*requests))
			}
			for _, want := range tc.wantOutput {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
				}
			}
			if tc.notOutput != "" && strings.Contains(stdout.String(), tc.notOutput) {
				t.Fatalf("stdout contains %q, want absent:\n%s", tc.notOutput, stdout.String())
			}
			if strings.TrimSpace(stderr.String()) != "" {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestStatusAndTraceResolveOmittedRunThroughActivePreference(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		lists      [][]any
		owner      string
		selected   string
		wantOutput string
	}{
		{
			name:       "status prefers running",
			args:       []string{"status"},
			lists:      [][]any{{validDiagnosticRunHeaderWithStatus("running-run", "running")}},
			owner:      "run.diagnose",
			selected:   "running-run",
			wantOutput: "Run: running-run",
		},
		{
			name:       "status falls back to paused",
			args:       []string{"status", "--no-diagnose"},
			lists:      [][]any{{}, {validDiagnosticRunHeaderWithStatus("paused-run", "paused")}},
			owner:      "run.get",
			selected:   "paused-run",
			wantOutput: "Run: paused-run",
		},
		{
			name:       "trace uses terminal fallback",
			args:       []string{"trace"},
			lists:      [][]any{{}, {}, {validDiagnosticRunHeaderWithStatus("terminal-run", "completed")}},
			owner:      "run.trace",
			selected:   "terminal-run",
			wantOutput: "run trace: run_id=terminal-run",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, callIndex int) map[string]any {
				if callIndex < len(tc.lists) {
					if req.Method != "run.list" {
						t.Fatalf("method[%d] = %q, want run.list", callIndex, req.Method)
					}
					wantParams := map[string]any{"limit": float64(1)}
					switch callIndex {
					case 0:
						wantParams["status"] = "running"
					case 1:
						wantParams["status"] = "paused"
					}
					if !reflect.DeepEqual(req.Params, wantParams) {
						t.Fatalf("params[%d] = %#v, want %#v", callIndex, req.Params, wantParams)
					}
					return map[string]any{"runs": tc.lists[callIndex]}
				}
				if req.Method != tc.owner {
					t.Fatalf("owner method = %q, want %s", req.Method, tc.owner)
				}
				if got := req.Params["run_id"]; got != tc.selected {
					t.Fatalf("owner run_id = %#v, want %s", got, tc.selected)
				}
				switch tc.owner {
				case "run.trace":
					return map[string]any{
						"trace": []any{map[string]any{
							"event_id":         "event-1",
							"event_name":       "scan.requested",
							"event_created_at": "2026-05-13T10:00:01Z",
						}},
					}
				case "run.get":
					return map[string]any{"run": validDiagnosticRunHeaderWithStatus(tc.selected, "paused")}
				default:
					return map[string]any{
						"run":               validDiagnosticRunHeader(tc.selected),
						"operational_state": "running",
						"blocking_layer":    "",
						"blocking_reason":   "",
						"heuristics":        []any{},
					}
				}
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if len(*requests) != len(tc.lists)+1 {
				t.Fatalf("requests = %d, want %d", len(*requests), len(tc.lists)+1)
			}
			if !strings.Contains(stdout.String(), tc.wantOutput) {
				t.Fatalf("stdout missing %q:\n%s", tc.wantOutput, stdout.String())
			}
		})
	}
}

func TestInvestigateRunIsRetiredWithoutRequest(t *testing.T) {
	for _, args := range [][]string{
		{"investigate", "run"},
		{"investigate", "run", "run-1"},
		{"investigate", "run", "run-1", "--no-diagnose"},
		{"investigate", "run", "--no-diagnose"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				writeJSONRPCResult(t, w, "unexpected", map[string]any{})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if got := stderr.String(); !strings.Contains(got, "ERROR: `swarm investigate run` was retired in CLI v2.") || !strings.Contains(got, "Use `swarm status`.") {
				t.Fatalf("stderr = %q, want retired migration message", got)
			}
			if calls.Load() != 0 {
				t.Fatalf("RPC calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestInvestigateTraceIsRetiredWithoutRequest(t *testing.T) {
	for _, args := range [][]string{
		{"investigate", "trace"},
		{"investigate", "trace", "run-1"},
		{"investigate", "trace", "run-1", "--limit", "5"},
		{"investigate", "trace", "--cursor", "trace-cur"},
		{"investigate", "trace", "run-1", "--follow"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				writeJSONRPCResult(t, w, "unexpected", map[string]any{})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if got := stderr.String(); !strings.Contains(got, "ERROR: `swarm investigate trace` was retired in CLI v2.") || !strings.Contains(got, "Use `swarm trace`.") {
				t.Fatalf("stderr = %q, want retired migration message", got)
			}
			if calls.Load() != 0 {
				t.Fatalf("RPC calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestTraceUsesRunTraceSnapshot(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
		if req.Method != "run.trace" {
			t.Fatalf("method = %q, want run.trace", req.Method)
		}
		wantParams := map[string]any{"run_id": "run-1"}
		if !reflect.DeepEqual(req.Params, wantParams) {
			t.Fatalf("params = %#v, want %#v", req.Params, wantParams)
		}
		return map[string]any{
			"trace": []any{map[string]any{
				"event_id":         "event-1",
				"event_name":       "scan.requested",
				"event_created_at": "2026-05-13T10:00:01Z",
				"delivery_status":  "delivered",
				"subscriber_type":  "agent",
				"subscriber_id":    "agent-1",
				"session_id":       "session-1",
				"turn_id":          "turn-1",
			}},
			"next_cursor": "trace-next",
		}
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"trace", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(*requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(*requests))
	}
	for _, want := range []string{"run trace: run_id=run-1", "event-1", "scan.requested", "agent/agent-1", "next_cursor=trace-next"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestTraceFollowUsesRunSubscribeTrace(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
		wsRows:           []map[string]any{validRunCommandTraceRow("evt-follow")},
		wsCloseAfterRows: true,
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"trace", "run-follow", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, nil)
	assertRunCommandTraceSubscription(t, wsRequests, "run-follow", false)
	if !strings.Contains(stdout.String(), "trace event_id=evt-follow") {
		t.Fatalf("stdout = %q, want trace row", stdout.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestTraceFollowOmittedRunReusesActivePreferenceResolver(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, callIndex int) map[string]any {
			if req.Method != "run.list" {
				t.Fatalf("method[%d] = %q, want run.list", callIndex, req.Method)
			}
			if !reflect.DeepEqual(req.Params, map[string]any{"status": "running", "limit": float64(1)}) {
				t.Fatalf("params = %#v, want running active preference", req.Params)
			}
			return map[string]any{"runs": []any{validDiagnosticRunHeaderWithStatus("active-run", "running")}}
		},
		wsRows:           []map[string]any{validRunCommandTraceRow("evt-active")},
		wsCloseAfterRows: true,
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"trace", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"run.list"})
	assertRunCommandTraceSubscription(t, wsRequests, "active-run", false)
	if !strings.Contains(stdout.String(), "trace event_id=evt-active") {
		t.Fatalf("stdout = %q, want trace row", stdout.String())
	}
}

func TestTraceFollowCtrlCDetachesWithoutRunStop(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	wsSubscribed := make(chan struct{})
	server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			if req.Method == "run.stop" {
				t.Fatal("read-only trace Ctrl-C must not call run.stop")
			}
			t.Fatalf("unexpected RPC method = %q", req.Method)
			return nil
		},
		wsSubscribed: wsSubscribed,
	})
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-wsSubscribed
		cancel()
	}()
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"trace", "run-active", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 130 {
		t.Fatalf("code = %d, want 130 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, nil)
	assertRunCommandTraceSubscription(t, wsRequests, "run-active", false)
	if !strings.Contains(stderr.String(), "detached from run trace") {
		t.Fatalf("stderr = %q, want detach message", stderr.String())
	}
}

func TestTraceFollowMalformedWebSocketFailuresExitThree(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	for _, tc := range []struct {
		name       string
		serverOpts runCommandServerOptions
		wantStderr string
	}{
		{
			name: "subscription response missing id",
			serverOpts: runCommandServerOptions{
				wsSubscriptionResult: map[string]any{},
			},
			wantStderr: "subscription_id is required",
		},
		{
			name: "notification missing event id",
			serverOpts: runCommandServerOptions{
				wsRows:           []map[string]any{{"event_name": "scan.requested", "event_created_at": "2026-05-13T10:00:01Z"}},
				wsCloseAfterRows: true,
			},
			wantStderr: "event_id is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, calls, wsRequests := newRunCommandServer(t, tc.serverOpts)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"trace", "run-bad-ws", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 3 {
				t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			assertRunCommandMethods(t, calls, nil)
			assertRunCommandTraceSubscription(t, wsRequests, "run-bad-ws", false)
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestHealthUsesHealthCheck(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
		if req.Method != "health.check" {
			t.Fatalf("method = %q, want health.check", req.Method)
		}
		if len(req.Params) != 0 {
			t.Fatalf("params = %#v, want empty", req.Params)
		}
		return map[string]any{
			"alive":      true,
			"ready":      true,
			"db_ok":      true,
			"runtime_ok": true,
			"bundle": map[string]any{
				"fingerprint":      "sha256:abc",
				"workflow_name":    "workflow",
				"workflow_version": "v1",
			},
		}
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"health"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(*requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(*requests))
	}
	for _, want := range []string{"alive=true", "ready=true", "db_ok=true", "runtime_ok=true", "fingerprint=sha256:abc", "workflow_name=workflow"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestInvestigateHealthIsRetiredWithoutRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"investigate", "health"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 2 {
		t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if got := stderr.String(); !strings.Contains(got, "ERROR: `swarm investigate health` was retired in CLI v2.") || !strings.Contains(got, "Use `swarm health`.") {
		t.Fatalf("stderr = %q, want retired migration message", got)
	}
	if calls.Load() != 0 {
		t.Fatalf("RPC calls = %d, want 0", calls.Load())
	}
}

func TestDiagnosticsRejectInvalidInputBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"runs": []any{}})
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "runs invalid limit", args: []string{"runs", "--limit", "0"}, wantStderr: "--limit must be between 1 and 500"},
		{name: "runs invalid since", args: []string{"runs", "--since", "yesterday"}, wantStderr: "--since must be an RFC3339 timestamp"},
		{name: "trace extra arg", args: []string{"trace", "run-1", "extra"}, wantStderr: "accepts at most 1 arg(s)"},
		{name: "trace unknown legacy flag", args: []string{"trace", "run-1", "--limit", "5"}, wantStderr: "unknown flag"},
		{name: "status extra arg", args: []string{"status", "run-1", "extra"}, wantStderr: "accepts at most 1 arg(s)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls.Load()
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.wantStderr)
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if calls.Load() != before {
				t.Fatalf("server calls changed from %d to %d, want no request", before, calls.Load())
			}
		})
	}
}

func TestDiagnosticsRequireAPITokenBeforeRequest(t *testing.T) {
	for _, args := range [][]string{
		{"runs"},
		{"trace", "run-1"},
		{"trace", "run-1", "--follow"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "")
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				writeJSONRPCResult(t, w, "unexpected", map[string]any{"runs": []any{}})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 4 {
				t.Fatalf("code = %d, want 4 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), "SWARM_API_TOKEN is required") {
				t.Fatalf("stderr = %q, want missing-token message", stderr.String())
			}
			if calls.Load() != 0 {
				t.Fatalf("server calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestOmittedRunResolverFailsClosedWhenNoRunsExist(t *testing.T) {
	for _, args := range [][]string{
		{"status"},
		{"status", "--no-diagnose"},
		{"trace"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
				if req.Method != "run.list" {
					t.Fatalf("method = %q, want run.list", req.Method)
				}
				return map[string]any{"runs": []any{}}
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 3 {
				t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if len(*requests) != 3 {
				t.Fatalf("requests = %d, want 3 active-preference run.list probes", len(*requests))
			}
			for _, req := range *requests {
				if req.Method != "run.list" {
					t.Fatalf("method = %q, want only run.list before no-runs failure", req.Method)
				}
			}
			if !strings.Contains(stderr.String(), "no runs found") {
				t.Fatalf("stderr = %q, want no-runs error", stderr.String())
			}
		})
	}
}

func TestDiagnosticsFailClosedOnAPIAndMalformedResults(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "http auth failure",
			args: []string{"runs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"invalid bearer token"}`))
			},
			wantCode:   4,
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name: "json rpc run not found",
			args: []string{"status", "missing"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				w.Header().Set("content-type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req.ID,
					"error": map[string]any{
						"code":    -32017,
						"message": "Application error: RUN_NOT_FOUND",
						"data": map[string]any{
							"code":           "RUN_NOT_FOUND",
							"details":        map[string]any{"run_id": "missing"},
							"retryable":      false,
							"correlation_id": "corr-1",
						},
					},
				})
			},
			wantCode:   5,
			wantStderr: "RUN_NOT_FOUND",
		},
		{
			name: "run list missing runs",
			args: []string{"runs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "runs is required",
		},
		{
			name: "run get missing required run id",
			args: []string{"status", "run-1", "--no-diagnose"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				run := validDiagnosticRunHeader("run-1")
				delete(run, "run_id")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"run": run})
			},
			wantCode:   3,
			wantStderr: "run.run_id is required",
		},
		{
			name: "run list invalid run status",
			args: []string{"runs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				run := validDiagnosticRunHeader("run-1")
				run["status"] = "waiting"
				writeJSONRPCResult(t, w, req.ID, map[string]any{"runs": []any{run}})
			},
			wantCode:   3,
			wantStderr: `runs[0].status="waiting" is not a valid RunStatus`,
		},
		{
			name: "run get invalid run status",
			args: []string{"status", "run-1", "--no-diagnose"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				run := validDiagnosticRunHeader("run-1")
				run["status"] = "waiting"
				writeJSONRPCResult(t, w, req.ID, map[string]any{"run": run})
			},
			wantCode:   3,
			wantStderr: `run.status="waiting" is not a valid RunStatus`,
		},
		{
			name: "run diagnose missing blocking layer",
			args: []string{"status", "run-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"run":               validDiagnosticRunHeader("run-1"),
					"operational_state": "running",
					"blocking_reason":   "",
					"heuristics":        []any{},
				})
			},
			wantCode:   3,
			wantStderr: "blocking_layer is required",
		},
		{
			name: "run diagnose invalid operational state",
			args: []string{"status", "run-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"run":               validDiagnosticRunHeader("run-1"),
					"operational_state": "blocked",
					"blocking_layer":    "",
					"blocking_reason":   "",
					"heuristics":        []any{},
				})
			},
			wantCode:   3,
			wantStderr: `operational_state="blocked" is not a valid OperationalState`,
		},
		{
			name: "run diagnose missing blocking reason",
			args: []string{"status", "run-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"run":               validDiagnosticRunHeader("run-1"),
					"operational_state": "running",
					"blocking_layer":    "",
					"heuristics":        []any{},
				})
			},
			wantCode:   3,
			wantStderr: "blocking_reason is required",
		},
		{
			name: "run diagnose missing heuristics",
			args: []string{"status", "run-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"run":               validDiagnosticRunHeader("run-1"),
					"operational_state": "running",
					"blocking_layer":    "",
					"blocking_reason":   "",
				})
			},
			wantCode:   3,
			wantStderr: "heuristics is required",
		},
		{
			name: "trace missing trace",
			args: []string{"trace", "run-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "trace is required",
		},
		{
			name: "health missing bundle fingerprint",
			args: []string{"health"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"alive":      true,
					"ready":      true,
					"db_ok":      true,
					"runtime_ok": true,
					"bundle":     map[string]any{},
				})
			},
			wantCode:   3,
			wantStderr: "bundle.fingerprint is required",
		},
		{
			name: "health missing workflow name",
			args: []string{"health"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"alive":      true,
					"ready":      true,
					"db_ok":      true,
					"runtime_ok": true,
					"bundle": map[string]any{
						"fingerprint":      "sha256:abc",
						"workflow_version": "v1",
					},
				})
			},
			wantCode:   3,
			wantStderr: "bundle.workflow_name is required",
		},
		{
			name: "health missing workflow version",
			args: []string{"health"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"alive":      true,
					"ready":      true,
					"db_ok":      true,
					"runtime_ok": true,
					"bundle": map[string]any{
						"fingerprint":   "sha256:abc",
						"workflow_name": "workflow",
					},
				})
			},
			wantCode:   3,
			wantStderr: "bundle.workflow_version is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
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

func newDiagnosticSuccessServer(t *testing.T, responder func(jsonRPCRequest, int) map[string]any) (*httptest.Server, *[]jsonRPCRequest) {
	t.Helper()
	requests := []jsonRPCRequest{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rpc" {
			t.Errorf("path = %q, want /v1/rpc", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		callIndex := len(requests)
		result := responder(req, callIndex)
		requests = append(requests, req)
		writeJSONRPCResult(t, w, req.ID, result)
	}))
	return server, &requests
}

func validDiagnosticRunHeader(runID string) map[string]any {
	return map[string]any{
		"run_id":             runID,
		"status":             "running",
		"trigger_event_type": "scan.requested",
		"trigger_event_id":   "event-root",
		"entity_count":       2,
		"event_count":        3,
		"started_at":         "2026-05-13T10:00:00Z",
	}
}

func validDiagnosticRunHeaderWithStatus(runID, status string) map[string]any {
	run := validDiagnosticRunHeader(runID)
	run["status"] = status
	return run
}
