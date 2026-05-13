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

func TestRunsAndInvestigateRunsUseRunListV1RPC(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		wantParams map[string]any
	}{
		{
			name: "swarm runs",
			args: []string{"runs", "--status", "RUNNING", "--limit", "2", "--cursor", "cur-1", "--since", "2026-05-13T10:00:00Z", "--until", "2026-05-13T11:00:00Z"},
			wantParams: map[string]any{
				"status": "running",
				"limit":  float64(2),
				"cursor": "cur-1",
				"since":  "2026-05-13T10:00:00Z",
				"until":  "2026-05-13T11:00:00Z",
			},
		},
		{
			name:       "swarm investigate runs",
			args:       []string{"investigate", "runs", "--limit", "1"},
			wantParams: map[string]any{"limit": float64(1)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if len(*requests) != 1 {
				t.Fatalf("requests = %d, want 1", len(*requests))
			}
			if !reflect.DeepEqual((*requests)[0].Params, tc.wantParams) {
				t.Fatalf("params = %#v, want %#v", (*requests)[0].Params, tc.wantParams)
			}
			for _, want := range []string{"RUN ID", "run-1", "running", "scan.requested", "next_cursor=next-1"} {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
				}
			}
			if strings.TrimSpace(stderr.String()) != "" {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestInvestigateRunUsesDiagnoseAndRunGet(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		wantMethod string
		wantOutput []string
		notOutput  string
	}{
		{
			name:       "diagnose default",
			args:       []string{"investigate", "run", "run-1"},
			wantMethod: "run.diagnose",
			wantOutput: []string{"Run: run-1", "operational_state=stalled", "blocking_layer=delivery_lifecycle", "dead letters exist"},
		},
		{
			name:       "header only",
			args:       []string{"investigate", "run", "run-1", "--no-diagnose"},
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

func TestInvestigateRunAndTraceResolveLatestRunThroughRunList(t *testing.T) {
	for _, tc := range []struct {
		name         string
		args         []string
		secondMethod string
		wantOutput   string
	}{
		{name: "run", args: []string{"investigate", "run"}, secondMethod: "run.diagnose", wantOutput: "Run: latest-run"},
		{name: "trace", args: []string{"investigate", "trace"}, secondMethod: "run.trace", wantOutput: "run trace: run_id=latest-run"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, callIndex int) map[string]any {
				switch callIndex {
				case 0:
					if req.Method != "run.list" {
						t.Fatalf("first method = %q, want run.list", req.Method)
					}
					if got := req.Params["limit"]; got != float64(1) {
						t.Fatalf("latest run limit = %#v, want 1", got)
					}
					return map[string]any{"runs": []any{validDiagnosticRunHeader("latest-run")}}
				case 1:
					if req.Method != tc.secondMethod {
						t.Fatalf("second method = %q, want %s", req.Method, tc.secondMethod)
					}
					if got := req.Params["run_id"]; got != "latest-run" {
						t.Fatalf("second run_id = %#v, want latest-run", got)
					}
					if tc.secondMethod == "run.trace" {
						return map[string]any{
							"trace": []any{map[string]any{
								"event_id":         "event-1",
								"event_name":       "scan.requested",
								"event_created_at": "2026-05-13T10:00:01Z",
							}},
						}
					}
					return map[string]any{
						"run":               validDiagnosticRunHeader("latest-run"),
						"operational_state": "running",
						"blocking_layer":    "",
						"blocking_reason":   "",
						"heuristics":        []any{},
					}
				default:
					t.Fatalf("unexpected request %d: %s", callIndex+1, req.Method)
				}
				return nil
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if len(*requests) != 2 {
				t.Fatalf("requests = %d, want 2", len(*requests))
			}
			if !strings.Contains(stdout.String(), tc.wantOutput) {
				t.Fatalf("stdout missing %q:\n%s", tc.wantOutput, stdout.String())
			}
		})
	}
}

func TestInvestigateTraceUsesRunTraceSnapshot(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
		if req.Method != "run.trace" {
			t.Fatalf("method = %q, want run.trace", req.Method)
		}
		wantParams := map[string]any{"run_id": "run-1", "limit": float64(5), "cursor": "trace-cur"}
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
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"investigate", "trace", "run-1", "--limit", "5", "--cursor", "trace-cur"}, &stdout, &stderr, testRootCommandOptions(server))
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

func TestInvestigateHealthUsesHealthCheck(t *testing.T) {
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
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"investigate", "health"}, &stdout, &stderr, testRootCommandOptions(server))
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
		{name: "trace invalid limit", args: []string{"investigate", "trace", "run-1", "--limit", "0"}, wantStderr: "--limit must be between 1 and 2000"},
		{name: "trace follow unsupported", args: []string{"investigate", "trace", "run-1", "--follow"}, wantStderr: "unknown flag"},
		{name: "run extra arg", args: []string{"investigate", "run", "run-1", "extra"}, wantStderr: "accepts at most 1 arg(s)"},
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
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"runs": []any{}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"runs"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 2 {
		t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "SWARM_API_TOKEN is required") {
		t.Fatalf("stderr = %q, want missing-token message", stderr.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("server calls = %d, want 0", calls.Load())
	}
}

func TestInvestigateOmittedRunFailsClosedWhenNoRunsExist(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
		if req.Method != "run.list" {
			t.Fatalf("method = %q, want run.list", req.Method)
		}
		return map[string]any{"runs": []any{}}
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"investigate", "run"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 2 {
		t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if len(*requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(*requests))
	}
	if !strings.Contains(stderr.String(), "no runs found") {
		t.Fatalf("stderr = %q, want no-runs error", stderr.String())
	}
}

func TestDiagnosticsFailClosedOnAPIAndMalformedResults(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantStderr string
	}{
		{
			name: "http auth failure",
			args: []string{"runs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"invalid bearer token"}`))
			},
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name: "json rpc run not found",
			args: []string{"investigate", "run", "missing"},
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
			wantStderr: "runs is required",
		},
		{
			name: "run get missing required run id",
			args: []string{"investigate", "run", "run-1", "--no-diagnose"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				run := validDiagnosticRunHeader("run-1")
				delete(run, "run_id")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"run": run})
			},
			wantStderr: "run.run_id is required",
		},
		{
			name: "run diagnose missing blocking layer",
			args: []string{"investigate", "run", "run-1"},
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
			wantStderr: "blocking_layer is required",
		},
		{
			name: "run diagnose missing blocking reason",
			args: []string{"investigate", "run", "run-1"},
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
			wantStderr: "blocking_reason is required",
		},
		{
			name: "run diagnose missing heuristics",
			args: []string{"investigate", "run", "run-1"},
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
			wantStderr: "heuristics is required",
		},
		{
			name: "trace missing trace",
			args: []string{"investigate", "trace", "run-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantStderr: "trace is required",
		},
		{
			name: "health missing bundle fingerprint",
			args: []string{"investigate", "health"},
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
			wantStderr: "bundle.fingerprint is required",
		},
		{
			name: "health missing workflow name",
			args: []string{"investigate", "health"},
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
			wantStderr: "bundle.workflow_name is required",
		},
		{
			name: "health missing workflow version",
			args: []string{"investigate", "health"},
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
			wantStderr: "bundle.workflow_version is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

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
