package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestRunsUseRunListV1RPC(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
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
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "list", "--status", "RUNNING", "--limit", "2", "--cursor", "cur-1", "--since", "2026-05-13T10:00:00Z", "--until", "2026-05-13T11:00:00Z"}, &stdout, &stderr, testRootCommandOptions(server))
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
				"Use `swarm run list`",
				"Use `swarm run status [run-id]`",
				"Use `swarm run trace [run-id] [--follow]`",
				"Use `swarm health`",
			},
		},
		{
			name: "investigate runs",
			args: []string{"investigate", "runs"},
			wantOutput: []string{
				"ERROR: `swarm investigate runs` was retired in CLI v2.",
				"Use `swarm run list`.",
			},
		},
		{
			name: "investigate runs legacy flags",
			args: []string{"investigate", "runs", "--status", "running", "--limit", "1", "--cursor", "cur", "--since", "2026-05-13T10:00:00Z", "--until", "2026-05-13T11:00:00Z"},
			wantOutput: []string{
				"ERROR: `swarm investigate runs` was retired in CLI v2.",
				"Use `swarm run list`.",
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
			args:       []string{"run", "status", "run-1"},
			wantMethod: "run.diagnose",
			wantOutput: []string{"Run run-1  stalled", "run status  running", "blocker     delivery lifecycle, no active deliveries", "dead letters exist"},
		},
		{
			name:       "header only",
			args:       []string{"run", "status", "run-1", "--no-diagnose"},
			wantMethod: "run.get",
			wantOutput: []string{"Run run-1  running", "trigger  scan.requested"},
			notOutput:  "blocker",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
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
				return validDiagnosticRunDiagnosis("run-1", "stalled", "delivery_lifecycle", "no_active_deliveries", []any{"dead letters exist for this run"})
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

func TestStatusHumanOutputPreservesCanonicalTerminalEvidence(t *testing.T) {
	failure := testRuntimeFailure("workflow_failed")
	for _, tc := range []struct {
		name       string
		status     string
		wantOutput []string
	}{
		{
			name:   "failure envelope",
			status: "failed",
			wantOutput: []string{
				"Run run-1  failed",
				"platform.connector_failure/workflow_failed",
				failure.Message,
				failure.Remediation,
			},
		},
		{
			name:   "control reason",
			status: "cancelled",
			wantOutput: []string{
				"Run run-1  cancelled",
				"control reason  operator_stopped",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server, _ := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
				if req.Method != "run.get" {
					t.Fatalf("method = %q, want run.get", req.Method)
				}
				return map[string]any{"run": validDiagnosticRunHeaderWithStatus("run-1", tc.status)}
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "status", "run-1", "--no-diagnose"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			for _, want := range tc.wantOutput {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
				}
			}
		})
	}
}

func TestStatusProjectsCanonicalScoringBlockerTuple(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server, _ := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
		if req.Method != "run.diagnose" {
			t.Fatalf("method = %q, want run.diagnose", req.Method)
		}
		return validDiagnosticRunDiagnosis("run-1", "stalled", "scoring_terminal_outcome", "terminal_scoring_outcome_missing", []any{})
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "status", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "blocker     scoring outcome, waiting for a terminal scoring outcome") {
		t.Fatalf("stdout does not project scoring blocker tuple:\n%s", stdout.String())
	}
	for _, machineLabel := range []string{"scoring_terminal_outcome", "terminal_scoring_outcome_missing"} {
		if strings.Contains(stdout.String(), machineLabel) {
			t.Fatalf("stdout leaks machine label %q:\n%s", machineLabel, stdout.String())
		}
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
			args:       []string{"run", "status"},
			lists:      [][]any{{validDiagnosticRunHeaderWithStatus("running-run", "running")}},
			owner:      "run.diagnose",
			selected:   "running-run",
			wantOutput: "Run running-run  running",
		},
		{
			name:       "status falls back to paused",
			args:       []string{"run", "status", "--no-diagnose"},
			lists:      [][]any{{}, {validDiagnosticRunHeaderWithStatus("paused-run", "paused")}},
			owner:      "run.get",
			selected:   "paused-run",
			wantOutput: "Run paused-run  paused",
		},
		{
			name:       "trace uses terminal fallback",
			args:       []string{"run", "trace"},
			lists:      [][]any{{}, {}, {validDiagnosticRunHeaderWithStatus("terminal-run", "completed")}},
			owner:      "run.trace",
			selected:   "terminal-run",
			wantOutput: "run trace terminal-run",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
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
					return validDiagnosticRunDiagnosis(tc.selected, "running", "", "", []any{})
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
			if got := stderr.String(); !strings.Contains(got, "ERROR: `swarm investigate run` was retired in CLI v2.") || !strings.Contains(got, "Use `swarm run status`.") {
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
			if got := stderr.String(); !strings.Contains(got, "ERROR: `swarm investigate trace` was retired in CLI v2.") || !strings.Contains(got, "Use `swarm run trace`.") {
				t.Fatalf("stderr = %q, want retired migration message", got)
			}
			if calls.Load() != 0 {
				t.Fatalf("RPC calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestTraceUsesRunTraceSnapshot(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
		if req.Method != "run.trace" {
			t.Fatalf("method = %q, want run.trace", req.Method)
		}
		wantParams := map[string]any{
			"run_id":           "run-1",
			"limit":            float64(2),
			"cursor":           "trace-cur",
			"since":            "2026-05-13T10:00:00Z",
			"until":            "2026-05-13T10:05:00Z",
			"include_internal": true,
			"filter": map[string]any{
				"event_name":      []any{"scan.requested", "scan.completed"},
				"entity_id":       []any{"entity-1", "entity-2"},
				"delivery_status": []any{"delivered", "failed"},
				"subscriber_id":   []any{"agent-1", "node-2"},
				"subscriber_type": []any{"agent", "node"},
			},
		}
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
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"run", "trace", "run-1",
		"--verbose",
		"--limit", "2",
		"--cursor", "trace-cur",
		"--since", "2026-05-13T10:00:00Z",
		"--until", "2026-05-13T10:05:00Z",
		"--event-name", "scan.requested",
		"--event-name", "scan.completed",
		"--entity-id", "entity-1",
		"--entity-id", "entity-2",
		"--delivery-status", "delivered",
		"--delivery-status", "FAILED",
		"--subscriber-id", "agent-1",
		"--subscriber-id", "node-2",
		"--subscriber-type", "agent",
		"--subscriber-type", "NODE",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(*requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(*requests))
	}
	for _, want := range []string{"run trace run-1", "event-1", "scan.requested", "agent/agent-1", "next_cursor=trace-next"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	for _, want := range []string{"SESSION", "TURN", "+0ms"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestTraceDeliveryDetailRendersRunTraceLifecycleFields(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
		if req.Method != "run.trace" {
			t.Fatalf("method = %q, want run.trace", req.Method)
		}
		wantParams := map[string]any{"run_id": "run-1", "limit": float64(1)}
		if !reflect.DeepEqual(req.Params, wantParams) {
			t.Fatalf("params = %#v, want %#v", req.Params, wantParams)
		}
		return map[string]any{
			"trace": []any{map[string]any{
				"event_id":              "event-1",
				"event_name":            "scan.requested",
				"event_created_at":      "2026-05-13T10:00:01Z",
				"delivery_id":           "delivery-1",
				"delivery_status":       "delivered",
				"delivery_reason_code":  "node_processed",
				"delivery_created_at":   "2026-05-13T10:00:02Z",
				"delivery_started_at":   "2026-05-13T10:00:04Z",
				"delivery_delivered_at": "2026-05-13T10:00:09Z",
				"subscriber_type":       "agent",
				"subscriber_id":         "agent-1",
				"session_id":            "session-1",
				"turn_id":               "turn-1",
			}},
		}
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "trace", "run-1", "--delivery-detail", "--limit", "1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(*requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(*requests))
	}
	for _, want := range []string{
		"DELIVERY ID",
		"delivery-1",
		"agent/agent-1",
		"delivered",
		"node_processed",
		"2026-05-13T10:00:02Z",
		"2026-05-13T10:00:04Z",
		"2026-05-13T10:00:09Z",
		"2s",
		"5s",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestTraceDeliverySummaryExhaustsRunTracePages(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, callIndex int) map[string]any {
		if req.Method != "run.trace" {
			t.Fatalf("method[%d] = %q, want run.trace", callIndex, req.Method)
		}
		baseParams := map[string]any{
			"run_id":           "run-1",
			"limit":            float64(1),
			"since":            "2026-05-13T10:00:00Z",
			"until":            "2026-05-13T10:05:00Z",
			"include_internal": true,
			"filter": map[string]any{
				"delivery_status": []any{"in_progress", "delivered", "failed"},
				"subscriber_type": []any{"agent", "node"},
			},
		}
		switch callIndex {
		case 0:
			baseParams["cursor"] = "start-cur"
			if !reflect.DeepEqual(req.Params, baseParams) {
				t.Fatalf("params[%d] = %#v, want %#v", callIndex, req.Params, baseParams)
			}
			return map[string]any{
				"trace": []any{
					map[string]any{
						"event_id":         "event-only",
						"event_name":       "scan.created",
						"event_created_at": "2026-05-13T10:00:00Z",
					},
					map[string]any{
						"event_id":            "event-1",
						"event_name":          "scan.requested",
						"event_created_at":    "2026-05-13T10:00:01Z",
						"delivery_id":         "delivery-1",
						"delivery_status":     "in_progress",
						"delivery_created_at": "2026-05-13T10:00:01Z",
						"delivery_started_at": "2026-05-13T10:00:04Z",
						"subscriber_type":     "agent",
						"subscriber_id":       "agent-1",
					},
				},
				"next_cursor": "page-2",
			}
		case 1:
			baseParams["cursor"] = "page-2"
			if !reflect.DeepEqual(req.Params, baseParams) {
				t.Fatalf("params[%d] = %#v, want %#v", callIndex, req.Params, baseParams)
			}
			return map[string]any{
				"trace": []any{
					map[string]any{
						"event_id":              "event-2",
						"event_name":            "scan.completed",
						"event_created_at":      "2026-05-13T10:00:05Z",
						"delivery_id":           "delivery-2",
						"delivery_status":       "delivered",
						"delivery_created_at":   "2026-05-13T10:00:05Z",
						"delivery_started_at":   "2026-05-13T10:00:07Z",
						"delivery_delivered_at": "2026-05-13T10:00:12Z",
						"subscriber_type":       "agent",
						"subscriber_id":         "agent-1",
					},
					map[string]any{
						"event_id":            "event-3",
						"event_name":          "scan.failed",
						"event_created_at":    "2026-05-13T10:00:06Z",
						"delivery_id":         "delivery-3",
						"delivery_status":     "failed",
						"delivery_failure":    testRuntimeFailure("handler_failed"),
						"delivery_created_at": "2026-05-13T10:00:06Z",
						"subscriber_type":     "node",
						"subscriber_id":       "node-1",
					},
				},
			}
		default:
			t.Fatalf("unexpected request[%d]: %#v", callIndex, req)
		}
		return nil
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"run", "trace", "run-1",
		"--delivery-summary",
		"--verbose",
		"--limit", "1",
		"--cursor", "start-cur",
		"--since", "2026-05-13T10:00:00Z",
		"--until", "2026-05-13T10:05:00Z",
		"--delivery-status", "in_progress",
		"--delivery-status", "delivered",
		"--delivery-status", "failed",
		"--subscriber-type", "agent",
		"--subscriber-type", "node",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(*requests) != 2 {
		t.Fatalf("requests = %d, want 2 paged run.trace calls", len(*requests))
	}
	for _, want := range []string{
		"run trace delivery summary: run_id=run-1 snapshot=point-in-time trace_rows=4 delivery_rows=3 non_delivery_rows=1",
		"SUBSCRIBER",
		"IN PROGRESS",
		"agent/agent-1",
		"node/node-1",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "next_cursor=") {
		t.Fatalf("stdout = %q, want summary to exhaust pages instead of printing next_cursor", stdout.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestTraceDeliverySummaryUsesOmittedRunResolver(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	wantReference := time.Date(2026, 5, 13, 10, 30, 0, 123456789, time.UTC)
	var capturedUntil string
	server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, callIndex int) map[string]any {
		switch callIndex {
		case 0:
			if req.Method != "run.list" {
				t.Fatalf("method[%d] = %q, want run.list", callIndex, req.Method)
			}
			return map[string]any{"runs": []any{validDiagnosticRunHeaderWithStatus("active-run", "running")}}
		case 1:
			if req.Method != "run.trace" {
				t.Fatalf("method[%d] = %q, want run.trace", callIndex, req.Method)
			}
			if req.Params["run_id"] != "active-run" {
				t.Fatalf("params[%d] = %#v, want active run trace", callIndex, req.Params)
			}
			until, ok := req.Params["until"].(string)
			if !ok || strings.TrimSpace(until) == "" {
				t.Fatalf("params[%d] = %#v, want captured until bound", callIndex, req.Params)
			}
			if _, err := time.Parse(time.RFC3339Nano, until); err != nil {
				t.Fatalf("until = %q, want RFC3339Nano: %v", until, err)
			}
			if _, ok := req.Params["cursor"]; ok {
				t.Fatalf("params[%d] = %#v, want no cursor on first summary page", callIndex, req.Params)
			}
			capturedUntil = until
			return map[string]any{"trace": []any{map[string]any{
				"event_id":              "event-1",
				"event_name":            "scan.completed",
				"event_created_at":      "2026-05-13T10:00:00Z",
				"delivery_id":           "delivery-1",
				"delivery_status":       "delivered",
				"delivery_created_at":   "2026-05-13T10:00:00Z",
				"delivery_started_at":   "2026-05-13T10:00:01Z",
				"delivery_delivered_at": "2026-05-13T10:00:03Z",
				"subscriber_type":       "agent",
				"subscriber_id":         "agent-1",
			}}, "next_cursor": "page-2"}
		case 2:
			if req.Method != "run.trace" {
				t.Fatalf("method[%d] = %q, want run.trace", callIndex, req.Method)
			}
			if req.Params["run_id"] != "active-run" || req.Params["cursor"] != "page-2" || req.Params["until"] != capturedUntil {
				t.Fatalf("params[%d] = %#v, want same captured until and advanced cursor", callIndex, req.Params)
			}
			return map[string]any{"trace": []any{}}
		default:
			t.Fatalf("unexpected request[%d]: %#v", callIndex, req)
		}
		return nil
	})
	defer server.Close()

	nowCalls := 0
	opts := testRootCommandOptions(server)
	opts.now = func() time.Time {
		nowCalls++
		return wantReference
	}
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "trace", "--delivery-summary"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(*requests) != 3 {
		t.Fatalf("requests = %d, want run.list then two run.trace pages", len(*requests))
	}
	if nowCalls != 1 {
		t.Fatalf("invocation clock calls = %d, want 1", nowCalls)
	}
	if capturedUntil != wantReference.Format(time.RFC3339Nano) {
		t.Fatalf("captured until = %q, want %q", capturedUntil, wantReference.Format(time.RFC3339Nano))
	}
	if !strings.Contains(stdout.String(), "run trace delivery summary: run_id=active-run snapshot=point-in-time") {
		t.Fatalf("stdout = %q, want resolved active-run summary", stdout.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func wantFullTraceFilterParams() map[string]any {
	return map[string]any{
		"event_name":      []any{"scan.requested", "scan.completed"},
		"entity_id":       []any{"entity-1", "entity-2"},
		"delivery_status": []any{"delivered", "failed"},
		"subscriber_id":   []any{"agent-1", "node-2"},
		"subscriber_type": []any{"agent", "node"},
	}
}

func TestTraceSnapshotFiltersUseOmittedRunResolver(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, callIndex int) map[string]any {
		switch callIndex {
		case 0:
			if req.Method != "run.list" {
				t.Fatalf("method[%d] = %q, want run.list", callIndex, req.Method)
			}
			wantParams := map[string]any{"status": "running", "limit": float64(1)}
			if !reflect.DeepEqual(req.Params, wantParams) {
				t.Fatalf("params[%d] = %#v, want %#v", callIndex, req.Params, wantParams)
			}
			return map[string]any{"runs": []any{validDiagnosticRunHeaderWithStatus("active-run", "running")}}
		case 1:
			if req.Method != "run.trace" {
				t.Fatalf("method[%d] = %q, want run.trace", callIndex, req.Method)
			}
			wantParams := map[string]any{
				"run_id": "active-run",
				"limit":  float64(3),
				"cursor": "trace-cur",
				"since":  "2026-05-13T10:00:00Z",
				"until":  "2026-05-13T10:05:00Z",
				"filter": map[string]any{"event_name": []any{"scan.requested"}},
			}
			if !reflect.DeepEqual(req.Params, wantParams) {
				t.Fatalf("params[%d] = %#v, want %#v", callIndex, req.Params, wantParams)
			}
			return map[string]any{"trace": []any{map[string]any{
				"event_id":         "event-active",
				"event_name":       "scan.requested",
				"event_created_at": "2026-05-13T10:00:01Z",
			}}}
		default:
			t.Fatalf("unexpected request[%d]: %#v", callIndex, req)
		}
		return nil
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "trace", "--limit", "3", "--cursor", "trace-cur", "--since", "2026-05-13T10:00:00Z", "--until", "2026-05-13T10:05:00Z", "--event-name", "scan.requested"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(*requests) != 2 {
		t.Fatalf("requests = %d, want run.list then run.trace", len(*requests))
	}
	if !strings.Contains(stdout.String(), "run trace active-run") {
		t.Fatalf("stdout = %q, want resolved run trace header", stdout.String())
	}
	for _, unwanted := range []string{"SESSION", "TURN"} {
		if strings.Contains(stdout.String(), unwanted) {
			t.Fatalf("stdout contains %q for non-agent trace:\n%s", unwanted, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestTraceFollowUsesRunSubscribeTrace(t *testing.T) {
	for _, tc := range []struct {
		name                string
		args                []string
		wantFilter          map[string]any
		wantIncludeInternal bool
	}{
		{
			name:                "long follow verbose",
			args:                []string{"run", "trace", "run-follow", "--follow", "--verbose", "--no-retry", "--event-name", "scan.requested", "--event-name", "scan.completed", "--entity-id", "entity-1", "--entity-id", "entity-2", "--delivery-status", "delivered", "--delivery-status", "failed", "--subscriber-id", "agent-1", "--subscriber-id", "node-2", "--subscriber-type", "agent", "--subscriber-type", "node"},
			wantFilter:          wantFullTraceFilterParams(),
			wantIncludeInternal: true,
		},
		{
			name:       "shorthand follow",
			args:       []string{"run", "trace", "run-follow", "-f", "--no-retry", "--event-name", "scan.requested"},
			wantFilter: map[string]any{"event_name": []any{"scan.requested"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
				wsRows:           []map[string]any{validRunCommandTraceRow("evt-follow")},
				wsCloseAfterRows: true,
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			assertRunCommandMethods(t, calls, nil)
			assertRunCommandTraceSubscriptionWithFilter(t, wsRequests, "run-follow", false, tc.wantFilter)
			if got := (*wsRequests)[0].Params["include_internal"]; got != tc.wantIncludeInternal {
				if tc.wantIncludeInternal || got != nil {
					t.Fatalf("ws include_internal = %#v, want %t", got, tc.wantIncludeInternal)
				}
			}
			if !strings.Contains(stdout.String(), "id=evt-follow") {
				t.Fatalf("stdout = %q, want trace row", stdout.String())
			}
			if strings.TrimSpace(stderr.String()) != "" {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestTraceFollowOmittedRunReusesActivePreferenceResolver(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
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
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "trace", "--follow", "--no-retry", "--event-name", "scan.requested", "--subscriber-id", "agent-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"run.list"})
	assertRunCommandTraceSubscriptionWithFilter(t, wsRequests, "active-run", false, map[string]any{
		"event_name":    []any{"scan.requested"},
		"subscriber_id": []any{"agent-1"},
	})
	if !strings.Contains(stdout.String(), "id=evt-active") {
		t.Fatalf("stdout = %q, want trace row", stdout.String())
	}
}

func TestTraceHelpPromotesNoRetryWithoutReplaySince(t *testing.T) {
	var stdout, stderr bytes.Buffer
	opts := defaultRootCommandOptions()
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "trace", "--help"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "--no-retry") {
		t.Fatalf("help missing --no-retry:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "-f, --follow") {
		t.Fatalf("help missing -f shorthand:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "--until") {
		t.Fatalf("help missing --until:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "--delivery-detail") {
		t.Fatalf("help missing --delivery-detail:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "--delivery-summary") {
		t.Fatalf("help missing --delivery-summary:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "--verbose") {
		t.Fatalf("help missing --verbose:\n%s", stdout.String())
	}
	if strings.Contains(stdout.String(), "--replay-since") {
		t.Fatalf("help exposed unpromoted --replay-since:\n%s", stdout.String())
	}
}

func TestTraceFollowRecoversWithReplaySinceAfterClose(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var mu sync.Mutex
	rpcRequests := []jsonRPCRequest{}
	wsRequests := []jsonRPCRequest{}
	secondRowSent := make(chan struct{})
	var signalSecondRow sync.Once
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/rpc":
			if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
				t.Errorf("Authorization = %q, want bearer token", got)
			}
			var req jsonRPCRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode request: %v", err)
				return
			}
			mu.Lock()
			rpcRequests = append(rpcRequests, req)
			mu.Unlock()
			if req.Method != "run.list" {
				t.Errorf("method = %q, want run.list", req.Method)
			}
			writeJSONRPCResult(t, w, req.ID, map[string]any{"runs": []any{validDiagnosticRunHeaderWithStatus("active-run", "running")}})
		case "/v1/ws":
			if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
				t.Errorf("WS Authorization = %q, want bearer token", got)
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Errorf("upgrade websocket: %v", err)
				return
			}
			defer conn.Close()
			var req jsonRPCRequest
			if err := conn.ReadJSON(&req); err != nil {
				t.Errorf("read ws request: %v", err)
				return
			}
			mu.Lock()
			wsRequests = append(wsRequests, req)
			callIndex := len(wsRequests)
			mu.Unlock()
			if req.Method != "run.subscribe_trace" {
				t.Errorf("ws method = %q, want run.subscribe_trace", req.Method)
			}
			subscriptionID := fmt.Sprintf("sub-%d", callIndex)
			if err := conn.WriteJSON(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  map[string]any{"subscription_id": subscriptionID},
			}); err != nil {
				t.Errorf("write ws subscription response: %v", err)
				return
			}
			row := validRunCommandTraceRow(fmt.Sprintf("evt-%d", callIndex))
			row["event_created_at"] = fmt.Sprintf("2026-05-13T10:00:0%dZ", callIndex)
			if err := conn.WriteJSON(map[string]any{
				"jsonrpc": "2.0",
				"method":  "rpc.subscription",
				"params": map[string]any{
					"subscription": subscriptionID,
					"result":       row,
				},
			}); err != nil {
				return
			}
			if callIndex == 1 {
				_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}
			time.Sleep(20 * time.Millisecond)
			signalSecondRow.Do(func() { close(secondRowSent) })
			<-r.Context().Done()
		default:
			t.Errorf("path = %q, want /v1/rpc or /v1/ws", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-secondRowSent
		cancel()
	}()
	var stdout, stderr bytes.Buffer
	wantFilter := wantFullTraceFilterParams()
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"run", "trace", "--follow", "--event-name", "scan.requested", "--event-name", "scan.completed", "--entity-id", "entity-1", "--entity-id", "entity-2", "--delivery-status", "delivered", "--delivery-status", "failed", "--subscriber-id", "agent-1", "--subscriber-id", "node-2", "--subscriber-type", "agent", "--subscriber-type", "node"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 130 {
		t.Fatalf("code = %d, want 130 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, &rpcRequests, []string{"run.list"})
	if len(wsRequests) != 2 {
		t.Fatalf("ws requests = %#v, want initial subscribe and one recovery subscribe", wsRequests)
	}
	for i, req := range wsRequests {
		if req.Method != "run.subscribe_trace" {
			t.Fatalf("ws method[%d] = %q, want run.subscribe_trace", i, req.Method)
		}
		if got := req.Params["run_id"]; got != "active-run" {
			t.Fatalf("ws run_id[%d] = %#v, want active-run", i, got)
		}
		if got := req.Params["filter"]; !reflect.DeepEqual(got, wantFilter) {
			t.Fatalf("ws filter[%d] = %#v, want %#v", i, got, wantFilter)
		}
	}
	if _, ok := wsRequests[0].Params["replay_since"]; ok {
		t.Fatalf("initial replay_since = %#v, want omitted", wsRequests[0].Params["replay_since"])
	}
	if got := wsRequests[1].Params["replay_since"]; got != "2026-05-13T10:00:01Z" {
		t.Fatalf("recovery replay_since = %#v, want first row timestamp", got)
	}
	for _, want := range []string{"id=evt-1", "id=evt-2"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if !strings.Contains(stderr.String(), "detached from run trace") {
		t.Fatalf("stderr = %q, want detach message", stderr.String())
	}
}

func TestTraceFollowRetriesRetryableReadFailure(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var mu sync.Mutex
	wsRequests := []jsonRPCRequest{}
	recoveredRowSent := make(chan struct{})
	var signalRecoveredRow sync.Once
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/ws" {
			t.Errorf("path = %q, want only /v1/ws", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("WS Authorization = %q, want bearer token", got)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer conn.Close()
		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			t.Errorf("read ws request: %v", err)
			return
		}
		mu.Lock()
		wsRequests = append(wsRequests, req)
		callIndex := len(wsRequests)
		mu.Unlock()
		if req.Method != "run.subscribe_trace" {
			t.Errorf("ws method = %q, want run.subscribe_trace", req.Method)
		}
		subscriptionID := fmt.Sprintf("sub-retry-%d", callIndex)
		if err := conn.WriteJSON(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  map[string]any{"subscription_id": subscriptionID},
		}); err != nil {
			t.Errorf("write ws subscription response: %v", err)
			return
		}
		if callIndex == 1 {
			_ = conn.UnderlyingConn().Close()
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"jsonrpc": "2.0",
			"method":  "rpc.subscription",
			"params": map[string]any{
				"subscription": subscriptionID,
				"result":       validRunCommandTraceRow("evt-recovered"),
			},
		}); err != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
		signalRecoveredRow.Do(func() { close(recoveredRowSent) })
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-recoveredRowSent
		cancel()
	}()
	var stdout, stderr bytes.Buffer
	wantFilter := map[string]any{"delivery_status": []any{"delivered"}, "subscriber_type": []any{"agent"}}
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"run", "trace", "run-retry", "--follow", "--delivery-status", "delivered", "--subscriber-type", "agent"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 130 {
		t.Fatalf("code = %d, want 130 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if len(wsRequests) != 2 {
		t.Fatalf("ws requests = %#v, want retry after read failure", wsRequests)
	}
	for i, req := range wsRequests {
		if got := req.Params["run_id"]; got != "run-retry" {
			t.Fatalf("ws run_id[%d] = %#v, want run-retry", i, got)
		}
		if _, ok := req.Params["replay_since"]; ok {
			t.Fatalf("ws replay_since[%d] = %#v, want omitted because no row was rendered before retry", i, req.Params["replay_since"])
		}
		if got := req.Params["filter"]; !reflect.DeepEqual(got, wantFilter) {
			t.Fatalf("ws filter[%d] = %#v, want %#v", i, got, wantFilter)
		}
	}
	if !strings.Contains(stdout.String(), "id=evt-recovered") {
		t.Fatalf("stdout = %q, want recovered row", stdout.String())
	}
	if !strings.Contains(stderr.String(), "detached from run trace") {
		t.Fatalf("stderr = %q, want detach message", stderr.String())
	}
}

func TestTraceFollowClassifiesTypedTransportAsRetryable(t *testing.T) {
	err := &cliAPITransportError{
		surface:   "runtime event stream",
		endpoint:  "ws://127.0.0.1:8081/v1/ws",
		operation: "notification read",
		err:       io.ErrUnexpectedEOF,
	}
	if !traceFollowRetryableSubscribeError(err) {
		t.Fatal("traceFollowRetryableSubscribeError = false, want true")
	}
	if !traceFollowRetryableStreamError(err) {
		t.Fatal("traceFollowRetryableStreamError = false, want true")
	}
}

func TestTraceFollowCtrlCDetachesWithoutRunStop(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
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
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"run", "trace", "run-active", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
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
	setCLIAPITestToken(t, "test-token")
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
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "trace", "run-bad-ws", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
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
	setCLIAPITestToken(t, "test-token")
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
	setCLIAPITestToken(t, "test-token")
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
		{name: "runs invalid limit", args: []string{"run", "list", "--limit", "0"}, wantStderr: "--limit must be between 1 and 500"},
		{name: "runs invalid since", args: []string{"run", "list", "--since", "yesterday"}, wantStderr: "--since must be an RFC3339 timestamp or a positive relative duration"},
		{name: "trace extra arg", args: []string{"run", "trace", "run-1", "extra"}, wantStderr: "accepts at most one argument"},
		{name: "trace invalid limit low", args: []string{"run", "trace", "run-1", "--limit", "0"}, wantStderr: "--limit must be between 1 and 2000"},
		{name: "trace invalid limit high", args: []string{"run", "trace", "run-1", "--limit", "2001"}, wantStderr: "--limit must be between 1 and 2000"},
		{name: "trace invalid since", args: []string{"run", "trace", "run-1", "--since", "yesterday"}, wantStderr: "--since must be an RFC3339 timestamp or a positive relative duration"},
		{name: "trace invalid until", args: []string{"run", "trace", "run-1", "--until", "tomorrow"}, wantStderr: "--until must be an RFC3339 timestamp or a positive relative duration"},
		{name: "trace since after until", args: []string{"run", "trace", "run-1", "--since", "2026-05-13T10:05:00Z", "--until", "2026-05-13T10:00:00Z"}, wantStderr: "--until must be greater than or equal to --since"},
		{name: "trace blank cursor", args: []string{"run", "trace", "run-1", "--cursor", " "}, wantStderr: "--cursor must not be empty"},
		{name: "trace no retry requires follow", args: []string{"run", "trace", "run-1", "--no-retry"}, wantStderr: "--no-retry requires --follow"},
		{name: "trace delivery detail and summary are mutually exclusive", args: []string{"run", "trace", "run-1", "--delivery-detail", "--delivery-summary"}, wantStderr: "--delivery-detail and --delivery-summary are mutually exclusive"},
		{name: "trace follow rejects limit", args: []string{"run", "trace", "run-1", "--follow", "--limit", "5"}, wantStderr: "--limit is not supported with --follow"},
		{name: "trace follow rejects cursor", args: []string{"run", "trace", "run-1", "--follow", "--cursor", "cur"}, wantStderr: "--cursor is not supported with --follow"},
		{name: "trace follow rejects since", args: []string{"run", "trace", "run-1", "--follow", "--since", "2026-05-13T10:00:00Z"}, wantStderr: "--since is not supported with --follow"},
		{name: "trace follow rejects until", args: []string{"run", "trace", "run-1", "--follow", "--until", "2026-05-13T10:05:00Z"}, wantStderr: "--until is not supported with --follow"},
		{name: "trace follow rejects delivery detail", args: []string{"run", "trace", "run-1", "--follow", "--delivery-detail"}, wantStderr: "--delivery-detail is not supported with --follow"},
		{name: "trace follow rejects delivery summary", args: []string{"run", "trace", "run-1", "--follow", "--delivery-summary"}, wantStderr: "--delivery-summary is not supported with --follow"},
		{name: "trace shorthand follow rejects limit", args: []string{"run", "trace", "run-1", "-f", "--limit", "5"}, wantStderr: "--limit is not supported with --follow"},
		{name: "trace shorthand follow rejects cursor", args: []string{"run", "trace", "run-1", "-f", "--cursor", "cur"}, wantStderr: "--cursor is not supported with --follow"},
		{name: "trace shorthand follow rejects since", args: []string{"run", "trace", "run-1", "-f", "--since", "2026-05-13T10:00:00Z"}, wantStderr: "--since is not supported with --follow"},
		{name: "trace shorthand follow rejects until", args: []string{"run", "trace", "run-1", "-f", "--until", "2026-05-13T10:05:00Z"}, wantStderr: "--until is not supported with --follow"},
		{name: "trace follow direct replay since remains unpromoted", args: []string{"run", "trace", "run-1", "--follow", "--replay-since", "2026-05-13T10:00:00Z"}, wantStderr: "unknown flag"},
		{name: "trace blank event name", args: []string{"run", "trace", "run-1", "--event-name", " "}, wantStderr: "--event-name must not be empty"},
		{name: "trace blank subscriber id", args: []string{"run", "trace", "run-1", "--subscriber-id", " "}, wantStderr: "--subscriber-id must not be empty"},
		{name: "trace invalid entity id", args: []string{"run", "trace", "run-1", "--entity-id", "bad id!"}, wantStderr: "--entity-id must match OpaqueId pattern"},
		{name: "trace invalid delivery status", args: []string{"run", "trace", "run-1", "--delivery-status", "done"}, wantStderr: "--delivery-status must be one of"},
		{name: "trace invalid subscriber type", args: []string{"run", "trace", "run-1", "--subscriber-type", "platform"}, wantStderr: "--subscriber-type must be one of"},
		{name: "trace follow blank event name", args: []string{"run", "trace", "run-1", "--follow", "--event-name", " "}, wantStderr: "--event-name must not be empty"},
		{name: "trace follow invalid entity id", args: []string{"run", "trace", "run-1", "--follow", "--entity-id", "bad id!"}, wantStderr: "--entity-id must match OpaqueId pattern"},
		{name: "trace follow invalid delivery status", args: []string{"run", "trace", "run-1", "--follow", "--delivery-status", "done"}, wantStderr: "--delivery-status must be one of"},
		{name: "trace follow blank subscriber id", args: []string{"run", "trace", "run-1", "--follow", "--subscriber-id", " "}, wantStderr: "--subscriber-id must not be empty"},
		{name: "trace follow invalid subscriber type", args: []string{"run", "trace", "run-1", "--follow", "--subscriber-type", "platform"}, wantStderr: "--subscriber-type must be one of"},
		{name: "status extra arg", args: []string{"run", "status", "run-1", "extra"}, wantStderr: "accepts at most one argument"},
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
		{"run", "list"},
		{"run", "trace", "run-1"},
		{"run", "trace", "run-1", "--verbose"},
		{"run", "trace", "run-1", "--limit", "2", "--cursor", "cur", "--since", "2026-05-13T10:00:00Z", "--until", "2026-05-13T10:05:00Z"},
		{"run", "trace", "run-1", "--delivery-detail"},
		{"run", "trace", "run-1", "--delivery-summary"},
		{"run", "trace", "run-1", "--event-name", "scan.requested", "--entity-id", "entity-1", "--delivery-status", "delivered", "--subscriber-id", "agent-1", "--subscriber-type", "agent"},
		{"run", "trace", "run-1", "--follow"},
		{"run", "trace", "run-1", "-f"},
		{"run", "trace", "run-1", "--follow", "--no-retry"},
		{"run", "trace", "run-1", "--follow", "--event-name", "scan.requested", "--entity-id", "entity-1", "--delivery-status", "delivered", "--subscriber-id", "agent-1", "--subscriber-type", "agent"},
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
			if !strings.Contains(stderr.String(), "API token source is required") {
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
		{"run", "status"},
		{"run", "status", "--no-diagnose"},
		{"run", "trace"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
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
			args: []string{"run", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"invalid bearer token"}`))
			},
			wantCode:   4,
			wantStderr: "rejected the request with status 401",
		},
		{
			name: "json rpc run not found",
			args: []string{"run", "status", "missing"},
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
			args: []string{"run", "list"},
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
			args: []string{"run", "status", "run-1", "--no-diagnose"},
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
			args: []string{"run", "list"},
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
			args: []string{"run", "status", "run-1", "--no-diagnose"},
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
			args: []string{"run", "status", "run-1"},
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
			args: []string{"run", "status", "run-1"},
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
			args: []string{"run", "status", "run-1"},
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
			args: []string{"run", "status", "run-1"},
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
			name: "run diagnose missing test quiescence",
			args: []string{"run", "status", "run-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"run":               validDiagnosticRunHeader("run-1"),
					"operational_state": "running",
					"blocking_layer":    "",
					"blocking_reason":   "",
					"heuristics":        []any{},
				})
			},
			wantCode:   3,
			wantStderr: "test_quiescence is required",
		},
		{
			name: "trace missing trace",
			args: []string{"run", "trace", "run-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "trace is required",
		},
		{
			name: "trace invalid delivery timestamp",
			args: []string{"run", "trace", "run-1", "--delivery-detail"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"trace": []any{map[string]any{
						"event_id":            "event-1",
						"event_name":          "scan.requested",
						"event_created_at":    "2026-05-13T10:00:01Z",
						"delivery_created_at": "yesterday",
					}},
				})
			},
			wantCode:   3,
			wantStderr: "trace[0].delivery_created_at must be an RFC3339 timestamp",
		},
		{
			name: "trace summary repeated cursor",
			args: []string{"run", "trace", "run-1", "--delivery-summary", "--cursor", "same-cur"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"trace":       []any{},
					"next_cursor": "same-cur",
				})
			},
			wantCode:   3,
			wantStderr: `repeated next_cursor "same-cur"`,
		},
		{
			name: "trace summary delivery row missing subscriber",
			args: []string{"run", "trace", "run-1", "--delivery-summary"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"trace": []any{map[string]any{
						"event_id":         "event-1",
						"event_name":       "scan.requested",
						"event_created_at": "2026-05-13T10:00:01Z",
						"delivery_id":      "delivery-1",
						"delivery_status":  "delivered",
					}},
				})
			},
			wantCode:   3,
			wantStderr: "trace[0].subscriber_type is required when delivery_id is present",
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
			name: "health invalid bundle hash",
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
						"bundle_hash":      "sha256:abc",
						"workflow_name":    "workflow",
						"workflow_version": "v1",
					},
				})
			},
			wantCode:   3,
			wantStderr: "bundle.bundle_hash must be bundle-v1:sha256:<64 lowercase hex>",
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

func validDiagnosticRunDiagnosis(runID, state, layer, reason string, heuristics []any) map[string]any {
	return map[string]any{
		"run":               validDiagnosticRunHeader(runID),
		"operational_state": state,
		"blocking_layer":    layer,
		"blocking_reason":   reason,
		"heuristics":        heuristics,
		"failed_deliveries": []any{},
		"test_quiescence": map[string]any{
			"ready":                     true,
			"active_deliveries":         0,
			"unsettled_pipeline_events": 0,
			"due_timers":                0,
			"active_session_leases":     0,
		},
	}
}

func validDiagnosticRunHeaderWithStatus(runID, status string) map[string]any {
	run := validDiagnosticRunHeader(runID)
	run["status"] = status
	switch status {
	case "failed":
		run["ended_at"] = "2026-05-13T10:01:00Z"
		run["failure"] = testRuntimeFailure("workflow_failed")
	case "cancelled":
		run["ended_at"] = "2026-05-13T10:01:00Z"
		run["control_reason"] = "operator_stopped"
	case "completed", "forked":
		run["ended_at"] = "2026-05-13T10:01:00Z"
	}
	return run
}
