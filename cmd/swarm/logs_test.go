package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/gorilla/websocket"
	"gopkg.in/yaml.v3"
)

func TestLogsUsesRuntimeLogsV1RPCWithSnapshotFilters(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	_, failureValue := runtimeLogTestFailure(t)
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
		log := validRuntimeLogEntry("log-1")
		log["failure"] = failureValue
		log["details"] = map[string]any{"attempt": 2, "action": "delivery_failed", "failure": failureValue}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{
			"logs":        []any{log},
			"next_cursor": "log-cursor-2",
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"logs",
		"--run-id", "run-1",
		"--entity-id", "entity-1",
		"--session-id", "session-1",
		"--component", "scheduler",
		"--level", "WARN",
		"--error-code", "DELIVERY_FAILED",
		"--source", "runtime",
		"--since", "2026-05-19T10:00:00Z",
		"--until", "2026-05-19T11:00:00Z",
		"--limit", "25",
		"--cursor", "cursor-1",
		"--order", "ASC",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != runtimeLogsMethodList {
		t.Fatalf("method = %q, want %s", captured.Method, runtimeLogsMethodList)
	}
	wantParams := map[string]any{
		"run_id":     "run-1",
		"entity_id":  "entity-1",
		"session_id": "session-1",
		"component":  "scheduler",
		"level":      "warn",
		"error_code": "DELIVERY_FAILED",
		"source":     "runtime",
		"since":      "2026-05-19T10:00:00Z",
		"until":      "2026-05-19T11:00:00Z",
		"limit":      float64(25),
		"cursor":     "cursor-1",
		"order":      "asc",
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{"TIME", "ACTION", "delivery_failed", "connector_failure/waiting", "log message", "run-1", "entity-1", "session-1", "Next cursor: log-cursor-2"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestLogsFollowUsesRuntimeSubscribeLogsV1WS(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server, wsRequests := newRuntimeLogWSServer(t, runtimeLogWSServerOptions{
		logs:           []map[string]any{validRuntimeLogEntry("log-live-1")},
		closeAfterRows: true,
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"logs",
		"--follow",
		"--run-id", "run-1",
		"--entity-id", "entity-1",
		"--session-id", "session-1",
		"--component", "scheduler",
		"--level", "info",
		"--error-code", "DELIVERY_FAILED",
		"--source", "runtime",
		"--replay-since", "2026-05-19T10:00:00Z",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(*wsRequests) != 1 {
		t.Fatalf("ws requests = %d, want 1", len(*wsRequests))
	}
	req := (*wsRequests)[0]
	if req.Method != runtimeLogsMethodSubscribe {
		t.Fatalf("method = %q, want %s", req.Method, runtimeLogsMethodSubscribe)
	}
	wantParams := map[string]any{
		"run_id":       "run-1",
		"entity_id":    "entity-1",
		"session_id":   "session-1",
		"component":    "scheduler",
		"level":        "info",
		"error_code":   "DELIVERY_FAILED",
		"source":       "runtime",
		"replay_since": "2026-05-19T10:00:00Z",
	}
	if !reflect.DeepEqual(req.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", req.Params, wantParams)
	}
	for _, want := range []string{"--- 2026-05-19 UTC ---", "log log_id=log-live-1", "ts=10:00:01.000", "action=delivery_failed", "run_id=run-1", "message=log message", "details={\"attempt\":2}"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestLogsFollowProjectsExactMessageAndValidFailureThroughWebSocket(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	_, failureValue := runtimeLogTestFailure(t)
	exact := validRuntimeLogEntry("log-exact")
	exact["component"] = "eventbus"
	exact["message"] = "Event was published to the event bus"
	exact["details"] = map[string]any{"action": "published"}
	failing := validRuntimeLogEntry("log-failure")
	failing["component"] = "connector"
	failing["message"] = "Connector request failed"
	failing["failure"] = failureValue
	failing["details"] = map[string]any{"action": "request_failed", "failure": failureValue}

	server, _ := newRuntimeLogWSServer(t, runtimeLogWSServerOptions{
		logs:           []map[string]any{exact, failing},
		closeAfterRows: true,
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"logs", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	text := stdout.String()
	for _, want := range []string{
		"log_id=log-exact ts=10:00:01.000 level=info component=eventbus action=published source=runtime",
		"log_id=log-failure ts=10:00:01.000 level=info component=connector action=request_failed source=runtime",
		"failure=connector_failure/waiting",
		"message=Connector request failed",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("stdout missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "message=Event was published to the event bus") {
		t.Fatalf("stdout retained exact redundant message:\n%s", text)
	}
}

func TestLogsRejectInvalidInputBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"logs": []any{}})
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "invalid level", args: []string{"logs", "--level", "fatal"}, wantStderr: "--level must be one of"},
		{name: "invalid order", args: []string{"logs", "--order", "sideways"}, wantStderr: "--order must be one of"},
		{name: "invalid limit", args: []string{"logs", "--limit", "0"}, wantStderr: "--limit must be between 1 and 1000"},
		{name: "invalid since", args: []string{"logs", "--since", "not-time"}, wantStderr: "--since must be an RFC3339 timestamp or a positive relative duration"},
		{name: "invalid window", args: []string{"logs", "--since", "2026-05-19T11:00:00Z", "--until", "2026-05-19T10:00:00Z"}, wantStderr: "--until must be greater than or equal to --since"},
		{name: "replay without follow", args: []string{"logs", "--replay-since", "2026-05-19T10:00:00Z"}, wantStderr: "--replay-since requires --follow"},
		{name: "follow rejects limit", args: []string{"logs", "--follow", "--limit", "10"}, wantStderr: "--limit is not supported with --follow"},
		{name: "follow rejects since", args: []string{"logs", "--follow", "--since", "2026-05-19T10:00:00Z"}, wantStderr: "--since is not supported with --follow; use --replay-since"},
		{name: "follow rejects cursor", args: []string{"logs", "--follow", "--cursor", "cursor-1"}, wantStderr: "--cursor is not supported with --follow"},
		{name: "follow invalid replay since", args: []string{"logs", "--follow", "--replay-since", "not-time"}, wantStderr: "--replay-since must be an RFC3339 timestamp"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
			if calls.Load() != 0 {
				t.Fatalf("calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestLogsFailClosedWithoutTokenBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"logs": []any{}})
	}))
	defer server.Close()

	for _, args := range [][]string{
		{"logs"},
		{"logs", "--follow"},
	} {
		calls.Store(0)
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
		if code != 4 {
			t.Fatalf("args=%v code = %d, want 4 stdout=%s stderr=%s", args, code, stdout.String(), stderr.String())
		}
		if !strings.Contains(stderr.String(), "API token source is required") {
			t.Fatalf("stderr = %q, want token failure", stderr.String())
		}
		if calls.Load() != 0 {
			t.Fatalf("args=%v calls = %d, want 0", args, calls.Load())
		}
	}
}

func TestLogsMapRuntimeFailuresAndMalformedResults(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "http auth exits four",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			wantCode:   4,
			wantStderr: "rejected the request with status 401",
		},
		{
			name: "http runtime exits three",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			},
			wantCode:   3,
			wantStderr: "returned status 503",
		},
		{
			name: "unknown rpc error exits three",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeRuntimeLogJSONRPCError(t, w, req.ID, "METHOD_UNAVAILABLE")
			},
			wantCode:   3,
			wantStderr: "METHOD_UNAVAILABLE",
		},
		{
			name: "unauthorized rpc exits four",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeRuntimeLogJSONRPCError(t, w, req.ID, "UNAUTHORIZED")
			},
			wantCode:   4,
			wantStderr: "UNAUTHORIZED",
		},
		{
			name: "malformed list missing logs exits three",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "logs is required",
		},
		{
			name: "malformed log row exits three",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				log := validRuntimeLogEntry("log-1")
				delete(log, "log_id")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"logs": []any{log}})
			},
			wantCode:   3,
			wantStderr: "log_id is required",
		},
		{
			name: "malformed log row missing message exits three",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				log := validRuntimeLogEntry("log-1")
				delete(log, "message")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"logs": []any{log}})
			},
			wantCode:   3,
			wantStderr: "message is required",
		},
		{
			name: "malformed log row missing source exits three",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				log := validRuntimeLogEntry("log-1")
				delete(log, "source")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"logs": []any{log}})
			},
			wantCode:   3,
			wantStderr: "source is required",
		},
		{
			name: "malformed log row blank source exits three",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				log := validRuntimeLogEntry("log-1")
				log["source"] = " "
				writeJSONRPCResult(t, w, req.ID, map[string]any{"logs": []any{log}})
			},
			wantCode:   3,
			wantStderr: "source is required",
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
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestLogsFollowMalformedWSFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name               string
		subscriptionResult map[string]any
		logs               []map[string]any
		wantStderr         string
	}{
		{
			name:               "missing subscription id",
			subscriptionResult: map[string]any{},
			wantStderr:         "subscription_id is required",
		},
		{
			name: "malformed notification",
			logs: []map[string]any{
				func() map[string]any {
					log := validRuntimeLogEntry("log-1")
					delete(log, "ts")
					return log
				}(),
			},
			wantStderr: "ts is required",
		},
		{
			name: "malformed notification missing message",
			logs: []map[string]any{
				func() map[string]any {
					log := validRuntimeLogEntry("log-1")
					delete(log, "message")
					return log
				}(),
			},
			wantStderr: "message is required",
		},
		{
			name: "malformed notification blank source",
			logs: []map[string]any{
				func() map[string]any {
					log := validRuntimeLogEntry("log-1")
					log["source"] = " "
					return log
				}(),
			},
			wantStderr: "source is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server, _ := newRuntimeLogWSServer(t, runtimeLogWSServerOptions{
				subscriptionResult: tc.subscriptionResult,
				logs:               tc.logs,
				closeAfterRows:     true,
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"logs", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 3 {
				t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestLogsRenderMissingActionGracefully(t *testing.T) {
	message := "without action"
	log := runtimeLogEntry{
		LogID:     "log-no-action",
		TS:        "2026-05-19T10:00:01Z",
		Level:     "info",
		Component: "scheduler",
		Source:    "runtime",
		Message:   &message,
		Details:   map[string]any{"attempt": float64(1)},
	}

	var snapshot bytes.Buffer
	if err := writeRuntimeLogListResult(&snapshot, runtimeLogListResult{Logs: []runtimeLogEntry{log}}, nil); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	for _, want := range []string{"TIME", "scheduler", "runtime", "without action"} {
		if !strings.Contains(snapshot.String(), want) {
			t.Fatalf("snapshot missing %q:\n%s", want, snapshot.String())
		}
	}
	if strings.Contains(snapshot.String(), "ACTION") {
		t.Fatalf("snapshot = %q, want page-empty ACTION elided", snapshot.String())
	}

	var follow bytes.Buffer
	if _, err := writeRuntimeLogFollowEntry(&follow, log, ""); err != nil {
		t.Fatalf("write follow: %v", err)
	}
	if strings.Contains(follow.String(), "action=") {
		t.Fatalf("follow = %q, want no action field", follow.String())
	}
	if !strings.Contains(follow.String(), "source=runtime") {
		t.Fatalf("follow = %q, want source", follow.String())
	}
}

func TestRuntimeLogProjectionExactMessageAndResidualDetails(t *testing.T) {
	failureRaw, failureValue := runtimeLogTestFailure(t)
	exactMessage := "Event was published to the event bus"
	log := runtimeLogEntry{
		LogID:     "log-1",
		TS:        "2026-05-19T10:00:01Z",
		Level:     "info",
		Component: "eventbus",
		Source:    "agent-1",
		RunID:     "run-1",
		EntityID:  "entity-1",
		SessionID: "session-1",
		Failure:   failureRaw,
		Message:   &exactMessage,
		Details: map[string]any{
			"component":  "eventbus",
			"action":     "published",
			"agent_id":   "agent-1",
			"run_id":     "run-1",
			"entity_id":  "entity-1",
			"session_id": "session-1",
			"failure":    failureValue,
			"attempt":    float64(2),
		},
	}

	projection, err := projectRuntimeLogEntry("test", log)
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	if projection.Action != "published" || projection.MessageVisible {
		t.Fatalf("projection = %#v, want published action and suppressed exact message", projection)
	}
	if projection.Failure != "connector_failure/waiting" {
		t.Fatalf("failure = %q", projection.Failure)
	}
	if !reflect.DeepEqual(projection.ResidualDetails, map[string]any{"attempt": float64(2)}) {
		t.Fatalf("residual details = %#v", projection.ResidualDetails)
	}

	nearMessage := exactMessage + "."
	log.Message = &nearMessage
	projection, err = projectRuntimeLogEntry("test", log)
	if err != nil {
		t.Fatalf("project near match: %v", err)
	}
	if !projection.MessageVisible || projection.Message != nearMessage {
		t.Fatalf("near-match projection = %#v", projection)
	}
}

func TestRuntimeLogRedundantMessageTuplesMatchSpec(t *testing.T) {
	cli := loadCLISpecification(t)
	catalog := driftMappingValue(cli, "command_catalog")
	logs := driftMappingValue(catalog, "logs")
	output := driftMappingValue(logs, "output")
	projection := driftMappingValue(output, "semantic_projection")
	tuples := driftMappingValue(projection, "exact_redundant_message_tuples")
	if tuples == nil || tuples.Kind != yaml.SequenceNode {
		t.Fatal("logs semantic projection exact_redundant_message_tuples must be a sequence")
	}
	want := map[runtimeLogMessageTuple]struct{}{}
	for _, row := range tuples.Content {
		want[runtimeLogMessageTuple{
			Component: yamlScalar(t, row, "component"),
			Action:    yamlScalar(t, row, "action"),
			Message:   yamlScalar(t, row, "message"),
		}] = struct{}{}
	}
	if !reflect.DeepEqual(runtimeLogRedundantMessageTuples, want) {
		t.Fatalf("runtime-log redundant-message registry differs from authoritative spec:\ngot:  %#v\nwant: %#v", runtimeLogRedundantMessageTuples, want)
	}
}

func TestRuntimeLogProjectionPreservesMismatchedResidualEvidence(t *testing.T) {
	failureRaw, failureValue := runtimeLogTestFailure(t)
	message := "meaningful"
	base := runtimeLogEntry{
		LogID:     "log-1",
		TS:        "2026-05-19T10:00:01Z",
		Level:     "warn",
		Component: "runtime",
		Source:    "runtime",
		RunID:     "run-1",
		EntityID:  "entity-1",
		SessionID: "session-1",
		Failure:   failureRaw,
		Message:   &message,
	}

	for _, tc := range []struct {
		name  string
		key   string
		value any
	}{
		{name: "mismatched component", key: "component", value: "scheduler"},
		{name: "malformed component", key: "component", value: map[string]any{"unexpected": true}},
		{name: "malformed action", key: "action", value: map[string]any{"unexpected": true}},
		{name: "mismatched agent", key: "agent_id", value: "agent-2"},
		{name: "malformed agent", key: "agent_id", value: map[string]any{"unexpected": true}},
		{name: "mismatched run", key: "run_id", value: "run-2"},
		{name: "malformed run", key: "run_id", value: map[string]any{"unexpected": true}},
		{name: "mismatched entity", key: "entity_id", value: "entity-2"},
		{name: "malformed entity", key: "entity_id", value: map[string]any{"unexpected": true}},
		{name: "mismatched session", key: "session_id", value: "session-2"},
		{name: "malformed session", key: "session_id", value: map[string]any{"unexpected": true}},
		{name: "mismatched failure", key: "failure", value: map[string]any{"class": "different"}},
		{name: "malformed failure detail", key: "failure", value: "not-an-envelope"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := base
			log.Details = map[string]any{
				"component": "runtime", "action": "failed", "agent_id": "runtime", "run_id": "run-1",
				"entity_id": "entity-1", "session_id": "session-1", "failure": failureValue, "extra": "kept",
			}
			log.Details[tc.key] = tc.value
			projection, err := projectRuntimeLogEntry("test", log)
			if err != nil {
				t.Fatalf("project: %v", err)
			}
			if !reflect.DeepEqual(projection.ResidualDetails[tc.key], tc.value) || projection.ResidualDetails["extra"] != "kept" {
				t.Fatalf("residual details = %#v", projection.ResidualDetails)
			}
		})
	}
}

func TestRuntimeLogFailureEquivalencePreservesExactLargeNumbers(t *testing.T) {
	topFailure := runtimeLogDataLimitFailure(t, "9007199254740992")
	identicalFailure := runtimeLogDataLimitFailure(t, "9007199254740992")
	distinctFailure := runtimeLogDataLimitFailure(t, "9007199254740993")

	for _, tc := range []struct {
		name            string
		detailFailure   json.RawMessage
		wantResidual    bool
		wantLimitNumber string
	}{
		{name: "identical large number deduplicates", detailFailure: identicalFailure},
		{name: "distinct large number remains", detailFailure: distinctFailure, wantResidual: true, wantLimitNumber: "9007199254740993"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := fmt.Sprintf(`{
				"log_id":"log-large-number",
				"ts":"2026-05-19T10:00:01Z",
				"level":"error",
				"component":"runtime",
				"source":"runtime",
				"failure":%s,
				"message":"Data limit exceeded",
				"details":{"failure":%s}
			}`, topFailure, tc.detailFailure)
			var log runtimeLogEntry
			if err := json.Unmarshal([]byte(raw), &log); err != nil {
				t.Fatalf("decode runtime log: %v", err)
			}
			projection, err := projectRuntimeLogEntry("test", log)
			if err != nil {
				t.Fatalf("project runtime log: %v", err)
			}
			residualFailure, exists := projection.ResidualDetails["failure"]
			if exists != tc.wantResidual {
				t.Fatalf("residual failure exists=%t, want %t: %#v", exists, tc.wantResidual, projection.ResidualDetails)
			}
			if !tc.wantResidual {
				return
			}
			failureMap := residualFailure.(map[string]any)
			detail := failureMap["detail"].(map[string]any)
			attributes := detail["attributes"].(map[string]any)
			limit, ok := attributes["limit"].(json.Number)
			if !ok || limit.String() != tc.wantLimitNumber {
				t.Fatalf("residual limit = %#v, want exact json.Number(%s)", attributes["limit"], tc.wantLimitNumber)
			}
		})
	}
}

func TestRuntimeLogSnapshotProjectionCoversMixedRowsDatesAndFailures(t *testing.T) {
	failureRaw, _ := runtimeLogTestFailure(t)
	exactMessage := "Event was published to the event bus"
	meaningfulMessage := "Connector request failed"
	logs := []runtimeLogEntry{
		{LogID: "log-1", TS: "2026-05-19T23:59:59.123456Z", Level: "info", Component: "eventbus", Source: "runtime", Message: &exactMessage, Details: map[string]any{"action": "published"}},
		{LogID: "log-2", TS: "2026-05-20T00:00:00.987654Z", Level: "error", Component: "connector", Source: "runtime", Failure: failureRaw, Message: &meaningfulMessage, Details: map[string]any{"action": "request_failed"}},
	}

	var out bytes.Buffer
	if err := writeRuntimeLogListResult(&out, runtimeLogListResult{Logs: logs, NextCursor: "cursor-2"}, nil); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	text := out.String()
	for _, want := range []string{"DATE", "TIME", "ACTION", "FAILURE", "MESSAGE", "2026-05-19", "23:59:59.123", "2026-05-20", "00:00:00.987", "connector_failure/waiting", "Connector request failed", "Next cursor: cursor-2"} {
		if !strings.Contains(text, want) {
			t.Fatalf("snapshot missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, exactMessage) || strings.Contains(text, "\t") {
		t.Fatalf("snapshot contains suppressed message or literal tab:\n%s", text)
	}
	if strings.Index(text, "23:59:59.123") > strings.Index(text, "00:00:00.987") {
		t.Fatalf("snapshot reordered rows:\n%s", text)
	}
}

func TestRuntimeLogSnapshotElidesPageEmptyOptionalColumns(t *testing.T) {
	message := ""
	log := runtimeLogEntry{LogID: "log-1", TS: "2026-05-19T10:00:01Z", Level: "info", Component: "runtime", Source: "runtime", Message: &message}
	var out bytes.Buffer
	if err := writeRuntimeLogListResult(&out, runtimeLogListResult{Logs: []runtimeLogEntry{log}}, nil); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	header := strings.Split(out.String(), "\n")[0]
	if got, want := strings.Fields(header), []string{"TIME", "LEVEL", "COMPONENT", "SOURCE"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("header = %q", header)
	}
}

func TestRuntimeLogSnapshotConvertsSingleDayTimeToUTCWithoutDateColumn(t *testing.T) {
	message := "visible"
	log := runtimeLogEntry{LogID: "log-1", TS: "2026-05-19T07:00:01.234567-03:00", Level: "info", Component: "runtime", Source: "runtime", Message: &message}
	var out bytes.Buffer
	if err := writeRuntimeLogListResult(&out, runtimeLogListResult{Logs: []runtimeLogEntry{log}}, nil); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	text := out.String()
	header := strings.Split(text, "\n")[0]
	if strings.Contains(header, "DATE") || !strings.Contains(text, "10:00:01.234") || strings.Contains(text, "07:00:01.234") {
		t.Fatalf("single-day UTC projection =\n%s", text)
	}
}

func TestRuntimeLogEmptyStateDistinguishesFilters(t *testing.T) {
	var unfiltered bytes.Buffer
	if err := writeRuntimeLogListResult(&unfiltered, runtimeLogListResult{Logs: []runtimeLogEntry{}}, nil); err != nil {
		t.Fatalf("write unfiltered: %v", err)
	}
	if got := strings.TrimSpace(unfiltered.String()); got != "No runtime log entries have been recorded." {
		t.Fatalf("unfiltered = %q", got)
	}

	params := map[string]any{
		"run_id": "run-1", "entity_id": "entity-1", "session_id": "session-1",
		"component": "runtime", "level": "warn", "error_code": "waiting", "source": "agent-1",
		"since": "2026-05-19T10:00:00Z", "until": "2026-05-19T11:00:00Z",
		"limit": 10, "cursor": "ignored", "order": "asc",
	}
	var filtered bytes.Buffer
	if err := writeRuntimeLogListResult(&filtered, runtimeLogListResult{Logs: []runtimeLogEntry{}}, params); err != nil {
		t.Fatalf("write filtered: %v", err)
	}
	want := "No log entries match --run-id run-1 --entity-id entity-1 --session-id session-1 --component runtime --level warn --error-code waiting --source agent-1 --since 2026-05-19T10:00:00Z --until 2026-05-19T11:00:00Z. Remove filters or widen the time range."
	if got := strings.TrimSpace(filtered.String()); got != want {
		t.Fatalf("filtered = %q, want %q", got, want)
	}
}

func TestRuntimeLogFollowProjectionDeduplicatesDetailsAndSeparatesDates(t *testing.T) {
	failureRaw, failureValue := runtimeLogTestFailure(t)
	message := "Connector request failed"
	log := runtimeLogEntry{
		LogID: "log-1", TS: "2026-05-19T23:59:59.123Z", Level: "error", Component: "connector", Source: "agent-1",
		RunID: "run-1", EntityID: "entity-1", SessionID: "session-1", Failure: failureRaw, Message: &message,
		Details: map[string]any{
			"component": "connector", "action": "request_failed", "agent_id": "agent-1", "run_id": "run-1",
			"entity_id": "entity-1", "session_id": "session-1", "failure": failureValue, "attempt": float64(2),
		},
	}
	var out bytes.Buffer
	lastDate, err := writeRuntimeLogFollowEntry(&out, log, "")
	if err != nil {
		t.Fatalf("write first follow row: %v", err)
	}
	log.LogID = "log-2"
	log.TS = "2026-05-20T00:00:00.456Z"
	if _, err := writeRuntimeLogFollowEntry(&out, log, lastDate); err != nil {
		t.Fatalf("write second follow row: %v", err)
	}
	text := out.String()
	for _, want := range []string{"--- 2026-05-19 UTC ---", "--- 2026-05-20 UTC ---", "action=request_failed", "run_id=run-1", "failure=connector_failure/waiting", "message=Connector request failed", "details={\"attempt\":2}"} {
		if !strings.Contains(text, want) {
			t.Fatalf("follow missing %q:\n%s", want, text)
		}
	}
	if strings.Count(text, "run_id=run-1") != 2 || strings.Contains(text, "\"run_id\"") || strings.Contains(text, "\"failure\"") || strings.Contains(text, "\"action\"") {
		t.Fatalf("follow retained projected duplicates:\n%s", text)
	}
}

func TestLogsMalformedFailureEvidenceIsVisibleAndFailsClosed(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		log := validRuntimeLogEntry("log-invalid-failure")
		log["failure"] = map[string]any{"schema_version": "platform.failure/v1", "class": "platform.unknown"}
		writeJSONRPCResult(t, w, req.ID, map[string]any{"logs": []any{log}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"logs"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 3 {
		t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	for _, want := range []string{"WARNING:", "log-invalid-failure", "platform.failure/v1", "platform.unknown"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q: %s", want, stderr.String())
		}
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("stdout = %q, want no successful table", stdout.String())
	}
}

func TestLogsFollowMalformedFailureEvidenceIsVisibleAndFailsClosed(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	log := validRuntimeLogEntry("log-invalid-follow-failure")
	log["failure"] = map[string]any{"schema_version": "platform.failure/v1", "class": "platform.unknown"}
	server, _ := newRuntimeLogWSServer(t, runtimeLogWSServerOptions{
		logs:           []map[string]any{log},
		closeAfterRows: true,
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"logs", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 3 {
		t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	for _, want := range []string{"WARNING:", "log-invalid-follow-failure", "platform.failure/v1", "platform.unknown"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q: %s", want, stderr.String())
		}
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("stdout = %q, want no successful log line", stdout.String())
	}
}

func TestLogsFollowMapsHandshakeAuthToAuthExit(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var rpcCalls atomic.Int32
	var wsCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/rpc":
			rpcCalls.Add(1)
			t.Errorf("unexpected RPC request for log follow")
			http.Error(w, "unexpected rpc", http.StatusInternalServerError)
		case "/v1/ws":
			wsCalls.Add(1)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"logs", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 4 {
		t.Fatalf("code = %d, want 4 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if rpcCalls.Load() != 0 {
		t.Fatalf("rpc calls = %d, want 0", rpcCalls.Load())
	}
	if wsCalls.Load() != 1 {
		t.Fatalf("ws calls = %d, want 1", wsCalls.Load())
	}
	if !strings.Contains(stderr.String(), "rejected the request with status 401") {
		t.Fatalf("stderr = %q, want WS auth status", stderr.String())
	}
}

func TestLogsFollowCancellationReturnsInterrupted(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	ctx, cancel := context.WithCancel(context.Background())
	server, _ := newRuntimeLogWSServer(t, runtimeLogWSServerOptions{
		afterSubscription: cancel,
	})
	defer server.Close()

	defer cancel()
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"logs", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 130 {
		t.Fatalf("code = %d, want 130 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "detached from runtime log stream") {
		t.Fatalf("stderr = %q, want detach message", stderr.String())
	}
}

type runtimeLogWSServerOptions struct {
	subscriptionResult map[string]any
	logs               []map[string]any
	closeAfterRows     bool
	afterSubscription  func()
}

func newRuntimeLogWSServer(t *testing.T, opts runtimeLogWSServerOptions) (*httptest.Server, *[]jsonRPCRequest) {
	t.Helper()
	var mu sync.Mutex
	wsRequests := []jsonRPCRequest{}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/rpc":
			t.Fatalf("unexpected RPC request for log follow")
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
			mu.Unlock()
			result := opts.subscriptionResult
			if result == nil {
				result = map[string]any{"subscription_id": "sub-logs"}
			}
			if err := conn.WriteJSON(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  result,
			}); err != nil {
				t.Errorf("write ws subscription response: %v", err)
				return
			}
			if opts.afterSubscription != nil {
				opts.afterSubscription()
			}
			for _, log := range opts.logs {
				if err := conn.WriteJSON(map[string]any{
					"jsonrpc": "2.0",
					"method":  "rpc.subscription",
					"params": map[string]any{
						"subscription": "sub-logs",
						"result":       log,
					},
				}); err != nil {
					return
				}
			}
			if opts.closeAfterRows {
				_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}
			<-r.Context().Done()
		default:
			t.Errorf("path = %q, want /v1/ws", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	return server, &wsRequests
}

func validRuntimeLogEntry(logID string) map[string]any {
	return map[string]any{
		"log_id":     logID,
		"ts":         "2026-05-19T10:00:01Z",
		"level":      "info",
		"component":  "scheduler",
		"source":     "runtime",
		"run_id":     "run-1",
		"entity_id":  "entity-1",
		"session_id": "session-1",
		"error_code": "DELIVERY_FAILED",
		"message":    "log message",
		"details": map[string]any{
			"attempt": 2,
			"action":  "delivery_failed",
		},
	}
}

func runtimeLogTestFailure(t *testing.T) (json.RawMessage, map[string]any) {
	t.Helper()
	failure := runtimefailures.Normalize(
		runtimefailures.New(runtimefailures.ClassConnectorFailure, "waiting", "connector", "wait", nil),
		"connector",
		"wait",
	)
	raw, err := runtimefailures.MarshalEnvelope(failure)
	if err != nil {
		t.Fatalf("marshal test failure: %v", err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode test failure: %v", err)
	}
	return raw, value
}

func runtimeLogDataLimitFailure(t *testing.T, limit string) json.RawMessage {
	t.Helper()
	failure := runtimefailures.Normalize(
		runtimefailures.New(
			runtimefailures.ClassDataLimitExceeded,
			"limit",
			"runtime",
			"read",
			map[string]any{
				"limit_kind": "bytes",
				"limit":      json.Number(limit),
				"actual":     json.Number(limit),
			},
		),
		"runtime",
		"read",
	)
	raw, err := runtimefailures.MarshalEnvelope(failure)
	if err != nil {
		t.Fatalf("marshal data-limit failure: %v", err)
	}
	return raw
}

func writeRuntimeLogJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
	t.Helper()
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32000,
			"message": code,
			"data": map[string]any{
				"code": code,
			},
		},
	}); err != nil {
		t.Fatalf("encode error response: %v", err)
	}
}
