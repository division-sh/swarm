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

func TestConversationsListUsesConversationListV1RPCWithFilters(t *testing.T) {
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
			"conversations": []map[string]any{validConversationSummary("sess-1")},
			"next_cursor":   "conversation-cursor-2",
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"conversation", "list",
		"--agent-id", "agent-1",
		"--run-id", "run-1",
		"--limit", "25",
		"--cursor", "cursor-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != conversationListMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", captured.JSONRPC, captured.Method, conversationListMethod)
	}
	wantParams := map[string]any{
		"agent_id": "agent-1",
		"run_id":   "run-1",
		"limit":    float64(25),
		"cursor":   "cursor-1",
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{"SESSION_ID", "sess-1", "agent-1", "run-1", "active", "next_cursor=conversation-cursor-2"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestConversationsListEmptyResultOmitsUnsetParams(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{"conversations": []any{}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"conversation", "list"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != conversationListMethod {
		t.Fatalf("method = %q, want %s", captured.Method, conversationListMethod)
	}
	if len(captured.Params) != 0 {
		t.Fatalf("params = %#v, want empty", captured.Params)
	}
	if !strings.Contains(stdout.String(), "No conversations match the current filters.") {
		t.Fatalf("stdout = %q, want empty-state text", stdout.String())
	}
}

func TestConversationViewUsesConversationGetAndRendersTurns(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validConversationDetail("sess-1"))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"conversation", "view", "sess-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != conversationGetMethod {
		t.Fatalf("method = %q, want %s", captured.Method, conversationGetMethod)
	}
	if !reflect.DeepEqual(captured.Params, map[string]any{"session_id": "sess-1"}) {
		t.Fatalf("params = %#v", captured.Params)
	}
	for _, want := range []string{
		"Conversation sess-1",
		"agent_id=agent-1 run_id=run-1 status=active turns=1 messages=2",
		"TURN  TURN_ID",
		"1     turn-1",
		"task.started  true      150",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestConversationTurnUsesConversationGetTurnAndRendersDeepTurn(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validConversationTurnDetail("sess-1", 2))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"conversation", "turn", "sess-1", "2"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != conversationGetTurnMethod {
		t.Fatalf("method = %q, want %s", captured.Method, conversationGetTurnMethod)
	}
	wantParams := map[string]any{"session_id": "sess-1", "turn_index": float64(2)}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{
		"Conversation sess-1 turn 2",
		"turn_id=turn-2",
		"dispatch.trigger_event_id=event-2 dispatch.trigger_event_type=task.completed",
		"advertised_tools=emit_done,read_state runtime_log_entries=1",
		"assistant_visible_output=done",
		"log_id=log-1",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestConversationTurnRuntimeLogProjection(t *testing.T) {
	_, failureValue := runtimeLogTestFailure(t)

	for _, tc := range []struct {
		name      string
		configure func(map[string]any)
		want      []string
		doNotWant []string
	}{
		{
			name: "exact redundant message keeps action visible",
			configure: func(log map[string]any) {
				log["component"] = "eventbus"
				log["message"] = "Event was published to the event bus"
				log["details"] = map[string]any{"action": "published"}
			},
			want:      []string{"component=eventbus action=published source=runtime"},
			doNotWant: []string{"message=Event was published to the event bus"},
		},
		{
			name: "near match remains visible",
			configure: func(log map[string]any) {
				log["component"] = "eventbus"
				log["message"] = "Event was published to the event bus."
				log["details"] = map[string]any{"action": "published"}
			},
			want: []string{"component=eventbus action=published source=runtime", "message=Event was published to the event bus."},
		},
		{
			name: "valid failure uses canonical class and detail",
			configure: func(log map[string]any) {
				log["component"] = "connector"
				log["message"] = "Connector request failed"
				log["failure"] = failureValue
				log["details"] = map[string]any{"action": "request_failed", "failure": failureValue}
			},
			want: []string{"component=connector action=request_failed source=runtime failure=connector_failure/waiting message=Connector request failed"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validConversationTurnDetail("sess-1", 2)
				turn := result["turn"].(map[string]any)
				log := turn["runtime_log_entries"].([]map[string]any)[0]
				tc.configure(log)
				writeJSONRPCResult(t, w, req.ID, result)
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"conversation", "turn", "sess-1", "2"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			for _, want := range tc.want {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
				}
			}
			for _, unwanted := range tc.doNotWant {
				if strings.Contains(stdout.String(), unwanted) {
					t.Fatalf("stdout contains %q:\n%s", unwanted, stdout.String())
				}
			}
		})
	}
}

func TestConversationTurnMalformedRuntimeLogFailureIsVisibleAndFailsClosed(t *testing.T) {
	for _, args := range [][]string{
		{"conversation", "turn", "sess-1", "2"},
		{"conversation", "turn", "sess-1", "2", "--json"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validConversationTurnDetail("sess-1", 2)
				turn := result["turn"].(map[string]any)
				log := turn["runtime_log_entries"].([]map[string]any)[0]
				log["failure"] = map[string]any{"schema_version": "platform.failure/v1", "class": "platform.unknown"}
				writeJSONRPCResult(t, w, req.ID, result)
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 3 {
				t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			for _, want := range []string{"WARNING:", "log-1", "platform.failure/v1", "platform.unknown"} {
				if !strings.Contains(stderr.String(), want) {
					t.Fatalf("stderr missing %q: %s", want, stderr.String())
				}
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want no successful document", stdout.String())
			}
		})
	}
}

func TestConversationCommandsRejectInvalidInputBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"conversations": []any{}})
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "list invalid limit low", args: []string{"conversation", "list", "--limit", "0"}, wantStderr: "--limit must be between 1 and 500"},
		{name: "list blank agent id", args: []string{"conversation", "list", "--agent-id", " "}, wantStderr: "--agent-id must not be empty"},
		{name: "list invalid run id", args: []string{"conversation", "list", "--run-id", "bad id!"}, wantStderr: "--run-id must match OpaqueId pattern"},
		{name: "list blank cursor", args: []string{"conversation", "list", "--cursor", " "}, wantStderr: "--cursor must not be empty"},
		{name: "view missing session", args: []string{"conversation", "view"}, wantStderr: "requires <session-id>"},
		{name: "view blank session", args: []string{"conversation", "view", " "}, wantStderr: "session id is required"},
		{name: "view invalid session", args: []string{"conversation", "view", "bad id!"}, wantStderr: "session id must match OpaqueId pattern"},
		{name: "turn missing index", args: []string{"conversation", "turn", "sess-1"}, wantStderr: "requires <turn-index> (got <session-id>)"},
		{name: "turn invalid session", args: []string{"conversation", "turn", "bad id!", "1"}, wantStderr: "session id must match OpaqueId pattern"},
		{name: "turn index zero", args: []string{"conversation", "turn", "sess-1", "0"}, wantStderr: "turn index must be an integer from 1 to 1000000"},
		{name: "turn index not integer", args: []string{"conversation", "turn", "sess-1", "first"}, wantStderr: "turn index must be an integer from 1 to 1000000"},
		{name: "turn index too high", args: []string{"conversation", "turn", "sess-1", "1000001"}, wantStderr: "turn index must be an integer from 1 to 1000000"},
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
				t.Fatalf("RPC calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestConversationCommandsFailClosedWithoutTokenBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"conversations": []any{}})
	}))
	defer server.Close()

	for _, args := range [][]string{
		{"conversation", "list"},
		{"conversation", "view", "sess-1"},
		{"conversation", "turn", "sess-1", "1"},
	} {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
		if code != 4 {
			t.Fatalf("%v code = %d, want 4 stdout=%s stderr=%s", args, code, stdout.String(), stderr.String())
		}
		if !strings.Contains(stderr.String(), "API token source is required") {
			t.Fatalf("%v stderr = %q, want token failure", args, stderr.String())
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("RPC calls = %d, want 0", calls.Load())
	}
}

func TestConversationCommandsMapRuntimeFailuresAndMalformedResults(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "list http auth exits four",
			args: []string{"conversation", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			wantCode:   4,
			wantStderr: "rejected the request with status 401",
		},
		{
			name: "list missing conversations exits three",
			args: []string{"conversation", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "conversations is required",
		},
		{
			name: "list malformed summary exits three",
			args: []string{"conversation", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				item := validConversationSummary("sess-1")
				delete(item, "status")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"conversations": []map[string]any{item}})
			},
			wantCode:   3,
			wantStderr: "status is required",
		},
		{
			name: "view session not found exits five",
			args: []string{"conversation", "view", "missing"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeConversationJSONRPCError(t, w, req.ID, "SESSION_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "SESSION_NOT_FOUND",
		},
		{
			name: "view missing turns exits three",
			args: []string{"conversation", "view", "sess-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validConversationDetail("sess-1")
				delete(result, "turns")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "turns is required",
		},
		{
			name: "turn missing turn exits five",
			args: []string{"conversation", "turn", "sess-1", "99"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeConversationJSONRPCError(t, w, req.ID, "TURN_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "TURN_NOT_FOUND",
		},
		{
			name: "turn missing advertised tools exits three",
			args: []string{"conversation", "turn", "sess-1", "1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validConversationTurnDetail("sess-1", 1)
				turn := result["turn"].(map[string]any)
				delete(turn, "advertised_tools")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "advertised_tools is required",
		},
		{
			name: "turn negative retry count exits three",
			args: []string{"conversation", "turn", "sess-1", "1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validConversationTurnDetail("sess-1", 1)
				turn := result["turn"].(map[string]any)
				turn["retry_count"] = -1
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "retry_count must be non-negative",
		},
		{
			name: "turn unknown rpc exits three",
			args: []string{"conversation", "turn", "sess-1", "1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeConversationJSONRPCError(t, w, req.ID, "METHOD_UNAVAILABLE")
			},
			wantCode:   3,
			wantStderr: "METHOD_UNAVAILABLE",
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

func validConversationSummary(sessionID string) map[string]any {
	return map[string]any{
		"session_id":    sessionID,
		"agent_id":      "agent-1",
		"run_id":        "run-1",
		"started_at":    "2026-05-20T01:00:00Z",
		"ended_at":      "2026-05-20T01:10:00Z",
		"turn_count":    1,
		"message_count": 2,
		"status":        "active",
	}
}

func validConversationDetail(sessionID string) map[string]any {
	return map[string]any{
		"conversation": validConversationSummary(sessionID),
		"turns": []map[string]any{
			validConversationTurn(1, "turn-1", "event-1", "task.started"),
		},
	}
}

func validConversationTurn(index int, turnID, eventID, eventType string) map[string]any {
	return map[string]any{
		"turn_index":         index,
		"turn_id":            turnID,
		"trigger_event_id":   eventID,
		"trigger_event_type": eventType,
		"parse_ok":           true,
		"latency_ms":         150,
		"request_payload":    map[string]any{"prompt": "go"},
		"response_payload":   map[string]any{"ok": true},
		"tool_calls":         []map[string]any{},
		"turn_blocks":        []map[string]any{{"kind": "assistant", "text": "done"}},
	}
}

func validConversationTurnDetail(sessionID string, index int) map[string]any {
	return map[string]any{
		"session": validConversationSummary(sessionID),
		"turn": map[string]any{
			"turn_index":   index,
			"turn_id":      "turn-2",
			"scope":        "global",
			"started_at":   "2026-05-20T01:01:00Z",
			"completed_at": "2026-05-20T01:01:01Z",
			"duration_ms":  1000,
			"outcome":      "completed",
			"parse_ok":     true,
			"retry_count":  0,
			"dispatch_metadata": map[string]any{
				"trigger_event_id":   "event-2",
				"trigger_event_type": "task.completed",
				"entity_id":          "entity-1",
				"task_id":            "task-1",
				"run_id":             "run-1",
			},
			"advertised_tools":                []any{"emit_done", "read_state"},
			"mcp_tools_listed":                []any{},
			"mcp_tools_visible":               []any{},
			"reasoning_blocks":                []any{},
			"progress_updates":                []any{},
			"tool_calls":                      []map[string]any{},
			"tool_results":                    []map[string]any{},
			"emitted_events":                  []any{"event-3"},
			"runtime_log_entries":             []map[string]any{validConversationRuntimeLog()},
			"provider_metadata":               map[string]any{"latency_ms": 1000},
			"request_payload":                 map[string]any{"prompt": "go"},
			"response_payload":                map[string]any{"ok": true},
			"full_prompt_context":             nil,
			"full_prompt_context_v2_reserved": true,
			"raw_llm_response":                nil,
			"raw_llm_response_v2_reserved":    true,
			"assistant_visible_output":        "done",
		},
		"turn_blocks_raw": []map[string]any{{"kind": "assistant", "text": "done"}},
	}
}

func validConversationRuntimeLog() map[string]any {
	return map[string]any{
		"log_id":     "log-1",
		"ts":         "2026-05-20T01:01:01Z",
		"level":      "info",
		"component":  "agent",
		"source":     "runtime",
		"run_id":     "run-1",
		"entity_id":  "entity-1",
		"session_id": "sess-1",
		"message":    "turn completed",
		"details":    map[string]any{"turn": 2},
	}
}

func writeConversationJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
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
