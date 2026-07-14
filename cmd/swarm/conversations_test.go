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

func TestConversationTurnIdentifierResolutionUsesCanonicalSessionScopedPages(t *testing.T) {
	for _, tc := range []struct {
		name        string
		input       string
		wantCode    int
		wantMethods string
		wantStderr  string
	}{
		{name: "exact", input: "turn-alpha", wantCode: cliExitOK, wantMethods: "conversation.get_turn"},
		{name: "unique prefix across pages", input: "turn-al", wantCode: cliExitOK, wantMethods: "conversation.get_turn,conversation.list_turns,conversation.list_turns,conversation.get_turn"},
		{name: "no match", input: "turn-no", wantCode: cliExitValidation, wantMethods: "conversation.get_turn,conversation.list_turns", wantStderr: `no turn ID matches "turn-no"`},
		{name: "ambiguous", input: "turn-al", wantCode: cliExitValidation, wantMethods: "conversation.get_turn,conversation.list_turns", wantStderr: `turn ID prefix "turn-al" is ambiguous`},
		{name: "repeated cursor", input: "turn-al", wantCode: cliExitRuntime, wantMethods: "conversation.get_turn,conversation.list_turns,conversation.list_turns", wantStderr: `repeated next_cursor "same-page"`},
		{name: "list failure", input: "turn-al", wantCode: cliExitRuntime, wantMethods: "conversation.get_turn,conversation.list_turns", wantStderr: "RUNTIME_UNAVAILABLE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			methods := []string{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Fatalf("decode request: %v", err)
				}
				methods = append(methods, request.Method)
				switch request.Method {
				case conversationGetTurnMethod:
					turnID, _ := request.Params["turn_id"].(string)
					if turnID != "turn-alpha" {
						writeIdentifierRPCError(t, w, request.ID, "TURN_NOT_FOUND")
						return
					}
					result := validConversationTurnDetail("sess-1", 1)
					result["turn"].(map[string]any)["turn_id"] = turnID
					writeJSONRPCResult(t, w, request.ID, result)
				case conversationListTurnsMethod:
					if request.Params["session_id"] != "sess-1" || request.Params["limit"] != float64(500) {
						t.Fatalf("conversation.list_turns params = %#v", request.Params)
					}
					page := map[string]any{"conversation": validConversationSummary("sess-1"), "turns": []map[string]any{}}
					switch tc.name {
					case "unique prefix across pages":
						if request.Params["cursor"] == nil {
							page["turns"] = []map[string]any{validConversationTurn(2, "turn-beta", "event-2", "task.progress")}
							page["next_cursor"] = "page-2"
						} else {
							page["turns"] = []map[string]any{validConversationTurn(1, "turn-alpha", "event-1", "task.started")}
						}
					case "ambiguous":
						page["turns"] = []map[string]any{
							validConversationTurn(1, "turn-alpha", "event-1", "task.started"),
							validConversationTurn(2, "turn-alpine", "event-2", "task.progress"),
						}
					case "repeated cursor":
						page["turns"] = []map[string]any{validConversationTurn(1, "turn-beta", "event-1", "task.started")}
						page["next_cursor"] = "same-page"
					case "list failure":
						writeIdentifierRPCError(t, w, request.ID, "RUNTIME_UNAVAILABLE")
						return
					}
					writeJSONRPCResult(t, w, request.ID, page)
				default:
					t.Fatalf("unexpected method %q", request.Method)
				}
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"conversation", "turn", "sess-1", tc.input}, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if got := strings.Join(methods, ","); got != tc.wantMethods {
				t.Fatalf("methods = %s, want %s", got, tc.wantMethods)
			}
			if tc.wantCode == cliExitOK && !strings.Contains(stdout.String(), "turn-alpha") {
				t.Fatalf("stdout = %q, want canonical turn id", stdout.String())
			}
			if tc.wantStderr != "" && !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.wantStderr)
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
		{name: "turn missing id", args: []string{"conversation", "turn", "sess-1"}, wantStderr: "requires <turn-id-or-prefix> (got <session-id>)"},
		{name: "turn invalid session", args: []string{"conversation", "turn", "bad id!", "turn-1"}, wantStderr: "session id must match OpaqueId pattern"},
		{name: "turn blank id", args: []string{"conversation", "turn", "sess-1", " "}, wantStderr: "turn id or prefix is required"},
		{name: "turn invalid id", args: []string{"conversation", "turn", "sess-1", "bad id!"}, wantStderr: "turn id or prefix must match OpaqueId pattern"},
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
		{"conversation", "turn", "sess-1", "turn-1"},
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
		"ordinal":                  index,
		"turn_id":                  turnID,
		"completed_at":             "2026-05-20T01:01:01Z",
		"duration_ms":              150,
		"trigger_event_id":         eventID,
		"trigger_event_type":       eventType,
		"parse_ok":                 true,
		"activity":                 []map[string]any{{"kind": "output", "text": "done"}},
		"assistant_visible_output": "done",
	}
}

func validConversationTurnDetail(sessionID string, index int) map[string]any {
	return map[string]any{
		"session": validConversationSummary(sessionID),
		"turn":    validConversationTurn(index, "turn-2", "event-2", "task.completed"),
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
