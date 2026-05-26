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

func TestForkChatNewCreatesForkAndOptionalChatViaCanonicalRPC(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var requests []jsonRPCRequest
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
		requests = append(requests, req)
		switch req.Method {
		case forkChatCreateMethod:
			writeJSONRPCResult(t, w, req.ID, validForkChatCreateResult("fork-1"))
		case forkChatChatMethod:
			writeJSONRPCResult(t, w, req.ID, validForkChatChatResult("fork-1", "fork-turn-1"))
		default:
			t.Errorf("unexpected method %q", req.Method)
			writeJSONRPCResult(t, w, req.ID, map[string]any{})
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"forkchat", "new", "sess-1",
		"--turn-index", "2",
		"--message", "inspect fork",
		"--idempotency-key", "idem-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(requests))
	}
	if requests[0].Method != forkChatCreateMethod || requests[1].Method != forkChatChatMethod {
		t.Fatalf("methods = %q/%q, want %q/%q", requests[0].Method, requests[1].Method, forkChatCreateMethod, forkChatChatMethod)
	}
	wantCreateParams := map[string]any{
		"source_session_id": "sess-1",
		"fork_point": map[string]any{
			"kind":       "turn",
			"turn_index": float64(2),
		},
		"idempotency_key": "idem-1",
	}
	if !reflect.DeepEqual(requests[0].Params, wantCreateParams) {
		t.Fatalf("create params = %#v, want %#v", requests[0].Params, wantCreateParams)
	}
	wantChatParams := map[string]any{
		"fork_id":         "fork-1",
		"message":         "inspect fork",
		"idempotency_key": "idem-1",
	}
	if !reflect.DeepEqual(requests[1].Params, wantChatParams) {
		t.Fatalf("chat params = %#v, want %#v", requests[1].Params, wantChatParams)
	}
	for _, want := range []string{"Fork fork-1", "Initial chat:", "snapshot_owner=conversation.fork_chat.snapshot.v1", "assistant=sandbox answer"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestForkChatNewSelectorMappings(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	for _, tc := range []struct {
		name          string
		args          []string
		wantForkPoint map[string]any
	}{
		{
			name:          "event",
			args:          []string{"forkchat", "new", "sess-1", "--event-id", "event-1"},
			wantForkPoint: map[string]any{"kind": "event", "event_id": "event-1"},
		},
		{
			name:          "time",
			args:          []string{"forkchat", "new", "sess-1", "--at", "2026-05-25T12:00:00+01:00"},
			wantForkPoint: map[string]any{"kind": "time", "at": "2026-05-25T11:00:00Z"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var captured jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
					t.Errorf("decode request: %v", err)
				}
				writeJSONRPCResult(t, w, captured.ID, validForkChatCreateResult("fork-1"))
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if captured.Method != forkChatCreateMethod {
				t.Fatalf("method = %q, want %s", captured.Method, forkChatCreateMethod)
			}
			wantParams := map[string]any{"source_session_id": "sess-1", "fork_point": tc.wantForkPoint}
			if !reflect.DeepEqual(captured.Params, wantParams) {
				t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
			}
		})
	}
}

func TestForkChatResumeJSONUsesConversationForkChat(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validForkChatChatResult("fork-1", "fork-turn-2"))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"forkchat", "resume", "fork-1",
		"--message", "continue",
		"--idempotency-key", "idem-chat",
		"--json",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != forkChatChatMethod {
		t.Fatalf("method = %q, want %s", captured.Method, forkChatChatMethod)
	}
	wantParams := map[string]any{"fork_id": "fork-1", "message": "continue", "idempotency_key": "idem-chat"}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	var result map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode stdout json: %v\n%s", err, stdout.String())
	}
	if result["fork_id"] != "fork-1" {
		t.Fatalf("json fork_id = %#v, want fork-1", result["fork_id"])
	}
}

func TestForkChatListViewDeleteUseCanonicalMethods(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var requests []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		requests = append(requests, req)
		switch req.Method {
		case forkChatListMethod:
			writeJSONRPCResult(t, w, req.ID, map[string]any{
				"forks":       []map[string]any{validForkChatSession("fork-1")},
				"next_cursor": "fork-cursor-2",
			})
		case forkChatViewMethod:
			writeJSONRPCResult(t, w, req.ID, validForkChatSession("fork-1"))
		case forkChatDeleteMethod:
			writeJSONRPCResult(t, w, req.ID, validForkChatDeleteResult("fork-1", false))
		default:
			t.Errorf("unexpected method %q", req.Method)
			writeJSONRPCResult(t, w, req.ID, map[string]any{})
		}
	}))
	defer server.Close()

	commands := []struct {
		args       []string
		wantStdout string
	}{
		{args: []string{"forkchat", "list", "--source-session-id", "sess-1", "--limit", "25", "--cursor", "cursor-1", "--quiet"}, wantStdout: "fork-1"},
		{args: []string{"forkchat", "view", "fork-1"}, wantStdout: "Fork fork-1"},
		{args: []string{"forkchat", "delete", "fork-1", "--idempotency-key", "idem-delete", "--quiet"}, wantStdout: "fork-1"},
	}
	for _, command := range commands {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), command.args, &stdout, &stderr, testRootCommandOptions(server))
		if code != 0 {
			t.Fatalf("args %v code = %d stderr=%s stdout=%s", command.args, code, stderr.String(), stdout.String())
		}
		if !strings.Contains(stdout.String(), command.wantStdout) {
			t.Fatalf("args %v stdout = %q, want substring %q", command.args, stdout.String(), command.wantStdout)
		}
	}
	if len(requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(requests))
	}
	assertForkChatRequest(t, requests[0], forkChatListMethod, map[string]any{
		"source_session_id": "sess-1",
		"limit":             float64(25),
		"cursor":            "cursor-1",
	})
	assertForkChatRequest(t, requests[1], forkChatViewMethod, map[string]any{"fork_id": "fork-1"})
	assertForkChatRequest(t, requests[2], forkChatDeleteMethod, map[string]any{"fork_id": "fork-1", "idempotency_key": "idem-delete"})
}

func TestForkChatCommandsRejectInvalidInputBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", validForkChatCreateResult("fork-1"))
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "new missing selector", args: []string{"forkchat", "new", "sess-1"}, wantStderr: "exactly one fork point selector is required"},
		{name: "new multiple selectors", args: []string{"forkchat", "new", "sess-1", "--turn-index", "1", "--event-id", "event-1"}, wantStderr: "exactly one fork point selector is required"},
		{name: "new turn index low", args: []string{"forkchat", "new", "sess-1", "--turn-index", "0"}, wantStderr: "--turn-index must be an integer from 1 to 1000000"},
		{name: "new blank event id", args: []string{"forkchat", "new", "sess-1", "--event-id", " "}, wantStderr: "--event-id must be non-empty"},
		{name: "new invalid at", args: []string{"forkchat", "new", "sess-1", "--at", "tomorrow"}, wantStderr: "--at must be an RFC3339 timestamp"},
		{name: "new blank message", args: []string{"forkchat", "new", "sess-1", "--turn-index", "1", "--message", " "}, wantStderr: "--message must be non-empty"},
		{name: "resume missing message", args: []string{"forkchat", "resume", "fork-1"}, wantStderr: "--message is required"},
		{name: "resume invalid fork id", args: []string{"forkchat", "resume", "bad id!", "--message", "hello"}, wantStderr: "fork id must match OpaqueId pattern"},
		{name: "list invalid limit", args: []string{"forkchat", "list", "--limit", "0"}, wantStderr: "--limit must be between 1 and 500"},
		{name: "list blank cursor", args: []string{"forkchat", "list", "--cursor", " "}, wantStderr: "--cursor must be non-empty"},
		{name: "view invalid fork id", args: []string{"forkchat", "view", "bad id!"}, wantStderr: "fork id must match OpaqueId pattern"},
		{name: "delete blank idempotency key", args: []string{"forkchat", "delete", "fork-1", "--idempotency-key", " "}, wantStderr: "--idempotency-key must be non-empty"},
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

func TestForkChatAPIErrorAndMalformedResultHandling(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	for _, tc := range []struct {
		name       string
		args       []string
		handler    func(t *testing.T, w http.ResponseWriter, req jsonRPCRequest)
		wantCode   int
		wantStderr string
	}{
		{
			name: "not found",
			args: []string{"forkchat", "view", "fork-missing"},
			handler: func(t *testing.T, w http.ResponseWriter, req jsonRPCRequest) {
				writeConversationJSONRPCError(t, w, req.ID, "FORK_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "FORK_NOT_FOUND",
		},
		{
			name: "idempotency conflict",
			args: []string{"forkchat", "delete", "fork-1", "--idempotency-key", "conflict"},
			handler: func(t *testing.T, w http.ResponseWriter, req jsonRPCRequest) {
				writeConversationJSONRPCError(t, w, req.ID, "IDEMPOTENCY_CONFLICT")
			},
			wantCode:   6,
			wantStderr: "IDEMPOTENCY_CONFLICT",
		},
		{
			name: "malformed list",
			args: []string{"forkchat", "list"},
			handler: func(t *testing.T, w http.ResponseWriter, req jsonRPCRequest) {
				writeJSONRPCResult(t, w, req.ID, map[string]any{"next_cursor": "cursor-2"})
			},
			wantCode:   3,
			wantStderr: "forks is required",
		},
		{
			name: "malformed delete ok false",
			args: []string{"forkchat", "delete", "fork-1"},
			handler: func(t *testing.T, w http.ResponseWriter, req jsonRPCRequest) {
				result := validForkChatDeleteResult("fork-1", false)
				result["ok"] = false
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "ok must be true",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("decode request: %v", err)
				}
				tc.handler(t, w, req)
			}))
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

func TestForkChatRejectsMalformedSnapshotIdentityFields(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	for _, tc := range []struct {
		name       string
		mutate     func(map[string]any)
		wantStderr string
	}{
		{
			name: "fork id",
			mutate: func(result map[string]any) {
				result["snapshot"].(map[string]any)["fork_id"] = "bad id!"
			},
			wantStderr: "snapshot.fork_id must match OpaqueId pattern",
		},
		{
			name: "source session id",
			mutate: func(result map[string]any) {
				result["snapshot"].(map[string]any)["source_session_id"] = "bad id!"
			},
			wantStderr: "snapshot.source_session_id must match OpaqueId pattern",
		},
		{
			name: "source run id",
			mutate: func(result map[string]any) {
				result["snapshot"].(map[string]any)["source_run_id"] = "bad id!"
			},
			wantStderr: "snapshot.source_run_id must match OpaqueId pattern",
		},
		{
			name: "source agent id",
			mutate: func(result map[string]any) {
				result["snapshot"].(map[string]any)["source_agent_id"] = "bad id!"
			},
			wantStderr: "snapshot.source_agent_id must match OpaqueId pattern",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("decode request: %v", err)
				}
				result := validForkChatChatResult("fork-1", "fork-turn-1")
				tc.mutate(result)
				writeJSONRPCResult(t, w, req.ID, result)
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"forkchat", "resume", "fork-1", "--message", "continue"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 3 {
				t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func assertForkChatRequest(t *testing.T, req jsonRPCRequest, wantMethod string, wantParams map[string]any) {
	t.Helper()
	if req.Method != wantMethod {
		t.Fatalf("method = %q, want %s", req.Method, wantMethod)
	}
	if !reflect.DeepEqual(req.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", req.Params, wantParams)
	}
}

func validForkChatCreateResult(forkID string) map[string]any {
	return map[string]any{
		"fork":                 validForkChatSession(forkID),
		"idempotency_replayed": false,
	}
}

func validForkChatSession(forkID string) map[string]any {
	return map[string]any{
		"fork_id":           forkID,
		"source_session_id": "sess-1",
		"source_run_id":     "run-1",
		"source_agent_id":   "agent-1",
		"fork_point": map[string]any{
			"kind":        "turn",
			"turn_index":  2,
			"turn_id":     "turn-2",
			"selected_at": "2026-05-25T11:00:00Z",
		},
		"created_by": "token-1",
		"created_at": "2026-05-25T11:01:00Z",
		"expires_at": "2026-05-26T11:01:00Z",
		"state":      "active",
		"turns":      []map[string]any{},
	}
}

func validForkChatChatResult(forkID, turnID string) map[string]any {
	return map[string]any{
		"fork_id":              forkID,
		"turn":                 validForkChatTurn(turnID),
		"snapshot":             validForkChatSnapshot(forkID),
		"sandbox_policy":       validForkChatSandboxPolicy(),
		"idempotency_replayed": false,
	}
}

func validForkChatTurn(turnID string) map[string]any {
	return map[string]any{
		"turn_index":       1,
		"turn_id":          turnID,
		"request_payload":  map[string]any{"message": "inspect fork"},
		"response_payload": map[string]any{"message": "sandbox answer"},
		"tool_calls":       []map[string]any{},
		"turn_blocks":      []map[string]any{},
		"parse_ok":         true,
		"latency_ms":       12,
	}
}

func validForkChatSnapshot(forkID string) map[string]any {
	return map[string]any{
		"fork_id":           forkID,
		"source_session_id": "sess-1",
		"source_run_id":     "run-1",
		"source_agent_id":   "agent-1",
		"source_turn":       map[string]any{"turn_id": "turn-2", "turn_index": 2},
		"entity_snapshot": []map[string]any{
			{"entity_id": "entity-1", "current_state": "active"},
		},
		"snapshot_owner": "conversation.fork_chat.snapshot.v1",
		"created_at":     "2026-05-25T11:02:00Z",
	}
}

func validForkChatSandboxPolicy() map[string]any {
	return map[string]any{
		"owner":                "conversation.fork_chat.sandbox.v1",
		"read_policy":          "fork_snapshot_only",
		"write_policy":         "stub_record_only_no_live_mutation",
		"side_effecting_tools": []string{},
		"stubbed_tools":        []string{},
	}
}

func validForkChatDeleteResult(forkID string, alreadyDeleted bool) map[string]any {
	return map[string]any{
		"ok":                   true,
		"fork_id":              forkID,
		"deleted":              !alreadyDeleted,
		"already_deleted":      alreadyDeleted,
		"idempotency_replayed": false,
	}
}
