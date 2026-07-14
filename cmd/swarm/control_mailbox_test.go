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
	"sync/atomic"
	"testing"
)

func TestMailboxListUsesTaggedNoticeAndDecisionCardProjection(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{
			"items": []any{
				map[string]any{"kind": "notice", "notice": mailboxNoticeResult("notice-1")},
				map[string]any{"kind": "decision_card", "decision_card": mailboxCardSummaryResult("card-1")},
				map[string]any{"kind": "decision_card", "decision_card": mailboxHumanTaskCardSummaryResult("human-card-1")},
			},
			"next_cursor": "cursor-2",
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"mailbox", "list", "--all", "--run-id", "run-1", "--entity-id", "entity-1", "--limit", "25", "--cursor", "cursor-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != "mailbox.list" {
		t.Fatalf("method = %q, want mailbox.list", captured.Method)
	}
	wantParams := map[string]any{"run_id": "run-1", "entity_id": "entity-1", "limit": float64(25), "cursor": "cursor-1"}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{"MAILBOX_ID", "notice-1", "notice", "card-1", "decision_card", "launch_review", "human-card-1", "human_task:strategic_decision", "next_cursor=cursor-2"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestMailboxListSendsSupersededStatus(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{"items": []any{}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"mailbox", "list", "--status", "superseded",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != "mailbox.list" || captured.Params["status"] != "superseded" {
		t.Fatalf("request = %#v, want mailbox.list superseded filter", captured)
	}
}

func TestMailboxViewRendersTaggedResources(t *testing.T) {
	for _, tc := range []struct {
		name, id string
		result   map[string]any
		wants    []string
	}{
		{name: "notice", id: "notice-1", result: map[string]any{"kind": "notice", "notice": map[string]any{"item": mailboxNoticeResult("notice-1"), "payload": map[string]any{"summary": "review"}}}, wants: []string{"Mailbox notice notice-1", "status=pending", `payload={"summary":"review"}`}},
		{name: "card", id: "card-1", result: map[string]any{"kind": "decision_card", "decision_card": mailboxCardDetailResult("card-1")}, wants: []string{"Decision card card-1", "decision=launch_review", "card_content_hash=content-hash", `"decision":"launch_review"`}},
		{name: "human task", id: "human-card-1", result: map[string]any{"kind": "decision_card", "decision_card": mailboxHumanTaskCardDetailResult("human-card-1")}, wants: []string{"Decision card human-card-1", "anchor_kind=human_task", "scope=root", "category=strategic_decision", "requested_by=ceo", "card_content_hash=human-content-hash"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			var captured jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&captured)
				writeJSONRPCResult(t, w, captured.ID, tc.result)
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"mailbox", "view", tc.id}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s", code, stderr.String())
			}
			if captured.Method != "mailbox.get" || captured.Params["mailbox_id"] != tc.id {
				t.Fatalf("request = %#v", captured)
			}
			for _, want := range tc.wants {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
				}
			}
		})
	}
}

func TestMailboxViewUsesCanonicalSemanticJSONForDecisionEvidence(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	result := mailboxCardDetailResult("card-numeric")
	result["snapshot"] = map[string]any{
		"decision": "launch_review",
		"context":  map[string]any{"safe_integer": int64(9007199254740991)},
		"outcomes": map[string]any{"approve": map[string]any{
			"Emit":       map[string]any{"Fields": map[string]any{"score": map[string]any{"Literal": int64(9007199254740990)}}},
			"EmitSchema": map[string]any{"properties": map[string]any{"score": map[string]any{"minimum": int64(9007199254740989)}}},
		}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var captured jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&captured)
		writeJSONRPCResult(t, w, captured.ID, map[string]any{"kind": "decision_card", "decision_card": result})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"mailbox", "view", "card-numeric"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{`"safe_integer":9007199254740991`, `"Literal":9007199254740990`, `"minimum":9007199254740989`} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("mailbox view rewrote %s:\n%s", want, stdout.String())
		}
	}
}

func TestMailboxViewRejectsUnsupportedNumericDecisionEvidence(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":"swarm-cli:mailbox.get","result":{"kind":"decision_card","decision_card":{"card_id":"card-unsafe","run_id":"run-1","anchor_kind":"stage_gate","anchor":{"flow_instance":"root","entity_id":"entity-1","stage":"review","stage_activation_id":"activation-1"},"scope":{"kind":"entity","flow_instance":"root","entity_id":"entity-1"},"decision":"launch_review","title":"Review","status":"pending","created_at":"2026-05-13T12:00:01Z","updated_at":"2026-05-13T12:00:01Z","card_content_hash":"hash","snapshot":{"context":{"unsafe":9007199254740992}}}}}`)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"mailbox", "view", "card-unsafe"}, &stdout, &stderr, testRootCommandOptions(server))
	if code == 0 || !strings.Contains(stderr.String(), "I-JSON safe range") || stdout.Len() != 0 {
		t.Fatalf("code=%d stdout=%s stderr=%s, want fail-closed unsupported evidence", code, stdout.String(), stderr.String())
	}
}

func TestMailboxDeferUsesTypedCardMutation(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&captured)
		writeJSONRPCResult(t, w, captured.ID, map[string]any{"ok": true, "card_id": "card-1", "status": "pending", "change_id": 7, "idempotency_replayed": false})
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"mailbox", "defer", "card-1", "--until", "2026-05-13T12:30:00Z", "--idempotency-key", "idem-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s", code, stderr.String())
	}
	if captured.Method != "mailbox.defer" {
		t.Fatalf("method = %q", captured.Method)
	}
	want := map[string]any{"card_id": "card-1", "until": "2026-05-13T12:30:00Z", "idempotency_key": "idem-1"}
	if !reflect.DeepEqual(captured.Params, want) {
		t.Fatalf("params = %#v, want %#v", captured.Params, want)
	}
	if !strings.Contains(stdout.String(), "mailbox defer ok: card_id=card-1 status=pending change_id=7 idempotency_replayed=false") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestMailboxCommandsRejectInvalidInputBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "list status", args: []string{"mailbox", "list", "--status", "unknown"}, want: "--status is invalid"},
		{name: "list limit", args: []string{"mailbox", "list", "--limit", "0"}, want: "--limit must be an integer from 1 to 200"},
		{name: "view missing", args: []string{"mailbox", "view"}, want: "requires <mailbox-id>"},
		{name: "view blank", args: []string{"mailbox", "view", " "}, want: "mailbox id is required"},
		{name: "defer missing", args: []string{"mailbox", "defer"}, want: "requires <card-id>"},
		{name: "defer timestamp", args: []string{"mailbox", "defer", "card-1", "--until", "tomorrow"}, want: "--until must be an RFC3339 timestamp"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls.Load()
			var stdout, stderr bytes.Buffer
			if code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server)); code != 2 {
				t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.want)
			}
			if calls.Load() != before {
				t.Fatalf("request was sent for invalid input")
			}
		})
	}
}

func TestRetiredMailboxDecisionCommandsDoNotExist(t *testing.T) {
	for _, action := range []string{"approve", "reject"} {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"mailbox", action, "card-1"}, &stdout, &stderr, defaultRootCommandOptions())
		if code != 2 || !strings.Contains(stderr.String(), "unknown command") {
			t.Fatalf("mailbox %s code=%d stderr=%q", action, code, stderr.String())
		}
	}
}

func TestControlMailboxNamespaceFailsClosedBeforeRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", "mailbox", "anything"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 2 || !strings.Contains(stderr.String(), "`swarm control mailbox` was removed") || calls.Load() != 0 {
		t.Fatalf("code=%d calls=%d stderr=%q", code, calls.Load(), stderr.String())
	}
}

func TestMailboxProjectionValidationFailsClosed(t *testing.T) {
	if err := validateMailboxProjection(mailboxProjection{Kind: "decision_card"}); err == nil {
		t.Fatal("missing decision card accepted")
	}
	if err := validateMailboxProjection(mailboxProjection{Kind: "legacy"}); err == nil {
		t.Fatal("unknown projection kind accepted")
	}
	if err := validateMailboxDetailResult(mailboxDetailProjection{Kind: "decision_card", DecisionCard: &mailboxDecisionCard{}}); err == nil {
		t.Fatal("malformed decision-card detail accepted")
	}
}

func mailboxNoticeResult(id string) map[string]any {
	return map[string]any{"mailbox_id": id, "type": "review_request", "status": "pending", "priority": "high", "source_event_id": "event-1", "source_flow": "validation", "source_entity_id": "entity-1", "payload": map[string]any{"summary": "review"}, "created_at": "2026-05-13T12:00:00Z"}
}

func mailboxCardSummaryResult(id string) map[string]any {
	return map[string]any{
		"kind": "decision_card", "card_id": id, "run_id": "run-1", "anchor_kind": "stage_gate",
		"anchor": map[string]any{
			"flow_instance": "root", "flow_id": "review", "entity_id": "entity-1",
			"stage": "launch_review", "stage_activation_id": "activation-1",
		},
		"scope":    map[string]any{"kind": "entity", "flow_instance": "root", "entity_id": "entity-1"},
		"decision": "launch_review", "title": "Launch review", "status": "pending",
		"created_at": "2026-05-13T12:00:01Z", "updated_at": "2026-05-13T12:00:01Z",
	}
}

func mailboxCardDetailResult(id string) map[string]any {
	out := mailboxCardSummaryResult(id)
	out["card_content_hash"] = "content-hash"
	out["snapshot"] = map[string]any{"decision": "launch_review", "context": map[string]any{"qa": "passed"}, "outcomes": map[string]any{"approve": map[string]any{"advances_to": "done"}}}
	return out
}

func mailboxHumanTaskCardSummaryResult(id string) map[string]any {
	return map[string]any{
		"kind": "decision_card", "card_id": id, "run_id": "run-1", "anchor_kind": "human_task",
		"anchor": map[string]any{
			"requester_agent_id": "ceo", "operation_id": "operation-1", "category": "strategic_decision",
			"scope": map[string]any{"kind": "flow", "flow_instance": "root"},
		},
		"scope":    map[string]any{"kind": "flow", "flow_instance": "root"},
		"category": "strategic_decision", "title": "Choose launch market", "status": "pending",
		"created_at": "2026-05-13T12:00:01Z", "updated_at": "2026-05-13T12:00:01Z",
	}
}

func mailboxHumanTaskCardDetailResult(id string) map[string]any {
	out := mailboxHumanTaskCardSummaryResult(id)
	out["card_content_hash"] = "human-content-hash"
	out["snapshot"] = map[string]any{
		"description": "Choose the launch market",
		"outcomes":    map[string]any{"approve": map[string]any{}, "reject": map[string]any{}},
	}
	return out
}

func testRootCommandOptions(server *httptest.Server) rootCommandOptions {
	opts := defaultRootCommandOptions()
	hasTokenSource := strings.TrimSpace(opts.apiTokenFile) != ""
	if cfg, err := loadCLICommandConfig(); err == nil && strings.TrimSpace(cfg.Connection.APITokenFile) != "" {
		hasTokenSource = true
	}
	if !hasTokenSource {
		opts.apiRPCEndpointOverride = "http://192.0.2.10:8081/v1/rpc"
		return opts
	}
	opts.apiRPCEndpointOverride = server.URL + "/v1/rpc"
	opts.httpClient = server.Client()
	return opts
}

func writeJSONRPCResult(t *testing.T, w http.ResponseWriter, id string, result map[string]any) {
	t.Helper()
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
