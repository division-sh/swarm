package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/store"
)

type fakeConversationForkLifecycleStore struct {
	createResult  store.OperatorConversationForkSession
	createErr     error
	listResult    store.ConversationForkListResult
	listErr       error
	viewResult    store.OperatorConversationForkSession
	viewErr       error
	prepareResult store.ConversationForkChatPrepared
	prepareErr    error
	recordResult  store.ConversationForkChatResult
	recordErr     error
	deleteResult  store.ConversationForkDeleteResult
	deleteErr     error

	createCalls  int
	listCalls    int
	viewCalls    int
	prepareCalls int
	recordCalls  int
	deleteCalls  int

	lastCreate  store.ConversationForkCreateRequest
	lastList    store.ConversationForkListOptions
	lastViewID  string
	lastPrepare store.ConversationForkChatPrepareRequest
	lastRecord  store.ConversationForkChatRecordRequest
	lastDelete  string
	lastNow     time.Time

	recordEffect func()
}

func (s *fakeConversationForkLifecycleStore) CreateOperatorConversationFork(_ context.Context, req store.ConversationForkCreateRequest) (store.OperatorConversationForkSession, error) {
	s.createCalls++
	s.lastCreate = req
	if s.createErr != nil {
		return store.OperatorConversationForkSession{}, s.createErr
	}
	if s.recordEffect != nil {
		s.recordEffect()
	}
	return s.createResult, nil
}

func (s *fakeConversationForkLifecycleStore) ListOperatorConversationForks(_ context.Context, opts store.ConversationForkListOptions) (store.ConversationForkListResult, error) {
	s.listCalls++
	s.lastList = opts
	return s.listResult, s.listErr
}

func (s *fakeConversationForkLifecycleStore) LoadOperatorConversationFork(_ context.Context, forkID string) (store.OperatorConversationForkSession, error) {
	s.viewCalls++
	s.lastViewID = forkID
	return s.viewResult, s.viewErr
}

func (s *fakeConversationForkLifecycleStore) PrepareOperatorConversationForkChat(_ context.Context, req store.ConversationForkChatPrepareRequest) (store.ConversationForkChatPrepared, error) {
	s.prepareCalls++
	s.lastPrepare = req
	if s.prepareErr != nil {
		return store.ConversationForkChatPrepared{}, s.prepareErr
	}
	return s.prepareResult, nil
}

func (s *fakeConversationForkLifecycleStore) RecordOperatorConversationForkChat(_ context.Context, req store.ConversationForkChatRecordRequest) (store.ConversationForkChatResult, error) {
	s.recordCalls++
	s.lastRecord = req
	if s.recordErr != nil {
		return store.ConversationForkChatResult{}, s.recordErr
	}
	if s.recordEffect != nil {
		s.recordEffect()
	}
	return s.recordResult, nil
}

func (s *fakeConversationForkLifecycleStore) DeleteOperatorConversationFork(_ context.Context, forkID string, now time.Time) (store.ConversationForkDeleteResult, error) {
	s.deleteCalls++
	s.lastDelete = forkID
	s.lastNow = now
	if s.deleteErr != nil {
		return store.ConversationForkDeleteResult{}, s.deleteErr
	}
	if s.recordEffect != nil {
		s.recordEffect()
	}
	return s.deleteResult, nil
}

type fakeForkChatExecutor struct {
	result       store.ConversationForkChatExecution
	err          error
	calls        int
	lastPrepared store.ConversationForkChatPrepared
	lastMessage  string
}

func (f *fakeForkChatExecutor) ExecuteForkChat(_ context.Context, prepared store.ConversationForkChatPrepared, message string) (store.ConversationForkChatExecution, error) {
	f.calls++
	f.lastPrepared = prepared
	f.lastMessage = message
	if f.err != nil {
		return store.ConversationForkChatExecution{}, f.err
	}
	return f.result, nil
}

func TestOperatorConversationForkHandlersUseCanonicalOwnerAndIdempotency(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	sourceSessionID := "00000000-0000-0000-0000-000000000201"
	forkID := "00000000-0000-0000-0000-000000000301"
	turnID := "00000000-0000-0000-0000-000000000401"
	created := now.Add(-time.Minute)
	fork := store.OperatorConversationForkSession{
		ForkID:          forkID,
		SourceSessionID: sourceSessionID,
		SourceRunID:     "00000000-0000-0000-0000-000000000501",
		SourceAgentID:   "agent-1",
		ForkPoint: store.ConversationForkPointDescriptor{
			Kind:       "turn",
			TurnIndex:  2,
			TurnID:     turnID,
			SelectedAt: created,
		},
		CreatedBy: "token",
		CreatedAt: created,
		ExpiresAt: created.Add(store.ConversationForkLifecycleTTL),
		State:     "active",
		Turns:     []store.OperatorConversationTurn{},
	}
	forks := &fakeConversationForkLifecycleStore{
		createResult: fork,
		listResult:   store.ConversationForkListResult{Forks: []store.OperatorConversationForkSession{fork}, NextCursor: "cursor-2"},
		viewResult:   fork,
		prepareResult: store.ConversationForkChatPrepared{
			Fork: fork,
			Snapshot: store.ConversationForkSnapshot{
				ForkID:          forkID,
				SourceSessionID: sourceSessionID,
				SourceRunID:     "00000000-0000-0000-0000-000000000501",
				SourceAgentID:   "agent-1",
				SourceTurn: store.ConversationForkSourceTurn{
					TurnID:     turnID,
					TurnIndex:  2,
					SelectedAt: created,
					CreatedAt:  created,
				},
				EntitySnapshot: []store.ConversationForkEntitySnapshot{},
				SnapshotOwner:  store.ConversationForkChatSnapshotOwner,
				CreatedAt:      now,
			},
			SandboxPolicy: store.ConversationForkSandboxPolicy{
				Owner:       store.ConversationForkChatSandboxOwner,
				ReadPolicy:  "fork_snapshot_only",
				WritePolicy: "stub_record_only_no_live_mutation",
			},
			AvailableTools: []string{"fork_snapshot_read_entities"},
		},
		recordResult: store.ConversationForkChatResult{
			ForkID: forkID,
			Turn: store.OperatorConversationTurn{
				TurnIndex:       1,
				TurnID:          "00000000-0000-0000-0000-000000000402",
				RequestPayload:  []byte(`{"message":"inspect"}`),
				ResponsePayload: []byte(`{"message":"forkchat sandbox response: inspect"}`),
				ParseOK:         true,
			},
			Snapshot: store.ConversationForkSnapshot{
				ForkID:          forkID,
				SourceSessionID: sourceSessionID,
				SourceRunID:     "00000000-0000-0000-0000-000000000501",
				SourceAgentID:   "agent-1",
				SourceTurn: store.ConversationForkSourceTurn{
					TurnID:     turnID,
					TurnIndex:  2,
					SelectedAt: created,
					CreatedAt:  created,
				},
				EntitySnapshot: []store.ConversationForkEntitySnapshot{},
				SnapshotOwner:  store.ConversationForkChatSnapshotOwner,
				CreatedAt:      now,
			},
			SandboxPolicy: store.ConversationForkSandboxPolicy{
				Owner:       store.ConversationForkChatSandboxOwner,
				ReadPolicy:  "fork_snapshot_only",
				WritePolicy: "stub_record_only_no_live_mutation",
			},
		},
		deleteResult: store.ConversationForkDeleteResult{ForkID: forkID, Deleted: true},
	}
	executor := &fakeForkChatExecutor{result: store.ConversationForkChatExecution{
		AssistantMessage: "forkchat sandbox response: inspect",
		ToolCalls: []store.OperatorConversationToolCall{{
			ToolUseID: "tool-1",
			Name:      "fork_snapshot_read_entities",
			Arguments: json.RawMessage(`{"entity_id":"entity-1"}`),
			Result:    json.RawMessage(`{"status":"read_from_snapshot"}`),
		}},
		AvailableTools: []string{"fork_snapshot_read_entities"},
	}}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:               func() time.Time { return now },
			ConversationForks: forks,
			ForkChatExecutor:  executor,
			Idempotency:       newMutatingProbeIdempotencyStore(),
		}),
	})

	create := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"create","method":"conversation.fork","params":{"source_session_id":"`+sourceSessionID+`","fork_point":{"kind":"turn","turn_index":2},"idempotency_key":"create-1"}}`)
	if create.Error != nil {
		t.Fatalf("conversation.fork error = %#v", create.Error)
	}
	createResult := asMap(t, create.Result)
	if createResult["idempotency_replayed"] != false {
		t.Fatalf("conversation.fork idempotency_replayed = %#v", createResult["idempotency_replayed"])
	}
	if got := asMap(t, createResult["fork"])["fork_id"]; got != forkID {
		t.Fatalf("conversation.fork fork_id = %#v, want %s", got, forkID)
	}
	if forks.createCalls != 1 || forks.lastCreate.SourceSessionID != sourceSessionID || forks.lastCreate.ForkPoint.Kind != "turn" || forks.lastCreate.ForkPoint.TurnIndex != 2 {
		t.Fatalf("create owner call = calls %d req %#v", forks.createCalls, forks.lastCreate)
	}

	replay := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"replay","method":"conversation.fork","params":{"source_session_id":"`+sourceSessionID+`","fork_point":{"kind":"turn","turn_index":2},"idempotency_key":"create-1"}}`)
	if replay.Error != nil {
		t.Fatalf("conversation.fork replay error = %#v", replay.Error)
	}
	if got := asMap(t, replay.Result)["idempotency_replayed"]; got != true {
		t.Fatalf("conversation.fork replay idempotency_replayed = %#v, want true", got)
	}
	if forks.createCalls != 1 {
		t.Fatalf("conversation.fork create owner calls after replay = %d, want 1", forks.createCalls)
	}

	list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list","method":"conversation.fork_list","params":{"source_session_id":"`+sourceSessionID+`","limit":25,"cursor":"cursor-1"}}`)
	if list.Error != nil {
		t.Fatalf("conversation.fork_list error = %#v", list.Error)
	}
	listResult := asMap(t, list.Result)
	if listResult["next_cursor"] != "cursor-2" {
		t.Fatalf("conversation.fork_list next_cursor = %#v", listResult["next_cursor"])
	}
	if forks.listCalls != 1 || forks.lastList.SourceSessionID != sourceSessionID || forks.lastList.Limit != 25 || forks.lastList.Cursor != "cursor-1" {
		t.Fatalf("list owner call = calls %d opts %#v", forks.listCalls, forks.lastList)
	}

	view := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"view","method":"conversation.fork_view","params":{"fork_id":"`+forkID+`"}}`)
	if view.Error != nil {
		t.Fatalf("conversation.fork_view error = %#v", view.Error)
	}
	if got := asMap(t, view.Result)["fork_id"]; got != forkID {
		t.Fatalf("conversation.fork_view fork_id = %#v, want %s", got, forkID)
	}
	if forks.viewCalls != 1 || forks.lastViewID != forkID {
		t.Fatalf("view owner call = calls %d fork_id %s", forks.viewCalls, forks.lastViewID)
	}

	chat := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"chat","method":"conversation.fork_chat","params":{"fork_id":"`+forkID+`","message":"inspect","idempotency_key":"chat-1"}}`)
	if chat.Error != nil {
		t.Fatalf("conversation.fork_chat error = %#v", chat.Error)
	}
	chatResult := asMap(t, chat.Result)
	if chatResult["fork_id"] != forkID || chatResult["idempotency_replayed"] != false {
		t.Fatalf("conversation.fork_chat result = %#v", chatResult)
	}
	if got := asMap(t, chatResult["snapshot"])["snapshot_owner"]; got != store.ConversationForkChatSnapshotOwner {
		t.Fatalf("conversation.fork_chat snapshot owner = %#v", got)
	}
	if forks.prepareCalls != 1 || forks.lastPrepare.ForkID != forkID || !forks.lastPrepare.Now.Equal(now) {
		t.Fatalf("chat prepare owner call = calls %d req %#v", forks.prepareCalls, forks.lastPrepare)
	}
	if executor.calls != 1 || executor.lastPrepared.Fork.ForkID != forkID || executor.lastMessage != "inspect" {
		t.Fatalf("chat executor call = calls %d prepared %#v message %q", executor.calls, executor.lastPrepared, executor.lastMessage)
	}
	if forks.recordCalls != 1 || forks.lastRecord.ForkID != forkID || forks.lastRecord.Message != "inspect" || !forks.lastRecord.Now.Equal(now) {
		t.Fatalf("chat record owner call = calls %d req %#v", forks.recordCalls, forks.lastRecord)
	}
	if got := forks.lastRecord.Execution.ToolCalls; len(got) != 1 || got[0].Name != "fork_snapshot_read_entities" {
		t.Fatalf("chat record execution tool calls = %#v", got)
	}

	chatReplay := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"chat-replay","method":"conversation.fork_chat","params":{"fork_id":"`+forkID+`","message":"inspect","idempotency_key":"chat-1"}}`)
	if chatReplay.Error != nil {
		t.Fatalf("conversation.fork_chat replay error = %#v", chatReplay.Error)
	}
	if got := asMap(t, chatReplay.Result)["idempotency_replayed"]; got != true {
		t.Fatalf("conversation.fork_chat replay idempotency_replayed = %#v, want true", got)
	}
	if forks.prepareCalls != 1 || executor.calls != 1 || forks.recordCalls != 1 {
		t.Fatalf("conversation.fork_chat calls after replay = prepare %d executor %d record %d, want 1 each", forks.prepareCalls, executor.calls, forks.recordCalls)
	}

	deleted := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete","method":"conversation.fork_delete","params":{"fork_id":"`+forkID+`","idempotency_key":"delete-1"}}`)
	if deleted.Error != nil {
		t.Fatalf("conversation.fork_delete error = %#v", deleted.Error)
	}
	deleteResult := asMap(t, deleted.Result)
	if deleteResult["ok"] != true || deleteResult["fork_id"] != forkID || deleteResult["deleted"] != true || deleteResult["idempotency_replayed"] != false {
		t.Fatalf("conversation.fork_delete result = %#v", deleteResult)
	}
	if forks.deleteCalls != 1 || forks.lastDelete != forkID || !forks.lastNow.Equal(now) {
		t.Fatalf("delete owner call = calls %d fork_id %s now %s", forks.deleteCalls, forks.lastDelete, forks.lastNow)
	}
}

func TestOperatorConversationForkHandlersTypedErrors(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	sourceSessionID := "00000000-0000-0000-0000-000000000201"
	forkID := "00000000-0000-0000-0000-000000000301"
	tests := []struct {
		name   string
		method string
		body   string
		mutate func(*fakeConversationForkLifecycleStore)
		code   string
		detail map[string]any
	}{
		{
			name:   "create missing source session",
			method: "conversation.fork",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork","params":{"source_session_id":"` + sourceSessionID + `","fork_point":{"kind":"turn","turn_index":1}}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.createErr = store.ErrSessionNotFound },
			code:   SessionNotFoundCode,
			detail: map[string]any{"session_id": sourceSessionID},
		},
		{
			name:   "create missing turn",
			method: "conversation.fork",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork","params":{"source_session_id":"` + sourceSessionID + `","fork_point":{"kind":"turn","turn_index":99}}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.createErr = store.ErrTurnNotFound },
			code:   TurnNotFoundCode,
			detail: map[string]any{"session_id": sourceSessionID, "turn_index": float64(99)},
		},
		{
			name:   "create missing event",
			method: "conversation.fork",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork","params":{"source_session_id":"` + sourceSessionID + `","fork_point":{"kind":"event","event_id":"00000000-0000-0000-0000-000000000999"}}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.createErr = store.ErrEventNotFound },
			code:   EventNotFoundCode,
			detail: map[string]any{"event_id": "00000000-0000-0000-0000-000000000999"},
		},
		{
			name:   "list bad cursor",
			method: "conversation.fork_list",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork_list","params":{"cursor":"bad"}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.listErr = store.ErrInvalidConversationForkCursor },
			code:   "",
		},
		{
			name:   "view missing fork",
			method: "conversation.fork_view",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork_view","params":{"fork_id":"` + forkID + `"}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.viewErr = store.ErrConversationForkNotFound },
			code:   ForkNotFoundCode,
			detail: map[string]any{"fork_id": forkID},
		},
		{
			name:   "chat missing fork",
			method: "conversation.fork_chat",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork_chat","params":{"fork_id":"` + forkID + `","message":"hello"}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.prepareErr = store.ErrConversationForkNotFound },
			code:   ForkNotFoundCode,
			detail: map[string]any{"fork_id": forkID},
		},
		{
			name:   "delete missing fork",
			method: "conversation.fork_delete",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork_delete","params":{"fork_id":"` + forkID + `"}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.deleteErr = store.ErrConversationForkNotFound },
			code:   ForkNotFoundCode,
			detail: map[string]any{"fork_id": forkID},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			forks := &fakeConversationForkLifecycleStore{
				createResult: store.OperatorConversationForkSession{ForkID: forkID, SourceSessionID: sourceSessionID, SourceAgentID: "agent-1", ForkPoint: store.ConversationForkPointDescriptor{Kind: "turn", TurnIndex: 1, TurnID: "00000000-0000-0000-0000-000000000401", SelectedAt: now}, CreatedBy: "token", CreatedAt: now, ExpiresAt: now.Add(time.Hour), State: "active", Turns: []store.OperatorConversationTurn{}},
				deleteResult: store.ConversationForkDeleteResult{ForkID: forkID, Deleted: true},
			}
			tt.mutate(forks)
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: OperatorReadHandlers(OperatorReadOptions{
					Now:               func() time.Time { return now },
					ConversationForks: forks,
					ForkChatExecutor: &fakeForkChatExecutor{result: store.ConversationForkChatExecution{
						AssistantMessage: "ok",
					}},
					Idempotency: newMutatingProbeIdempotencyStore(),
				}),
			})
			resp := rpcCall(t, handler, tt.body)
			if tt.code == "" {
				if resp.Error == nil || resp.Error.Code != codeInvalidParams {
					t.Fatalf("%s error = %#v, want invalid params", tt.method, resp.Error)
				}
				return
			}
			if resp.Error == nil {
				t.Fatalf("%s error = nil, want %s", tt.method, tt.code)
			}
			data := asMap(t, resp.Error.Data)
			if data["code"] != tt.code {
				t.Fatalf("%s error data = %#v, want code %s", tt.method, data, tt.code)
			}
			details := asMap(t, data["details"])
			for key, want := range tt.detail {
				if details[key] != want {
					t.Fatalf("%s error details[%s] = %#v, want %#v in %#v", tt.method, key, details[key], want, details)
				}
			}
		})
	}
}

func TestLLMForkChatExecutorUsesRuntimeRequestedToolsOnly(t *testing.T) {
	prepared := store.ConversationForkChatPrepared{
		Fork: store.OperatorConversationForkSession{
			ForkID:        "fork-1",
			SourceRunID:   "run-1",
			SourceAgentID: "agent-source",
		},
		Snapshot: store.ConversationForkSnapshot{
			SnapshotOwner: store.ConversationForkChatSnapshotOwner,
			EntitySnapshot: []store.ConversationForkEntitySnapshot{{
				EntityID:     "entity-1",
				CurrentState: "draft",
				Fields:       map[string]any{"name": "Before"},
			}},
		},
		SandboxPolicy: store.ConversationForkSandboxPolicy{
			Owner:        store.ConversationForkChatSandboxOwner,
			ReadPolicy:   "fork_snapshot_only",
			WritePolicy:  "stub_record_only_no_live_mutation",
			StubbedTools: []string{"save_entity_field", "emit_event", "mailbox.approve", "run.start", "run.stop"},
		},
		AvailableTools: []string{"fork_snapshot_read_entities", "save_entity_field", "emit_event", "mailbox_approve", "run_start", "run_stop"},
	}
	rt := &forkChatScriptedRuntime{
		responses: []*runtimellm.Response{
			{
				Message: runtimellm.Message{Role: "assistant", Content: "checking tools"},
				ToolCalls: []runtimellm.ToolCall{
					{Name: "fork_snapshot_read_entities", Arguments: map[string]any{"entity_id": "entity-1"}},
					{Name: "save_entity_field", Arguments: map[string]any{"entity_id": "entity-1", "field": "name", "value": "Sandbox"}},
					{Name: "emit_event", Arguments: map[string]any{"event_name": "forkchat.note"}},
					{Name: "mailbox_approve", Arguments: map[string]any{"mailbox_id": "mb-1"}},
					{Name: "run_start", Arguments: map[string]any{"event_name": "scan.requested"}},
				},
			},
			{Message: runtimellm.Message{Role: "assistant", Content: "snapshot says Before; writes were stubbed"}},
		},
	}
	execution, err := NewLLMForkChatExecutor(rt).ExecuteForkChat(context.Background(), prepared, "inspect and try sandbox writes")
	if err != nil {
		t.Fatalf("ExecuteForkChat: %v", err)
	}
	if rt.startAgentID != "agent-source" {
		t.Fatalf("StartSession agentID = %q, want source agent", rt.startAgentID)
	}
	if !strings.Contains(rt.systemPrompt, "isolated forensic sandbox") || !strings.Contains(rt.systemPrompt, store.ConversationForkChatSnapshotOwner) {
		t.Fatalf("system prompt = %q, want forkchat sandbox/snapshot context", rt.systemPrompt)
	}
	if got := forkChatToolNames(rt.tools); !stringSetContainsAll(got, "fork_snapshot_read_entities", "save_entity_field", "emit_event", "mailbox_approve", "run_start") {
		t.Fatalf("runtime tools = %v", got)
	}
	if len(rt.messages) != 2 || rt.messages[0].Role != "user" || rt.messages[1].Role != "tool" {
		t.Fatalf("runtime messages = %#v, want user then tool result follow-up", rt.messages)
	}
	if execution.AssistantMessage != "snapshot says Before; writes were stubbed" {
		t.Fatalf("assistant message = %q", execution.AssistantMessage)
	}
	if len(execution.ToolCalls) != 5 {
		t.Fatalf("tool calls = %#v, want only five runtime-requested calls", execution.ToolCalls)
	}
	if findConversationForkToolCall(execution.ToolCalls, "run_stop") != nil {
		t.Fatalf("unrequested run_stop was persisted: %#v", execution.ToolCalls)
	}
	read := requireAPIForkToolCall(t, execution.ToolCalls, "fork_snapshot_read_entities")
	readResult := decodeJSONMap(t, read.Result)
	if readResult["status"] != "read_from_snapshot" || readResult["snapshot_owner"] != store.ConversationForkChatSnapshotOwner || readResult["entity_count"] != float64(1) {
		t.Fatalf("snapshot read result = %#v", readResult)
	}
	for _, name := range []string{"save_entity_field", "emit_event", "mailbox_approve", "run_start"} {
		call := requireAPIForkToolCall(t, execution.ToolCalls, name)
		result := decodeJSONMap(t, call.Result)
		if result["status"] != "stubbed" || result["owner"] != store.ConversationForkChatSandboxOwner || result["live_mutation"] != false {
			t.Fatalf("%s result = %#v, want stubbed no-live-mutation", name, result)
		}
	}
	toolResults := decodeJSONArray(t, rt.messages[1].Content)
	if len(toolResults) != 5 {
		t.Fatalf("tool result payload = %#v, want five results", toolResults)
	}
}

type forkChatScriptedRuntime struct {
	responses    []*runtimellm.Response
	startAgentID string
	systemPrompt string
	tools        []runtimellm.ToolDefinition
	messages     []runtimellm.Message
}

func (r *forkChatScriptedRuntime) StartSession(_ context.Context, agentID, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	r.startAgentID = agentID
	r.systemPrompt = systemPrompt
	r.tools = append([]runtimellm.ToolDefinition(nil), tools...)
	return &runtimellm.Session{ID: "forkchat-runtime-session", AgentID: agentID, RuntimeMode: "task"}, nil
}

func (r *forkChatScriptedRuntime) ContinueSession(_ context.Context, _ *runtimellm.Session, message runtimellm.Message) (*runtimellm.Response, error) {
	r.messages = append(r.messages, message)
	if len(r.responses) == 0 {
		return &runtimellm.Response{Message: runtimellm.Message{Role: "assistant", Content: "done"}}, nil
	}
	resp := r.responses[0]
	r.responses = r.responses[1:]
	return resp, nil
}

func forkChatToolNames(tools []runtimellm.ToolDefinition) []string {
	out := make([]string, 0, len(tools))
	for _, tool := range tools {
		out = append(out, tool.Name)
	}
	return out
}

func stringSetContainsAll(values []string, wants ...string) bool {
	seen := map[string]struct{}{}
	for _, value := range values {
		seen[value] = struct{}{}
	}
	for _, want := range wants {
		if _, ok := seen[want]; !ok {
			return false
		}
	}
	return true
}

func requireAPIForkToolCall(t *testing.T, calls []store.OperatorConversationToolCall, name string) store.OperatorConversationToolCall {
	t.Helper()
	if call := findConversationForkToolCall(calls, name); call != nil {
		if len(call.Result) == 0 {
			t.Fatalf("%s tool call missing result: %#v", name, *call)
		}
		return *call
	}
	t.Fatalf("tool call %s missing from %#v", name, calls)
	return store.OperatorConversationToolCall{}
}

func findConversationForkToolCall(calls []store.OperatorConversationToolCall, name string) *store.OperatorConversationToolCall {
	for i := range calls {
		if calls[i].Name == name {
			return &calls[i]
		}
	}
	return nil
}

func decodeJSONMap(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode JSON map %s: %v", string(raw), err)
	}
	return out
}

func decodeJSONArray(t *testing.T, raw string) []map[string]any {
	t.Helper()
	var out []map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("decode JSON array %s: %v", raw, err)
	}
	return out
}

func TestOperatorConversationForkRejectsInvalidForkPointBeforeOwner(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	tests := []struct {
		name      string
		forkPoint string
		wantField string
	}{
		{
			name:      "unsupported kind",
			forkPoint: `{"kind":"bogus"}`,
			wantField: "fork_point.kind",
		},
		{
			name:      "unknown entity snapshot",
			forkPoint: `{"kind":"turn","turn_index":1,"entity_snapshot":{}}`,
			wantField: "fork_point.entity_snapshot",
		},
		{
			name:      "unknown include original",
			forkPoint: `{"kind":"turn","turn_index":1,"include_original":true}`,
			wantField: "fork_point.include_original",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			forks := &fakeConversationForkLifecycleStore{}
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: OperatorReadHandlers(OperatorReadOptions{
					Now:               func() time.Time { return now },
					ConversationForks: forks,
					Idempotency:       newMutatingProbeIdempotencyStore(),
				}),
			})
			resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"bad","method":"conversation.fork","params":{"source_session_id":"00000000-0000-0000-0000-000000000201","fork_point":`+tt.forkPoint+`}}`)
			if resp.Error == nil || resp.Error.Code != codeInvalidParams {
				t.Fatalf("conversation.fork malformed fork_point error = %#v, want invalid params", resp.Error)
			}
			if got := asMap(t, asMap(t, resp.Error.Data)["details"])["field"]; got != tt.wantField {
				t.Fatalf("conversation.fork invalid field = %#v, want %s", got, tt.wantField)
			}
			if forks.createCalls != 0 {
				t.Fatalf("create owner calls = %d, want 0 for malformed fork_point", forks.createCalls)
			}
		})
	}
}

func TestOperatorConversationForkIdempotencyConflict(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	sourceSessionID := "00000000-0000-0000-0000-000000000201"
	forks := &fakeConversationForkLifecycleStore{
		createResult: store.OperatorConversationForkSession{
			ForkID:          "00000000-0000-0000-0000-000000000301",
			SourceSessionID: sourceSessionID,
			SourceAgentID:   "agent-1",
			ForkPoint:       store.ConversationForkPointDescriptor{Kind: "turn", TurnIndex: 1, TurnID: "00000000-0000-0000-0000-000000000401", SelectedAt: now},
			CreatedBy:       "token",
			CreatedAt:       now,
			ExpiresAt:       now.Add(time.Hour),
			State:           "active",
			Turns:           []store.OperatorConversationTurn{},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:               func() time.Time { return now },
			ConversationForks: forks,
			Idempotency:       newMutatingProbeIdempotencyStore(),
		}),
	})
	first := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"first","method":"conversation.fork","params":{"source_session_id":"`+sourceSessionID+`","fork_point":{"kind":"turn","turn_index":1},"idempotency_key":"fork-key"}}`)
	if first.Error != nil {
		t.Fatalf("conversation.fork first error = %#v", first.Error)
	}
	conflict := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"conflict","method":"conversation.fork","params":{"source_session_id":"`+sourceSessionID+`","fork_point":{"kind":"turn","turn_index":2},"idempotency_key":"fork-key"}}`)
	if conflict.Error == nil {
		t.Fatal("conversation.fork conflict error = nil")
	}
	data := asMap(t, conflict.Error.Data)
	if data["code"] != IdempotencyConflictCode {
		t.Fatalf("conversation.fork conflict data = %#v, want %s", data, IdempotencyConflictCode)
	}
}

func TestConversationForkErrorMapsParamErrors(t *testing.T) {
	err := conversationForkError(&store.EntityReadParamError{Field: "source_session_id", Reason: "must be a UUID"}, conversationForkErrorDetails{})
	var invalid *InvalidParamsError
	if !errors.As(err, &invalid) {
		t.Fatalf("conversationForkError = %T, want InvalidParamsError", err)
	}
}
