package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

type staticForkChatRuntimeResolver struct {
	runtime runtimellm.Runtime
}

func (r staticForkChatRuntimeResolver) ResolveAgentRuntime(actor runtimeactors.AgentConfig) (runtimellm.AgentRuntimeResolution, error) {
	return runtimellm.AgentRuntimeResolution{Actor: actor, Runtime: r.runtime}, nil
}

type fakeConversationForkLifecycleStore struct {
	createResult  runfork.OperatorConversationForkSession
	createErr     error
	listResult    runfork.ConversationForkListResult
	listErr       error
	viewResult    runfork.OperatorConversationForkSession
	viewErr       error
	prepareResult runfork.ConversationForkChatPrepared
	admitErr      error
	prepareErr    error
	recordResult  runfork.ConversationForkChatResult
	recordErr     error
	heartbeatErr  error
	deleteResult  runfork.ConversationForkDeleteResult
	deleteErr     error

	createCalls    int
	listCalls      int
	viewCalls      int
	admitCalls     int
	prepareCalls   int
	recordCalls    int
	heartbeatCalls int
	failCalls      int
	deleteCalls    int

	lastCreate  runfork.ConversationForkCreateRequest
	lastList    runfork.ConversationForkListOptions
	lastViewID  string
	lastAdmitID string
	lastPosture executionposture.Posture
	lastPrepare runfork.ConversationForkChatPrepareRequest
	lastRecord  runfork.ConversationForkChatRecordRequest
	lastFailure runfork.ConversationForkChatFailureRequest
	lastDelete  string
	lastNow     time.Time

	recordEffect func()
}

func (s *fakeConversationForkLifecycleStore) CreateOperatorConversationFork(_ context.Context, req runfork.ConversationForkCreateRequest) (runfork.OperatorConversationForkSession, error) {
	s.createCalls++
	s.lastCreate = req
	if s.createErr != nil {
		return runfork.OperatorConversationForkSession{}, s.createErr
	}
	if s.recordEffect != nil {
		s.recordEffect()
	}
	return s.createResult, nil
}

func (s *fakeConversationForkLifecycleStore) AdmitOperatorConversationForkChat(_ context.Context, forkID string, posture executionposture.Posture) error {
	s.admitCalls++
	s.lastAdmitID = forkID
	s.lastPosture = posture
	return s.admitErr
}

func (s *fakeConversationForkLifecycleStore) ListOperatorConversationForks(_ context.Context, opts runfork.ConversationForkListOptions) (runfork.ConversationForkListResult, error) {
	s.listCalls++
	s.lastList = opts
	return s.listResult, s.listErr
}

func (s *fakeConversationForkLifecycleStore) LoadOperatorConversationFork(_ context.Context, forkID string) (runfork.OperatorConversationForkSession, error) {
	s.viewCalls++
	s.lastViewID = forkID
	return s.viewResult, s.viewErr
}

func (s *fakeConversationForkLifecycleStore) PrepareOperatorConversationForkChat(_ context.Context, req runfork.ConversationForkChatPrepareRequest) (runfork.ConversationForkChatPrepared, error) {
	s.prepareCalls++
	s.lastPrepare = req
	if s.prepareErr != nil {
		return runfork.ConversationForkChatPrepared{}, s.prepareErr
	}
	return s.prepareResult, nil
}

func (s *fakeConversationForkLifecycleStore) RecordOperatorConversationForkChat(_ context.Context, req runfork.ConversationForkChatRecordRequest) (runfork.ConversationForkChatResult, error) {
	s.recordCalls++
	s.lastRecord = req
	if s.recordErr != nil {
		return runfork.ConversationForkChatResult{}, s.recordErr
	}
	if s.recordEffect != nil {
		s.recordEffect()
	}
	return s.recordResult, nil
}

func (s *fakeConversationForkLifecycleStore) HeartbeatOperatorConversationForkChat(_ context.Context, _ runfork.ConversationForkChatPrepared, _ time.Time) error {
	s.heartbeatCalls++
	return s.heartbeatErr
}

func (s *fakeConversationForkLifecycleStore) FailOperatorConversationForkChat(_ context.Context, req runfork.ConversationForkChatFailureRequest) error {
	s.failCalls++
	s.lastFailure = req
	return nil
}

func (s *fakeConversationForkLifecycleStore) DeleteOperatorConversationFork(_ context.Context, forkID string, now time.Time) (runfork.ConversationForkDeleteResult, error) {
	s.deleteCalls++
	s.lastDelete = forkID
	s.lastNow = now
	if s.deleteErr != nil {
		return runfork.ConversationForkDeleteResult{}, s.deleteErr
	}
	if s.recordEffect != nil {
		s.recordEffect()
	}
	return s.deleteResult, nil
}

type fakeForkChatExecutor struct {
	result       runfork.ConversationForkChatExecution
	err          error
	calls        int
	lastPrepared runfork.ConversationForkChatPrepared
	lastMessage  string
}

func (f *fakeForkChatExecutor) ExecuteForkChat(_ context.Context, prepared runfork.ConversationForkChatPrepared, message string) (runfork.ConversationForkChatExecution, error) {
	f.calls++
	f.lastPrepared = prepared
	f.lastMessage = message
	if f.err != nil {
		return runfork.ConversationForkChatExecution{}, f.err
	}
	return f.result, nil
}

func TestOperatorConversationForkHandlersSegregateReadAndLifecycleCapabilities(t *testing.T) {
	readOnly := testOperatorConversationForkHandlers(testOperatorCapabilities{
		ConversationForks: &fakeConversationForkLifecycleStore{},
		Idempotency:       newMutatingProbeIdempotencyStore(),
	})
	for _, method := range []string{"conversation.fork_list", "conversation.fork_view"} {
		if readOnly[method] == nil {
			t.Fatalf("read-only fork capability missing %s", method)
		}
	}
	for _, method := range []string{"conversation.fork", "conversation.fork_chat", "conversation.fork_delete"} {
		if readOnly[method] != nil {
			t.Fatalf("read-only fork capability unexpectedly registered mutating method %s", method)
		}
	}

	lifecycleOnly := testOperatorConversationForkHandlers(testOperatorCapabilities{
		ConversationForkLifecycle: &fakeConversationForkLifecycleStore{},
		Idempotency:               newMutatingProbeIdempotencyStore(),
	})
	for _, method := range []string{"conversation.fork", "conversation.fork_chat", "conversation.fork_delete"} {
		if lifecycleOnly[method] == nil {
			t.Fatalf("lifecycle fork capability missing %s", method)
		}
	}
	for _, method := range []string{"conversation.fork_list", "conversation.fork_view"} {
		if lifecycleOnly[method] != nil {
			t.Fatalf("lifecycle fork capability unexpectedly registered read method %s", method)
		}
	}

	withoutIdempotency := testOperatorConversationForkHandlers(testOperatorCapabilities{
		ConversationForkLifecycle: &fakeConversationForkLifecycleStore{},
	})
	for _, method := range []string{"conversation.fork", "conversation.fork_chat", "conversation.fork_delete"} {
		if withoutIdempotency[method] != nil {
			t.Fatalf("lifecycle fork capability without idempotency unexpectedly registered %s", method)
		}
	}
}

func TestOperatorConversationForkHandlersUseCanonicalOwnerAndIdempotency(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	sourceSessionID := "00000000-0000-0000-0000-000000000201"
	forkID := "00000000-0000-0000-0000-000000000301"
	turnID := "00000000-0000-0000-0000-000000000401"
	created := now.Add(-time.Minute)
	fork := runfork.OperatorConversationForkSession{
		ForkID:          forkID,
		SourceSessionID: sourceSessionID,
		SourceRunID:     "00000000-0000-0000-0000-000000000501",
		SourceAgentID:   "agent-1",
		ForkPoint: runfork.ConversationForkPointDescriptor{
			Kind:       "turn",
			TurnIndex:  2,
			TurnID:     turnID,
			SelectedAt: created,
		},
		CreatedBy: "token",
		CreatedAt: created,
		ExpiresAt: created.Add(runfork.ConversationForkLifecycleTTL),
		State:     "active",
		Turns:     []operatorread.OperatorConversationTurn{},
	}
	forks := &fakeConversationForkLifecycleStore{
		createResult: fork,
		listResult:   runfork.ConversationForkListResult{Forks: []runfork.OperatorConversationForkSession{fork}, NextCursor: "cursor-2"},
		viewResult:   fork,
		prepareResult: runfork.ConversationForkChatPrepared{
			Fork: fork,
			Snapshot: runfork.ConversationForkSnapshot{
				ForkID:          forkID,
				SourceSessionID: sourceSessionID,
				SourceRunID:     "00000000-0000-0000-0000-000000000501",
				SourceAgentID:   "agent-1",
				SourceTurn: runfork.ConversationForkSourceTurn{
					TurnID:     turnID,
					TurnIndex:  2,
					SelectedAt: created,
					CreatedAt:  created,
				},
				EntitySnapshot: []runfork.ConversationForkEntitySnapshot{},
				SnapshotOwner:  runfork.ConversationForkChatSnapshotOwner,
				CreatedAt:      now,
			},
			SandboxPolicy: runfork.ConversationForkSandboxPolicy{
				Owner:       runfork.ConversationForkChatSandboxOwner,
				ReadPolicy:  "fork_snapshot_only",
				WritePolicy: "stub_record_only_no_live_mutation",
			},
			AvailableTools: []string{"fork_snapshot_read_entities"},
		},
		recordResult: runfork.ConversationForkChatResult{
			ForkID: forkID,
			Turn: operatorread.OperatorConversationTurn{
				TurnIndex:       1,
				TurnID:          "00000000-0000-0000-0000-000000000402",
				ExecutionMode:   "live",
				RequestPayload:  []byte(`{"message":"inspect"}`),
				ResponsePayload: []byte(`{"message":"forkchat sandbox response: inspect"}`),
				ParseOK:         true,
			},
			Snapshot: runfork.ConversationForkSnapshot{
				ForkID:          forkID,
				SourceSessionID: sourceSessionID,
				SourceRunID:     "00000000-0000-0000-0000-000000000501",
				SourceAgentID:   "agent-1",
				SourceTurn: runfork.ConversationForkSourceTurn{
					TurnID:     turnID,
					TurnIndex:  2,
					SelectedAt: created,
					CreatedAt:  created,
				},
				EntitySnapshot: []runfork.ConversationForkEntitySnapshot{},
				SnapshotOwner:  runfork.ConversationForkChatSnapshotOwner,
				CreatedAt:      now,
			},
			SandboxPolicy: runfork.ConversationForkSandboxPolicy{
				Owner:       runfork.ConversationForkChatSandboxOwner,
				ReadPolicy:  "fork_snapshot_only",
				WritePolicy: "stub_record_only_no_live_mutation",
			},
		},
		deleteResult: runfork.ConversationForkDeleteResult{ForkID: forkID, Deleted: true},
	}
	executor := &fakeForkChatExecutor{result: runfork.ConversationForkChatExecution{
		AssistantMessage: "forkchat sandbox response: inspect",
		ToolCalls: []operatorread.OperatorConversationToolCall{{
			ToolUseID: "tool-1",
			Name:      "fork_snapshot_read_entities",
			Arguments: json.RawMessage(`{"entity_id":"entity-1"}`),
			Result:    json.RawMessage(`{"status":"read_from_snapshot"}`),
		}},
		AvailableTools: []string{"fork_snapshot_read_entities"},
	}}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:                       func() time.Time { return now },
			ConversationForks:         forks,
			ConversationForkLifecycle: forks,
			ForkChatExecutor:          executor,
			Idempotency:               newMutatingProbeIdempotencyStore(),
		}),
	})

	create := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"create","method":"conversation.fork","params":{"source_session_id":"`+sourceSessionID+`","fork_point":{"kind":"turn","turn_id":"`+turnID+`"},"idempotency_key":"create-1"}}`)
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
	if forks.createCalls != 1 || forks.lastCreate.SourceSessionID != sourceSessionID || forks.lastCreate.ForkPoint.Kind != "turn" || forks.lastCreate.ForkPoint.TurnID != turnID {
		t.Fatalf("create owner call = calls %d req %#v", forks.createCalls, forks.lastCreate)
	}

	replay := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"replay","method":"conversation.fork","params":{"source_session_id":"`+sourceSessionID+`","fork_point":{"kind":"turn","turn_id":"`+turnID+`"},"idempotency_key":"create-1"}}`)
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
	if got := asMap(t, chatResult["snapshot"])["snapshot_owner"]; got != runfork.ConversationForkChatSnapshotOwner {
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
	if forks.heartbeatCalls != 1 {
		t.Fatalf("chat heartbeat owner calls = %d, want 1", forks.heartbeatCalls)
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
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork","params":{"source_session_id":"` + sourceSessionID + `","fork_point":{"kind":"turn","turn_id":"00000000-0000-0000-0000-000000000401"}}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.createErr = operatorread.ErrSessionNotFound },
			code:   SessionNotFoundCode,
			detail: map[string]any{"session_id": sourceSessionID},
		},
		{
			name:   "create missing turn",
			method: "conversation.fork",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork","params":{"source_session_id":"` + sourceSessionID + `","fork_point":{"kind":"turn","turn_id":"00000000-0000-0000-0000-000000000999"}}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.createErr = operatorread.ErrTurnNotFound },
			code:   TurnNotFoundCode,
			detail: map[string]any{"session_id": sourceSessionID, "turn_id": "00000000-0000-0000-0000-000000000999"},
		},
		{
			name:   "create missing event",
			method: "conversation.fork",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork","params":{"source_session_id":"` + sourceSessionID + `","fork_point":{"kind":"event","event_id":"00000000-0000-0000-0000-000000000999"}}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.createErr = operatorread.ErrEventNotFound },
			code:   EventNotFoundCode,
			detail: map[string]any{"event_id": "00000000-0000-0000-0000-000000000999"},
		},
		{
			name:   "list bad cursor",
			method: "conversation.fork_list",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork_list","params":{"cursor":"bad"}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.listErr = runfork.ErrInvalidConversationForkCursor },
			code:   "",
		},
		{
			name:   "view missing fork",
			method: "conversation.fork_view",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork_view","params":{"fork_id":"` + forkID + `"}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.viewErr = runfork.ErrConversationForkNotFound },
			code:   ForkNotFoundCode,
			detail: map[string]any{"fork_id": forkID},
		},
		{
			name:   "chat missing fork",
			method: "conversation.fork_chat",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork_chat","params":{"fork_id":"` + forkID + `","message":"hello"}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.prepareErr = runfork.ErrConversationForkNotFound },
			code:   ForkNotFoundCode,
			detail: map[string]any{"fork_id": forkID},
		},
		{
			name:   "delete missing fork",
			method: "conversation.fork_delete",
			body:   `{"jsonrpc":"2.0","id":"err","method":"conversation.fork_delete","params":{"fork_id":"` + forkID + `"}}`,
			mutate: func(s *fakeConversationForkLifecycleStore) { s.deleteErr = runfork.ErrConversationForkNotFound },
			code:   ForkNotFoundCode,
			detail: map[string]any{"fork_id": forkID},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			forks := &fakeConversationForkLifecycleStore{
				createResult: runfork.OperatorConversationForkSession{ForkID: forkID, SourceSessionID: sourceSessionID, SourceAgentID: "agent-1", ForkPoint: runfork.ConversationForkPointDescriptor{Kind: "turn", TurnIndex: 1, TurnID: "00000000-0000-0000-0000-000000000401", SelectedAt: now}, CreatedBy: "token", CreatedAt: now, ExpiresAt: now.Add(time.Hour), State: "active", Turns: []operatorread.OperatorConversationTurn{}},
				deleteResult: runfork.ConversationForkDeleteResult{ForkID: forkID, Deleted: true},
			}
			tt.mutate(forks)
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: testOperatorHandlers(testOperatorCapabilities{
					Now:                       func() time.Time { return now },
					ConversationForks:         forks,
					ConversationForkLifecycle: forks,
					ForkChatExecutor: &fakeForkChatExecutor{result: runfork.ConversationForkChatExecution{
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

func TestOperatorConversationForkChatHeartbeatFailurePreventsExecution(t *testing.T) {
	now := time.Now().UTC()
	forkID := uuid.NewString()
	prepared := runfork.ConversationForkChatPrepared{
		Fork: runfork.OperatorConversationForkSession{ForkID: forkID}, ForkTurnID: uuid.NewString(),
		RequestOccurrenceID: uuid.NewString(), RequestHash: "request-hash", ActorTokenID: testToken,
		ExecutionOwner: "forkchat-owner", LeaseExpiresAt: now.Add(time.Minute), FenceGeneration: 1,
	}
	forks := &fakeConversationForkLifecycleStore{prepareResult: prepared, heartbeatErr: errors.New("stale forkchat authority")}
	executor := &fakeForkChatExecutor{result: runfork.ConversationForkChatExecution{AssistantMessage: "must not run"}}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now: func() time.Time { return now }, ConversationForkLifecycle: forks,
			ForkChatExecutor: executor, Idempotency: newMutatingProbeIdempotencyStore(),
		}),
	})
	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"heartbeat","method":"conversation.fork_chat","params":{"fork_id":"`+forkID+`","message":"inspect"}}`)
	if resp.Error == nil {
		t.Fatal("forkchat heartbeat failure returned success")
	}
	if forks.heartbeatCalls != 1 || executor.calls != 0 || forks.recordCalls != 0 || forks.failCalls != 1 {
		t.Fatalf("heartbeat failure calls heartbeat=%d execute=%d record=%d fail=%d", forks.heartbeatCalls, executor.calls, forks.recordCalls, forks.failCalls)
	}
	if forks.lastFailure.Prepared.ForkTurnID != prepared.ForkTurnID || forks.lastFailure.Cause == nil {
		t.Fatalf("heartbeat failure terminalization=%#v", forks.lastFailure)
	}
}

func TestOperatorConversationForkChatAdmitsExactSourceModeBeforeIdempotency(t *testing.T) {
	forkID := "00000000-0000-0000-0000-000000000301"
	forks := &fakeConversationForkLifecycleStore{admitErr: errors.New("mock_only rejects live source actor")}
	idempotency := newMutatingProbeIdempotencyStore()
	executor := &fakeForkChatExecutor{}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorConversationForkHandlers(testOperatorCapabilities{
			ExecutionPosture:          executionposture.MockOnly,
			ConversationForkLifecycle: forks,
			ForkChatExecutor:          executor,
			Idempotency:               idempotency,
		}),
	})

	response := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"chat","method":"conversation.fork_chat","params":{"fork_id":"`+forkID+`","message":"inspect","idempotency_key":"chat-1"}}`)
	if response.Error == nil {
		t.Fatalf("conversation.fork_chat response = %#v, want preflight rejection", response)
	}
	if forks.admitCalls != 1 || forks.lastAdmitID != forkID || forks.lastPosture != executionposture.MockOnly {
		t.Fatalf("chat admission = calls %d fork %q posture %q", forks.admitCalls, forks.lastAdmitID, forks.lastPosture)
	}
	if idempotency.calls != 0 || forks.prepareCalls != 0 || forks.heartbeatCalls != 0 || executor.calls != 0 || forks.recordCalls != 0 {
		t.Fatalf("rejected chat mutated downstream owners: idempotency=%d prepare=%d heartbeat=%d execute=%d record=%d", idempotency.calls, forks.prepareCalls, forks.heartbeatCalls, executor.calls, forks.recordCalls)
	}
}

func TestLLMForkChatExecutorUsesRuntimeRequestedToolsOnly(t *testing.T) {
	forkID := uuid.NewString()
	forkTurnID := uuid.NewString()
	requestOccurrenceID := uuid.NewString()
	runtimeInstanceID := uuid.NewString()
	bundleHash := "bundle-v1:sha256:" + strings.Repeat("a", 64)
	prepared := runfork.ConversationForkChatPrepared{
		Fork: runfork.OperatorConversationForkSession{
			ForkID:        forkID,
			SourceRunID:   "run-1",
			SourceAgentID: "agent-source",
		},
		Snapshot: runfork.ConversationForkSnapshot{
			SnapshotOwner: runfork.ConversationForkChatSnapshotOwner,
			SourceAgent: runtimeactors.AgentConfig{
				ID: "agent-source", Type: "managed", Role: "researcher", Model: llmselection.ModelAliasRegular,
				ExecutionMode: runtimeeffects.ExecutionModeLive, Memory: agentmemory.PlatformDefault(),
				NativeTools: runtimeactors.NativeToolConfig{Bash: true, WebSearch: true, FileIO: true},
			},
			EntitySnapshot: []runfork.ConversationForkEntitySnapshot{{
				EntityID:     "entity-1",
				CurrentState: "draft",
				Fields:       map[string]any{"name": "Before"},
			}},
		},
		SandboxPolicy: runfork.CanonicalConversationForkSandboxPolicy(),
		ForkTurnID:    forkTurnID, SourceBundleHash: bundleHash, RequestOccurrenceID: requestOccurrenceID, RequestHash: "request-hash",
		ActorTokenID: "actor-token", ExecutionOwner: "forkchat-test-owner", LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1,
	}
	prepared.AvailableTools = prepared.SandboxPolicy.AvailableToolNames()
	rt := &forkChatScriptedRuntime{
		responses: []*runtimellm.Response{
			{
				Message: runtimellm.Message{Role: "assistant", Content: "checking tools"},
				ToolCalls: []runtimellm.ToolCall{
					{Name: "fork_snapshot_read_entities", Arguments: map[string]any{"entity_id": "entity-1"}},
					{Name: "save_entity_field", Arguments: map[string]any{"entity_id": "entity-1", "field": "name", "value": "Sandbox"}},
					{Name: "emit_event", Arguments: map[string]any{"event_name": "forkchat.note"}},
					{Name: "run_start", Arguments: map[string]any{"event_name": "scan.requested"}},
				},
			},
			{Message: runtimellm.Message{Role: "assistant", Content: "snapshot says Before; writes were stubbed"}},
		},
	}
	ctx := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.RuntimeScope(runtimeInstanceID))
	execution, err := NewLLMForkChatExecutor(staticForkChatRuntimeResolver{runtime: rt}).ExecuteForkChat(ctx, prepared, "inspect and try sandbox writes")
	if err != nil {
		t.Fatalf("ExecuteForkChat: %v", err)
	}
	if rt.startAgentID != "agent-source" {
		t.Fatalf("StartSession agentID = %q, want source agent", rt.startAgentID)
	}
	if rt.actorModel != llmselection.ModelAliasRegular || rt.authority.Kind != runtimeeffects.AuthorityConversationForkChat || rt.authority.ID != forkTurnID {
		t.Fatalf("forkchat runtime authority = model:%q authority:%#v", rt.actorModel, rt.authority)
	}
	if !slices.Equal(rt.policyTools, forkChatToolNames(rt.tools)) {
		t.Fatalf("forkchat invocation policy tools = %#v, runtime tools = %#v", rt.policyTools, forkChatToolNames(rt.tools))
	}
	if rt.actorNativeTools != (runtimeactors.NativeToolConfig{}) {
		t.Fatalf("source actor native tools leaked into forkchat actor: %#v", rt.actorNativeTools)
	}
	if rt.authority.ForkChat.BundleHash != bundleHash || rt.scope != runtimeauthoractivity.BundleScope(runtimeInstanceID, bundleHash) {
		t.Fatalf("forkchat source scope = authority:%#v context:%#v", rt.authority.ForkChat, rt.scope)
	}
	if !strings.Contains(rt.systemPrompt, "isolated forensic sandbox") || !strings.Contains(rt.systemPrompt, runfork.ConversationForkChatSnapshotOwner) {
		t.Fatalf("system prompt = %q, want forkchat sandbox/snapshot context", rt.systemPrompt)
	}
	if got := forkChatToolNames(rt.tools); !stringSetContainsAll(got, "fork_snapshot_read_entities", "save_entity_field", "emit_event", "run_start") {
		t.Fatalf("runtime tools = %v", got)
	}
	if len(rt.messages) != 2 || rt.messages[0].Role != "user" || rt.messages[1].Role != "tool" {
		t.Fatalf("runtime messages = %#v, want user then tool result follow-up", rt.messages)
	}
	if execution.AssistantMessage != "snapshot says Before; writes were stubbed" {
		t.Fatalf("assistant message = %q", execution.AssistantMessage)
	}
	if len(execution.ToolCalls) != 4 {
		t.Fatalf("tool calls = %#v, want only four runtime-requested calls", execution.ToolCalls)
	}
	if findConversationForkToolCall(execution.ToolCalls, "run_stop") != nil {
		t.Fatalf("unrequested run_stop was persisted: %#v", execution.ToolCalls)
	}
	read := requireAPIForkToolCall(t, execution.ToolCalls, "fork_snapshot_read_entities")
	readResult := decodeJSONMap(t, read.Result)
	if readResult["status"] != "read_from_snapshot" || readResult["snapshot_owner"] != runfork.ConversationForkChatSnapshotOwner || readResult["entity_count"] != float64(1) {
		t.Fatalf("snapshot read result = %#v", readResult)
	}
	for _, name := range []string{"save_entity_field", "emit_event", "run_start"} {
		call := requireAPIForkToolCall(t, execution.ToolCalls, name)
		result := decodeJSONMap(t, call.Result)
		if result["status"] != "stubbed" || result["owner"] != runfork.ConversationForkChatSandboxOwner || result["live_mutation"] != false {
			t.Fatalf("%s result = %#v, want stubbed no-live-mutation", name, result)
		}
	}
	toolResults := decodeJSONArray(t, rt.messages[1].Content)
	if len(toolResults) != 4 {
		t.Fatalf("tool result payload = %#v, want four results", toolResults)
	}
}

func TestLLMForkChatExecutorRederivesSourceAgentAgainstCurrentRuntimeSet(t *testing.T) {
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendOpenAIResponses)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	rt := &forkChatScriptedRuntime{responses: []*runtimellm.Response{{
		Message: runtimellm.Message{Role: "assistant", Content: "rederived"},
	}}}
	runtimes, err := runtimellm.NewAgentRuntimeSet(profile, runtimellm.RuntimeFactory{}, rt)
	if err != nil {
		t.Fatalf("NewAgentRuntimeSet: %v", err)
	}
	bundleHash := "bundle-v1:sha256:" + strings.Repeat("b", 64)
	policy := runfork.CanonicalConversationForkSandboxPolicy()
	prepared := runfork.ConversationForkChatPrepared{
		Fork: runfork.OperatorConversationForkSession{
			ForkID: uuid.NewString(), SourceRunID: "run-1", SourceAgentID: "agent-source",
		},
		Snapshot: runfork.ConversationForkSnapshot{
			SnapshotOwner: runfork.ConversationForkChatSnapshotOwner,
			SourceAgent: runtimeactors.AgentConfig{
				ID: "agent-source", Role: "researcher",
				ResolvedLLMBackend:   llmselection.BackendAnthropic,
				ResolvedLLMProvider:  llmselection.ProviderAnthropic,
				ResolvedLLMTransport: llmselection.TransportAPI,
				ExecutionMode:        runtimeeffects.ExecutionModeLive,
			},
		},
		SandboxPolicy:  policy,
		AvailableTools: policy.AvailableToolNames(),
		ForkTurnID:     uuid.NewString(), SourceBundleHash: bundleHash,
		RequestOccurrenceID: uuid.NewString(), RequestHash: "request-hash",
		ActorTokenID: "actor-token", ExecutionOwner: "forkchat-test-owner",
		LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1,
	}
	ctx := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.RuntimeScope(uuid.NewString()))
	if _, err := NewLLMForkChatExecutor(runtimes).ExecuteForkChat(ctx, prepared, "inspect"); err != nil {
		t.Fatalf("ExecuteForkChat: %v", err)
	}
	if rt.actorAuthoredBackend != "" {
		t.Fatalf("forkchat authored llm_backend = %q, want preserved blank intent", rt.actorAuthoredBackend)
	}
	if rt.actorResolvedBackend != llmselection.BackendOpenAIResponses {
		t.Fatalf("forkchat resolved llm backend = %q, want %q", rt.actorResolvedBackend, llmselection.BackendOpenAIResponses)
	}
}

type forkChatScriptedRuntime struct {
	responses            []*runtimellm.Response
	startAgentID         string
	systemPrompt         string
	tools                []runtimellm.ToolDefinition
	messages             []runtimellm.Message
	actorModel           string
	actorAuthoredBackend string
	actorResolvedBackend string
	actorNativeTools     runtimeactors.NativeToolConfig
	authority            runtimeeffects.Authority
	scope                runtimeauthoractivity.Scope
	policyTools          []string
}

func (r *forkChatScriptedRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	r.startAgentID = agentID
	r.systemPrompt = systemPrompt
	r.tools = append([]runtimellm.ToolDefinition(nil), tools...)
	actor, _ := runtimeactors.ActorFromContext(ctx)
	r.actorModel = actor.Model
	r.actorAuthoredBackend = actor.LLMBackend
	r.actorResolvedBackend = actor.ResolvedLLMBackend
	r.actorNativeTools = actor.NativeTools
	r.authority, _ = runtimeeffects.CompletionAuthorityFromContext(ctx)
	r.scope, _ = runtimeauthoractivity.ScopeFromContext(ctx)
	r.policyTools, _ = runtimellm.ConversationForkSandboxInvocationPolicyFromContext(ctx)
	return &runtimellm.Session{ID: "forkchat-runtime-session", AgentID: agentID}, nil
}

func (r *forkChatScriptedRuntime) ContinueForkChatSession(ctx context.Context, session *runtimellm.Session, call runtimellm.ForkChatCall) (*runtimellm.Response, error) {
	message, err := call.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
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

func requireAPIForkToolCall(t *testing.T, calls []operatorread.OperatorConversationToolCall, name string) operatorread.OperatorConversationToolCall {
	t.Helper()
	if call := findConversationForkToolCall(calls, name); call != nil {
		if len(call.Result) == 0 {
			t.Fatalf("%s tool call missing result: %#v", name, *call)
		}
		return *call
	}
	t.Fatalf("tool call %s missing from %#v", name, calls)
	return operatorread.OperatorConversationToolCall{}
}

func findConversationForkToolCall(calls []operatorread.OperatorConversationToolCall, name string) *operatorread.OperatorConversationToolCall {
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
			forkPoint: `{"kind":"turn","turn_id":"00000000-0000-0000-0000-000000000401","entity_snapshot":{}}`,
			wantField: "fork_point.entity_snapshot",
		},
		{
			name:      "unknown include original",
			forkPoint: `{"kind":"turn","turn_id":"00000000-0000-0000-0000-000000000401","include_original":true}`,
			wantField: "fork_point.include_original",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			forks := &fakeConversationForkLifecycleStore{}
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: testOperatorHandlers(testOperatorCapabilities{
					Now:                       func() time.Time { return now },
					ConversationForkLifecycle: forks,
					Idempotency:               newMutatingProbeIdempotencyStore(),
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
		createResult: runfork.OperatorConversationForkSession{
			ForkID:          "00000000-0000-0000-0000-000000000301",
			SourceSessionID: sourceSessionID,
			SourceAgentID:   "agent-1",
			ForkPoint:       runfork.ConversationForkPointDescriptor{Kind: "turn", TurnIndex: 1, TurnID: "00000000-0000-0000-0000-000000000401", SelectedAt: now},
			CreatedBy:       "token",
			CreatedAt:       now,
			ExpiresAt:       now.Add(time.Hour),
			State:           "active",
			Turns:           []operatorread.OperatorConversationTurn{},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:                       func() time.Time { return now },
			ConversationForkLifecycle: forks,
			Idempotency:               newMutatingProbeIdempotencyStore(),
		}),
	})
	first := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"first","method":"conversation.fork","params":{"source_session_id":"`+sourceSessionID+`","fork_point":{"kind":"turn","turn_id":"00000000-0000-0000-0000-000000000401"},"idempotency_key":"fork-key"}}`)
	if first.Error != nil {
		t.Fatalf("conversation.fork first error = %#v", first.Error)
	}
	conflict := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"conflict","method":"conversation.fork","params":{"source_session_id":"`+sourceSessionID+`","fork_point":{"kind":"turn","turn_id":"00000000-0000-0000-0000-000000000402"},"idempotency_key":"fork-key"}}`)
	if conflict.Error == nil {
		t.Fatal("conversation.fork conflict error = nil")
	}
	data := asMap(t, conflict.Error.Data)
	if data["code"] != IdempotencyConflictCode {
		t.Fatalf("conversation.fork conflict data = %#v, want %s", data, IdempotencyConflictCode)
	}
}

func TestConversationForkErrorMapsParamErrors(t *testing.T) {
	err := conversationForkError(&operatorread.EntityReadParamError{Field: "source_session_id", Reason: "must be a UUID"}, conversationForkErrorDetails{})
	var invalid *InvalidParamsError
	if !errors.As(err, &invalid) {
		t.Fatalf("conversationForkError = %T, want InvalidParamsError", err)
	}
}
