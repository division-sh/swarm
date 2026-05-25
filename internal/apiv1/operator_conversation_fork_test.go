package apiv1

import (
	"context"
	"errors"
	"testing"
	"time"

	"swarm/internal/store"
)

type fakeConversationForkLifecycleStore struct {
	createResult store.OperatorConversationForkSession
	createErr    error
	listResult   store.ConversationForkListResult
	listErr      error
	viewResult   store.OperatorConversationForkSession
	viewErr      error
	deleteResult store.ConversationForkDeleteResult
	deleteErr    error

	createCalls int
	listCalls   int
	viewCalls   int
	deleteCalls int

	lastCreate store.ConversationForkCreateRequest
	lastList   store.ConversationForkListOptions
	lastViewID string
	lastDelete string
	lastNow    time.Time

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
		deleteResult: store.ConversationForkDeleteResult{ForkID: forkID, Deleted: true},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:               func() time.Time { return now },
			ConversationForks: forks,
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
					Idempotency:       newMutatingProbeIdempotencyStore(),
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

func TestOperatorConversationForkRejectsUnsupportedForkPointKindBeforeOwner(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	forks := &fakeConversationForkLifecycleStore{}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:               func() time.Time { return now },
			ConversationForks: forks,
			Idempotency:       newMutatingProbeIdempotencyStore(),
		}),
	})
	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"bad","method":"conversation.fork","params":{"source_session_id":"00000000-0000-0000-0000-000000000201","fork_point":{"kind":"bogus"}}}`)
	if resp.Error == nil || resp.Error.Code != codeInvalidParams {
		t.Fatalf("conversation.fork malformed fork_point error = %#v, want invalid params", resp.Error)
	}
	if forks.createCalls != 0 {
		t.Fatalf("create owner calls = %d, want 0 for malformed fork_point", forks.createCalls)
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
