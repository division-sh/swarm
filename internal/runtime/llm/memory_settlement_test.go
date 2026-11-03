package llm

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type settlementMemoryAcquirer struct {
	sessions.Registry
	record ConversationRecord
	calls  int
}

func (s *settlementMemoryAcquirer) AcquireLiveSession(ctx context.Context, identity agentmemory.Identity, owner string) (*sessions.Lease, ConversationRecord, error) {
	s.calls++
	lease, err := s.Registry.Acquire(ctx, identity, owner)
	return lease, s.record, err
}

func TestUnemittedMemorySettlementUsesAcquiredSnapshotAndReleasesGrant(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed", true: "write_refusal"}[fail], func(t *testing.T) {
			ctx := context.Background()
			identity := testMemoryIdentity("worker", "scope/instance")
			registry := sessions.NewInMemoryRegistry(time.Minute)
			lease, err := registry.Acquire(ctx, identity, "owner")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := registry.ReleaseOutcome(ctx, lease); err != nil {
				t.Fatal(err)
			}
			messages := []Message{{Role: "user", Content: "environment", RuntimeContext: "environment"},
				{Role: "user", Content: "previous"}, {Role: "assistant", ToolCalls: []ToolCall{{ID: "previous-call", Name: "emit_done"}}},
				{Role: "tool", Content: "previous-result"}, {Role: "user", Content: "current [EMAIL]"},
				{Role: "assistant", Content: "unsent"}}
			session := &Session{ID: lease.SessionID, AgentID: identity.AgentID(), Memory: testMemory(), MemoryIdentity: identity,
				Messages: append([]Message(nil), messages...), TurnCount: 3}
			session.Messages[4].Content = "current person@example.test"
			acquirer := &settlementMemoryAcquirer{Registry: registry, record: ConversationRecord{
				SessionID: session.ID, AgentID: session.AgentID, Identity: identity, Memory: session.Memory,
				Messages: messages, TurnCount: 3, Status: "active", Summary: "unchanged summary", Watchdog: &ConversationWatchdog{State: "unchanged"}}}
			store := &captureConversationStore{}
			var persistence ConversationPersistence = store
			wantErr := errors.New("settlement write refused")
			if fail {
				persistence = &failingConversationStore{err: wantErr}
			}
			before := append([]Message(nil), session.Messages...)
			err = settleUnemittedMemory(ctx, session, acquirer, registry, persistence, "owner", nil)
			if fail {
				if !errors.Is(err, wantErr) || !reflect.DeepEqual(session.Messages, before) {
					t.Fatalf("failed write changed local memory: err=%v messages=%+v", err, session.Messages)
				}
			} else {
				want := acquirer.record
				want.Messages = messages[:5]
				if err != nil || store.upsertCount != 1 || !reflect.DeepEqual(store.record, want) || !reflect.DeepEqual(session.Messages, want.Messages) {
					t.Fatalf("settled memory lost acquired metadata/prefix: err=%v record=%+v local=%+v", err, store.record, session.Messages)
				}
			}
			current, err := registry.Acquire(ctx, identity, "different-owner")
			if err != nil {
				t.Fatalf("settlement leaked exact grant: %v", err)
			}
			if _, err := registry.ReleaseOutcome(ctx, current); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUnemittedMemorySettlementRefusesSnapshotDrift(t *testing.T) {
	for _, field := range []string{"session", "identity", "turn_count", "message_role", "tool_link"} {
		t.Run(field, func(t *testing.T) {
			ctx := context.Background()
			identity := testMemoryIdentity("worker", "scope/instance")
			registry := sessions.NewInMemoryRegistry(time.Minute)
			lease, err := registry.Acquire(ctx, identity, "owner")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := registry.ReleaseOutcome(ctx, lease); err != nil {
				t.Fatal(err)
			}
			session := &Session{ID: lease.SessionID, AgentID: identity.AgentID(), Memory: testMemory(), MemoryIdentity: identity, TurnCount: 1,
				Messages: []Message{{Role: "user", Content: "current"}, {Role: "assistant", ToolCalls: []ToolCall{{ID: "call", Name: "inspect"}}}}}
			record, _, err := memoryConversationRecord(session)
			if err != nil {
				t.Fatal(err)
			}
			record.Messages = append([]Message(nil), record.Messages...)
			switch field {
			case "session":
				record.SessionID = "foreign"
			case "identity":
				record.Identity.RunID = "foreign"
			case "turn_count":
				record.TurnCount++
			case "message_role":
				record.Messages[1].Role = "tool"
			case "tool_link":
				record.Messages[1].ToolCalls = []ToolCall{{ID: "foreign", Name: "inspect"}}
			}
			store := &captureConversationStore{}
			acquirer := &settlementMemoryAcquirer{Registry: registry, record: record}
			if err := settleUnemittedMemory(ctx, session, acquirer, registry, store, "owner", nil); err == nil || store.upsertCount != 0 {
				t.Fatalf("snapshot drift admitted: field=%s err=%v writes=%d", field, err, store.upsertCount)
			}
			current, err := registry.Acquire(ctx, identity, "different-owner")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := registry.ReleaseOutcome(ctx, current); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMissingEmissionRecorderDoesNotSettleMemory(t *testing.T) {
	runtime := NewOpenAICompatibleRuntime(&config.Config{}, nil, "owner", &captureConversationStore{}, nil)
	conversation := &Conversation{runtime: runtime, Session: &Session{Memory: testMemory()}}
	if err := conversation.settleUnemittedMemory(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runtime.conversations.(*captureConversationStore).upsertCount != 0 {
		t.Fatal("missing recorder became a no-emission outcome")
	}
}
