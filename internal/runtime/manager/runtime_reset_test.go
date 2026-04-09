package manager

import (
	"context"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/sessions"
)

type resetTestBus struct {
	entries []runtimepipeline.RuntimeLogEntry
}

func (b *resetTestBus) Publish(context.Context, events.Event) error { return nil }
func (b *resetTestBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (b *resetTestBus) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	return nil
}
func (b *resetTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (b *resetTestBus) Unsubscribe(string)           {}
func (b *resetTestBus) Store() runtimebus.EventStore { return runtimebus.InMemoryEventStore{} }
func (b *resetTestBus) ResetInMemoryState() error    { return nil }
func (b *resetTestBus) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.entries = append(b.entries, entry)
	return nil
}

type resetTestRegistry struct {
	summary sessions.ResetSummary
}

func (*resetTestRegistry) Acquire(context.Context, string, sessions.RuntimeMode, sessions.SessionScope, string, string) (*sessions.Lease, error) {
	return nil, nil
}
func (*resetTestRegistry) Release(context.Context, *sessions.Lease) error { return nil }
func (*resetTestRegistry) Rotate(context.Context, string, sessions.RuntimeMode, sessions.SessionScope, string, sessions.RotationMetadata, string) (*sessions.Lease, error) {
	return nil, nil
}
func (*resetTestRegistry) IncrementTurn(context.Context, string, sessions.RuntimeMode, sessions.SessionScope, string, string) error {
	return nil
}
func (r *resetTestRegistry) ResetAll(sessions.RuntimeMode, sessions.ResetMetadata) (sessions.ResetSummary, error) {
	return r.summary, nil
}

func TestResetRuntimeState_OnlyResetsOwnedTurnContextRegistry(t *testing.T) {
	registryA := runtimemcp.NewTurnContextRegistry(nil)
	registryB := runtimemcp.NewTurnContextRegistry(nil)

	registryA.PutTurnContextForTest("ctx-shared", runtimemcp.TurnContext{
		Actor:     runtimeactors.AgentConfig{ID: "agent-a"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	registryB.PutTurnContextForTest("ctx-shared", runtimemcp.TurnContext{
		Actor:     runtimeactors.AgentConfig{ID: "agent-b"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{
		ResetRuntimeOwnedState: registryA.Reset,
	})
	if err := am.ResetRuntimeState(); err != nil {
		t.Fatalf("ResetRuntimeState: %v", err)
	}

	if _, ok := registryA.ResolveTurnContext("ctx-shared"); ok {
		t.Fatal("registryA should be empty after reset")
	}
	turn, ok := registryB.ResolveTurnContext("ctx-shared")
	if !ok {
		t.Fatal("registryB should retain its turn context")
	}
	if turn.Actor.ID != "agent-b" {
		t.Fatalf("registryB actor id = %q, want agent-b", turn.Actor.ID)
	}
}

func TestResetRuntimeState_LogsCanonicalOrphanedSessionAftermath(t *testing.T) {
	bus := &resetTestBus{}
	registry := &resetTestRegistry{summary: sessions.ResetSummary{
		OrphanedSessions: []sessions.ResetDisposition{{
			SessionID:         "sess-1",
			AgentID:           "agent-a",
			RuntimeMode:       sessions.RuntimeModeSession,
			ScopeKey:          "global",
			PreviousStatus:    "active",
			TerminationReason: sessions.TerminationReasonOrphaned.String(),
			TerminationDetail: "builder_api",
		}},
	}}

	am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{
		RuntimeMode: "session",
		Sessions:    registry,
	})
	if err := am.ResetRuntimeStateWithSource("builder_api"); err != nil {
		t.Fatalf("ResetRuntimeStateWithSource: %v", err)
	}

	if len(bus.entries) != 1 {
		t.Fatalf("runtime log entry count = %d, want 1", len(bus.entries))
	}
	entry := bus.entries[0]
	if entry.Component != "runtime" {
		t.Fatalf("runtime log component = %q, want runtime", entry.Component)
	}
	if entry.Action != "reset_orphaned_sessions" {
		t.Fatalf("runtime log action = %q, want reset_orphaned_sessions", entry.Action)
	}
	if entry.Level != "warn" {
		t.Fatalf("runtime log level = %q, want warn", entry.Level)
	}
	detail, ok := entry.Detail.(map[string]any)
	if !ok {
		t.Fatalf("runtime log detail = %#v, want map", entry.Detail)
	}
	if got := detail["source"]; got != "builder_api" {
		t.Fatalf("detail.source = %#v, want builder_api", got)
	}
	if got := detail["orphaned_session_count"]; got != 1 {
		t.Fatalf("detail.orphaned_session_count = %#v, want 1", got)
	}
	records, ok := detail["orphaned_sessions"].([]map[string]any)
	if !ok || len(records) != 1 {
		t.Fatalf("detail.orphaned_sessions = %#v, want one record", detail["orphaned_sessions"])
	}
	if got := records[0]["session_id"]; got != "sess-1" {
		t.Fatalf("detail.orphaned_sessions[0].session_id = %#v, want sess-1", got)
	}
	if got := records[0]["termination_reason"]; got != "orphaned" {
		t.Fatalf("detail.orphaned_sessions[0].termination_reason = %#v, want orphaned", got)
	}
}
