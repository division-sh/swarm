package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type resetTestBus struct {
	entries    []runtimepipeline.RuntimeLogEntry
	publishes  []events.Event
	publishErr error
	resetErr   error
}

func (b *resetTestBus) Publish(_ context.Context, evt events.Event) error {
	if b.publishErr != nil {
		return b.publishErr
	}
	b.publishes = append(b.publishes, evt)
	return nil
}
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
func (b *resetTestBus) ResetInMemoryState() error    { return b.resetErr }
func (b *resetTestBus) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.entries = append(b.entries, entry)
	return nil
}

type resetTestRegistry struct {
	summary sessions.ResetSummary
}

func (*resetTestRegistry) Acquire(context.Context, agentmemory.Identity, string) (*sessions.Lease, error) {
	return nil, nil
}
func (*resetTestRegistry) Release(context.Context, *sessions.Lease) error { return nil }
func (*resetTestRegistry) Rotate(context.Context, agentmemory.Identity, string, sessions.RotationMetadata) (*sessions.Lease, error) {
	return nil, nil
}
func (*resetTestRegistry) IncrementTurn(context.Context, agentmemory.Identity, string) error {
	return nil
}
func (r *resetTestRegistry) ResetAll(sessions.ResetMetadata) (sessions.ResetSummary, error) {
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
			RunID:             "run-1",
			FlowInstance:      "support/inst-1",
			PreviousStatus:    "active",
			TerminationReason: sessions.TerminationReasonOrphaned.String(),
			TerminationDetail: "admin_cli",
		}},
	}}

	am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{
		Sessions: registry,
	})
	if err := am.ResetRuntimeStateWithSource("admin_cli"); err != nil {
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
	if got := detail["source"]; got != "admin_cli" {
		t.Fatalf("detail.source = %#v, want admin_cli", got)
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

func TestResetRuntimeStateWithSource_PublishesPlatformResetOnlyForExplicitAdminSources(t *testing.T) {
	t.Run("admin_cli", func(t *testing.T) {
		bus := &resetTestBus{}
		am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{})

		if err := am.ResetRuntimeStateWithSource("admin_cli"); err != nil {
			t.Fatalf("ResetRuntimeStateWithSource(admin_cli): %v", err)
		}
		if len(bus.publishes) != 1 {
			t.Fatalf("published event count = %d, want 1", len(bus.publishes))
		}
		if got := string(bus.publishes[0].Type()); got != "platform.reset" {
			t.Fatalf("published event type = %q, want platform.reset", got)
		}
	})

	t.Run("builder api retired", func(t *testing.T) {
		bus := &resetTestBus{}
		am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{})

		if err := am.ResetRuntimeStateWithSource("builder_api"); err != nil {
			t.Fatalf("ResetRuntimeStateWithSource(builder_api): %v", err)
		}
		if len(bus.publishes) != 0 {
			t.Fatalf("published event count = %d, want 0", len(bus.publishes))
		}
	})

	t.Run("startup recovery fallback", func(t *testing.T) {
		bus := &resetTestBus{}
		am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{})

		if err := am.ResetRuntimeStateWithSource("startup_recovery_failed"); err != nil {
			t.Fatalf("ResetRuntimeStateWithSource(startup_recovery_failed): %v", err)
		}
		if len(bus.publishes) != 0 {
			t.Fatalf("published event count = %d, want 0", len(bus.publishes))
		}
	})
}

func TestResetRuntimeStateFailureAlwaysLeavesResetPhase(t *testing.T) {
	tests := []struct {
		name          string
		bus           *resetTestBus
		source        string
		wantCellAfter bool
	}{
		{name: "bus reset failure preserves lifecycle cells", bus: &resetTestBus{resetErr: errors.New("reset failed")}, wantCellAfter: true},
		{name: "reset event failure finalizes cleared lifecycle cells", bus: &resetTestBus{publishErr: errors.New("publish failed")}, source: "admin_cli", wantCellAfter: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			am := NewAgentManagerWithOptions(tt.bus, nil, AgentManagerOptions{})
			rec := lifecycleTestPersistedAgent()
			if err := am.lifecycle.register(context.Background(), rec, false); err != nil {
				t.Fatalf("register lifecycle cell: %v", err)
			}

			if err := am.ResetRuntimeStateWithSource(tt.source); err == nil {
				t.Fatal("ResetRuntimeStateWithSource succeeded despite injected failure")
			}
			if phase := am.lifecycle.phaseSnapshot(); phase != runtimeLifecycleStopped {
				t.Fatalf("lifecycle phase = %q, want stopped", phase)
			}
			am.lifecycle.mu.Lock()
			_, cellExists := am.lifecycle.cells[rec.Config.ID]
			am.lifecycle.mu.Unlock()
			if cellExists != tt.wantCellAfter {
				t.Fatalf("lifecycle cell exists = %v, want %v", cellExists, tt.wantCellAfter)
			}
			if _, started := am.lifecycle.beginRun(context.Background(), AgentRunModeStandard); !started {
				t.Fatal("runtime remained blocked after reset failure")
			}
			am.lifecycle.beginShutdownAdmission()
			am.lifecycle.cancelShutdownWork()
			am.lifecycle.finishShutdown()
		})
	}
}
