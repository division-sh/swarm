package manager

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type reconfigureTestAgent struct{ id string }

func (a reconfigureTestAgent) ID() string { return a.id }
func (reconfigureTestAgent) Type() string { return "generic" }
func (reconfigureTestAgent) Subscriptions() []events.EventType {
	return nil
}
func (reconfigureTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func TestReconfigureAgent_SameCurrentPreservesExecutionIdentityWithoutFactoryInvocation(t *testing.T) {
	builds := 0
	am := NewAgentManager(nil, func(cfg models.AgentConfig) (Agent, error) {
		builds++
		return &reconfigureTestAgent{id: cfg.ID}, nil
	})
	cfg := models.AgentConfig{ID: "same-current-agent", Tools: []string{"tool-a"}}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	beforeExecution, ok := am.lifecycle.executionSnapshot(cfg.ID)
	if !ok {
		t.Fatal("spawned execution is absent")
	}
	before := beforeExecution.Agent
	beforeGeneration := lifecycleGenerationForTest(t, am, cfg.ID)

	if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{Tools: []string{"tool-a"}}); err != nil {
		t.Fatalf("ReconfigureAgent(same current): %v", err)
	}

	if builds != 1 {
		t.Fatalf("factory builds = %d, want 1", builds)
	}
	afterExecution, ok := am.lifecycle.executionSnapshot(cfg.ID)
	if !ok {
		t.Fatal("same-current execution is absent")
	}
	if got := afterExecution.Agent; got != before {
		t.Fatalf("execution agent changed on same-current reconfigure: before=%p after=%p", before, got)
	}
	if got := lifecycleGenerationForTest(t, am, cfg.ID); got != beforeGeneration {
		t.Fatalf("generation = %d, want unchanged %d", got, beforeGeneration)
	}
}

func TestReconfigureAgent_RotatesFlowScopedSession(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := NewAgentManagerWithOptions(nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})

	cfg := models.AgentConfig{
		ID:               "flow-agent",
		ConversationMode: sessions.RuntimeModeSession.String(),
		SessionScope:     sessions.SessionScopeFlow.String(),
		FlowPath:         "review/inst-1",
	}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	seedCtx := effects.WithDifferentOwner(models.WithActor(context.Background(), cfg), effects.OwnerBuildTestInfrastructure)
	lease, err := registry.Acquire(seedCtx, cfg.ID, sessions.NormalizeConversationRuntimeMode(cfg.ConversationMode), sessions.NormalizeSessionScope(cfg.SessionScope), "reconfigure", cfg.CanonicalFlowPath())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{Tools: []string{"agent_message"}}); err != nil {
		t.Fatalf("ReconfigureAgent(flow): %v", err)
	}

	rec, ok := registry.Snapshot(cfg.ID)
	if !ok {
		t.Fatal("expected flow-scoped session record")
	}
	if rec.SessionID == lease.SessionID {
		t.Fatalf("expected rotated session id, got unchanged %q", rec.SessionID)
	}
	if rec.ScopeKey != cfg.CanonicalFlowPath() {
		t.Fatalf("scope_key = %q, want %q", rec.ScopeKey, cfg.CanonicalFlowPath())
	}
}

func TestReconfigureAgent_RotatesEntityScopedSession(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := NewAgentManagerWithOptions(nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})

	cfg := models.AgentConfig{
		ID:               "entity-agent",
		ConversationMode: sessions.RuntimeModeSessionPerEntity.String(),
		SessionScope:     sessions.SessionScopeEntity.String(),
		FlowPath:         "review/inst-1",
		EntityID:         "entity-1",
	}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	seedCtx := effects.WithDifferentOwner(models.WithActor(context.Background(), cfg), effects.OwnerBuildTestInfrastructure)
	lease, err := registry.Acquire(seedCtx, cfg.ID, sessions.NormalizeConversationRuntimeMode(cfg.ConversationMode), sessions.NormalizeSessionScope(cfg.SessionScope), "reconfigure", cfg.EffectiveEntityID())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{Tools: []string{"agent_message"}}); err != nil {
		t.Fatalf("ReconfigureAgent(entity): %v", err)
	}

	rec, ok := registry.Snapshot(cfg.ID)
	if !ok {
		t.Fatal("expected entity-scoped session record")
	}
	if rec.SessionID == lease.SessionID {
		t.Fatalf("expected rotated session id, got unchanged %q", rec.SessionID)
	}
	if rec.ScopeKey != cfg.EffectiveEntityID() {
		t.Fatalf("scope_key = %q, want %q", rec.ScopeKey, cfg.EffectiveEntityID())
	}
}

func TestReconfigureAgent_ClearsSessionScopeWhenSwitchingToTask(t *testing.T) {
	am := NewAgentManager(nil, func(cfg models.AgentConfig) (Agent, error) {
		if _, err := sessions.ValidateAgentSessionScopeConfig(cfg); err != nil {
			return nil, err
		}
		return reconfigureTestAgent{id: cfg.ID}, nil
	})

	cfg := models.AgentConfig{
		ID:               "task-switch-agent",
		ConversationMode: sessions.RuntimeModeSession.String(),
		SessionScope:     sessions.SessionScopeFlow.String(),
		FlowPath:         "support/inst-1",
	}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{ConversationMode: sessions.RuntimeModeTask.String()}); err != nil {
		t.Fatalf("ReconfigureAgent(task): %v", err)
	}

	got, ok := am.GetAgentConfig(cfg.ID)
	if !ok {
		t.Fatal("reconfigured config is absent")
	}
	if got.ConversationMode != sessions.RuntimeModeTask.String() {
		t.Fatalf("ConversationMode = %q, want %q", got.ConversationMode, sessions.RuntimeModeTask.String())
	}
	if got.SessionScope != "" {
		t.Fatalf("SessionScope = %q, want empty", got.SessionScope)
	}
}

func TestReconfigureAgent_ReplaySessionToTaskIsNoOp(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := NewAgentManagerWithOptions(nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})

	cfg := models.AgentConfig{
		ID:               "session-to-task-replay-agent",
		ConversationMode: sessions.RuntimeModeSession.String(),
		SessionScope:     sessions.SessionScopeFlow.String(),
		FlowPath:         "support/inst-1",
	}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	seedCtx := effects.WithDifferentOwner(models.WithActor(context.Background(), cfg), effects.OwnerBuildTestInfrastructure)
	if _, err := registry.Acquire(seedCtx, cfg.ID, sessions.RuntimeModeSession, sessions.SessionScopeFlow, "reconfigure", cfg.CanonicalFlowPath()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	target := models.AgentConfig{ConversationMode: sessions.RuntimeModeTask.String()}
	if err := am.ReconfigureAgent(cfg.ID, target); err != nil {
		t.Fatalf("first ReconfigureAgent(task): %v", err)
	}
	generation := lifecycleGenerationForTest(t, am, cfg.ID)
	if err := am.ReconfigureAgent(cfg.ID, target); err != nil {
		t.Fatalf("replayed ReconfigureAgent(task): %v", err)
	}
	if got := lifecycleGenerationForTest(t, am, cfg.ID); got != generation {
		t.Fatalf("replayed generation = %d, want unchanged %d", got, generation)
	}
}

func TestReconfigureAgent_ReplayTaskToSessionIsNoOp(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := NewAgentManagerWithOptions(nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})

	cfg := models.AgentConfig{ID: "task-to-session-replay-agent", ConversationMode: sessions.RuntimeModeTask.String(), FlowPath: "support/inst-1"}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	target := models.AgentConfig{
		ConversationMode: sessions.RuntimeModeSession.String(),
		SessionScope:     sessions.SessionScopeFlow.String(),
	}
	if err := am.ReconfigureAgent(cfg.ID, target); err != nil {
		t.Fatalf("first ReconfigureAgent(session): %v", err)
	}
	generation := lifecycleGenerationForTest(t, am, cfg.ID)
	if err := am.ReconfigureAgent(cfg.ID, target); err != nil {
		t.Fatalf("replayed ReconfigureAgent(session): %v", err)
	}
	if got := lifecycleGenerationForTest(t, am, cfg.ID); got != generation {
		t.Fatalf("replayed generation = %d, want unchanged %d", got, generation)
	}
}

func TestReconfigureAgent_RepeatedTargetEdgesAreDistinctOccurrences(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := NewAgentManagerWithOptions(nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})
	cfg := models.AgentConfig{
		ID:               "reconfigure-cycle-agent",
		ConversationMode: sessions.RuntimeModeSession.String(),
		SessionScope:     sessions.SessionScopeFlow.String(),
		FlowPath:         "support/inst-cycle",
	}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	seedCtx := effects.WithDifferentOwner(models.WithActor(context.Background(), cfg), effects.OwnerBuildTestInfrastructure)
	lease, err := registry.Acquire(seedCtx, cfg.ID, sessions.RuntimeModeSession, sessions.SessionScopeFlow, "reconfigure", cfg.CanonicalFlowPath())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	initialGeneration := lifecycleGenerationForTest(t, am, cfg.ID)
	previousSessionID := lease.SessionID
	for i, tool := range []string{"tool-a", "tool-b", "tool-a", "tool-b"} {
		if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{Tools: []string{tool}}); err != nil {
			t.Fatalf("ReconfigureAgent occurrence %d (%s): %v", i+1, tool, err)
		}
		if got, want := lifecycleGenerationForTest(t, am, cfg.ID), initialGeneration+uint64(i)+1; got != want {
			t.Fatalf("occurrence %d generation = %d, want %d", i+1, got, want)
		}
		rec, ok := registry.Snapshot(cfg.ID)
		if !ok {
			t.Fatalf("occurrence %d session snapshot missing", i+1)
		}
		if rec.SessionID == previousSessionID {
			t.Fatalf("occurrence %d did not rotate session %q", i+1, previousSessionID)
		}
		previousSessionID = rec.SessionID
	}

	finalGeneration := lifecycleGenerationForTest(t, am, cfg.ID)
	if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{Tools: []string{"tool-b"}}); err != nil {
		t.Fatalf("same-current ReconfigureAgent: %v", err)
	}
	if got := lifecycleGenerationForTest(t, am, cfg.ID); got != finalGeneration {
		t.Fatalf("same-current generation = %d, want unchanged %d", got, finalGeneration)
	}
	if rec, ok := registry.Snapshot(cfg.ID); !ok || rec.SessionID != previousSessionID {
		t.Fatalf("same-current session = %#v ok=%v, want unchanged %q", rec, ok, previousSessionID)
	}
}

func TestReconfigureAgent_ConcurrentPartialPatchesSerializeFromCurrentConfig(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	firstBuildEntered := make(chan struct{}, 1)
	releaseFirstBuild := make(chan struct{})
	secondBuildEntered := make(chan struct{}, 1)
	releaseSecondBuild := make(chan struct{})
	am := NewAgentManagerWithOptions(nil, func(cfg models.AgentConfig) (Agent, error) {
		if cfg.ID == "serialized-partial-patch-agent" {
			switch {
			case cfg.ConversationMode == sessions.RuntimeModeTask.String() && len(cfg.Tools) == 1 && cfg.Tools[0] == "tool-a":
				firstBuildEntered <- struct{}{}
				<-releaseFirstBuild
			case len(cfg.Tools) == 1 && cfg.Tools[0] == "tool-b":
				secondBuildEntered <- struct{}{}
				<-releaseSecondBuild
			}
		}
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})
	cfg := models.AgentConfig{
		ID:               "serialized-partial-patch-agent",
		ConversationMode: sessions.RuntimeModeSession.String(),
		SessionScope:     sessions.SessionScopeFlow.String(),
		FlowPath:         "support/serialized",
		Tools:            []string{"tool-a"},
	}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	seedCtx := effects.WithDifferentOwner(models.WithActor(context.Background(), cfg), effects.OwnerBuildTestInfrastructure)
	lease, err := registry.Acquire(seedCtx, cfg.ID, sessions.RuntimeModeSession, sessions.SessionScopeFlow, "reconfigure", cfg.CanonicalFlowPath())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	initialGeneration := lifecycleGenerationForTest(t, am, cfg.ID)

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- am.ReconfigureAgent(cfg.ID, models.AgentConfig{ConversationMode: sessions.RuntimeModeTask.String()})
	}()
	<-firstBuildEntered
	secondStarted := make(chan struct{})
	secondErr := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondErr <- am.ReconfigureAgent(cfg.ID, models.AgentConfig{Tools: []string{"tool-b"}})
	}()
	<-secondStarted

	secondBuiltBeforeFirstCommit := false
	select {
	case <-secondBuildEntered:
		secondBuiltBeforeFirstCommit = true
	case <-time.After(500 * time.Millisecond):
	}
	close(releaseFirstBuild)
	if err := <-firstErr; err != nil {
		t.Fatalf("task reconfigure: %v", err)
	}
	if !secondBuiltBeforeFirstCommit {
		<-secondBuildEntered
	}
	close(releaseSecondBuild)
	if err := <-secondErr; err != nil {
		t.Fatalf("tools reconfigure: %v", err)
	}
	if secondBuiltBeforeFirstCommit {
		t.Fatal("disjoint partial patch was built before the prior reconfigure committed and projected")
	}

	got, ok := am.GetAgentConfig(cfg.ID)
	if !ok {
		t.Fatal("reconfigured agent config missing")
	}
	if got.ConversationMode != sessions.RuntimeModeTask.String() || len(got.Tools) != 1 || got.Tools[0] != "tool-b" {
		t.Fatalf("final config = mode:%q tools:%v, want task + tool-b", got.ConversationMode, got.Tools)
	}
	if got := lifecycleGenerationForTest(t, am, cfg.ID); got != initialGeneration+2 {
		t.Fatalf("final generation = %d, want %d", got, initialGeneration+2)
	}
	if current, ok := registry.Snapshot(cfg.ID); ok {
		t.Fatalf("current session survived task transition: %#v", current)
	}
	history := registry.History(cfg.ID)
	if len(history) != 1 || history[0].SessionID != lease.SessionID || history[0].Status != "terminated" || history[0].SuccessorSessionID != "" {
		t.Fatalf("session history = %#v, want exact terminated predecessor without successor", history)
	}
}

func lifecycleGenerationForTest(t *testing.T, am *AgentManager, agentID string) uint64 {
	t.Helper()
	am.lifecycle.mu.Lock()
	defer am.lifecycle.mu.Unlock()
	cell := am.lifecycle.cells[agentID]
	if cell == nil {
		t.Fatalf("lifecycle cell %q is absent", agentID)
	}
	return cell.generation
}

func TestReconfigureAgent_RejectsAuthoredGlobalSessionScope(t *testing.T) {
	am := NewAgentManager(nil, func(cfg models.AgentConfig) (Agent, error) {
		if _, err := sessions.ValidateAgentSessionScopeConfig(cfg); err != nil {
			return nil, err
		}
		return reconfigureTestAgent{id: cfg.ID}, nil
	})

	cfg := models.AgentConfig{
		ID:               "global-reconfigure-agent",
		ConversationMode: sessions.RuntimeModeSession.String(),
		SessionScope:     sessions.SessionScopeFlow.String(),
		FlowPath:         "support/inst-1",
	}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{SessionScope: sessions.SessionScopeGlobal.String()})
	if err == nil {
		t.Fatal("expected authored global session scope reconfigure to fail")
	}
	if got := err.Error(); !strings.Contains(got, "authored normal agents cannot declare session_scope global") {
		t.Fatalf("ReconfigureAgent error = %q", got)
	}
}

func TestSpawnAgent_AllowsPlatformInternalGlobalSessionScope(t *testing.T) {
	am := NewAgentManager(nil, func(cfg models.AgentConfig) (Agent, error) {
		if _, err := sessions.ValidateAgentSessionScopeConfig(cfg); err != nil {
			return nil, err
		}
		return reconfigureTestAgent{id: cfg.ID}, nil
	})

	cfg := models.AgentConfig{
		ID:                    "platform-global-agent",
		Role:                  "platform",
		ConversationMode:      sessions.RuntimeModeSession.String(),
		SessionScope:          sessions.SessionScopeGlobal.String(),
		SessionScopeAuthority: models.SessionScopeAuthorityPlatformInternal,
	}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent(platform internal global): %v", err)
	}
	got, ok := am.GetAgentConfig(cfg.ID)
	if !ok {
		t.Fatalf("expected spawned platform internal agent")
	}
	if got.SessionScope != sessions.SessionScopeGlobal.String() || !got.HasPlatformInternalSessionScopeAuthority() {
		t.Fatalf("spawned cfg = %+v, want platform internal global session", got)
	}
}
