package sessions

import (
	"context"
	"testing"
	"time"

	runtimeactors "swarm/internal/runtime/core/actors"
)

func TestScopeFromContext_DefaultsToTask(t *testing.T) {
	scope := ScopeFromContext(nil)
	if scope.ConversationMode != "" || scope.SessionScope != "" || scope.ScopeKey != "" {
		t.Fatalf("unexpected nil-context scope: %+v", scope)
	}

	ctx := WithScope(context.Background(), "TASK", " FLOW ", "  abc  ")
	scope = ScopeFromContext(ctx)
	if scope.ConversationMode != "TASK" || scope.SessionScope != "FLOW" || scope.ScopeKey != "abc" {
		t.Fatalf("unexpected scoped context: %+v", scope)
	}
}

func TestNormalizeConversationRuntimeMode_AcceptsStatelessAlias(t *testing.T) {
	if got := NormalizeConversationRuntimeMode("stateless"); got != RuntimeModeTask {
		t.Fatalf("NormalizeConversationRuntimeMode(stateless) = %q, want %q", got, RuntimeModeTask)
	}
	scope, err := ResolveScope(context.Background(), NormalizeConversationRuntimeMode("stateless"), "", "ignored")
	if err != nil {
		t.Fatalf("ResolveScope(stateless): %v", err)
	}
	if scope.RuntimeMode != RuntimeModeTask || !scope.Stateless {
		t.Fatalf("ResolveScope(stateless) = %+v, want task/stateless", scope)
	}
}

func TestResolveScope_SessionUsesExplicitIntent(t *testing.T) {
	flowCtx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID:       "flow-agent",
		FlowPath: "review/inst-1",
	})
	flowScope, err := ResolveScope(flowCtx, RuntimeModeSession, SessionScopeFlow, "")
	if err != nil {
		t.Fatalf("ResolveScope(flow session): %v", err)
	}
	if flowScope.Scope != "flow" || flowScope.ScopeKey != "review/inst-1" || flowScope.FlowInstance != "review/inst-1" {
		t.Fatalf("unexpected flow scope: %+v", flowScope)
	}

	globalCtx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID: "global-agent",
	})
	globalScope, err := ResolveScope(globalCtx, RuntimeModeSession, SessionScopeGlobal, "")
	if err != nil {
		t.Fatalf("ResolveScope(global session): %v", err)
	}
	if globalScope.Scope != "global" || globalScope.ScopeKey != "global" {
		t.Fatalf("unexpected global scope: %+v", globalScope)
	}
}

func TestResolveScope_InvalidSessionConfigurationsFailClosed(t *testing.T) {
	if _, err := ResolveScope(context.Background(), RuntimeModeSession, "", ""); err == nil {
		t.Fatal("expected session scope without declaration to fail")
	}
	if _, err := ResolveScope(context.Background(), RuntimeModeSession, SessionScopeFlow, ""); err == nil {
		t.Fatal("expected flow session without flow context to fail")
	}
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID: "entity-agent",
	})
	if _, err := ResolveScope(ctx, RuntimeModeSessionPerEntity, SessionScopeEntity, "entity-1"); err == nil {
		t.Fatal("expected session_per_entity without flow path to fail")
	}
}

func TestLeaseHeartbeatInterval_ClampsRange(t *testing.T) {
	if got := LeaseHeartbeatInterval(time.Time{}); got != 30*time.Second {
		t.Fatalf("zero expiry: got %s", got)
	}
	if got := LeaseHeartbeatInterval(time.Now().Add(3 * time.Second)); got != minLeaseHeartbeatInterval {
		t.Fatalf("min clamp: got %s", got)
	}
	if got := LeaseHeartbeatInterval(time.Now().Add(5 * time.Minute)); got != maxLeaseHeartbeatInterval {
		t.Fatalf("max clamp: got %s", got)
	}
}
