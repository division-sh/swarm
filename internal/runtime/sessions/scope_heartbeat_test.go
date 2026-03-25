package sessions

import (
	"context"
	"testing"
	"time"
)

func TestScopeFromContext_DefaultsToSession(t *testing.T) {
	scope := ScopeFromContext(nil)
	if scope.ConversationMode != "session" || scope.ScopeKey != "" {
		t.Fatalf("unexpected nil-context scope: %+v", scope)
	}

	ctx := WithScope(context.Background(), "TASK", "  abc  ")
	scope = ScopeFromContext(ctx)
	if scope.ConversationMode != "task" || scope.ScopeKey != "abc" {
		t.Fatalf("unexpected scoped context: %+v", scope)
	}
}

func TestNormalizeConversationRuntimeMode_AcceptsStatelessAlias(t *testing.T) {
	if got := NormalizeConversationRuntimeMode("stateless"); got != RuntimeModeTask {
		t.Fatalf("NormalizeConversationRuntimeMode(stateless) = %q, want %q", got, RuntimeModeTask)
	}
	scope := ResolveScope("stateless", "ignored")
	if scope.RuntimeMode != RuntimeModeTask || !scope.Stateless {
		t.Fatalf("ResolveScope(stateless) = %+v, want task/stateless", scope)
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
