package runtime

import (
	"context"
	"testing"
)

func TestNoopRuntime_Behavior(t *testing.T) {
	var rt NoopRuntime
	s, err := rt.StartSession(context.Background(), "a1", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s.RuntimeMode != "noop" || s.AgentID != "a1" {
		t.Fatalf("unexpected session: %+v", s)
	}

	resp, err := rt.ContinueSession(context.Background(), s, Message{Role: "user", Content: "hi"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp.Message.Content == "" {
		t.Fatal("expected content")
	}
	if err := rt.PersistConversationSnapshot(context.Background(), s); err != nil {
		t.Fatalf("PersistConversationSnapshot: %v", err)
	}
}

