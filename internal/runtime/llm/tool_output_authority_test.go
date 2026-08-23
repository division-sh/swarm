package llm

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestToolOutputAuthorityMintsStableExactEventIdentity(t *testing.T) {
	authority := ToolOutputAuthority{
		ProviderOperationID: uuid.NewString(),
		SettledAt:           time.Date(2026, 8, 23, 18, 19, 20, 123456000, time.UTC),
	}
	first, err := authority.eventIdentity("tool_call:1:0:mock-1:emit_done")
	if err != nil {
		t.Fatalf("mint first identity: %v", err)
	}
	replayed, err := authority.eventIdentity("tool_call:1:0:mock-1:emit_done")
	if err != nil {
		t.Fatalf("mint replayed identity: %v", err)
	}
	if first.EventID() != replayed.EventID() || !first.CreatedAt().Equal(replayed.CreatedAt()) {
		t.Fatalf("replayed identity = (%s, %s), want exact (%s, %s)", replayed.EventID(), replayed.CreatedAt(), first.EventID(), first.CreatedAt())
	}
	sibling, err := authority.eventIdentity("tool_call:1:1:mock-2:emit_done")
	if err != nil {
		t.Fatalf("mint sibling identity: %v", err)
	}
	if sibling.EventID() == first.EventID() {
		t.Fatal("different tool-call coordinates minted the same event identity")
	}

	raw, err := json.Marshal(Response{Message: Message{Role: "assistant"}, ToolOutputAuthority: &authority})
	if err != nil {
		t.Fatalf("marshal response authority: %v", err)
	}
	var restored Response
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("restore response authority: %v", err)
	}
	if restored.ToolOutputAuthority == nil || restored.ToolOutputAuthority.Validate() != nil || *restored.ToolOutputAuthority != authority {
		t.Fatalf("restored authority = %#v, want %#v", restored.ToolOutputAuthority, authority)
	}
}

func TestManagedToolOutputRejectsGenericExecutor(t *testing.T) {
	authority := ToolOutputAuthority{
		ProviderOperationID: uuid.NewString(),
		SettledAt:           time.Date(2026, 8, 23, 18, 19, 20, 123456000, time.UTC),
	}
	identity, err := authority.eventIdentity("tool_call:1:0:mock-1:emit_done")
	if err != nil {
		t.Fatal(err)
	}
	conversation := &Conversation{toolExecutor: &fakeToolExec{}}
	if _, err := conversation.safeExecuteOutputEvent(context.Background(), "emit_done", map[string]any{}, identity); err == nil || !strings.Contains(err.Error(), "tool_output_event_executor_missing") {
		t.Fatalf("generic executor error = %v, want tool_output_event_executor_missing", err)
	}
}
