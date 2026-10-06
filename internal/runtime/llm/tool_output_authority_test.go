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

func TestSettledToolOutputCallRejectsForeignNameArgumentsAndOccurrence(t *testing.T) {
	authority := ToolOutputAuthority{ProviderOperationID: uuid.NewString(), SettledAt: time.Unix(10, 0).UTC()}
	ctx, err := withToolOutputCall(context.Background(), authority, "one-call", "emit_event", map[string]any{"event_name": "done"})
	if err != nil {
		t.Fatal(err)
	}
	call, ok := ToolOutputCallFromContext(ctx)
	if !ok {
		t.Fatal("sealed output call missing")
	}
	want, err := authority.eventIdentity("one-call")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, tool, event, occurrence string
		valid                         bool
	}{
		{"exact", "emit_event", "done", "one-call", true},
		{"foreign_tool", "save_entity_field", "done", "one-call", false},
		{"foreign_arguments", "emit_event", "foreign", "one-call", false},
		{"foreign_occurrence", "emit_event", "done", "foreign", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity, err := call.Authorize(test.tool, map[string]any{"event_name": test.event}, test.occurrence)
			if test.valid {
				if err != nil || identity.EventID() != want.EventID() || !identity.CreatedAt().Equal(want.CreatedAt()) {
					t.Fatalf("exact settled output changed: %+v %v", identity, err)
				}
			} else if err == nil {
				t.Fatal("foreign call acquired settled output authority")
			}
		})
	}
}
