package agentcontrol

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewDirectiveEventPayloadPreservesDirectiveMode(t *testing.T) {
	evt, err := NewDirectiveEvent(SendDirectiveRequest{
		AgentID:   "agent-1",
		Directive: "run corpus",
		Source:    DirectiveSourceV1RPC,
	}, RunTargetResolution{
		RunID: "00000000-0000-0000-0000-000000000701",
		Mode:  RunResolutionSpecified,
	}, time.Date(2026, 5, 14, 3, 10, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewDirectiveEvent: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["mode"] != DirectiveEventMode {
		t.Fatalf("mode = %#v, want %q", payload["mode"], DirectiveEventMode)
	}
	if payload["directive_text"] != "run corpus" || payload["run_id"] != evt.RunID {
		t.Fatalf("payload = %#v", payload)
	}
	if _, ok := payload["kill_previous"]; ok {
		t.Fatalf("payload = %#v, want no kill_previous field", payload)
	}
}
