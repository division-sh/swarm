package engine

import "testing"

func TestBuildBaseContext_CopiesPayloadMetadataAndPolicy(t *testing.T) {
	input := ContextBuilderInput{
		EntityID: "entity-1",
		FlowID:   "flow-1",
		State: StateSnapshot{
			WorkflowName:    "wf",
			WorkflowVersion: "v1",
			CurrentState:    "researching",
			Metadata:        map[string]any{"k": "v"},
		},
		Payload: map[string]any{"p": "x"},
	}

	base := BuildBaseContext(input)

	if got := base.Entity["entity_id"]; got != "entity-1" {
		t.Fatalf("entity_id = %#v", got)
	}
	if got := base.Entity["k"]; got != "v" {
		t.Fatalf("entity metadata not reflected into entity context: %#v", got)
	}
	if got := base.Entity["current_state"]; got != "researching" {
		t.Fatalf("current_state = %#v", got)
	}
	input.Payload["p"] = "changed"
	input.State.Metadata["k"] = "changed"
	if got := base.Payload["p"]; got != "x" {
		t.Fatalf("payload clone lost isolation: %#v", got)
	}
	if got := base.Metadata["k"]; got != "v" {
		t.Fatalf("metadata clone lost isolation: %#v", got)
	}
}

func TestContextOverlayHelpers_DoNotMutateOriginal(t *testing.T) {
	base := BaseContext{
		Payload:     map[string]any{"a": 1},
		Accumulated: map[string]any{"b": 2},
		FanOut:      map[string]any{"c": 3},
	}

	withPayload := WithPayload(base, map[string]any{"a": 9})
	withAccumulated := WithAccumulated(base, map[string]any{"b": 8})
	withFanOut := WithFanOutItem(base, map[string]any{"c": 7})

	if got := base.Payload["a"]; got != 1 {
		t.Fatalf("base payload mutated: %#v", got)
	}
	if got := base.Accumulated["b"]; got != 2 {
		t.Fatalf("base accumulated mutated: %#v", got)
	}
	if got := base.FanOut["c"]; got != 3 {
		t.Fatalf("base fanout mutated: %#v", got)
	}
	if got := withPayload.Payload["a"]; got != 9 {
		t.Fatalf("withPayload wrong value: %#v", got)
	}
	if got := withAccumulated.Accumulated["b"]; got != 8 {
		t.Fatalf("withAccumulated wrong value: %#v", got)
	}
	if got := withFanOut.FanOut["c"]; got != 7 {
		t.Fatalf("withFanOut wrong value: %#v", got)
	}
}
