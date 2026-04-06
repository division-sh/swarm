package runtime

import (
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
)

func TestRuntimePayloadValidator_AllowsValidSchemaPayload(t *testing.T) {
	t.Parallel()

	validator := newRuntimePayloadValidator(nil, map[string]runtimecontracts.EventSchema{
		"task.completed": {
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"ok": map[string]any{"type": "boolean"},
				},
				"required":             []any{"ok"},
				"additionalProperties": false,
			},
		},
	})

	if err := validator("task.completed", []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("validator(valid payload): %v", err)
	}
}

func TestRuntimePayloadValidator_RejectsInvalidJSON(t *testing.T) {
	t.Parallel()

	validator := newRuntimePayloadValidator(nil, map[string]runtimecontracts.EventSchema{
		"task.completed": {
			Schema: map[string]any{
				"type": "object",
			},
		},
	})

	if err := validator("task.completed", []byte(`{"ok":`)); err == nil {
		t.Fatal("expected invalid JSON payload to be rejected")
	}
}

func TestRuntimePayloadValidator_RejectsSchemaMismatch(t *testing.T) {
	t.Parallel()

	validator := newRuntimePayloadValidator(nil, map[string]runtimecontracts.EventSchema{
		"task.completed": {
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"ok": map[string]any{"type": "boolean"},
				},
				"required":             []any{"ok"},
				"additionalProperties": false,
			},
		},
	})

	if err := validator("task.completed", []byte(`{"ok":"yes"}`)); err == nil {
		t.Fatal("expected schema-invalid payload to be rejected")
	}
}

func TestRuntimePayloadValidator_AllowsUndeclaredCanonicalContextFields(t *testing.T) {
	t.Parallel()

	validator := newRuntimePayloadValidator(nil, map[string]runtimecontracts.EventSchema{
		"task.completed": {
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"ok": map[string]any{"type": "boolean"},
				},
				"required": []any{"ok"},
			},
		},
	})

	err := validator("task.completed", []byte(`{
			"ok": true,
			"entity_id": "ent-1",
			"trigger_event_type": "task.started",
			"current_state": "running",
			"status": "derived-context"
		}`))
	if err != nil {
		t.Fatalf("validator(extra canonical context): %v", err)
	}
}
