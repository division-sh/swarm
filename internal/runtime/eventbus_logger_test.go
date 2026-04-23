package runtime

import (
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
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
			"flow_instance": "flow/inst-1",
			"trigger_event_type": "task.started",
			"current_state": "running"
		}`))
	if err != nil {
		t.Fatalf("validator(extra canonical context): %v", err)
	}
}

func TestRuntimePayloadValidator_RejectsTriggerSchemaFieldWhenTargetSchemaDisallowsIt(t *testing.T) {
	t.Parallel()

	validator := newRuntimePayloadValidator(nil, map[string]runtimecontracts.EventSchema{
		"task.started": {
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"score": map[string]any{"type": "integer"},
				},
				"additionalProperties": false,
			},
		},
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

	err := validator("task.completed", []byte(`{
			"ok": true,
			"score": 10,
			"trigger_event_type": "task.started"
		}`))
	if err == nil {
		t.Fatal("expected trigger-schema-only field to be rejected by target schema validation")
	}
}

func TestRuntimePayloadValidator_RejectsUndeclaredCallerPayloadFieldWhenAdditionalPropertiesFalse(t *testing.T) {
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

	err := validator("task.completed", []byte(`{
			"ok": true,
			"surprise": "x"
		}`))
	if err == nil {
		t.Fatal("expected undeclared caller payload field to be rejected")
	}
}

func TestRuntimePayloadValidator_RejectsScalarAliasUUIDViolationFromEmitRegistrySnapshot(t *testing.T) {
	t.Parallel()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Scalars: map[string]runtimecontracts.ScalarTypeDecl{
				"TraceID": {Base: "uuid"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"trace_id": {Type: "TraceID"},
					},
					Required: []string{"trace_id"},
				},
			},
		},
	})
	emitRegistry := runtimetools.NewEmitRegistry(source, nil)
	validator := newRuntimePayloadValidator(nil, emitRegistry.EventSchemaSnapshot())

	if err := validator("task.completed", []byte(`{"trace_id":"not-a-uuid"}`)); err == nil {
		t.Fatal("expected scalar-alias uuid violation to be rejected")
	}
}
