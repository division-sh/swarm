package runtime

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

func TestEventBusRejectsMalformedFailureBeforeRuntimeLog(t *testing.T) {
	logger := NewRuntimeLogger(nil)
	eventBus, err := newRuntimeEventBus(nil, logger, nil, "", runtimecorrelation.BundleSourceFact{}, "", nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("newRuntimeEventBus: %v", err)
	}

	err = eventBus.LogRuntime(testAuthorActivityContext(context.Background()), runtimepipeline.RuntimeLogEntry{
		Component: "test",
		Action:    "malformed_failure",
		Failure: &runtimefailures.Envelope{
			SchemaVersion: "forged",
			Class:         runtimefailures.ClassConnectorFailure,
		},
	})
	if err == nil {
		t.Fatal("LogRuntime() accepted malformed failure evidence")
	}
}

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

	if err := validator(context.Background(), "task.completed", []byte(`{"ok":true}`)); err != nil {
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

	if err := validator(context.Background(), "task.completed", []byte(`{"ok":`)); err == nil {
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

	if err := validator(context.Background(), "task.completed", []byte(`{"ok":"yes"}`)); err == nil {
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

	err := validator(context.Background(), "task.completed", []byte(`{
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

	err := validator(context.Background(), "task.completed", []byte(`{
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

	err := validator(context.Background(), "task.completed", []byte(`{
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

	if err := validator(context.Background(), "task.completed", []byte(`{"trace_id":"not-a-uuid"}`)); err == nil {
		t.Fatal("expected scalar-alias uuid violation to be rejected")
	}
}
