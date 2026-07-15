package tools

import (
	"errors"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
)

func TestNewToolInputValidator_NilDefinitionsFailClosed(t *testing.T) {
	validator := NewToolInputValidator(nil)

	err := validator.Validate(nil, "save_entity_field", map[string]any{"entity_id": "x"})
	if !errors.Is(err, errToolDefinitionsProviderRequired) {
		t.Fatalf("Validate() error = %v, want errToolDefinitionsProviderRequired", err)
	}
}

func TestToolInputValidator_ValidateNilReceiverFailsClosed(t *testing.T) {
	var validator *ToolInputValidator

	err := validator.Validate(nil, "save_entity_field", map[string]any{"entity_id": "x"})
	if !errors.Is(err, errToolDefinitionsProviderRequired) {
		t.Fatalf("Validate() error = %v, want errToolDefinitionsProviderRequired", err)
	}
}

func TestToolInputValidator_EmitToolsStillBypassDefinitionsLookup(t *testing.T) {
	validator := NewToolInputValidator(nil)

	if err := validator.Validate(nil, "emit_customer.updated", map[string]any{"id": "123"}); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestToolInputValidator_UsesDefinitionsProviderWhenConfigured(t *testing.T) {
	validator := NewToolInputValidator(func(*models.AgentConfig) ([]llm.ToolDefinition, error) {
		return []llm.ToolDefinition{{
			Name:   "save_entity_field",
			Schema: map[string]any{"type": "object"},
		}}, nil
	})

	if err := validator.Validate(nil, "save_entity_field", map[string]any{"entity_id": "x"}); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestToolInputValidator_UsesActorScopedDefinitionsWhenAvailable(t *testing.T) {
	validator := NewToolInputValidator(func(actor *models.AgentConfig) ([]llm.ToolDefinition, error) {
		enumValues := []any{"root_only"}
		if actor != nil && actor.ID == "child-agent" {
			enumValues = []any{"child_only"}
		}
		return []llm.ToolDefinition{{
			Name: "save_entity_field",
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"entity_id": map[string]any{"type": "string"},
					"field": map[string]any{
						"type": "string",
						"enum": enumValues,
					},
					"value": map[string]any{},
				},
				"required": []any{"entity_id", "field", "value"},
			},
		}}, nil
	})

	child := &models.AgentConfig{ExecutionMode: "live", ID: "child-agent"}
	if err := validator.Validate(child, "save_entity_field", map[string]any{
		"entity_id": "entity-1",
		"field":     "child_only",
		"value":     "ok",
	}); err != nil {
		t.Fatalf("Validate(child actor) error = %v, want nil", err)
	}
	if err := validator.Validate(nil, "save_entity_field", map[string]any{
		"entity_id": "entity-1",
		"field":     "child_only",
		"value":     "ok",
	}); err == nil {
		t.Fatal("Validate(nil actor) succeeded, want schema rejection for child_only")
	}
}

func TestToolInputValidator_RejectsUnknownKeysAgainstClosedSchema(t *testing.T) {
	validator := NewToolInputValidator(func(*models.AgentConfig) ([]llm.ToolDefinition, error) {
		return []llm.ToolDefinition{{
			Name:            "save_validation_case_business_brief",
			GeneratedSchema: true,
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"value": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"summary":    map[string]any{"type": "string"},
							"confidence": map[string]any{"type": "integer"},
						},
						"required":             []any{"summary", "confidence"},
						"additionalProperties": false,
					},
				},
				"required":             []any{"value"},
				"additionalProperties": false,
			},
		}}, nil
	})

	err := validator.Validate(nil, "save_validation_case_business_brief", map[string]any{
		"value": map[string]any{
			"summary":    "ok",
			"confidence": 8,
			"notes":      "must not be silently pruned",
		},
	})
	if err == nil {
		t.Fatal("Validate() succeeded, want undeclared nested key rejection")
	}
}
