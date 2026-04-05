package tools

import (
	"errors"
	"testing"

	llm "swarm/internal/runtime/llm"
)

func TestNewToolInputValidator_NilDefinitionsFailClosed(t *testing.T) {
	validator := NewToolInputValidator(nil)

	err := validator.Validate("save_entity_field", map[string]any{"entity_id": "x"})
	if !errors.Is(err, errToolDefinitionsProviderRequired) {
		t.Fatalf("Validate() error = %v, want errToolDefinitionsProviderRequired", err)
	}
}

func TestToolInputValidator_ValidateNilReceiverFailsClosed(t *testing.T) {
	var validator *ToolInputValidator

	err := validator.Validate("save_entity_field", map[string]any{"entity_id": "x"})
	if !errors.Is(err, errToolDefinitionsProviderRequired) {
		t.Fatalf("Validate() error = %v, want errToolDefinitionsProviderRequired", err)
	}
}

func TestToolInputValidator_EmitToolsStillBypassDefinitionsLookup(t *testing.T) {
	validator := NewToolInputValidator(nil)

	if err := validator.Validate("emit_customer.updated", map[string]any{"id": "123"}); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestToolInputValidator_UsesDefinitionsProviderWhenConfigured(t *testing.T) {
	validator := NewToolInputValidator(func() ([]llm.ToolDefinition, error) {
		return []llm.ToolDefinition{{
			Name:   "save_entity_field",
			Schema: map[string]any{"type": "object"},
		}}, nil
	})

	if err := validator.Validate("save_entity_field", map[string]any{"entity_id": "x"}); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}
