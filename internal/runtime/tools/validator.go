package tools

import (
	"errors"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
)

type ToolInputValidator struct {
	definitions func(actor *models.AgentConfig) ([]llm.ToolDefinition, error)
}

var errToolDefinitionsProviderRequired = errors.New("tool definitions provider is required")

func NewToolInputValidator(definitions func(actor *models.AgentConfig) ([]llm.ToolDefinition, error)) *ToolInputValidator {
	if definitions == nil {
		definitions = func(*models.AgentConfig) ([]llm.ToolDefinition, error) {
			return nil, errToolDefinitionsProviderRequired
		}
	}
	return &ToolInputValidator{definitions: definitions}
}

func (v *ToolInputValidator) Validate(actor *models.AgentConfig, name string, input any) error {
	name = normalizeNativeToolName(name)
	if name == "" || toolKindPolicy(name) == toolcapabilities.KindEmit {
		return nil
	}
	if v == nil || v.definitions == nil {
		return errToolDefinitionsProviderRequired
	}
	input = validatorNormalizeRuntimeToolInput(name, input)
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return err
	}
	if payload == nil {
		payload = map[string]any{}
	}

	defs, defsErr := v.definitions(actor)
	if defsErr != nil {
		return defsErr
	}

	contractSchema, generatedSchema, foundContract := validatorToolSchemaForName(defs, name)
	if foundContract && contractSchema != nil {
		if !generatedSchema {
			payload = validatorPruneSchemaUnknownKeys(payload, contractSchema)
		}
		return ValidatePayloadAgainstSchema(contractSchema, payload)
	}
	return nil
}

func validatorToolSchemaForName(defs []llm.ToolDefinition, name string) (map[string]any, bool, bool) {
	name = normalizeNativeToolName(name)
	for _, def := range defs {
		if strings.TrimSpace(def.Name) != name {
			continue
		}
		schema, ok := def.Schema.(map[string]any)
		return schema, def.GeneratedSchema, ok
	}
	return nil, false, false
}

func validatorPruneSchemaUnknownKeys(payload map[string]any, schema map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}
	props := schemaProperties(schema["properties"])
	if len(props) == 0 {
		return payload
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		if _, ok := props[key]; ok {
			out[key] = value
		}
	}
	return out
}

func validatorNormalizeRuntimeToolInput(name string, input any) any {
	return canonicalRuntimeToolInput(name, input)
}
