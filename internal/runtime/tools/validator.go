package tools

import (
	"strings"

	"swarm/internal/runtime/core/toolcapabilities"
	llm "swarm/internal/runtime/llm"
)

type ToolInputValidator struct {
	definitions func() ([]llm.ToolDefinition, error)
}

func NewToolInputValidator(definitions func() ([]llm.ToolDefinition, error)) *ToolInputValidator {
	return &ToolInputValidator{definitions: definitions}
}

func (v *ToolInputValidator) Validate(name string, input any) error {
	name = normalizeNativeToolName(name)
	if name == "" || toolKindPolicy(name) == toolcapabilities.KindEmit {
		return nil
	}
	input = validatorNormalizeRuntimeToolInput(name, input)
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return err
	}
	if payload == nil {
		payload = map[string]any{}
	}

	defs, defsErr := v.definitions()
	if defsErr != nil {
		return defsErr
	}

	contractSchema, foundContract := validatorToolSchemaForName(defs, name)
	if foundContract && contractSchema != nil {
		return ValidatePayloadAgainstSchema(contractSchema, validatorPruneSchemaUnknownKeys(payload, contractSchema))
	}
	return nil
}

func validatorToolSchemaForName(defs []llm.ToolDefinition, name string) (map[string]any, bool) {
	name = normalizeNativeToolName(name)
	for _, def := range defs {
		if strings.TrimSpace(def.Name) != name {
			continue
		}
		schema, ok := def.Schema.(map[string]any)
		return schema, ok
	}
	return nil, false
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
