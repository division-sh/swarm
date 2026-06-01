package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
)

// ValidateGeneratedToolSchemaClosureForSource enforces the Path alpha generated
// tool-schema invariants at the boot/static boundary. It intentionally covers
// generated role-scoped entity tools and generated emit tools, not transitional
// legacy generic entity tools.
func ValidateGeneratedToolSchemaClosureForSource(source semanticview.Source) []error {
	if source == nil {
		return nil
	}
	registry := NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))
	actors := providerSchemaValidationActors(source)
	var errs []error
	for _, actor := range actors {
		errs = append(errs, validateGeneratedRoleScopedEntitySchemasForActor(source, actor)...)
		for _, tool := range registry.GenerateEmitToolsForActor(actor, nil) {
			errs = append(errs, validateGeneratedToolDefinitionSchema("agent "+strings.TrimSpace(actor.ID), tool)...)
		}
	}
	return errs
}

func validateGeneratedRoleScopedEntitySchemasForActor(source semanticview.Source, actor models.AgentConfig) []error {
	if !roleScopedEntityToolsEnabledForActor(source, actor) {
		return nil
	}
	contract, ok := resolveEntityToolContract(source, &actor)
	if !ok {
		return nil
	}
	entries := roleScopedEntityToolSchemaEntriesForActor(source, actor, contract)
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	var errs []error
	for _, name := range names {
		entry := entries[name]
		location := fmt.Sprintf("agent %s tool %s", strings.TrimSpace(actor.ID), strings.TrimSpace(name))
		errs = append(errs, validateGeneratedJSONSchema(location+" input", entry.InputSchema)...)
		if len(entry.OutputSchema) > 0 {
			errs = append(errs, validateGeneratedJSONSchema(location+" output", entry.OutputSchema)...)
		}
	}
	return errs
}

func validateGeneratedToolDefinitionSchema(location string, tool llm.ToolDefinition) []error {
	if err := llm.ValidateProviderToolSchema(tool.Name, tool.Schema); err != nil {
		return []error{fmt.Errorf("%s: %w", strings.TrimSpace(location), err)}
	}
	schema, ok := tool.Schema.(map[string]any)
	if !ok {
		if tool.Schema == nil {
			return nil
		}
		return []error{fmt.Errorf("%s tool %s input schema must be an object, got %T", strings.TrimSpace(location), strings.TrimSpace(tool.Name), tool.Schema)}
	}
	return validateGeneratedJSONSchema(fmt.Sprintf("%s tool %s input", strings.TrimSpace(location), strings.TrimSpace(tool.Name)), schema)
}

func validateGeneratedJSONSchema(location string, schema map[string]any) []error {
	var errs []error
	validateGeneratedJSONSchemaNode(strings.TrimSpace(location), schema, &errs)
	return errs
}

func closeGeneratedJSONSchema(schema map[string]any) map[string]any {
	if schema == nil {
		return nil
	}
	closed := deepCloneMap(schema)
	closeGeneratedJSONSchemaNode(closed)
	return closed
}

func closeGeneratedJSONSchemaNode(schema map[string]any) {
	if schema == nil {
		return
	}
	props := schemaProperties(schema["properties"])
	required := make([]string, 0, len(props))
	for name, child := range props {
		required = append(required, name)
		closeGeneratedJSONSchemaNode(child)
	}
	sort.Strings(required)
	schemaType := strings.TrimSpace(asString(schema["type"]))
	isObject := schemaType == "object" || len(props) > 0 || len(required) > 0
	if isObject {
		if _, ok := schema["type"]; !ok {
			schema["type"] = "object"
		}
		schema["additionalProperties"] = false
		if len(required) > 0 {
			schema["required"] = required
		} else {
			delete(schema, "required")
		}
	}
	if items, ok := schema["items"].(map[string]any); ok {
		closeGeneratedJSONSchemaNode(items)
	}
}

func validateGeneratedJSONSchemaNode(path string, schema map[string]any, errs *[]error) {
	if schema == nil {
		return
	}
	schemaType := strings.TrimSpace(asString(schema["type"]))
	props := schemaProperties(schema["properties"])
	required := requiredSchemaSet(schema["required"])
	isObject := schemaType == "object" || len(props) > 0 || len(required) > 0
	if isObject {
		if schema["additionalProperties"] != false {
			*errs = append(*errs, fmt.Errorf("%s object schema must set additionalProperties=false", path))
		}
		for name := range props {
			if _, ok := required[name]; !ok {
				*errs = append(*errs, fmt.Errorf("%s object schema must require declared property %s", path, name))
			}
		}
		for name := range required {
			if _, ok := props[name]; !ok {
				*errs = append(*errs, fmt.Errorf("%s required property %s is not declared", path, name))
			}
		}
	}
	if enumRaw, ok := schema["enum"]; ok {
		if len(schemaEnumValues(enumRaw)) == 0 {
			*errs = append(*errs, fmt.Errorf("%s enum schema must declare at least one allowed value", path))
		}
	}
	for name, child := range props {
		validateGeneratedJSONSchemaNode(path+".properties."+name, child, errs)
	}
	if items, ok := schema["items"].(map[string]any); ok {
		validateGeneratedJSONSchemaNode(path+".items", items, errs)
	}
}

func requiredSchemaSet(raw any) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range runtimesharedjson.RequiredList(raw) {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func schemaEnumValues(raw any) []any {
	switch values := raw.(type) {
	case []any:
		return values
	case []string:
		out := make([]any, 0, len(values))
		for _, value := range values {
			out = append(out, value)
		}
		return out
	default:
		return nil
	}
}
