package tools

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
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
	actors, errs := providerSchemaValidationActors(source)
	for _, actor := range actors {
		errs = append(errs, validateGeneratedRoleScopedEntitySchemasForActor(source, actor)...)
		for _, eventType := range UniqueNonEmpty(actor.EmitEvents) {
			if _, err := registry.admitActorEventSchema(actor, eventType); err != nil {
				errs = append(errs, err)
			}
		}
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
	var closed map[string]any
	// Schema integer bounds must retain their lexical kind for strict admission.
	if raw, err := json.Marshal(schema); err == nil {
		var preserved map[string]any
		if err := canonicaljson.DecodePreservingNumberLexemes(raw, &preserved); err == nil {
			closed = preserved
		}
	}
	if closed == nil {
		closed = deepCloneMap(schema)
	}
	closeGeneratedJSONSchemaNode(closed)
	return closed
}

func closeGeneratedJSONSchemaNode(schema map[string]any) {
	closeGeneratedJSONSchemaNodeWithPresence(schema, false)
}

func closeGeneratedJSONSchemaNodeWithPresence(schema map[string]any, preserveRequired bool) {
	if schema == nil {
		return
	}
	if _, branched := schema["oneOf"]; branched {
		preserveRequired = true
	}
	props := schemaProperties(schema["properties"])
	required := make([]string, 0, len(props))
	for name, child := range props {
		required = append(required, name)
		closeGeneratedJSONSchemaNodeWithPresence(child, preserveRequired)
	}
	sort.Strings(required)
	schemaType := strings.TrimSpace(asString(schema["type"]))
	isObject := schemaType == "object" || len(props) > 0 || len(required) > 0
	if isObject {
		if _, ok := schema["type"]; !ok {
			schema["type"] = "object"
		}
		if additional, typedMap := schema["additionalProperties"].(map[string]any); typedMap {
			closeGeneratedJSONSchemaNodeWithPresence(additional, preserveRequired)
		} else {
			schema["additionalProperties"] = false
		}
		// Exclusive operation branches retain omitted/default selectors and
		// optional catalog fields instead of requiring every branch's keys.
		if !preserveRequired {
			if len(required) > 0 {
				schema["required"] = required
			} else {
				delete(schema, "required")
			}
		}
	}
	if items, ok := schema["items"].(map[string]any); ok {
		closeGeneratedJSONSchemaNodeWithPresence(items, preserveRequired)
	}
	if names, ok := schema["propertyNames"].(map[string]any); ok {
		closeGeneratedJSONSchemaNodeWithPresence(names, preserveRequired)
	}
	for _, branch := range schemaEnumValues(schema["oneOf"]) {
		if child, ok := branch.(map[string]any); ok {
			closeGeneratedJSONSchemaNodeWithPresence(child, preserveRequired)
		}
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
		_, typedMap := schema["additionalProperties"].(map[string]any)
		if schema["additionalProperties"] != false && !typedMap {
			*errs = append(*errs, fmt.Errorf("%s object schema must set additionalProperties=false", path))
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
	if additional, ok := schema["additionalProperties"].(map[string]any); ok {
		validateGeneratedJSONSchemaNode(path+".additionalProperties", additional, errs)
	}
	if raw, declared := schema["propertyNames"]; declared {
		if names, ok := raw.(map[string]any); ok {
			validateGeneratedJSONSchemaNode(path+".propertyNames", names, errs)
		} else {
			*errs = append(*errs, fmt.Errorf("%s.propertyNames must be a schema object", path))
		}
	}
	validateGeneratedJSONSchemaOneOf(path, schema, errs)
}

func validateGeneratedJSONSchemaOneOf(path string, schema map[string]any, errs *[]error) {
	if raw, declared := schema["oneOf"]; declared {
		branches := schemaEnumValues(raw)
		if len(branches) == 0 {
			*errs = append(*errs, fmt.Errorf("%s.oneOf must be a non-empty schema array", path))
		}
		for index, branch := range branches {
			child, ok := branch.(map[string]any)
			if !ok || child == nil {
				*errs = append(*errs, fmt.Errorf("%s.oneOf[%d] must be a schema object", path, index))
				continue
			}
			validateGeneratedJSONSchemaNode(fmt.Sprintf("%s.oneOf[%d]", path, index), child, errs)
		}
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
