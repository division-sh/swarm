package llm

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

var providerJSONSchemaTypes = map[string]struct{}{
	"array":   {},
	"boolean": {},
	"integer": {},
	"null":    {},
	"number":  {},
	"object":  {},
	"string":  {},
}

// ValidateProviderToolSchema validates the JSON Schema subset this runtime
// generates for provider-facing tool input schemas. Providers reject unknown
// JSON Schema type values before agent execution starts, so verify must catch
// those recursively instead of relying on payload validation.
func ValidateProviderToolSchema(toolName string, schema any) error {
	// An absent tool definition is defaulted by its presentation owner. A
	// declared schema node, including a typed-nil map, may never encode null.
	if schema == nil {
		return nil
	}
	path := strings.TrimSpace(toolName)
	if path == "" {
		path = "tool"
	}
	if err := validateProviderSchemaNode(schema, path+".input_schema"); err != nil {
		return fmt.Errorf("provider tool schema %s: %w", path, err)
	}
	return nil
}

func ValidateProviderToolDefinitions(tools []ToolDefinition) error {
	for _, tool := range tools {
		if err := ValidateProviderToolSchema(tool.Name, tool.Schema); err != nil {
			return err
		}
	}
	return nil
}

func validateProviderSchemaNode(schema any, path string) error {
	obj, ok := schema.(map[string]any)
	if !ok || obj == nil {
		return fmt.Errorf("%s must be a JSON object schema, got %T", path, schema)
	}
	if rawType, ok := obj["type"]; ok {
		if err := validateProviderSchemaType(rawType, path+".type"); err != nil {
			return err
		}
	}
	if rawProperties, ok := obj["properties"]; ok {
		props, ok := rawProperties.(map[string]any)
		if !ok || props == nil {
			return fmt.Errorf("%s.properties must be an object, got %T", path, rawProperties)
		}
		for _, name := range slices.Sorted(maps.Keys(props)) {
			if err := validateProviderSchemaNode(props[name], path+".properties."+name); err != nil {
				return err
			}
		}
	}
	return validateProviderSchemaConstraints(obj, path)
}

func validateProviderSchemaConstraints(obj map[string]any, path string) error {
	if rawItems, ok := obj["items"]; ok {
		if err := validateProviderSchemaNode(rawItems, path+".items"); err != nil {
			return err
		}
	}
	if rawNames, ok := obj["propertyNames"]; ok {
		if rawNames == nil {
			return fmt.Errorf("%s.propertyNames must be a JSON object schema", path)
		}
		if err := validateProviderSchemaNode(rawNames, path+".propertyNames"); err != nil {
			return err
		}
	}
	if raw, declared := obj["oneOf"]; declared {
		branches, ok := raw.([]any)
		if !ok || len(branches) == 0 {
			return fmt.Errorf("%s.oneOf must be a non-empty schema array", path)
		}
		for index, branch := range branches {
			if branch == nil {
				return fmt.Errorf("%s.oneOf[%d] must be a JSON object schema", path, index)
			}
			if err := validateProviderSchemaNode(branch, fmt.Sprintf("%s.oneOf[%d]", path, index)); err != nil {
				return err
			}
		}
	}
	if rawAdditional, ok := obj["additionalProperties"]; ok {
		switch additional := rawAdditional.(type) {
		case bool:
		case map[string]any:
			if err := validateProviderSchemaNode(additional, path+".additionalProperties"); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%s.additionalProperties must be boolean or object schema, got %T", path, rawAdditional)
		}
	}
	if rawRequired, ok := obj["required"]; ok {
		if err := validateProviderRequired(rawRequired, path+".required"); err != nil {
			return err
		}
	}
	if rawEnum, ok := obj["enum"]; ok {
		values, ok := rawEnum.([]any)
		if !ok {
			return fmt.Errorf("%s.enum must be an array, got %T", path, rawEnum)
		}
		if values == nil {
			return fmt.Errorf("%s.enum must be a non-null array", path)
		}
	}
	return nil
}

func validateProviderSchemaType(raw any, path string) error {
	if value, ok := raw.(string); ok {
		return validateProviderSchemaTypeToken(value, path)
	}
	values, ok := providerSchemaStringItems(raw)
	if !ok {
		return fmt.Errorf("%s must be a string or string array, got %T", path, raw)
	}
	if len(values) == 0 {
		return fmt.Errorf("%s must be a non-empty string array", path)
	}
	seen := make(map[string]bool, len(values))
	for i, item := range values {
		value, ok := item.(string)
		itemPath := fmt.Sprintf("%s[%d]", path, i)
		if !ok {
			return fmt.Errorf("%s must be a string, got %T", itemPath, item)
		}
		if err := validateProviderSchemaTypeToken(value, itemPath); err != nil {
			return err
		}
		if seen[value] {
			return fmt.Errorf("%s repeats JSON Schema type %q", itemPath, value)
		}
		seen[value] = true
	}
	return nil
}

func validateProviderSchemaTypeToken(value, path string) error {
	if _, ok := providerJSONSchemaTypes[value]; !ok {
		return fmt.Errorf("%s has unsupported JSON Schema type %q", path, value)
	}
	return nil
}

func validateProviderRequired(raw any, path string) error {
	values, ok := providerSchemaStringItems(raw)
	if !ok {
		return fmt.Errorf("%s must be an array, got %T", path, raw)
	}
	if values == nil {
		return fmt.Errorf("%s must be a non-null array", path)
	}
	seen := make(map[string]bool, len(values))
	for i, item := range values {
		value, ok := item.(string)
		if !ok {
			return fmt.Errorf("%s[%d] must be a string, got %T", path, i, item)
		}
		if seen[value] {
			return fmt.Errorf("%s[%d] repeats required property %q", path, i, value)
		}
		seen[value] = true
	}
	return nil
}

func providerSchemaStringItems(raw any) ([]any, bool) {
	switch values := raw.(type) {
	case []any:
		return values, true
	case []string:
		if values == nil {
			return nil, true
		}
		items := make([]any, len(values))
		for i, value := range values {
			items[i] = value
		}
		return items, true
	default:
		return nil, false
	}
}
