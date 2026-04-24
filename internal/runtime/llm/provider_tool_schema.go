package llm

import (
	"fmt"
	"sort"
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
	if schema == nil {
		return nil
	}
	obj, ok := schema.(map[string]any)
	if !ok {
		return fmt.Errorf("%s must be a JSON object schema, got %T", path, schema)
	}
	if rawType, ok := obj["type"]; ok {
		if err := validateProviderSchemaType(rawType, path+".type"); err != nil {
			return err
		}
	}
	if rawProperties, ok := obj["properties"]; ok {
		props, ok := rawProperties.(map[string]any)
		if !ok {
			return fmt.Errorf("%s.properties must be an object, got %T", path, rawProperties)
		}
		names := make([]string, 0, len(props))
		for name := range props {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if err := validateProviderSchemaNode(props[name], path+".properties."+name); err != nil {
				return err
			}
		}
	}
	if rawItems, ok := obj["items"]; ok {
		if err := validateProviderSchemaNode(rawItems, path+".items"); err != nil {
			return err
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
		if _, ok := rawEnum.([]any); !ok {
			return fmt.Errorf("%s.enum must be an array, got %T", path, rawEnum)
		}
	}
	return nil
}

func validateProviderSchemaType(raw any, path string) error {
	switch value := raw.(type) {
	case string:
		value = strings.TrimSpace(value)
		if _, ok := providerJSONSchemaTypes[value]; !ok {
			return fmt.Errorf("%s has unsupported JSON Schema type %q", path, value)
		}
	case []any:
		for i, item := range value {
			if err := validateProviderSchemaType(item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	case []string:
		for i, item := range value {
			if err := validateProviderSchemaType(item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%s must be a string or string array, got %T", path, raw)
	}
	return nil
}

func validateProviderRequired(raw any, path string) error {
	switch values := raw.(type) {
	case []string:
		return nil
	case []any:
		for i, value := range values {
			if _, ok := value.(string); !ok {
				return fmt.Errorf("%s[%d] must be a string, got %T", path, i, value)
			}
		}
	default:
		return fmt.Errorf("%s must be an array, got %T", path, raw)
	}
	return nil
}
