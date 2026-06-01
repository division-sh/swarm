package eventschema

import (
	"fmt"
	"strings"
	"time"

	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
	"github.com/google/uuid"
)

func ValidatePayloadAgainstSchema(schema map[string]any, payload map[string]any) error {
	if schema == nil {
		return nil
	}
	return validateSchemaObject("$", schema, payload)
}

func validateSchemaObject(path string, schema map[string]any, payload map[string]any) error {
	if schemaType := strings.TrimSpace(asString(schema["type"])); schemaType != "" && schemaType != "object" {
		return nil
	}
	required := requiredList(schema["required"])
	for _, key := range required {
		if _, ok := payload[key]; !ok {
			return fmt.Errorf("schema validation failed: %s.%s is required", path, key)
		}
	}
	props := schemaProperties(schema["properties"])
	allowAdditional := schemaAdditionalProps(schema["additionalProperties"])
	for k, v := range payload {
		propSchema, known := props[k]
		if !known {
			if allowAdditional {
				continue
			}
			return fmt.Errorf("schema validation failed: %s.%s is not allowed", path, k)
		}
		if err := validateValue(path+"."+k, propSchema, v); err != nil {
			return err
		}
	}
	return nil
}

func validateValue(path string, schema map[string]any, value any) error {
	st := strings.TrimSpace(asString(schema["type"]))
	if st == "" {
		props := schemaProperties(schema["properties"])
		switch {
		case len(props) > 0 || len(requiredList(schema["required"])) > 0:
			st = "object"
		case schema["items"] != nil:
			st = "array"
		default:
			return nil
		}
	}
	if enumRaw, ok := schema["enum"]; ok {
		if !valueInEnum(value, enumRaw) {
			return fmt.Errorf("schema validation failed: %s has invalid enum value %v", path, value)
		}
	}
	switch st {
	case "string":
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("schema validation failed: %s must be string", path)
		}
		if err := validateStringFormat(path, schema, text); err != nil {
			return err
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("schema validation failed: %s must be boolean", path)
		}
	case "number":
		if !isNumeric(value) {
			return fmt.Errorf("schema validation failed: %s must be number", path)
		}
		if err := validateNumericBounds(path, schema, value); err != nil {
			return err
		}
	case "integer":
		if !isInteger(value) {
			return fmt.Errorf("schema validation failed: %s must be integer", path)
		}
		if err := validateNumericBounds(path, schema, value); err != nil {
			return err
		}
	case "array":
		arr, ok := asArray(value)
		if !ok {
			return fmt.Errorf("schema validation failed: %s must be array", path)
		}
		if itemsRaw, ok := schema["items"]; ok {
			if itemSchema, ok := itemsRaw.(map[string]any); ok {
				for i, it := range arr {
					if err := validateValue(fmt.Sprintf("%s[%d]", path, i), itemSchema, it); err != nil {
						return err
					}
				}
			}
		}
	case "object":
		obj, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("schema validation failed: %s must be object", path)
		}
		if err := validateSchemaObject(path, schema, obj); err != nil {
			return err
		}
	case "null":
		if value != nil {
			return fmt.Errorf("schema validation failed: %s must be null", path)
		}
	default:
		return fmt.Errorf("schema validation failed: %s has unsupported schema type %q", path, st)
	}
	return nil
}

func validateNumericBounds(path string, schema map[string]any, value any) error {
	n, ok := runtimesharedjson.AsFloat64(value)
	if !ok {
		return fmt.Errorf("schema validation failed: %s must be numeric", path)
	}
	if minRaw, ok := schema["minimum"]; ok {
		min, ok := runtimesharedjson.AsFloat64(minRaw)
		if ok && n < min {
			return fmt.Errorf("schema validation failed: %s must be >= %v", path, min)
		}
	}
	if maxRaw, ok := schema["maximum"]; ok {
		max, ok := runtimesharedjson.AsFloat64(maxRaw)
		if ok && n > max {
			return fmt.Errorf("schema validation failed: %s must be <= %v", path, max)
		}
	}
	return nil
}

func schemaProperties(raw any) map[string]map[string]any {
	return runtimesharedjson.SchemaProperties(raw)
}

func schemaAdditionalProps(raw any) bool { return runtimesharedjson.SchemaAdditionalProps(raw) }
func requiredList(raw any) []string      { return runtimesharedjson.RequiredList(raw) }

func validateStringFormat(path string, schema map[string]any, value string) error {
	switch strings.TrimSpace(asString(schema["format"])) {
	case "":
		return nil
	case "date-time":
		if _, err := time.Parse(time.RFC3339, strings.TrimSpace(value)); err != nil {
			return fmt.Errorf("schema validation failed: %s must be RFC3339 date-time", path)
		}
		return nil
	case "uuid":
		if _, err := uuid.Parse(strings.TrimSpace(value)); err != nil {
			return fmt.Errorf("schema validation failed: %s must be uuid", path)
		}
		return nil
	default:
		return nil
	}
}

func valueInEnum(value any, enumRaw any) bool {
	enum, ok := enumRaw.([]any)
	if !ok {
		switch t := enumRaw.(type) {
		case []string:
			for _, v := range t {
				if strings.TrimSpace(asString(value)) == strings.TrimSpace(v) {
					return true
				}
			}
			return false
		default:
			return true
		}
	}
	for _, v := range enum {
		if strings.TrimSpace(asString(value)) == strings.TrimSpace(asString(v)) {
			return true
		}
	}
	return false
}

func isNumeric(v any) bool        { return runtimesharedjson.IsNumeric(v) }
func isInteger(v any) bool        { return runtimesharedjson.IsInteger(v) }
func asArray(v any) ([]any, bool) { return runtimesharedjson.AsArray(v) }
func asString(v any) string       { return runtimesharedjson.AsString(v) }
