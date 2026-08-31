package eventschema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
	"github.com/google/uuid"
)

type Violation struct {
	Path       string
	Constraint string
	Expected   string
	Actual     string
	Detail     string
}

func (v *Violation) Error() string {
	if v == nil {
		return "schema validation failed"
	}
	return "schema validation failed: " + v.Detail
}

func violation(path, constraint, expected, actual, detail string, args ...any) error {
	return &Violation{
		Path:       strings.TrimSpace(path),
		Constraint: strings.TrimSpace(constraint),
		Expected:   strings.TrimSpace(expected),
		Actual:     actual,
		Detail:     fmt.Sprintf(detail, args...),
	}
}

func schemaValueType(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, json.Number:
		return "number"
	case []any, []string:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "unsupported"
	}
}

func ValidatePayloadAgainstSchema(schema map[string]any, payload map[string]any) error {
	if schema == nil {
		return nil
	}
	return validateSchemaObject("$", CanonicalAcceptanceSchema(schema), payload)
}

// ValidateValueAgainstSchema validates one value against an already-resolved
// JSON schema. Contract admission uses this for literal fields before a full
// event payload exists; runtime publication uses ValidatePayloadAgainstSchema.
func ValidateValueAgainstSchema(schema map[string]any, value any) error {
	if schema == nil {
		return nil
	}
	return validateValue("$", CanonicalAcceptanceSchema(schema), value)
}

// CanonicalAcceptanceSchema projects the exact schema subset enforced by this
// package. Presentation metadata is deliberately excluded so semantic hashes
// change with accepted values, not labels or descriptions.
func CanonicalAcceptanceSchema(schema map[string]any) map[string]any {
	if schema == nil {
		return nil
	}
	out := map[string]any{}
	if value := strings.TrimSpace(asString(schema["type"])); value != "" {
		out["type"] = value
	}
	if schemaAllowsNull(schema) {
		out["nullable"] = true
	}
	if raw, ok := schema["enum"]; ok {
		if values, ok := asArray(raw); ok {
			out["enum"] = canonicalEnumValues(values)
		}
	}
	if value := strings.TrimSpace(asString(schema["format"])); value == "date-time" || value == "uuid" {
		out["format"] = value
	}
	if value, ok := schema["pattern"].(string); ok && value != "" {
		out["pattern"] = value
	}
	if value := strings.TrimSpace(asString(schema["x-swarm-equalTo"])); value != "" {
		out["x-swarm-equalTo"] = value
	}
	for _, key := range []string{"minLength", "maxLength", "minItems", "maxItems"} {
		if raw, exists := schema[key]; exists {
			if value, ok := runtimesharedjson.AsFloat64(raw); ok && math.Trunc(value) == value && value >= 0 {
				out[key] = int(value)
			} else {
				out[key] = raw
			}
		}
	}
	for _, key := range []string{"minimum", "maximum"} {
		if raw, exists := schema[key]; exists {
			if value, ok := runtimesharedjson.AsFloat64(raw); ok {
				out[key] = value
			} else {
				out[key] = raw
			}
		}
	}
	required := uniqueSortedStrings(exactRequiredList(schema["required"]), false)
	if len(required) > 0 {
		out["required"] = required
	}
	properties := schemaProperties(schema["properties"])
	if len(properties) > 0 {
		projected := make(map[string]any, len(properties))
		for name, property := range properties {
			projected[name] = CanonicalAcceptanceSchema(property)
		}
		out["properties"] = projected
	}
	if items, ok := schema["items"].(map[string]any); ok {
		out["items"] = CanonicalAcceptanceSchema(items)
	}
	_, hasAdditionalProperties := schema["additionalProperties"]
	if strings.TrimSpace(asString(schema["type"])) == "object" || len(properties) > 0 || len(required) > 0 || hasAdditionalProperties {
		switch additional := schema["additionalProperties"].(type) {
		case map[string]any:
			out["additionalProperties"] = CanonicalAcceptanceSchema(additional)
		case bool:
			out["additionalProperties"] = additional
		default:
			out["additionalProperties"] = true
		}
	}
	return out
}

func uniqueSortedStrings(values []string, preserveEmpty bool) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if !preserveEmpty && value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func canonicalEnumValues(values []any) []any {
	byCanonical := make(map[string]any, len(values))
	keys := make([]string, 0, len(values))
	for _, value := range values {
		raw, err := canonicaljson.Bytes(value)
		if err != nil {
			continue
		}
		key := string(raw)
		if _, exists := byCanonical[key]; exists {
			continue
		}
		var normalized any
		if err := canonicaljson.DecodeInto(raw, &normalized); err != nil {
			continue
		}
		byCanonical[key] = normalized
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, byCanonical[key])
	}
	return out
}

func validateSchemaObject(path string, schema map[string]any, payload map[string]any) error {
	if schemaType := strings.TrimSpace(asString(schema["type"])); schemaType != "" && schemaType != "object" {
		return nil
	}
	required := exactRequiredList(schema["required"])
	for _, key := range required {
		if _, ok := payload[key]; !ok {
			return violation(path+"."+key, "required", "present", "missing", "%s.%s is required", path, key)
		}
	}
	props := schemaProperties(schema["properties"])
	allowAdditional, additionalSchema := schemaAdditionalProperties(schema["additionalProperties"])
	for _, k := range sortedMapKeys(payload) {
		v := payload[k]
		propSchema, known := props[k]
		if !known {
			if additionalSchema != nil {
				if err := validateValue(path+"."+k, additionalSchema, v); err != nil {
					return err
				}
				continue
			}
			if allowAdditional {
				continue
			}
			return violation(path+"."+k, "additionalProperties", "declared property", "undeclared property", "%s.%s is not allowed", path, k)
		}
		if err := validateValue(path+"."+k, propSchema, v); err != nil {
			return err
		}
	}
	for _, key := range sortedMapKeys(props) {
		propSchema := props[key]
		target := strings.TrimSpace(asString(propSchema["x-swarm-equalTo"]))
		if target == "" {
			continue
		}
		value, ok := payload[key]
		if !ok {
			continue
		}
		other, ok := payload[target]
		if !ok {
			return violation(path+"."+key, "x-swarm-equalTo", path+"."+target, "target missing", "%s.%s must equal %s.%s, but target is missing", path, key, path, target)
		}
		if !semanticValuesEqual(value, other) {
			return violation(path+"."+key, "x-swarm-equalTo", path+"."+target, "different value", "%s.%s must equal %s.%s", path, key, path, target)
		}
	}
	return nil
}

func sortedMapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func semanticValuesEqual(left, right any) bool {
	leftValue, err := canonicaljson.FromGo(left)
	if err != nil {
		return false
	}
	rightValue, err := canonicaljson.FromGo(right)
	return err == nil && leftValue.Equal(rightValue)
}

func validateValue(path string, schema map[string]any, value any) error {
	if value == nil && schemaAllowsNull(schema) {
		return nil
	}
	st := strings.TrimSpace(asString(schema["type"]))
	if st == "" {
		props := schemaProperties(schema["properties"])
		switch {
		case len(props) > 0 || len(exactRequiredList(schema["required"])) > 0:
			st = "object"
		case schema["items"] != nil:
			st = "array"
		default:
			return nil
		}
	}
	if enumRaw, ok := schema["enum"]; ok {
		if !valueInEnum(value, enumRaw) {
			return violation(path, "enum", "declared enum member", fmt.Sprint(value), "%s has invalid enum value %v", path, value)
		}
	}
	switch st {
	case "string":
		text, ok := value.(string)
		if !ok {
			return violation(path, "type", "string", schemaValueType(value), "%s must be string", path)
		}
		if err := validateStringFormat(path, schema, text); err != nil {
			return err
		}
		if err := validateStringRefinements(path, schema, text); err != nil {
			return err
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return violation(path, "type", "boolean", schemaValueType(value), "%s must be boolean", path)
		}
	case "number":
		if !isNumeric(value) {
			return violation(path, "type", "number", schemaValueType(value), "%s must be number", path)
		}
		if err := validateNumericBounds(path, schema, value); err != nil {
			return err
		}
	case "integer":
		if !isInteger(value) {
			return violation(path, "type", "integer", schemaValueType(value), "%s must be integer", path)
		}
		if err := validateNumericBounds(path, schema, value); err != nil {
			return err
		}
	case "array":
		arr, ok := asArray(value)
		if !ok {
			return violation(path, "type", "array", schemaValueType(value), "%s must be array", path)
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
		if err := validateArrayLength(path, schema, len(arr)); err != nil {
			return err
		}
	case "object":
		obj, ok := value.(map[string]any)
		if !ok {
			return violation(path, "type", "object", schemaValueType(value), "%s must be object", path)
		}
		if err := validateSchemaObject(path, schema, obj); err != nil {
			return err
		}
	case "null":
		if value != nil {
			return violation(path, "type", "null", schemaValueType(value), "%s must be null", path)
		}
	default:
		return violation(path, "type", "supported schema type", st, "%s has unsupported schema type %q", path, st)
	}
	return nil
}

func validateStringRefinements(path string, schema map[string]any, value string) error {
	if pattern, ok := schema["pattern"].(string); ok && pattern != "" {
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return violation(path, "pattern", "valid regular expression", pattern, "%s has invalid pattern refinement", path)
		}
		if !compiled.MatchString(value) {
			return violation(path, "pattern", pattern, value, "%s must match pattern %q", path, pattern)
		}
	}
	length := utf8.RuneCountInString(value)
	if minRaw, ok := schema["minLength"]; ok {
		min, ok := runtimesharedjson.AsFloat64(minRaw)
		if !ok || math.Trunc(min) != min || min < 0 {
			return violation(path, "minLength", "non-negative integer", fmt.Sprint(minRaw), "%s minLength is not a supported non-negative integer", path)
		}
		if length < int(min) {
			return violation(path, "minLength", fmt.Sprintf(">= %d", int(min)), fmt.Sprintf("%d", length), "%s length must be >= %d", path, int(min))
		}
	}
	if maxRaw, ok := schema["maxLength"]; ok {
		max, ok := runtimesharedjson.AsFloat64(maxRaw)
		if !ok || math.Trunc(max) != max || max < 0 {
			return violation(path, "maxLength", "non-negative integer", fmt.Sprint(maxRaw), "%s maxLength is not a supported non-negative integer", path)
		}
		if length > int(max) {
			return violation(path, "maxLength", fmt.Sprintf("<= %d", int(max)), fmt.Sprintf("%d", length), "%s length must be <= %d", path, int(max))
		}
	}
	return nil
}

func validateArrayLength(path string, schema map[string]any, length int) error {
	if minRaw, ok := schema["minItems"]; ok {
		min, ok := runtimesharedjson.AsFloat64(minRaw)
		if !ok || math.Trunc(min) != min || min < 0 {
			return violation(path, "minItems", "non-negative integer", fmt.Sprint(minRaw), "%s minItems is not a supported non-negative integer", path)
		}
		if length < int(min) {
			return violation(path, "minItems", fmt.Sprintf(">= %d", int(min)), fmt.Sprintf("%d", length), "%s length must be >= %d", path, int(min))
		}
	}
	if maxRaw, ok := schema["maxItems"]; ok {
		max, ok := runtimesharedjson.AsFloat64(maxRaw)
		if !ok || math.Trunc(max) != max || max < 0 {
			return violation(path, "maxItems", "non-negative integer", fmt.Sprint(maxRaw), "%s maxItems is not a supported non-negative integer", path)
		}
		if length > int(max) {
			return violation(path, "maxItems", fmt.Sprintf("<= %d", int(max)), fmt.Sprintf("%d", length), "%s length must be <= %d", path, int(max))
		}
	}
	return nil
}

func validateNumericBounds(path string, schema map[string]any, value any) error {
	n, ok := runtimesharedjson.AsFloat64(value)
	if !ok {
		return violation(path, "numeric", "number", schemaValueType(value), "%s must be numeric", path)
	}
	if minRaw, ok := schema["minimum"]; ok {
		min, ok := runtimesharedjson.AsFloat64(minRaw)
		if !ok {
			return violation(path, "minimum", "supported JSON number", fmt.Sprint(minRaw), "%s minimum is not a supported JSON number", path)
		}
		if n < min {
			return violation(path, "minimum", fmt.Sprintf(">= %v", min), fmt.Sprint(n), "%s must be >= %v", path, min)
		}
	}
	if maxRaw, ok := schema["maximum"]; ok {
		max, ok := runtimesharedjson.AsFloat64(maxRaw)
		if !ok {
			return violation(path, "maximum", "supported JSON number", fmt.Sprint(maxRaw), "%s maximum is not a supported JSON number", path)
		}
		if n > max {
			return violation(path, "maximum", fmt.Sprintf("<= %v", max), fmt.Sprint(n), "%s must be <= %v", path, max)
		}
	}
	return nil
}

func schemaProperties(raw any) map[string]map[string]any {
	return runtimesharedjson.SchemaProperties(raw)
}

func exactRequiredList(raw any) []string {
	switch typed := raw.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, value := range typed {
			if text, ok := value.(string); ok {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func schemaAdditionalProperties(raw any) (bool, map[string]any) {
	switch typed := raw.(type) {
	case nil:
		return true, nil
	case bool:
		return typed, nil
	case map[string]any:
		return true, typed
	default:
		return false, nil
	}
}

func schemaAllowsNull(schema map[string]any) bool {
	if schema == nil {
		return false
	}
	if b, ok := schema["nullable"].(bool); ok && b {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(asString(schema["nullable"])), "true")
}

func validateStringFormat(path string, schema map[string]any, value string) error {
	switch strings.TrimSpace(asString(schema["format"])) {
	case "":
		return nil
	case "date-time":
		if _, err := time.Parse(time.RFC3339, strings.TrimSpace(value)); err != nil {
			return violation(path, "format", "RFC3339 date-time", value, "%s must be RFC3339 date-time", path)
		}
		return nil
	case "uuid":
		if _, err := uuid.Parse(strings.TrimSpace(value)); err != nil {
			return violation(path, "format", "uuid", value, "%s must be uuid", path)
		}
		return nil
	default:
		return nil
	}
}

func valueInEnum(value any, enumRaw any) bool {
	enum, ok := asArray(enumRaw)
	if !ok {
		return false
	}
	want, err := canonicaljson.Bytes(value)
	if err != nil {
		return false
	}
	for _, candidate := range enum {
		got, err := canonicaljson.Bytes(candidate)
		if err == nil && bytes.Equal(want, got) {
			return true
		}
	}
	return false
}

func isNumeric(v any) bool        { return runtimesharedjson.IsNumeric(v) }
func isInteger(v any) bool        { return runtimesharedjson.IsInteger(v) }
func asArray(v any) ([]any, bool) { return runtimesharedjson.AsArray(v) }
func asString(v any) string       { return runtimesharedjson.AsString(v) }
