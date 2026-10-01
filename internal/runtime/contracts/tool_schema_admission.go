package contracts

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/yamlsource"
)

var toolSchemaFields = map[string]struct{}{
	"type": {}, "description": {}, "properties": {}, "required": {}, "items": {}, "enum": {},
	"additionalProperties": {}, "minimum": {}, "maximum": {}, "pattern": {}, "format": {},
	"x-swarm-equalTo": {}, "minLength": {}, "maxLength": {}, "minItems": {}, "maxItems": {},
}

// The platform envelope remains a separate grammar. Its authored schema
// children nevertheless retain their original tags and source coordinates.
func admitPlatformInterfaceSchemaValues(root yamlsource.Value) (map[string]map[string]map[string]ToolInputSchema, error) {
	out := map[string]map[string]map[string]ToolInputSchema{}
	interfaces, err := root.Lookup("interfaces")
	if err != nil || interfaces.Presence == yamlsource.PresenceMissing || interfaces.Presence == yamlsource.PresenceNull {
		return out, err
	}
	if err := interfaces.Value.ValidateExpansion(); err != nil {
		return nil, err
	}
	families, err := uniqueYAMLMappingFields(interfaces.Value, "interfaces")
	if err != nil {
		return nil, err
	}
	for _, family := range families {
		if family.Value.Presence() == yamlsource.PresenceNull {
			continue
		}
		versions, err := uniqueYAMLMappingFields(family.Value, "interface versions")
		if err != nil {
			return nil, err
		}
		out[family.Name] = map[string]map[string]ToolInputSchema{}
		for _, version := range versions {
			if version.Value.Presence() == yamlsource.PresenceNull {
				continue
			}
			lookup, err := version.Value.Lookup("schemas")
			if err != nil {
				return nil, err
			}
			if lookup.Presence == yamlsource.PresenceMissing {
				continue
			}
			children, err := uniqueYAMLMappingFields(lookup.Value, "interface schemas")
			admitted := make(map[string]ToolInputSchema, len(children))
			for _, child := range children {
				schema, err := AdmitToolInputSchemaValue(child.Value)
				if err != nil {
					return nil, err
				}
				admitted[child.Name] = schema
			}
			out[family.Name][version.Name] = admitted
		}
	}
	return out, nil
}

// Authored and programmatic declarations terminate at the same semantic
// constructor. Source locations explain refusal; they never change admission.
func AdmitToolInputSchemaValue(value yamlsource.Value) (ToolInputSchema, error) {
	if err := value.ValidateExpansion(); err != nil {
		return ToolInputSchema{}, err
	}
	if err := value.ValidateUniqueMappings(); err != nil {
		return ToolInputSchema{}, err
	}
	if _, err := value.Mapping(); err != nil {
		return ToolInputSchema{}, nodeValueError(value, fmt.Errorf("tool schema must be a mapping"))
	}
	locations := map[string]yamlsource.Value{}
	raw, err := projectToolSchemaValue(value, "schema", 0, locations)
	if err != nil {
		return ToolInputSchema{}, err
	}
	schema, err := AdmitToolInputSchemaMap(raw)
	if err != nil {
		var refusal *toolSchemaRefusal
		if errors.As(err, &refusal) {
			if source, ok := locations[refusal.path]; ok {
				return ToolInputSchema{}, nodeValueError(source, err)
			}
		}
		return ToolInputSchema{}, nodeValueError(value, err)
	}
	return schema, nil
}

func projectToolSchemaValue(value yamlsource.Value, path string, depth int, locations map[string]yamlsource.Value) (map[string]any, error) {
	locations[path] = value
	if depth > MaxToolInputSchemaDepth {
		return nil, nodeValueError(value, fmt.Errorf("tool schema exceeds maximum depth %d", MaxToolInputSchemaDepth))
	}
	fields, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	for _, field := range fields {
		fieldPath := path + "." + field.Name
		locations[fieldPath] = field.Value
		var raw any
		switch field.Name {
		case "properties":
			members, err := field.Value.Mapping()
			if err != nil {
				return nil, err
			}
			properties := map[string]any{}
			for _, member := range members {
				child, err := projectToolSchemaValue(member.Value, fieldPath+"["+strconv.Quote(member.Name)+"]", depth+1, locations)
				if err != nil {
					return nil, err
				}
				properties[member.Name] = child
			}
			raw = properties
		case "items", "additionalProperties":
			if field.Value.Presence() == yamlsource.PresenceMapping || field.Value.Presence() == yamlsource.PresenceEmptyMapping {
				raw, err = projectToolSchemaValue(field.Value, fieldPath, depth+1, locations)
			} else {
				err = field.Value.Project(&raw)
			}
		case "enum":
			var items []yamlsource.Value
			items, err = field.Value.Sequence()
			values := make([]any, len(items))
			for i, item := range items {
				if err != nil {
					break
				}
				values[i], err = toolInputSchemaEnumLiteralValue(item)
			}
			raw = values
		default:
			err = field.Value.Project(&raw)
		}
		if err != nil {
			return nil, nodeValueError(field.Value, err)
		}
		out[field.Name] = raw
	}
	return out, nil
}

func toolInputSchemaEnumLiteralValue(value yamlsource.Value) (any, error) {
	switch value.Presence() {
	case yamlsource.PresenceNull, yamlsource.PresenceEmptyScalar, yamlsource.PresenceScalar:
		scalar, err := value.Scalar()
		if err != nil {
			return nil, err
		}
		if scalar.Tag == "!!str" {
			if !utf8.ValidString(scalar.Value) {
				return nil, fmt.Errorf("enum string is not valid UTF-8")
			}
			return scalar.Value, nil
		}
		if scalar.Tag != "!!bool" && scalar.Tag != "!!int" && scalar.Tag != "!!float" && scalar.Tag != "!!null" {
			return nil, fmt.Errorf("enum scalar tag %q is not JSON", scalar.Tag)
		}
		var literal any
		if err := canonicaljson.DecodeInto([]byte(scalar.Value), &literal); err != nil {
			return nil, fmt.Errorf("enum literal %q is not JSON: %w", scalar.Value, err)
		}
		switch scalar.Tag {
		case "!!bool":
			if _, ok := literal.(bool); !ok {
				return nil, fmt.Errorf("enum boolean tag does not match value")
			}
		case "!!int":
			number, ok := literal.(float64)
			if !ok || math.Trunc(number) != number {
				return nil, fmt.Errorf("enum integer tag does not match value")
			}
		case "!!float":
			if _, ok := literal.(float64); !ok {
				return nil, fmt.Errorf("enum numeric tag does not match value")
			}
		case "!!null":
			if literal != nil {
				return nil, fmt.Errorf("enum null tag does not match value")
			}
		}
		return literal, nil
	case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
		items, err := value.Sequence()
		if err != nil {
			return nil, err
		}
		out := make([]any, len(items))
		for i, item := range items {
			out[i], err = toolInputSchemaEnumLiteralValue(item)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
		fields, err := value.Mapping()
		if err != nil {
			return nil, err
		}
		out := map[string]any{}
		for _, field := range fields {
			out[field.Name], err = toolInputSchemaEnumLiteralValue(field.Value)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("enum literal is missing")
	}
}

func AdmitToolInputSchemaMap(raw map[string]any) (ToolInputSchema, error) {
	return admitToolSchemaMap(raw, "schema", 0, false)
}

type toolSchemaRefusal struct {
	path  string
	cause error
}

func (e *toolSchemaRefusal) Error() string { return e.cause.Error() }
func (e *toolSchemaRefusal) Unwrap() error { return e.cause }
func toolSchemaFieldError(format string, path string, args ...any) error {
	return &toolSchemaRefusal{path: path, cause: fmt.Errorf(format, append([]any{path}, args...)...)}
}

func admitToolSchemaMap(raw map[string]any, path string, depth int, property bool) (ToolInputSchema, error) {
	if raw == nil || depth > MaxToolInputSchemaDepth {
		return ToolInputSchema{}, toolSchemaFieldError("%s must be a mapping within maximum depth %d", path, MaxToolInputSchemaDepth)
	}
	if len(raw) == 0 {
		return NewToolInputSchema(ToolSchemaAny)
	}
	kind, ok := raw["type"].(string)
	if !ok || kind == "" {
		return ToolInputSchema{}, toolSchemaFieldError("%s must be non-empty text", path+".type")
	}
	options := []ToolInputSchemaOption{}
	for _, name := range sortedContractKeys(raw) {
		value := raw[name]
		fieldPath := path + "." + name
		if _, ok := toolSchemaFields[name]; !ok {
			return ToolInputSchema{}, toolSchemaFieldError("%s: unsupported tool schema field", fieldPath)
		}
		if value == nil {
			return ToolInputSchema{}, toolSchemaFieldError("%s must not be null", fieldPath)
		}
		switch name {
		case "type":
		case "description", "pattern", "format", "x-swarm-equalTo":
			text, ok := value.(string)
			if !ok || (name != "description" && strings.TrimSpace(text) == "") {
				return ToolInputSchema{}, toolSchemaFieldError("%s must be %stext", fieldPath, map[bool]string{true: "non-empty "}[name != "description"])
			}
			switch name {
			case "description":
				options = append(options, ToolSchemaDescription(text))
			case "pattern":
				options = append(options, ToolSchemaPattern(text))
			case "format":
				options = append(options, ToolSchemaFormat(text))
			case "x-swarm-equalTo":
				if !property {
					options = append(options, ToolSchemaEqualTo(text))
				}
			}
		case "properties":
			members, ok := value.(map[string]any)
			if !ok || members == nil {
				return ToolInputSchema{}, toolSchemaFieldError("%s must be a mapping", fieldPath)
			}
			properties := map[string]ToolInputSchema{}
			equalities := map[string]string{}
			for _, key := range sortedContractKeys(members) {
				childPath := fieldPath + "[" + strconv.Quote(key) + "]"
				child, ok := members[key].(map[string]any)
				if !ok {
					return ToolInputSchema{}, toolSchemaFieldError("%s must be a mapping", childPath)
				}
				admitted, err := admitToolSchemaMap(child, childPath, depth+1, true)
				if err != nil {
					return ToolInputSchema{}, err
				}
				properties[key] = admitted
				if equal, exists := child["x-swarm-equalTo"]; exists {
					equalities[key] = equal.(string)
				}
			}
			options = append(options, ToolSchemaProperties(properties))
			if len(equalities) > 0 {
				options = append(options, toolSchemaPropertyEqualities(equalities))
			}
		case "required", "enum":
			sequence := reflect.ValueOf(value)
			if !sequence.IsValid() || (sequence.Kind() != reflect.Slice && sequence.Kind() != reflect.Array) || (sequence.Kind() == reflect.Slice && sequence.IsNil()) {
				return ToolInputSchema{}, toolSchemaFieldError("%s must be a sequence", fieldPath)
			}
			values := make([]any, sequence.Len())
			for i := range values {
				values[i] = sequence.Index(i).Interface()
			}
			if name == "enum" {
				options = append(options, ToolSchemaEnum(values...))
				continue
			}
			names := make([]string, len(values))
			for i, item := range values {
				text, ok := item.(string)
				if !ok {
					return ToolInputSchema{}, toolSchemaFieldError("%s[%d] must be text", fieldPath, i)
				}
				names[i] = text
			}
			options = append(options, ToolSchemaRequired(names...))
		case "items", "additionalProperties":
			if allowed, ok := value.(bool); name == "additionalProperties" && ok {
				options = append(options, ToolSchemaAdditionalPropertiesAllowed(allowed))
				continue
			}
			child, ok := value.(map[string]any)
			if !ok {
				return ToolInputSchema{}, toolSchemaFieldError("%s must be a mapping%s", fieldPath, map[bool]string{true: " or boolean"}[name == "additionalProperties"])
			}
			admitted, err := admitToolSchemaMap(child, fieldPath, depth+1, false)
			if err != nil {
				return ToolInputSchema{}, err
			}
			if name == "items" {
				options = append(options, ToolSchemaItems(admitted))
			} else {
				options = append(options, ToolSchemaAdditionalPropertiesSchema(admitted))
			}
		case "minimum", "maximum":
			literal, err := canonicaljson.FromGo(value)
			if err != nil {
				return ToolInputSchema{}, toolSchemaFieldError("%s: %w", fieldPath, err)
			}
			number, ok := literal.Number()
			if !ok {
				return ToolInputSchema{}, toolSchemaFieldError("%s must be a finite number", fieldPath)
			}
			if name == "minimum" {
				options = append(options, ToolSchemaMinimum(number))
			} else {
				options = append(options, ToolSchemaMaximum(number))
			}
		case "minLength", "maxLength", "minItems", "maxItems":
			number := reflect.ValueOf(value)
			var integer int64
			switch number.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				integer = number.Int()
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if number.Uint() > uint64(^uint(0)>>1) {
					return ToolInputSchema{}, toolSchemaFieldError("%s integer overflows", fieldPath)
				}
				integer = int64(number.Uint())
			default:
				return ToolInputSchema{}, toolSchemaFieldError("%s must be a nonnegative integer, not a floating or quoted number", fieldPath)
			}
			if integer < 0 || int64(int(integer)) != integer {
				return ToolInputSchema{}, toolSchemaFieldError("%s must be a nonnegative integer within native bounds", fieldPath)
			}
			n := int(integer)
			switch name {
			case "minLength":
				options = append(options, ToolSchemaMinLength(n))
			case "maxLength":
				options = append(options, ToolSchemaMaxLength(n))
			case "minItems":
				options = append(options, ToolSchemaMinItems(n))
			case "maxItems":
				options = append(options, ToolSchemaMaxItems(n))
			}
		}
	}
	schema, err := NewToolInputSchema(ToolSchemaKind(kind), options...)
	if err != nil {
		return ToolInputSchema{}, toolSchemaFieldError("%s: %w", path, err)
	}
	return schema, nil
}
