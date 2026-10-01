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
			if err != nil {
				return nil, err
			}
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
			raw, err = projectToolSchemaPropertiesValue(field.Value, fieldPath, depth, locations)
		case "items", "additionalProperties":
			if field.Value.Presence() == yamlsource.PresenceMapping || field.Value.Presence() == yamlsource.PresenceEmptyMapping {
				raw, err = projectToolSchemaValue(field.Value, fieldPath, depth+1, locations)
			} else {
				err = field.Value.Project(&raw)
			}
		case "enum":
			raw, err = projectToolSchemaEnumValue(field.Value)
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

func projectToolSchemaPropertiesValue(value yamlsource.Value, path string, depth int, locations map[string]yamlsource.Value) (map[string]any, error) {
	members, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	for _, member := range members {
		child, err := projectToolSchemaValue(member.Value, path+"["+strconv.Quote(member.Name)+"]", depth+1, locations)
		if err != nil {
			return nil, err
		}
		out[member.Name] = child
	}
	return out, nil
}

func projectToolSchemaEnumValue(value yamlsource.Value) ([]any, error) {
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
}

func toolInputSchemaEnumLiteralValue(value yamlsource.Value) (any, error) {
	switch value.Presence() {
	case yamlsource.PresenceNull, yamlsource.PresenceEmptyScalar, yamlsource.PresenceScalar:
		return toolInputSchemaEnumScalarValue(value)
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

func toolInputSchemaEnumScalarValue(value yamlsource.Value) (any, error) {
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
	if err := validateToolEnumScalarTag(scalar.Tag, literal); err != nil {
		return nil, err
	}
	return literal, nil
}

func validateToolEnumScalarTag(tag string, literal any) error {
	switch tag {
	case "!!bool":
		if _, ok := literal.(bool); !ok {
			return fmt.Errorf("enum boolean tag does not match value")
		}
	case "!!int":
		number, ok := literal.(float64)
		if !ok || math.Trunc(number) != number {
			return fmt.Errorf("enum integer tag does not match value")
		}
	case "!!float":
		if _, ok := literal.(float64); !ok {
			return fmt.Errorf("enum numeric tag does not match value")
		}
	case "!!null":
		if literal != nil {
			return fmt.Errorf("enum null tag does not match value")
		}
	}
	return nil
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
		fieldOptions, err := admitToolSchemaOptions(name, value, fieldPath, depth, property)
		if err != nil {
			return ToolInputSchema{}, err
		}
		options = append(options, fieldOptions...)
	}
	schema, err := NewToolInputSchema(ToolSchemaKind(kind), options...)
	if err != nil {
		return ToolInputSchema{}, toolSchemaFieldError("%s: %w", path, err)
	}
	return schema, nil
}

func admitToolSchemaOptions(name string, value any, path string, depth int, property bool) ([]ToolInputSchemaOption, error) {
	switch name {
	case "type":
		return nil, nil
	case "description", "pattern", "format", "x-swarm-equalTo":
		return admitToolSchemaTextOption(name, value, path, property)
	case "properties":
		return admitToolSchemaPropertyOptions(value, path, depth)
	case "required", "enum":
		option, err := admitToolSchemaSequenceOption(name, value, path)
		return []ToolInputSchemaOption{option}, err
	case "items", "additionalProperties":
		option, err := admitToolSchemaNestedOption(name, value, path, depth)
		return []ToolInputSchemaOption{option}, err
	case "minimum", "maximum":
		option, err := admitToolSchemaNumberOption(name, value, path)
		return []ToolInputSchemaOption{option}, err
	default:
		option, err := admitToolSchemaLengthOption(name, value, path)
		return []ToolInputSchemaOption{option}, err
	}
}

func admitToolSchemaTextOption(name string, value any, path string, property bool) ([]ToolInputSchemaOption, error) {
	text, ok := value.(string)
	if !ok || (name != "description" && strings.TrimSpace(text) == "") {
		return nil, toolSchemaFieldError("%s must be %stext", path, map[bool]string{true: "non-empty "}[name != "description"])
	}
	switch name {
	case "description":
		return []ToolInputSchemaOption{ToolSchemaDescription(text)}, nil
	case "pattern":
		return []ToolInputSchemaOption{ToolSchemaPattern(text)}, nil
	case "format":
		return []ToolInputSchemaOption{ToolSchemaFormat(text)}, nil
	default:
		if property {
			return nil, nil
		}
		return []ToolInputSchemaOption{ToolSchemaEqualTo(text)}, nil
	}
}

func admitToolSchemaPropertyOptions(value any, path string, depth int) ([]ToolInputSchemaOption, error) {
	members, ok := value.(map[string]any)
	if !ok || members == nil {
		return nil, toolSchemaFieldError("%s must be a mapping", path)
	}
	properties := map[string]ToolInputSchema{}
	equalities := map[string]string{}
	for _, key := range sortedContractKeys(members) {
		childPath := path + "[" + strconv.Quote(key) + "]"
		child, ok := members[key].(map[string]any)
		if !ok {
			return nil, toolSchemaFieldError("%s must be a mapping", childPath)
		}
		admitted, err := admitToolSchemaMap(child, childPath, depth+1, true)
		if err != nil {
			return nil, err
		}
		properties[key] = admitted
		if equal, exists := child["x-swarm-equalTo"]; exists {
			equalities[key] = equal.(string)
		}
	}
	options := []ToolInputSchemaOption{ToolSchemaProperties(properties)}
	if len(equalities) > 0 {
		options = append(options, toolSchemaPropertyEqualities(equalities))
	}
	return options, nil
}

func admitToolSchemaSequenceOption(name string, value any, path string) (ToolInputSchemaOption, error) {
	sequence := reflect.ValueOf(value)
	if !sequence.IsValid() || (sequence.Kind() != reflect.Slice && sequence.Kind() != reflect.Array) || (sequence.Kind() == reflect.Slice && sequence.IsNil()) {
		return nil, toolSchemaFieldError("%s must be a sequence", path)
	}
	values := make([]any, sequence.Len())
	for i := range values {
		values[i] = sequence.Index(i).Interface()
	}
	if name == "enum" {
		return ToolSchemaEnum(values...), nil
	}
	names := make([]string, len(values))
	for i, item := range values {
		text, ok := item.(string)
		if !ok {
			return nil, toolSchemaFieldError("%s[%d] must be text", path, i)
		}
		names[i] = text
	}
	return ToolSchemaRequired(names...), nil
}

func admitToolSchemaNestedOption(name string, value any, path string, depth int) (ToolInputSchemaOption, error) {
	if allowed, ok := value.(bool); name == "additionalProperties" && ok {
		return ToolSchemaAdditionalPropertiesAllowed(allowed), nil
	}
	child, ok := value.(map[string]any)
	if !ok {
		return nil, toolSchemaFieldError("%s must be a mapping%s", path, map[bool]string{true: " or boolean"}[name == "additionalProperties"])
	}
	admitted, err := admitToolSchemaMap(child, path, depth+1, false)
	if err != nil {
		return nil, err
	}
	if name == "items" {
		return ToolSchemaItems(admitted), nil
	}
	return ToolSchemaAdditionalPropertiesSchema(admitted), nil
}

func admitToolSchemaNumberOption(name string, value any, path string) (ToolInputSchemaOption, error) {
	literal, err := canonicaljson.FromGo(value)
	if err != nil {
		return nil, toolSchemaFieldError("%s: %w", path, err)
	}
	number, ok := literal.Number()
	if !ok {
		return nil, toolSchemaFieldError("%s must be a finite number", path)
	}
	if name == "minimum" {
		return ToolSchemaMinimum(number), nil
	}
	return ToolSchemaMaximum(number), nil
}

func admitToolSchemaLengthOption(name string, value any, path string) (ToolInputSchemaOption, error) {
	number := reflect.ValueOf(value)
	var integer int64
	switch number.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		integer = number.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if number.Uint() > uint64(^uint(0)>>1) {
			return nil, toolSchemaFieldError("%s integer overflows", path)
		}
		integer = int64(number.Uint())
	default:
		return nil, toolSchemaFieldError("%s must be a nonnegative integer, not a floating or quoted number", path)
	}
	if integer < 0 || int64(int(integer)) != integer {
		return nil, toolSchemaFieldError("%s must be a nonnegative integer within native bounds", path)
	}
	n := int(integer)
	switch name {
	case "minLength":
		return ToolSchemaMinLength(n), nil
	case "maxLength":
		return ToolSchemaMaxLength(n), nil
	case "minItems":
		return ToolSchemaMinItems(n), nil
	default:
		return ToolSchemaMaxItems(n), nil
	}
}
