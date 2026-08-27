package contracts

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"gopkg.in/yaml.v3"
)

// ToolSchemaKind is the closed JSON type vocabulary admitted by tool schemas.
type ToolSchemaKind string

const (
	ToolSchemaAny     ToolSchemaKind = "any"
	ToolSchemaString  ToolSchemaKind = "string"
	ToolSchemaInteger ToolSchemaKind = "integer"
	ToolSchemaNumber  ToolSchemaKind = "number"
	ToolSchemaBoolean ToolSchemaKind = "boolean"
	ToolSchemaObject  ToolSchemaKind = "object"
	ToolSchemaArray   ToolSchemaKind = "array"
	ToolSchemaNull    ToolSchemaKind = "null"
)

func (k ToolSchemaKind) Valid() bool {
	switch k {
	case ToolSchemaAny, ToolSchemaString, ToolSchemaInteger, ToolSchemaNumber, ToolSchemaBoolean, ToolSchemaObject, ToolSchemaArray, ToolSchemaNull:
		return true
	default:
		return false
	}
}

type toolInputSchemaValue struct {
	kind        ToolSchemaKind
	description string

	properties map[string]ToolInputSchema
	required   []string

	items    ToolInputSchema
	hasItems bool

	enum         []semanticvalue.Value
	enumDeclared bool

	additionalAllowed         bool
	additionalAllowedDeclared bool
	additionalSchema          ToolInputSchema
	hasAdditionalSchema       bool

	minimum    float64
	hasMinimum bool
	maximum    float64
	hasMaximum bool

	pattern string
	format  toolSchemaFormat
	equalTo string

	minLength    int
	hasMinLength bool
	maxLength    int
	hasMaxLength bool
	minItems     int
	hasMinItems  bool
	maxItems     int
	hasMaxItems  bool
}

type toolSchemaFormat uint8

const (
	toolSchemaFormatUUID toolSchemaFormat = iota + 1
	toolSchemaFormatDateTime
)

func parseToolSchemaFormat(raw string) (toolSchemaFormat, error) {
	switch strings.TrimSpace(raw) {
	case "":
		return 0, nil
	case "uuid":
		return toolSchemaFormatUUID, nil
	case "date-time":
		return toolSchemaFormatDateTime, nil
	default:
		return 0, fmt.Errorf("unsupported schema format %q", raw)
	}
}

func (f toolSchemaFormat) String() string {
	switch f {
	case 0:
		return ""
	case toolSchemaFormatUUID:
		return "uuid"
	case toolSchemaFormatDateTime:
		return "date-time"
	default:
		return ""
	}
}

// ToolInputSchema is an immutable admitted schema. Its zero value carries no
// authority and is used only for optional schema positions.
type ToolInputSchema struct {
	value *toolInputSchemaValue
}

type toolInputSchemaDraft struct {
	value toolInputSchemaValue
}

// ToolInputSchemaOption is a closed construction option. Callers can request
// schema features but cannot retain or mutate the admitted representation.
type ToolInputSchemaOption interface {
	applyToolInputSchema(*toolInputSchemaDraft) error
}

type toolInputSchemaOption func(*toolInputSchemaDraft) error

func (o toolInputSchemaOption) applyToolInputSchema(draft *toolInputSchemaDraft) error {
	return o(draft)
}

func ToolSchemaDescription(description string) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		if !utf8.ValidString(description) {
			return fmt.Errorf("description is not valid UTF-8")
		}
		draft.value.description = description
		return nil
	})
}

func ToolSchemaProperties(properties map[string]ToolInputSchema) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.properties = make(map[string]ToolInputSchema, len(properties))
		for name, schema := range properties {
			draft.value.properties[name] = schema
		}
		return nil
	})
}

func toolSchemaPropertyEqualities(equalities map[string]string) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		for name, target := range equalities {
			property, exists := draft.value.properties[name]
			if !exists || property.value == nil {
				return fmt.Errorf("x-swarm-equalTo property %q is not declared", name)
			}
			copyValue := *property.value
			copyValue.equalTo = target
			draft.value.properties[name] = ToolInputSchema{value: &copyValue}
		}
		return nil
	})
}

func ToolSchemaRequired(names ...string) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.required = append([]string(nil), names...)
		return nil
	})
}

func ToolSchemaItems(items ToolInputSchema) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.items = items
		draft.value.hasItems = true
		return nil
	})
}

func ToolSchemaEnum(values ...any) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.enumDeclared = true
		draft.value.enum = make([]semanticvalue.Value, 0, len(values))
		for index, raw := range values {
			value, err := canonicaljson.FromGo(raw)
			if err != nil {
				return fmt.Errorf("enum[%d]: %w", index, err)
			}
			draft.value.enum = append(draft.value.enum, value)
		}
		return nil
	})
}

func ToolSchemaAdditionalPropertiesAllowed(allowed bool) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.additionalAllowed = allowed
		draft.value.additionalAllowedDeclared = true
		return nil
	})
}

func ToolSchemaAdditionalPropertiesSchema(schema ToolInputSchema) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.additionalSchema = schema
		draft.value.hasAdditionalSchema = true
		return nil
	})
}

func ToolSchemaMinimum(value float64) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.minimum = value
		draft.value.hasMinimum = true
		return nil
	})
}

func ToolSchemaMaximum(value float64) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.maximum = value
		draft.value.hasMaximum = true
		return nil
	})
}

func ToolSchemaPattern(pattern string) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.pattern = pattern
		return nil
	})
}

func ToolSchemaFormat(format string) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		admitted, err := parseToolSchemaFormat(format)
		if err != nil {
			return err
		}
		draft.value.format = admitted
		return nil
	})
}

func ToolSchemaEqualTo(field string) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		field = strings.TrimSpace(field)
		if field == "" || !utf8.ValidString(field) {
			return fmt.Errorf("x-swarm-equalTo requires a valid field name")
		}
		draft.value.equalTo = field
		return nil
	})
}

func ToolSchemaMinLength(value int) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.minLength = value
		draft.value.hasMinLength = true
		return nil
	})
}

func ToolSchemaMaxLength(value int) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.maxLength = value
		draft.value.hasMaxLength = true
		return nil
	})
}

func ToolSchemaMinItems(value int) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.minItems = value
		draft.value.hasMinItems = true
		return nil
	})
}

func ToolSchemaMaxItems(value int) ToolInputSchemaOption {
	return toolInputSchemaOption(func(draft *toolInputSchemaDraft) error {
		draft.value.maxItems = value
		draft.value.hasMaxItems = true
		return nil
	})
}

func NewToolInputSchema(kind ToolSchemaKind, options ...ToolInputSchemaOption) (ToolInputSchema, error) {
	draft := toolInputSchemaDraft{value: toolInputSchemaValue{kind: kind}}
	for _, option := range options {
		if option == nil {
			return ToolInputSchema{}, fmt.Errorf("tool schema option is nil")
		}
		if err := option.applyToolInputSchema(&draft); err != nil {
			return ToolInputSchema{}, err
		}
	}
	schema := ToolInputSchema{value: &draft.value}
	if err := validateAdmittedToolInputSchema("$", schema, 0); err != nil {
		return ToolInputSchema{}, err
	}
	return schema, nil
}

// MustToolInputSchema is for static declarations and tests. Dynamic authoring
// and generated catalogs must use NewToolInputSchema and return admission
// errors to their caller.
func MustToolInputSchema(kind ToolSchemaKind, options ...ToolInputSchemaOption) ToolInputSchema {
	schema, err := NewToolInputSchema(kind, options...)
	if err != nil {
		panic(err)
	}
	return schema
}

// AdmitToolInputSchemaMap admits programmatic JSON Schema through the same
// bounded lexical decoder used by authored schemas.
func AdmitToolInputSchemaMap(raw map[string]any) (ToolInputSchema, error) {
	if raw == nil {
		return ToolInputSchema{}, fmt.Errorf("tool schema is missing")
	}
	data, err := yaml.Marshal(raw)
	if err != nil {
		return ToolInputSchema{}, err
	}
	var schema ToolInputSchema
	if err := yaml.Unmarshal(data, &schema); err != nil {
		return ToolInputSchema{}, err
	}
	return schema, nil
}

func validateAdmittedToolInputSchema(path string, schema ToolInputSchema, depth int) error {
	return validateAdmittedToolInputSchemaActive(path, schema, depth, false, map[*toolInputSchemaValue]struct{}{})
}

func validateAdmittedToolInputSchemaActive(path string, schema ToolInputSchema, depth int, allowEqualTo bool, active map[*toolInputSchemaValue]struct{}) error {
	if schema.value == nil {
		return fmt.Errorf("%s schema is missing", path)
	}
	if depth > MaxToolInputSchemaDepth {
		return fmt.Errorf("%s exceeds maximum schema depth %d", path, MaxToolInputSchemaDepth)
	}
	value := schema.value
	if _, cyclic := active[value]; cyclic {
		return fmt.Errorf("%s contains a schema cycle", path)
	}
	active[value] = struct{}{}
	defer delete(active, value)
	if !value.kind.Valid() {
		return fmt.Errorf("%s requires an explicit supported JSON type, got %q", path, value.kind)
	}

	if value.kind != ToolSchemaObject && (len(value.properties) > 0 || len(value.required) > 0 || value.additionalAllowedDeclared || value.hasAdditionalSchema) {
		return fmt.Errorf("%s type %s cannot declare object constraints", path, value.kind)
	}
	if value.kind != ToolSchemaArray && (value.hasItems || value.hasMinItems || value.hasMaxItems) {
		return fmt.Errorf("%s type %s cannot declare array constraints", path, value.kind)
	}
	if value.kind != ToolSchemaString && (value.pattern != "" || value.format != 0 || value.hasMinLength || value.hasMaxLength) {
		return fmt.Errorf("%s type %s cannot declare string constraints", path, value.kind)
	}
	if value.equalTo != "" && !allowEqualTo {
		return fmt.Errorf("%s x-swarm-equalTo is only valid on a declared object property", path)
	}
	if value.kind != ToolSchemaInteger && value.kind != ToolSchemaNumber && (value.hasMinimum || value.hasMaximum) {
		return fmt.Errorf("%s type %s cannot declare numeric constraints", path, value.kind)
	}

	if value.hasMinimum {
		if !isFiniteSchemaNumber(value.minimum) || isNegativeZero(value.minimum) {
			return fmt.Errorf("%s minimum must be a finite non-negative-zero JSON number", path)
		}
		if _, err := semanticvalue.Number(value.minimum); err != nil {
			return fmt.Errorf("%s minimum is not a supported semantic JSON number: %w", path, err)
		}
	}
	if value.hasMaximum {
		if !isFiniteSchemaNumber(value.maximum) || isNegativeZero(value.maximum) {
			return fmt.Errorf("%s maximum must be a finite non-negative-zero JSON number", path)
		}
		if _, err := semanticvalue.Number(value.maximum); err != nil {
			return fmt.Errorf("%s maximum is not a supported semantic JSON number: %w", path, err)
		}
	}
	if value.hasMinimum && value.hasMaximum && value.minimum > value.maximum {
		return fmt.Errorf("%s minimum must be <= maximum", path)
	}
	if err := validateAdmittedBounds(path, "Length", value.minLength, value.hasMinLength, value.maxLength, value.hasMaxLength); err != nil {
		return err
	}
	if err := validateAdmittedBounds(path, "Items", value.minItems, value.hasMinItems, value.maxItems, value.hasMaxItems); err != nil {
		return err
	}
	if value.pattern != "" {
		if !utf8.ValidString(value.pattern) {
			return fmt.Errorf("%s pattern is not valid UTF-8", path)
		}
		if _, err := regexp.Compile(value.pattern); err != nil {
			return fmt.Errorf("%s pattern is invalid: %w", path, err)
		}
	}

	if value.kind == ToolSchemaArray {
		if !value.hasItems {
			return fmt.Errorf("%s array requires items", path)
		}
		if err := validateAdmittedToolInputSchemaActive(path+".items", value.items, depth+1, false, active); err != nil {
			return err
		}
	}
	if value.kind == ToolSchemaObject {
		if value.additionalAllowedDeclared && value.hasAdditionalSchema {
			return fmt.Errorf("%s additionalProperties must declare a boolean or schema, not both", path)
		}
		for name, property := range value.properties {
			if name == "" || !utf8.ValidString(name) || name != strings.TrimSpace(name) {
				return fmt.Errorf("%s property name %q is not canonical", path, name)
			}
			if err := validateAdmittedToolInputSchemaActive(path+".properties["+name+"]", property, depth+1, true, active); err != nil {
				return err
			}
		}
		for name, property := range value.properties {
			target := property.value.equalTo
			if target == "" {
				continue
			}
			if _, exists := value.properties[target]; !exists {
				return fmt.Errorf("%s property %q x-swarm-equalTo target %q is not declared", path, name, target)
			}
		}
		seenRequired := map[string]struct{}{}
		for _, name := range value.required {
			if name == "" || !utf8.ValidString(name) || name != strings.TrimSpace(name) {
				return fmt.Errorf("%s required property name %q is not canonical", path, name)
			}
			if _, duplicate := seenRequired[name]; duplicate {
				return fmt.Errorf("%s required property %q is duplicated", path, name)
			}
			seenRequired[name] = struct{}{}
			if _, exists := value.properties[name]; !exists {
				return fmt.Errorf("%s required property %q is not declared", path, name)
			}
		}
		if value.hasAdditionalSchema {
			if err := validateAdmittedToolInputSchemaActive(path+".additionalProperties", value.additionalSchema, depth+1, false, active); err != nil {
				return err
			}
		}
	}

	if value.enumDeclared && len(value.enum) == 0 {
		return fmt.Errorf("%s enum must contain at least one value", path)
	}
	for index, candidate := range value.enum {
		if err := validateToolSchemaValue(path+fmt.Sprintf(".enum[%d]", index), schema, candidate, false); err != nil {
			return err
		}
		for prior := 0; prior < index; prior++ {
			if candidate.Equal(value.enum[prior]) {
				return fmt.Errorf("%s enum[%d] duplicates another semantic value", path, index)
			}
		}
	}
	return nil
}

func validateAdmittedBounds(path, kind string, minimum int, hasMinimum bool, maximum int, hasMaximum bool) error {
	if hasMinimum && minimum < 0 {
		return fmt.Errorf("%s min%s must be non-negative", path, kind)
	}
	if hasMaximum && maximum < 0 {
		return fmt.Errorf("%s max%s must be non-negative", path, kind)
	}
	if hasMinimum && hasMaximum && minimum > maximum {
		return fmt.Errorf("%s min%s must be <= max%s", path, kind, kind)
	}
	return nil
}

func (s ToolInputSchema) IsZero() bool {
	return s.value == nil
}

func (s ToolInputSchema) ValidateDefinition() error {
	return validateAdmittedToolInputSchema("$", s, 0)
}

func (s ToolInputSchema) Kind() ToolSchemaKind {
	if s.value == nil {
		return ""
	}
	return s.value.kind
}

func (s ToolInputSchema) Description() string {
	if s.value == nil {
		return ""
	}
	return s.value.description
}

func (s ToolInputSchema) Properties() map[string]ToolInputSchema {
	if s.value == nil || len(s.value.properties) == 0 {
		return nil
	}
	out := make(map[string]ToolInputSchema, len(s.value.properties))
	for name, property := range s.value.properties {
		out[name] = property
	}
	return out
}

func (s ToolInputSchema) Property(name string) (ToolInputSchema, bool) {
	if s.value == nil {
		return ToolInputSchema{}, false
	}
	property, ok := s.value.properties[name]
	return property, ok
}

func (s ToolInputSchema) PropertyNames() []string {
	if s.value == nil {
		return nil
	}
	names := make([]string, 0, len(s.value.properties))
	for name := range s.value.properties {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s ToolInputSchema) RequiredProperties() []string {
	if s.value == nil {
		return nil
	}
	return append([]string(nil), s.value.required...)
}

func (s ToolInputSchema) IsRequired(name string) bool {
	if s.value == nil {
		return false
	}
	for _, required := range s.value.required {
		if required == name {
			return true
		}
	}
	return false
}

func (s ToolInputSchema) ItemsSchema() (ToolInputSchema, bool) {
	if s.value == nil || !s.value.hasItems {
		return ToolInputSchema{}, false
	}
	return s.value.items, true
}

func (s ToolInputSchema) EnumValues() ([]semanticvalue.Value, bool) {
	if s.value == nil || !s.value.enumDeclared {
		return nil, false
	}
	return append([]semanticvalue.Value(nil), s.value.enum...), true
}

func (s ToolInputSchema) AdditionalPropertiesAllowed() (bool, bool) {
	if s.value == nil || !s.value.additionalAllowedDeclared {
		return false, false
	}
	return s.value.additionalAllowed, true
}

func (s ToolInputSchema) AdditionalPropertiesSchema() (ToolInputSchema, bool) {
	if s.value == nil || !s.value.hasAdditionalSchema {
		return ToolInputSchema{}, false
	}
	return s.value.additionalSchema, true
}

func (s ToolInputSchema) Minimum() (float64, bool) {
	if s.value == nil || !s.value.hasMinimum {
		return 0, false
	}
	return s.value.minimum, true
}

func (s ToolInputSchema) Maximum() (float64, bool) {
	if s.value == nil || !s.value.hasMaximum {
		return 0, false
	}
	return s.value.maximum, true
}

func (s ToolInputSchema) Pattern() string {
	if s.value == nil {
		return ""
	}
	return s.value.pattern
}

func (s ToolInputSchema) Format() string {
	if s.value == nil {
		return ""
	}
	return s.value.format.String()
}

func (s ToolInputSchema) EqualTo() string {
	if s.value == nil {
		return ""
	}
	return s.value.equalTo
}

func (s ToolInputSchema) MinLength() (int, bool) {
	if s.value == nil || !s.value.hasMinLength {
		return 0, false
	}
	return s.value.minLength, true
}

func (s ToolInputSchema) MaxLength() (int, bool) {
	if s.value == nil || !s.value.hasMaxLength {
		return 0, false
	}
	return s.value.maxLength, true
}

func (s ToolInputSchema) MinItems() (int, bool) {
	if s.value == nil || !s.value.hasMinItems {
		return 0, false
	}
	return s.value.minItems, true
}

func (s ToolInputSchema) MaxItems() (int, bool) {
	if s.value == nil || !s.value.hasMaxItems {
		return 0, false
	}
	return s.value.maxItems, true
}

func (s ToolInputSchema) WithoutEnum() ToolInputSchema {
	if s.value == nil || !s.value.enumDeclared {
		return s
	}
	copyValue := *s.value
	copyValue.enum = nil
	copyValue.enumDeclared = false
	return ToolInputSchema{value: &copyValue}
}

func (s ToolInputSchema) WithoutMaximum() ToolInputSchema {
	if s.value == nil || !s.value.hasMaximum {
		return s
	}
	copyValue := *s.value
	copyValue.maximum = 0
	copyValue.hasMaximum = false
	return ToolInputSchema{value: &copyValue}
}

func (s ToolInputSchema) WithoutPattern() ToolInputSchema {
	if s.value == nil || s.value.pattern == "" {
		return s
	}
	copyValue := *s.value
	copyValue.pattern = ""
	return ToolInputSchema{value: &copyValue}
}

func (s ToolInputSchema) WithoutMaxLength() ToolInputSchema {
	if s.value == nil || !s.value.hasMaxLength {
		return s
	}
	copyValue := *s.value
	copyValue.maxLength = 0
	copyValue.hasMaxLength = false
	return ToolInputSchema{value: &copyValue}
}

func (s ToolInputSchema) WithMaxLength(maximum int) (ToolInputSchema, error) {
	if s.value == nil || s.value.kind != ToolSchemaString {
		return ToolInputSchema{}, fmt.Errorf("schema is not a string")
	}
	copyValue := *s.value
	copyValue.maxLength = maximum
	copyValue.hasMaxLength = true
	out := ToolInputSchema{value: &copyValue}
	if err := validateAdmittedToolInputSchema("$", out, 0); err != nil {
		return ToolInputSchema{}, err
	}
	return out, nil
}

func (s ToolInputSchema) WithLengthBounds(minimum, maximum int) (ToolInputSchema, error) {
	if s.value == nil || s.value.kind != ToolSchemaString {
		return ToolInputSchema{}, fmt.Errorf("schema is not a string")
	}
	copyValue := *s.value
	copyValue.minLength = minimum
	copyValue.hasMinLength = true
	copyValue.maxLength = maximum
	copyValue.hasMaxLength = true
	out := ToolInputSchema{value: &copyValue}
	if err := validateAdmittedToolInputSchema("$", out, 0); err != nil {
		return ToolInputSchema{}, err
	}
	return out, nil
}

func (s ToolInputSchema) WithProperty(name string, property ToolInputSchema) (ToolInputSchema, error) {
	if s.value == nil || s.value.kind != ToolSchemaObject {
		return ToolInputSchema{}, fmt.Errorf("schema is not an object")
	}
	properties := s.Properties()
	properties[name] = property
	copyValue := *s.value
	copyValue.properties = properties
	out := ToolInputSchema{value: &copyValue}
	if err := validateAdmittedToolInputSchema("$", out, 0); err != nil {
		return ToolInputSchema{}, err
	}
	return out, nil
}

func (s ToolInputSchema) WithRequiredProperty(name string, property ToolInputSchema) (ToolInputSchema, error) {
	if s.value == nil || s.value.kind != ToolSchemaObject {
		return ToolInputSchema{}, fmt.Errorf("schema is not an object")
	}
	properties := s.Properties()
	properties[name] = property
	required := s.RequiredProperties()
	if !s.IsRequired(name) {
		required = append(required, name)
	}
	copyValue := *s.value
	copyValue.properties = properties
	copyValue.required = required
	out := ToolInputSchema{value: &copyValue}
	if err := validateAdmittedToolInputSchema("$", out, 0); err != nil {
		return ToolInputSchema{}, err
	}
	return out, nil
}

func (s ToolInputSchema) WithItems(items ToolInputSchema) (ToolInputSchema, error) {
	if s.value == nil || s.value.kind != ToolSchemaArray {
		return ToolInputSchema{}, fmt.Errorf("schema is not an array")
	}
	copyValue := *s.value
	copyValue.items = items
	copyValue.hasItems = true
	out := ToolInputSchema{value: &copyValue}
	if err := validateAdmittedToolInputSchema("$", out, 0); err != nil {
		return ToolInputSchema{}, err
	}
	return out, nil
}

// IntersectBounds returns the finite constraint intersection used by channel
// compilation. Structural shape and enum authority remain owned by the receiver.
func (s ToolInputSchema) IntersectBounds(other ToolInputSchema) (ToolInputSchema, error) {
	if s.value == nil || other.value == nil || s.value.kind != other.value.kind {
		return ToolInputSchema{}, fmt.Errorf("schemas have incompatible types")
	}
	if s.value.pattern != "" && other.value.pattern != "" && s.value.pattern != other.value.pattern {
		return ToolInputSchema{}, fmt.Errorf("schemas have incompatible patterns")
	}
	copyValue := *s.value
	if copyValue.pattern == "" {
		copyValue.pattern = other.value.pattern
	}
	copyValue.minLength, copyValue.hasMinLength = maxAdmittedInt(
		s.value.minLength, s.value.hasMinLength,
		other.value.minLength, other.value.hasMinLength,
	)
	copyValue.maxLength, copyValue.hasMaxLength = minAdmittedInt(
		s.value.maxLength, s.value.hasMaxLength,
		other.value.maxLength, other.value.hasMaxLength,
	)
	copyValue.minItems, copyValue.hasMinItems = maxAdmittedInt(
		s.value.minItems, s.value.hasMinItems,
		other.value.minItems, other.value.hasMinItems,
	)
	copyValue.maxItems, copyValue.hasMaxItems = minAdmittedInt(
		s.value.maxItems, s.value.hasMaxItems,
		other.value.maxItems, other.value.hasMaxItems,
	)
	out := ToolInputSchema{value: &copyValue}
	if err := validateAdmittedToolInputSchema("$", out, 0); err != nil {
		return ToolInputSchema{}, err
	}
	return out, nil
}

func maxAdmittedInt(left int, hasLeft bool, right int, hasRight bool) (int, bool) {
	if !hasLeft {
		return right, hasRight
	}
	if !hasRight || left >= right {
		return left, true
	}
	return right, true
}

func minAdmittedInt(left int, hasLeft bool, right int, hasRight bool) (int, bool) {
	if !hasLeft {
		return right, hasRight
	}
	if !hasRight || left <= right {
		return left, true
	}
	return right, true
}

func (s ToolInputSchema) Validate(value any) error {
	admitted, err := canonicaljson.FromGo(value)
	if err != nil {
		return err
	}
	return validateToolSchemaValue("$", s, admitted, true)
}

func (s ToolInputSchema) Project() (map[string]any, error) {
	if err := validateAdmittedToolInputSchema("$", s, 0); err != nil {
		return nil, err
	}
	return projectAdmittedToolInputSchema(s), nil
}

// Projection is the authority-free JSON Schema view of an already admitted
// schema. Its zero value projects to nil for optional output positions.
func (s ToolInputSchema) Projection() map[string]any {
	if s.value == nil {
		return nil
	}
	return projectAdmittedToolInputSchema(s)
}

func projectAdmittedToolInputSchema(schema ToolInputSchema) map[string]any {
	value := schema.value
	out := map[string]any{}
	if value.kind != ToolSchemaAny {
		out["type"] = string(value.kind)
	}
	if value.description != "" {
		out["description"] = value.description
	}
	if len(value.properties) > 0 {
		properties := make(map[string]any, len(value.properties))
		for name, property := range value.properties {
			properties[name] = projectAdmittedToolInputSchema(property)
		}
		out["properties"] = properties
	}
	if len(value.required) > 0 {
		out["required"] = append([]string(nil), value.required...)
	}
	if value.hasItems {
		out["items"] = projectAdmittedToolInputSchema(value.items)
	}
	if value.enumDeclared {
		enum := make([]any, 0, len(value.enum))
		for _, item := range value.enum {
			enum = append(enum, item.Interface())
		}
		out["enum"] = enum
	}
	if value.additionalAllowedDeclared {
		out["additionalProperties"] = value.additionalAllowed
	} else if value.hasAdditionalSchema {
		out["additionalProperties"] = projectAdmittedToolInputSchema(value.additionalSchema)
	} else if value.kind == ToolSchemaObject {
		out["additionalProperties"] = true
	}
	if value.hasMinimum {
		out["minimum"] = value.minimum
	}
	if value.hasMaximum {
		out["maximum"] = value.maximum
	}
	if value.pattern != "" {
		out["pattern"] = value.pattern
	}
	if value.format != 0 {
		out["format"] = value.format.String()
	}
	if value.equalTo != "" {
		out["x-swarm-equalTo"] = value.equalTo
	}
	if value.hasMinLength {
		out["minLength"] = value.minLength
	}
	if value.hasMaxLength {
		out["maxLength"] = value.maxLength
	}
	if value.hasMinItems {
		out["minItems"] = value.minItems
	}
	if value.hasMaxItems {
		out["maxItems"] = value.maxItems
	}
	return out
}

func (s ToolInputSchema) CanonicalHash() (string, error) {
	projected, err := s.Project()
	if err != nil {
		return "", err
	}
	return canonicaljson.Hash(projected)
}

func (s ToolInputSchema) Equal(other ToolInputSchema) bool {
	if s.value == nil || other.value == nil {
		return s.value == nil && other.value == nil
	}
	left, err := s.CanonicalHash()
	if err != nil {
		return false
	}
	right, err := other.CanonicalHash()
	return err == nil && left == right
}

func (s ToolInputSchema) MarshalJSON() ([]byte, error) {
	projected, err := s.Project()
	if err != nil {
		return nil, err
	}
	return canonicaljson.Bytes(projected)
}

func (s ToolInputSchema) MarshalYAML() (any, error) {
	if err := s.ValidateDefinition(); err != nil {
		return nil, err
	}
	type admittedSchemaYAML struct {
		Type                 string                     `yaml:"type"`
		Description          string                     `yaml:"description,omitempty"`
		Properties           map[string]ToolInputSchema `yaml:"properties,omitempty"`
		Required             []string                   `yaml:"required,omitempty"`
		Items                *ToolInputSchema           `yaml:"items,omitempty"`
		Enum                 []any                      `yaml:"enum,omitempty"`
		AdditionalProperties any                        `yaml:"additionalProperties,omitempty"`
		Minimum              *float64                   `yaml:"minimum,omitempty"`
		Maximum              *float64                   `yaml:"maximum,omitempty"`
		Pattern              string                     `yaml:"pattern,omitempty"`
		Format               string                     `yaml:"format,omitempty"`
		EqualTo              string                     `yaml:"x-swarm-equalTo,omitempty"`
		MinLength            *int                       `yaml:"minLength,omitempty"`
		MaxLength            *int                       `yaml:"maxLength,omitempty"`
		MinItems             *int                       `yaml:"minItems,omitempty"`
		MaxItems             *int                       `yaml:"maxItems,omitempty"`
	}
	value := s.value
	out := admittedSchemaYAML{
		Type:        string(value.kind),
		Description: value.description,
		Properties:  s.Properties(),
		Required:    s.RequiredProperties(),
		Pattern:     value.pattern,
		Format:      value.format.String(),
		EqualTo:     value.equalTo,
	}
	if items, ok := s.ItemsSchema(); ok {
		out.Items = &items
	}
	if enum, declared := s.EnumValues(); declared {
		out.Enum = make([]any, 0, len(enum))
		for _, item := range enum {
			out.Enum = append(out.Enum, item.Interface())
		}
	}
	if allowed, declared := s.AdditionalPropertiesAllowed(); declared {
		out.AdditionalProperties = allowed
	} else if schema, declared := s.AdditionalPropertiesSchema(); declared {
		out.AdditionalProperties = schema
	}
	if minimum, declared := s.Minimum(); declared {
		out.Minimum = &minimum
	}
	if maximum, declared := s.Maximum(); declared {
		out.Maximum = &maximum
	}
	if minimum, declared := s.MinLength(); declared {
		out.MinLength = &minimum
	}
	if maximum, declared := s.MaxLength(); declared {
		out.MaxLength = &maximum
	}
	if minimum, declared := s.MinItems(); declared {
		out.MinItems = &minimum
	}
	if maximum, declared := s.MaxItems(); declared {
		out.MaxItems = &maximum
	}
	return out, nil
}

func isFiniteSchemaNumber(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func isNegativeZero(value float64) bool {
	return value == 0 && math.Signbit(value)
}
