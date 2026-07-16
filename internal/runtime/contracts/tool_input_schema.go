package contracts

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"gopkg.in/yaml.v3"
)

// ValidateToolInputSchema is the semantic admission owner for the closed
// ToolInputSchema vocabulary used by pack interfaces, triggers, channels, and
// connectors.
const MaxToolInputSchemaDepth = 64

func ValidateToolInputSchema(schema ToolInputSchema) error {
	return validateToolInputSchema("$", &schema, 0, map[*ToolInputSchema]string{})
}

func ToolInputSchemaIsZero(schema ToolInputSchema) bool {
	return schema.Type == "" && schema.Description == "" && len(schema.Properties) == 0 && len(schema.Required) == 0 &&
		schema.Items == nil && len(schema.Enum) == 0 && !schema.enumDeclared &&
		schema.AdditionalProperties.Allowed == nil && schema.AdditionalProperties.Schema == nil &&
		schema.Minimum == nil && schema.Maximum == nil && schema.Pattern == "" &&
		schema.MinLength == nil && schema.MaxLength == nil && schema.MinItems == nil && schema.MaxItems == nil
}

func ToolInputSchemaWithoutEnum(schema ToolInputSchema) ToolInputSchema {
	out := CloneToolInputSchema(schema)
	out.Enum = nil
	out.enumDeclared = false
	return out
}

func validateToolInputSchema(path string, schema *ToolInputSchema, depth int, ancestors map[*ToolInputSchema]string) error {
	if schema == nil {
		return fmt.Errorf("%s schema is missing", path)
	}
	if depth > MaxToolInputSchemaDepth {
		return fmt.Errorf("%s exceeds maximum schema depth %d", path, MaxToolInputSchemaDepth)
	}
	if ancestor, exists := ancestors[schema]; exists {
		return fmt.Errorf("%s forms a schema cycle through %s", path, ancestor)
	}
	ancestors[schema] = path
	defer delete(ancestors, schema)

	typeName := schema.Type
	if !utf8.ValidString(typeName) || typeName != strings.TrimSpace(typeName) || typeName != strings.ToLower(typeName) {
		return fmt.Errorf("%s type %q is not canonical", path, schema.Type)
	}
	switch typeName {
	case "string", "integer", "number", "boolean", "object", "array", "null":
	default:
		return fmt.Errorf("%s requires an explicit supported JSON type, got %q", path, schema.Type)
	}

	if typeName != "object" && (len(schema.Properties) > 0 || len(schema.Required) > 0 || schema.AdditionalProperties.Allowed != nil || schema.AdditionalProperties.Schema != nil) {
		return fmt.Errorf("%s type %s cannot declare object constraints", path, typeName)
	}
	if typeName != "array" && (schema.Items != nil || schema.MinItems != nil || schema.MaxItems != nil) {
		return fmt.Errorf("%s type %s cannot declare array constraints", path, typeName)
	}
	if typeName != "string" && (schema.Pattern != "" || schema.MinLength != nil || schema.MaxLength != nil) {
		return fmt.Errorf("%s type %s cannot declare string constraints", path, typeName)
	}
	if typeName != "integer" && typeName != "number" && (schema.Minimum != nil || schema.Maximum != nil) {
		return fmt.Errorf("%s type %s cannot declare numeric constraints", path, typeName)
	}

	if schema.Minimum != nil && (!isFiniteSchemaNumber(*schema.Minimum) || isNegativeZero(*schema.Minimum)) {
		return fmt.Errorf("%s minimum must be a finite non-negative-zero JSON number", path)
	}
	if schema.Maximum != nil && (!isFiniteSchemaNumber(*schema.Maximum) || isNegativeZero(*schema.Maximum)) {
		return fmt.Errorf("%s maximum must be a finite non-negative-zero JSON number", path)
	}
	if schema.Minimum != nil {
		if _, err := semanticvalue.Number(*schema.Minimum); err != nil {
			return fmt.Errorf("%s minimum is not a supported semantic JSON number: %w", path, err)
		}
	}
	if schema.Maximum != nil {
		if _, err := semanticvalue.Number(*schema.Maximum); err != nil {
			return fmt.Errorf("%s maximum is not a supported semantic JSON number: %w", path, err)
		}
	}
	if schema.Minimum != nil && schema.Maximum != nil && *schema.Minimum > *schema.Maximum {
		return fmt.Errorf("%s minimum must be <= maximum", path)
	}
	if err := validateNonNegativeBounds(path, "Length", schema.MinLength, schema.MaxLength); err != nil {
		return err
	}
	if err := validateNonNegativeBounds(path, "Items", schema.MinItems, schema.MaxItems); err != nil {
		return err
	}
	if schema.Pattern != "" {
		if !utf8.ValidString(schema.Pattern) {
			return fmt.Errorf("%s pattern is not valid UTF-8", path)
		}
		if _, err := regexp.Compile(schema.Pattern); err != nil {
			return fmt.Errorf("%s pattern is invalid: %w", path, err)
		}
	}

	if typeName == "array" {
		if schema.Items == nil {
			return fmt.Errorf("%s array requires items", path)
		}
		if err := validateToolInputSchema(path+".items", schema.Items, depth+1, ancestors); err != nil {
			return err
		}
	}
	if typeName == "object" {
		if schema.AdditionalProperties.Allowed != nil && schema.AdditionalProperties.Schema != nil {
			return fmt.Errorf("%s additionalProperties must declare a boolean or schema, not both", path)
		}
		for name := range schema.Properties {
			if name == "" || !utf8.ValidString(name) || name != strings.TrimSpace(name) {
				return fmt.Errorf("%s property name %q is not canonical", path, name)
			}
			property := schema.Properties[name]
			if err := validateToolInputSchema(path+".properties["+name+"]", &property, depth+1, ancestors); err != nil {
				return err
			}
		}
		seenRequired := map[string]struct{}{}
		for _, name := range schema.Required {
			if name == "" || !utf8.ValidString(name) || name != strings.TrimSpace(name) {
				return fmt.Errorf("%s required property name %q is not canonical", path, name)
			}
			if _, duplicate := seenRequired[name]; duplicate {
				return fmt.Errorf("%s required property %q is duplicated", path, name)
			}
			seenRequired[name] = struct{}{}
			if _, exists := schema.Properties[name]; !exists {
				return fmt.Errorf("%s required property %q is not declared", path, name)
			}
		}
		if schema.AdditionalProperties.Schema != nil {
			if err := validateToolInputSchema(path+".additionalProperties", schema.AdditionalProperties.Schema, depth+1, ancestors); err != nil {
				return err
			}
		}
	}

	if schema.enumDeclared && len(schema.Enum) == 0 {
		return fmt.Errorf("%s enum must contain at least one value", path)
	}
	seenEnums := []semanticvalue.Value{}
	for index, literal := range schema.Enum {
		value, err := toolSchemaLiteralValue(literal)
		if err != nil {
			return fmt.Errorf("%s enum[%d]: %w", path, index, err)
		}
		if err := validateToolSchemaValue(path+fmt.Sprintf(".enum[%d]", index), *schema, value, false); err != nil {
			return err
		}
		for _, prior := range seenEnums {
			if value.Equal(prior) {
				return fmt.Errorf("%s enum[%d] duplicates another semantic value", path, index)
			}
		}
		seenEnums = append(seenEnums, value)
	}
	return nil
}

func validateNonNegativeBounds(path, kind string, minimum, maximum *int) error {
	if minimum != nil && *minimum < 0 {
		return fmt.Errorf("%s min%s must be non-negative", path, kind)
	}
	if maximum != nil && *maximum < 0 {
		return fmt.Errorf("%s max%s must be non-negative", path, kind)
	}
	if minimum != nil && maximum != nil && *minimum > *maximum {
		return fmt.Errorf("%s min%s must be <= max%s", path, kind, kind)
	}
	return nil
}

func isFiniteSchemaNumber(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func isNegativeZero(value float64) bool {
	return value == 0 && math.Signbit(value)
}

func toolSchemaLiteralValue(literal SchemaLiteral) (semanticvalue.Value, error) {
	if literal.Node.Kind == 0 {
		return semanticvalue.Value{}, fmt.Errorf("value is missing")
	}
	var decoded any
	if err := literal.Node.Decode(&decoded); err != nil {
		return semanticvalue.Value{}, fmt.Errorf("decode value: %w", err)
	}
	value, err := canonicaljson.FromGo(decoded)
	if err != nil {
		return semanticvalue.Value{}, fmt.Errorf("value is not semantic JSON: %w", err)
	}
	return value, nil
}

func validateToolSchemaValue(path string, schema ToolInputSchema, value semanticvalue.Value, checkEnum bool) error {
	if checkEnum && len(schema.Enum) > 0 {
		matched := false
		for _, literal := range schema.Enum {
			candidate, err := toolSchemaLiteralValue(literal)
			if err == nil && value.Equal(candidate) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s is not one of the declared enum values", path)
		}
	}
	switch schema.Type {
	case "string":
		text, ok := value.String()
		if !ok {
			return fmt.Errorf("%s must be string", path)
		}
		if schema.Pattern != "" && !regexp.MustCompile(schema.Pattern).MatchString(text) {
			return fmt.Errorf("%s does not match pattern %q", path, schema.Pattern)
		}
		length := utf8.RuneCountInString(text)
		if schema.MinLength != nil && length < *schema.MinLength {
			return fmt.Errorf("%s length must be >= %d", path, *schema.MinLength)
		}
		if schema.MaxLength != nil && length > *schema.MaxLength {
			return fmt.Errorf("%s length must be <= %d", path, *schema.MaxLength)
		}
	case "number", "integer":
		number, ok := value.Number()
		if !ok || (schema.Type == "integer" && math.Trunc(number) != number) {
			return fmt.Errorf("%s must be %s", path, schema.Type)
		}
		if schema.Minimum != nil && number < *schema.Minimum {
			return fmt.Errorf("%s must be >= %v", path, *schema.Minimum)
		}
		if schema.Maximum != nil && number > *schema.Maximum {
			return fmt.Errorf("%s must be <= %v", path, *schema.Maximum)
		}
	case "boolean":
		if _, ok := value.Bool(); !ok {
			return fmt.Errorf("%s must be boolean", path)
		}
	case "null":
		if value.Kind() != semanticvalue.KindNull {
			return fmt.Errorf("%s must be null", path)
		}
	case "array":
		if value.Kind() != semanticvalue.KindArray {
			return fmt.Errorf("%s must be array", path)
		}
		if schema.MinItems != nil && value.Len() < *schema.MinItems {
			return fmt.Errorf("%s length must be >= %d", path, *schema.MinItems)
		}
		if schema.MaxItems != nil && value.Len() > *schema.MaxItems {
			return fmt.Errorf("%s length must be <= %d", path, *schema.MaxItems)
		}
		for index := 0; index < value.Len(); index++ {
			item, _ := value.At(index)
			if err := validateToolSchemaValue(fmt.Sprintf("%s[%d]", path, index), *schema.Items, item, true); err != nil {
				return err
			}
		}
	case "object":
		members, ok := value.ObjectMap()
		if !ok {
			return fmt.Errorf("%s must be object", path)
		}
		for _, name := range schema.Required {
			if _, exists := members[name]; !exists {
				return fmt.Errorf("%s.%s is required", path, name)
			}
		}
		for name, member := range members {
			property, known := schema.Properties[name]
			if known {
				if err := validateToolSchemaValue(path+"."+name, property, member, true); err != nil {
					return err
				}
				continue
			}
			if schema.AdditionalProperties.Schema != nil {
				if err := validateToolSchemaValue(path+"."+name, *schema.AdditionalProperties.Schema, member, true); err != nil {
					return err
				}
				continue
			}
			if schema.AdditionalProperties.Allowed != nil && !*schema.AdditionalProperties.Allowed {
				return fmt.Errorf("%s.%s is not allowed", path, name)
			}
		}
	}
	return nil
}

// CloneToolInputSchema returns a mutation-isolated admitted schema, including
// every structured enum YAML node and alias. Invalid cyclic or over-depth
// values become an invalid non-empty schema so no later consumer can treat
// them as an omitted schema and widen admission.
func CloneToolInputSchema(in ToolInputSchema) ToolInputSchema {
	out, ok := cloneToolInputSchema(in, 0, map[*ToolInputSchema]struct{}{})
	if !ok {
		return ToolInputSchema{Type: "__invalid_recursive_schema__"}
	}
	return out
}

func cloneToolInputSchema(in ToolInputSchema, depth int, ancestors map[*ToolInputSchema]struct{}) (ToolInputSchema, bool) {
	if depth > MaxToolInputSchemaDepth {
		return ToolInputSchema{}, false
	}
	out := in
	if in.Properties != nil {
		out.Properties = make(map[string]ToolInputSchema, len(in.Properties))
		for name, property := range in.Properties {
			cloned, ok := cloneToolInputSchema(property, depth+1, ancestors)
			if !ok {
				return ToolInputSchema{}, false
			}
			out.Properties[name] = cloned
		}
	}
	out.Required = append([]string(nil), in.Required...)
	if in.Enum != nil {
		out.Enum = make([]SchemaLiteral, len(in.Enum))
		for index, literal := range in.Enum {
			out.Enum[index] = SchemaLiteral{Node: cloneToolSchemaYAMLNode(literal.Node)}
		}
	}
	if in.Items != nil {
		if _, cyclic := ancestors[in.Items]; cyclic {
			return ToolInputSchema{}, false
		}
		ancestors[in.Items] = struct{}{}
		items, ok := cloneToolInputSchema(*in.Items, depth+1, ancestors)
		delete(ancestors, in.Items)
		if !ok {
			return ToolInputSchema{}, false
		}
		out.Items = &items
	}
	if in.AdditionalProperties.Allowed != nil {
		allowed := *in.AdditionalProperties.Allowed
		out.AdditionalProperties.Allowed = &allowed
	}
	if in.AdditionalProperties.Schema != nil {
		if _, cyclic := ancestors[in.AdditionalProperties.Schema]; cyclic {
			return ToolInputSchema{}, false
		}
		ancestors[in.AdditionalProperties.Schema] = struct{}{}
		additional, ok := cloneToolInputSchema(*in.AdditionalProperties.Schema, depth+1, ancestors)
		delete(ancestors, in.AdditionalProperties.Schema)
		if !ok {
			return ToolInputSchema{}, false
		}
		out.AdditionalProperties.Schema = &additional
	}
	out.Minimum = cloneToolSchemaFloat(in.Minimum)
	out.Maximum = cloneToolSchemaFloat(in.Maximum)
	out.MinLength = cloneToolSchemaInt(in.MinLength)
	out.MaxLength = cloneToolSchemaInt(in.MaxLength)
	out.MinItems = cloneToolSchemaInt(in.MinItems)
	out.MaxItems = cloneToolSchemaInt(in.MaxItems)
	return out, true
}

// CloneToolSchemaEntry is the sole mutation-isolation owner for tool schema
// carriers returned by accepted registries, semantic sources, and plans.
func CloneToolSchemaEntry(in ToolSchemaEntry) ToolSchemaEntry {
	out := in
	out.InputSchema = CloneToolInputSchema(in.InputSchema)
	out.OutputSchema = CloneToolInputSchema(in.OutputSchema)
	if in.HTTP != nil {
		httpSpec := *in.HTTP
		httpSpec.Headers = cloneToolStringMap(in.HTTP.Headers)
		httpSpec.Body = cloneToolSchemaCarrierValue(in.HTTP.Body)
		out.HTTP = &httpSpec
	}
	out.ResponseMapping = cloneToolSchemaCarrierMap(in.ResponseMapping)
	if in.ResponseSuccess != nil {
		response := *in.ResponseSuccess
		response.Equals = cloneToolSchemaCarrierValue(in.ResponseSuccess.Equals)
		out.ResponseSuccess = &response
	}
	out.Credentials = append([]string(nil), in.Credentials...)
	if in.ManagedCredential != nil {
		managed := *in.ManagedCredential
		managed.Scopes = append([]string(nil), in.ManagedCredential.Scopes...)
		managed.TokenRequest.StaticHeaders = cloneToolStringMap(in.ManagedCredential.TokenRequest.StaticHeaders)
		out.ManagedCredential = &managed
	}
	if in.CompiledResult != nil {
		projection := &CompiledResultProjection{
			Fields:       make(map[string]CompiledResultField, len(in.CompiledResult.Fields)),
			OutputSchema: CloneToolInputSchema(in.CompiledResult.OutputSchema),
		}
		for target, field := range in.CompiledResult.Fields {
			projection.Fields[target] = field
		}
		out.CompiledResult = projection
	}
	return out
}

func CloneToolSchemaEntries(in map[string]ToolSchemaEntry) map[string]ToolSchemaEntry {
	out := make(map[string]ToolSchemaEntry, len(in))
	for name, entry := range in {
		out[name] = CloneToolSchemaEntry(entry)
	}
	return out
}

// CloneEventCatalogEntry returns a mutation-isolated catalog carrier. Exact
// provider schema survives catalog and runtime-registry projection through the
// same ToolInputSchema clone owner used by accepted pack descriptors.
func CloneEventCatalogEntry(in EventCatalogEntry) EventCatalogEntry {
	out := in
	out.Swarm.Producer = append([]string(nil), in.Swarm.Producer...)
	out.Swarm.Consumer = append([]string(nil), in.Swarm.Consumer...)
	out.Producer = append([]string(nil), in.Producer...)
	out.AlternateEmitters = append([]string(nil), in.AlternateEmitters...)
	out.Consumer = append([]string(nil), in.Consumer...)
	out.ConsumerType = append([]string(nil), in.ConsumerType...)
	out.Required = append([]string(nil), in.Required...)
	out.Payload.Required = append([]string(nil), in.Payload.Required...)
	out.Payload.Properties = make(map[string]EventFieldSpec, len(in.Payload.Properties))
	for name, field := range in.Payload.Properties {
		field.Citation.AllowedClasses = append([]string(nil), field.Citation.AllowedClasses...)
		field.Refinements.Length.Min = cloneToolSchemaInt(field.Refinements.Length.Min)
		field.Refinements.Length.Max = cloneToolSchemaInt(field.Refinements.Length.Max)
		field.Refinements.Range.Min = cloneToolSchemaFloat(field.Refinements.Range.Min)
		field.Refinements.Range.Max = cloneToolSchemaFloat(field.Refinements.Range.Max)
		if field.ExactSchema != nil {
			schema := CloneToolInputSchema(*field.ExactSchema)
			field.ExactSchema = &schema
		}
		out.Payload.Properties[name] = field
	}
	return out
}

func CloneEventCatalogEntries(in map[string]EventCatalogEntry) map[string]EventCatalogEntry {
	out := make(map[string]EventCatalogEntry, len(in))
	for name, entry := range in {
		out[name] = CloneEventCatalogEntry(entry)
	}
	return out
}

func cloneToolSchemaYAMLNode(node yaml.Node) yaml.Node {
	cloned := cloneToolSchemaYAMLNodePointer(&node, map[*yaml.Node]*yaml.Node{})
	if cloned == nil {
		return yaml.Node{}
	}
	return *cloned
}

func cloneToolSchemaYAMLNodePointer(node *yaml.Node, seen map[*yaml.Node]*yaml.Node) *yaml.Node {
	if node == nil {
		return nil
	}
	if cloned, ok := seen[node]; ok {
		return cloned
	}
	out := *node
	out.Content = nil
	out.Alias = nil
	seen[node] = &out
	for _, child := range node.Content {
		out.Content = append(out.Content, cloneToolSchemaYAMLNodePointer(child, seen))
	}
	out.Alias = cloneToolSchemaYAMLNodePointer(node.Alias, seen)
	return &out
}

func cloneToolSchemaInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneToolSchemaFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneToolStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneToolSchemaCarrierMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneToolSchemaCarrierValue(value)
	}
	return out
}

func cloneToolSchemaCarrierValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneToolSchemaCarrierMap(typed)
	case map[any]any:
		out := make(map[any]any, len(typed))
		for key, item := range typed {
			out[key] = cloneToolSchemaCarrierValue(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, item := range typed {
			out[index] = cloneToolSchemaCarrierValue(item)
		}
		return out
	case yaml.Node:
		return cloneToolSchemaYAMLNode(typed)
	case *yaml.Node:
		if typed == nil {
			return (*yaml.Node)(nil)
		}
		cloned := cloneToolSchemaYAMLNode(*typed)
		return &cloned
	default:
		return value
	}
}
