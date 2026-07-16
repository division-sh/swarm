package packs

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
)

// compiledChannelMappingTopology is the single target topology validated by
// the compiler and consumed by operation preparation.
type compiledChannelMappingTopology struct {
	Targets     []string
	ItemTargets map[string][]string
}

func (t compiledChannelMappingTopology) clone() compiledChannelMappingTopology {
	out := compiledChannelMappingTopology{
		Targets:     append([]string(nil), t.Targets...),
		ItemTargets: make(map[string][]string, len(t.ItemTargets)),
	}
	for target, paths := range t.ItemTargets {
		out.ItemTargets[target] = append([]string(nil), paths...)
	}
	return out
}

func compileChannelMappingTopology(subject string, mappings map[string]ChannelMapping) (compiledChannelMappingTopology, error) {
	topology := compiledChannelMappingTopology{ItemTargets: map[string][]string{}}
	targets := newChannelPathCardinality(subject + " target")
	for _, target := range sortedKeys(mappings) {
		if err := targets.add(target); err != nil {
			return compiledChannelMappingTopology{}, err
		}
		topology.Targets = append(topology.Targets, target)
		mapping := mappings[target]
		if mapping.Each == "" {
			continue
		}
		if len(mapping.Item) != 1 {
			return compiledChannelMappingTopology{}, fmt.Errorf("%s %q must construct exactly one item object", subject, target)
		}
		itemTargets := newChannelPathCardinality(subject + " " + target + " item target")
		for _, itemTarget := range sortedKeys(mapping.Item[0]) {
			if err := itemTargets.add(itemTarget); err != nil {
				return compiledChannelMappingTopology{}, err
			}
			topology.ItemTargets[target] = append(topology.ItemTargets[target], itemTarget)
		}
	}
	return topology, nil
}

type channelPathCardinality struct {
	subject string
	paths   []string
}

func newChannelPathCardinality(subject string) *channelPathCardinality {
	return &channelPathCardinality{subject: strings.TrimSpace(subject)}
}

func (c *channelPathCardinality) add(path string) error {
	path = strings.TrimSpace(path)
	for _, existing := range c.paths {
		if channelPathsOverlap(existing, path) {
			return fmt.Errorf("%s path %q overlaps %q; each semantic path must be used exactly once", c.subject, path, existing)
		}
	}
	c.paths = append(c.paths, path)
	sort.Strings(c.paths)
	return nil
}

func (c *channelPathCardinality) values() []string {
	return append([]string(nil), c.paths...)
}

func validateRequiredPathCardinality(subject string, required, mapped []string) error {
	for _, path := range required {
		count := 0
		for _, candidate := range mapped {
			if channelPathCovers(candidate, path) {
				count++
			}
		}
		if count != 1 {
			return fmt.Errorf("%s required path %q is covered %d times; require exactly once", subject, path, count)
		}
	}
	return nil
}

func channelPathCovers(mapped, required string) bool {
	mappedParts := channelPathParts(mapped)
	requiredParts := channelPathParts(required)
	if len(mappedParts) == 0 || len(mappedParts) > len(requiredParts) {
		return len(mappedParts) == 0 && len(requiredParts) == 0
	}
	for index := range mappedParts {
		if mappedParts[index] != requiredParts[index] {
			return false
		}
	}
	return true
}

func channelPathsOverlap(left, right string) bool {
	return channelPathCovers(left, right) || channelPathCovers(right, left)
}

func channelPathParts(path string) []string {
	path = strings.ReplaceAll(strings.TrimSpace(path), "[]", ".[]")
	parts := strings.Split(path, ".")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// validateDirectionalRelation proves that every admitted source value is
// valid for the target. It deliberately implements the channel contract's
// finite schema subset rather than general JSON-Schema implication.
func validateDirectionalRelation(subject string, source, target *runtimecontracts.ToolInputSchema) error {
	if source == nil || target == nil {
		return fmt.Errorf("%s has no source or target schema", subject)
	}
	return validateChannelSchemaSubset(subject, *source, *target)
}

func validateChannelSchemaSubset(subject string, source, target runtimecontracts.ToolInputSchema) error {
	sourceType := channelSchemaType(source)
	targetType := channelSchemaType(target)
	if sourceType != targetType && !(sourceType == "integer" && targetType == "number") {
		return fmt.Errorf("%s has incompatible types %s and %s", subject, sourceType, targetType)
	}

	finite, err := validateFiniteSourceEnum(subject, source, target)
	if err != nil {
		return err
	}
	if finite {
		return nil
	}
	if len(target.Enum) > 0 {
		return fmt.Errorf("%s target enum is narrower than an unbounded source", subject)
	}

	switch sourceType {
	case "string":
		if err := validateIntBoundsSubset(subject+" string length", source.MinLength, source.MaxLength, target.MinLength, target.MaxLength); err != nil {
			return err
		}
		if target.Pattern != "" && source.Pattern != target.Pattern {
			return fmt.Errorf("%s source pattern %q is not provably assignable to target pattern %q", subject, source.Pattern, target.Pattern)
		}
	case "integer", "number":
		if err := validateFloatBoundsSubset(subject+" numeric range", source.Minimum, source.Maximum, target.Minimum, target.Maximum); err != nil {
			return err
		}
	case "array":
		if err := validateIntBoundsSubset(subject+" array length", source.MinItems, source.MaxItems, target.MinItems, target.MaxItems); err != nil {
			return err
		}
		if target.Items != nil {
			if source.Items == nil {
				return fmt.Errorf("%s source array items are unconstrained while target items are constrained", subject)
			}
			if err := validateChannelSchemaSubset(subject+"[]", *source.Items, *target.Items); err != nil {
				return err
			}
		}
	case "object":
		return validateChannelObjectSubset(subject, source, target)
	case "boolean", "null":
	default:
		return fmt.Errorf("%s has unsupported schema type %q", subject, sourceType)
	}
	return nil
}

func validateFiniteSourceEnum(subject string, source, target runtimecontracts.ToolInputSchema) (bool, error) {
	if len(source.Enum) == 0 {
		return false, nil
	}
	targetEnums, err := channelEnumSet(target.Enum)
	if err != nil {
		return false, fmt.Errorf("%s target enum: %w", subject, err)
	}
	sourceConstraint := runtimecontracts.ToolInputSchemaWithoutEnum(source)
	targetConstraint := runtimecontracts.ToolInputSchemaWithoutEnum(target)
	for _, literal := range source.Enum {
		value, key, err := channelEnumLiteral(literal)
		if err != nil {
			return false, fmt.Errorf("%s source enum: %w", subject, err)
		}
		if err := eventschema.ValidateValueAgainstSchema(runtimecontracts.ToolInputSchemaJSONSchema(sourceConstraint), value); err != nil {
			return false, fmt.Errorf("%s source enum value is outside its declared schema: %w", subject, err)
		}
		if len(targetEnums) > 0 {
			if _, ok := targetEnums[key]; !ok {
				return false, fmt.Errorf("%s source enum value %s is absent from target enum", subject, key)
			}
		}
		if err := eventschema.ValidateValueAgainstSchema(runtimecontracts.ToolInputSchemaJSONSchema(targetConstraint), value); err != nil {
			return false, fmt.Errorf("%s source enum value is outside target schema: %w", subject, err)
		}
	}
	return true, nil
}

func channelEnumSet(values []runtimecontracts.SchemaLiteral) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(values))
	for _, literal := range values {
		_, key, err := channelEnumLiteral(literal)
		if err != nil {
			return nil, err
		}
		out[key] = struct{}{}
	}
	return out, nil
}

func channelEnumLiteral(literal runtimecontracts.SchemaLiteral) (any, string, error) {
	if literal.Node.Kind == 0 {
		return nil, "null", nil
	}
	var value any
	if err := literal.Node.Decode(&value); err != nil {
		return nil, "", err
	}
	raw, err := canonicaljson.Bytes(value)
	if err != nil {
		return nil, "", err
	}
	return value, string(raw), nil
}

func validateIntBoundsSubset(subject string, sourceMin, sourceMax, targetMin, targetMax *int) error {
	if targetMin != nil && (sourceMin == nil || *sourceMin < *targetMin) {
		return fmt.Errorf("%s source minimum is broader than target minimum %d", subject, *targetMin)
	}
	if targetMax != nil && (sourceMax == nil || *sourceMax > *targetMax) {
		return fmt.Errorf("%s source maximum is broader than target maximum %d", subject, *targetMax)
	}
	return nil
}

func validateFloatBoundsSubset(subject string, sourceMin, sourceMax, targetMin, targetMax *float64) error {
	if targetMin != nil && (sourceMin == nil || *sourceMin < *targetMin) {
		return fmt.Errorf("%s source minimum is broader than target minimum %v", subject, *targetMin)
	}
	if targetMax != nil && (sourceMax == nil || *sourceMax > *targetMax) {
		return fmt.Errorf("%s source maximum is broader than target maximum %v", subject, *targetMax)
	}
	return nil
}

func validateChannelObjectSubset(subject string, source, target runtimecontracts.ToolInputSchema) error {
	sourceRequired := stringSet(source.Required)
	for _, name := range target.Required {
		if _, ok := sourceRequired[name]; !ok {
			return fmt.Errorf("%s target requires property %q that source does not require", subject, name)
		}
	}

	targetAdditional := channelAdditionalProperties(target)
	for _, name := range sortedKeys(source.Properties) {
		sourceProperty := source.Properties[name]
		if targetProperty, ok := target.Properties[name]; ok {
			if err := validateChannelSchemaSubset(subject+"."+name, sourceProperty, targetProperty); err != nil {
				return err
			}
			continue
		}
		if !targetAdditional.allowed {
			return fmt.Errorf("%s source property %q is forbidden by target", subject, name)
		}
		if targetAdditional.schema != nil {
			if err := validateChannelSchemaSubset(subject+"."+name, sourceProperty, *targetAdditional.schema); err != nil {
				return err
			}
		}
	}

	sourceAdditional := channelAdditionalProperties(source)
	if !sourceAdditional.allowed {
		return nil
	}
	if !targetAdditional.allowed {
		return fmt.Errorf("%s source admits additional properties while target is closed", subject)
	}
	if targetAdditional.schema == nil {
		return nil
	}
	if sourceAdditional.schema == nil {
		return fmt.Errorf("%s source admits unconstrained additional properties while target constrains them", subject)
	}
	return validateChannelSchemaSubset(subject+".*", *sourceAdditional.schema, *targetAdditional.schema)
}

type channelAdditionalPropertyConstraint struct {
	allowed bool
	schema  *runtimecontracts.ToolInputSchema
}

func channelAdditionalProperties(schema runtimecontracts.ToolInputSchema) channelAdditionalPropertyConstraint {
	if schema.AdditionalProperties.Allowed != nil {
		return channelAdditionalPropertyConstraint{allowed: *schema.AdditionalProperties.Allowed}
	}
	if schema.AdditionalProperties.Schema != nil {
		cloned := cloneSchema(*schema.AdditionalProperties.Schema)
		return channelAdditionalPropertyConstraint{allowed: true, schema: &cloned}
	}
	return channelAdditionalPropertyConstraint{allowed: true}
}

func channelSchemaType(schema runtimecontracts.ToolInputSchema) string {
	if value := normalizeSchemaType(schema.Type); value != "" {
		return value
	}
	if len(schema.Properties) > 0 || len(schema.Required) > 0 || schema.AdditionalProperties.Allowed != nil || schema.AdditionalProperties.Schema != nil {
		return "object"
	}
	if schema.Items != nil {
		return "array"
	}
	return ""
}
