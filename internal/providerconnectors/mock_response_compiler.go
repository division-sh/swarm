package providerconnectors

import (
	"fmt"
	"math"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// CompileMockResponsePlan derives the exact deterministic responder catalog
// from every effective provider connector in one semantic source.
func CompileMockResponsePlan(source semanticview.Source) (*MockResponsePlan, error) {
	if source == nil {
		return nil, fmt.Errorf("compile mock connector responses: semantic source is required")
	}
	tools := source.ToolEntries()
	toolIDs := make([]string, 0, len(tools))
	for rawToolID, tool := range tools {
		if isProviderConnector(tool) {
			toolID := strings.TrimSpace(rawToolID)
			if toolID == "" || toolID != rawToolID {
				return nil, fmt.Errorf("compile mock connector responses: effective provider connector tool id %q is not canonical", rawToolID)
			}
			toolIDs = append(toolIDs, strings.TrimSpace(toolID))
		}
	}
	sort.Strings(toolIDs)
	if len(toolIDs) == 0 {
		return nil, nil
	}

	responses := make(map[string]any, len(toolIDs))
	for _, toolID := range toolIDs {
		tool := tools[toolID]
		if errs := validateTool(toolID, tool); len(errs) > 0 {
			parts := make([]string, 0, len(errs))
			for _, err := range errs {
				parts = append(parts, err.Error())
			}
			return nil, fmt.Errorf("compile mock connector response for tool %q: %s", toolID, strings.Join(parts, "; "))
		}
		if err := validateMockResponseSchema(tool.OutputSchema(), "output_schema"); err != nil {
			return nil, fmt.Errorf("compile mock connector response for tool %q: %w", toolID, err)
		}
		value, err := deterministicMockSchemaValue(tool.OutputSchema(), "output_schema")
		if err != nil {
			return nil, fmt.Errorf("compile mock connector response for tool %q: %w", toolID, err)
		}
		responses[toolID] = value
	}

	plan, err := NewMockResponsePlan(responses)
	if err != nil {
		return nil, fmt.Errorf("compile mock connector response plan: %w", err)
	}
	for _, toolID := range toolIDs {
		if _, err := plan.Admit(toolID, tools[toolID]); err != nil {
			return nil, fmt.Errorf("compile mock connector response for tool %q: generated value failed canonical output_schema validation: %w", toolID, err)
		}
	}
	return plan, nil
}

func validateMockResponseSchema(schema runtimecontracts.ToolInputSchema, path string) error {
	kind := schema.Kind()
	switch kind {
	case runtimecontracts.ToolSchemaObject, runtimecontracts.ToolSchemaArray, runtimecontracts.ToolSchemaString,
		runtimecontracts.ToolSchemaBoolean, runtimecontracts.ToolSchemaNumber, runtimecontracts.ToolSchemaInteger,
		runtimecontracts.ToolSchemaNull:
	default:
		return fmt.Errorf("%s: unsupported schema type %q", path, kind)
	}
	if err := validateMockNumericBounds(schema, path); err != nil {
		return err
	}

	enum, enumPresent := schema.EnumValues()
	for index, value := range enum {
		if err := schema.Validate(value.Interface()); err != nil {
			return fmt.Errorf("%s.enum[%d]: value does not match declared schema: %w", path, index, err)
		}
	}
	_ = enumPresent

	if kind == runtimecontracts.ToolSchemaObject {
		for _, required := range sortedUniqueStrings(schema.RequiredProperties()) {
			if _, ok := schema.Property(required); !ok {
				return fmt.Errorf("%s.properties.%s: required property has no declared schema", path, required)
			}
		}
		for _, name := range schema.PropertyNames() {
			property, _ := schema.Property(name)
			if err := validateMockResponseSchema(property, path+".properties."+name); err != nil {
				return err
			}
		}
		if additional, ok := schema.AdditionalPropertiesSchema(); ok {
			if err := validateMockResponseSchema(additional, path+".additionalProperties"); err != nil {
				return err
			}
		}
	}
	if kind == runtimecontracts.ToolSchemaArray {
		items, ok := schema.ItemsSchema()
		if ok {
			if err := validateMockResponseSchema(items, path+".items"); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateMockNumericBounds(schema runtimecontracts.ToolInputSchema, path string) error {
	minimum, hasMinimum := schema.Minimum()
	maximum, hasMaximum := schema.Maximum()
	if hasMinimum && (math.IsNaN(minimum) || math.IsInf(minimum, 0)) {
		return fmt.Errorf("%s: minimum must be finite", path)
	}
	if hasMaximum && (math.IsNaN(maximum) || math.IsInf(maximum, 0)) {
		return fmt.Errorf("%s: maximum must be finite", path)
	}
	if hasMinimum && hasMaximum && minimum > maximum {
		return fmt.Errorf("%s: minimum %v exceeds maximum %v", path, minimum, maximum)
	}
	if schema.Kind() == runtimecontracts.ToolSchemaInteger {
		lower := math.Inf(-1)
		upper := math.Inf(1)
		if hasMinimum {
			lower = minimum
		}
		if hasMaximum {
			upper = maximum
		}
		if math.Ceil(lower) > math.Floor(upper) {
			return fmt.Errorf("%s: bounds contain no integer", path)
		}
	}
	return nil
}

func deterministicMockSchemaValue(schema runtimecontracts.ToolInputSchema, path string) (any, error) {
	enum, enumPresent := schema.EnumValues()
	if enumPresent {
		if len(enum) == 0 {
			return nil, fmt.Errorf("%s.enum: explicitly declared enum must contain at least one value", path)
		}
		return enum[0].Interface(), nil
	}
	switch schema.Kind() {
	case runtimecontracts.ToolSchemaObject:
		required := sortedUniqueStrings(schema.RequiredProperties())
		value := make(map[string]any, len(required))
		for _, name := range required {
			property, ok := schema.Property(name)
			if !ok {
				return nil, fmt.Errorf("%s.properties.%s: required property has no declared schema", path, name)
			}
			generated, err := deterministicMockSchemaValue(property, path+".properties."+name)
			if err != nil {
				return nil, err
			}
			value[name] = generated
		}
		return value, nil
	case runtimecontracts.ToolSchemaArray:
		count := 0
		if minimum, ok := schema.MinItems(); ok {
			count = minimum
		}
		items, ok := schema.ItemsSchema()
		if !ok {
			return nil, fmt.Errorf("%s.items: array item schema is required", path)
		}
		value := make([]any, 0, count)
		for index := 0; index < count; index++ {
			generated, err := deterministicMockSchemaValue(items, fmt.Sprintf("%s[%d]", path, index))
			if err != nil {
				return nil, err
			}
			value = append(value, generated)
		}
		return value, nil
	case runtimecontracts.ToolSchemaString:
		return "", nil
	case runtimecontracts.ToolSchemaBoolean:
		return false, nil
	case runtimecontracts.ToolSchemaNumber:
		return deterministicMockNumber(schema), nil
	case runtimecontracts.ToolSchemaInteger:
		return deterministicMockInteger(schema), nil
	case runtimecontracts.ToolSchemaNull:
		return nil, nil
	default:
		return nil, fmt.Errorf("%s: unsupported schema type %q", path, schema.Kind())
	}
}

func deterministicMockNumber(schema runtimecontracts.ToolInputSchema) float64 {
	value := float64(0)
	if minimum, ok := schema.Minimum(); ok && value < minimum {
		value = minimum
	}
	if maximum, ok := schema.Maximum(); ok && value > maximum {
		value = maximum
	}
	return value
}

func deterministicMockInteger(schema runtimecontracts.ToolInputSchema) float64 {
	value := float64(0)
	if minimum, ok := schema.Minimum(); ok && value < minimum {
		value = math.Ceil(minimum)
	}
	if maximum, ok := schema.Maximum(); ok && value > maximum {
		value = math.Floor(maximum)
	}
	return value
}

func sortedUniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
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
