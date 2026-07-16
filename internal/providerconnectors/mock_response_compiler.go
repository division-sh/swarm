package providerconnectors

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
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

	responses := make(map[string]map[string]any, len(toolIDs))
	for _, toolID := range toolIDs {
		tool := tools[toolID]
		if errs := validateTool(toolID, tool); len(errs) > 0 {
			parts := make([]string, 0, len(errs))
			for _, err := range errs {
				parts = append(parts, err.Error())
			}
			return nil, fmt.Errorf("compile mock connector response for tool %q: %s", toolID, strings.Join(parts, "; "))
		}
		if err := validateMockResponseSchema(tool.OutputSchema, "output_schema", true); err != nil {
			return nil, fmt.Errorf("compile mock connector response for tool %q: %w", toolID, err)
		}
		value, err := deterministicMockSchemaValue(tool.OutputSchema, "output_schema")
		if err != nil {
			return nil, fmt.Errorf("compile mock connector response for tool %q: %w", toolID, err)
		}
		response, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("compile mock connector response for tool %q: output_schema: provider connector mock response root must be object", toolID)
		}
		responses[toolID] = response
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

func validateMockResponseSchema(schema runtimecontracts.ToolInputSchema, path string, root bool) error {
	typeName := strings.TrimSpace(schema.Type)
	if typeName == "" {
		typeName = "object"
	}
	if root && typeName != "object" {
		return fmt.Errorf("%s: provider connector mock response root must be object, got %q", path, typeName)
	}
	switch typeName {
	case "object", "array", "string", "boolean", "number", "integer", "null":
	default:
		return fmt.Errorf("%s: unsupported schema type %q", path, typeName)
	}
	if err := validateMockNumericBounds(schema, path, typeName); err != nil {
		return err
	}

	for index := range schema.Enum {
		value, err := decodeMockSchemaLiteral(schema.Enum[index])
		if err != nil {
			return fmt.Errorf("%s.enum[%d]: %w", path, index, err)
		}
		if err := eventschema.ValidateValueAgainstSchema(runtimecontracts.ToolInputSchemaJSONSchema(schema), value); err != nil {
			return fmt.Errorf("%s.enum[%d]: value does not match declared schema: %w", path, index, err)
		}
	}

	if typeName == "object" {
		for _, required := range sortedUniqueStrings(schema.Required) {
			if _, ok := schema.Properties[required]; !ok {
				return fmt.Errorf("%s.properties.%s: required property has no declared schema", path, required)
			}
		}
		propertyNames := make([]string, 0, len(schema.Properties))
		for name := range schema.Properties {
			propertyNames = append(propertyNames, name)
		}
		sort.Strings(propertyNames)
		for _, name := range propertyNames {
			if err := validateMockResponseSchema(schema.Properties[name], path+".properties."+name, false); err != nil {
				return err
			}
		}
		if schema.AdditionalProperties.Schema != nil {
			if err := validateMockResponseSchema(*schema.AdditionalProperties.Schema, path+".additionalProperties", false); err != nil {
				return err
			}
		}
	}
	if typeName == "array" && schema.Items != nil {
		if err := validateMockResponseSchema(*schema.Items, path+".items", false); err != nil {
			return err
		}
	}
	return nil
}

func validateMockNumericBounds(schema runtimecontracts.ToolInputSchema, path, typeName string) error {
	if schema.Minimum != nil && (math.IsNaN(*schema.Minimum) || math.IsInf(*schema.Minimum, 0)) {
		return fmt.Errorf("%s: minimum must be finite", path)
	}
	if schema.Maximum != nil && (math.IsNaN(*schema.Maximum) || math.IsInf(*schema.Maximum, 0)) {
		return fmt.Errorf("%s: maximum must be finite", path)
	}
	if schema.Minimum != nil && schema.Maximum != nil && *schema.Minimum > *schema.Maximum {
		return fmt.Errorf("%s: minimum %v exceeds maximum %v", path, *schema.Minimum, *schema.Maximum)
	}
	if typeName == "integer" {
		minimum := math.Inf(-1)
		maximum := math.Inf(1)
		if schema.Minimum != nil {
			minimum = *schema.Minimum
		}
		if schema.Maximum != nil {
			maximum = *schema.Maximum
		}
		if math.Ceil(minimum) > math.Floor(maximum) {
			return fmt.Errorf("%s: bounds contain no integer", path)
		}
	}
	return nil
}

func deterministicMockSchemaValue(schema runtimecontracts.ToolInputSchema, path string) (any, error) {
	if len(schema.Enum) > 0 {
		value, err := decodeMockSchemaLiteral(schema.Enum[0])
		if err != nil {
			return nil, fmt.Errorf("%s.enum[0]: %w", path, err)
		}
		return value, nil
	}
	typeName := strings.TrimSpace(schema.Type)
	if typeName == "" {
		typeName = "object"
	}
	switch typeName {
	case "object":
		value := make(map[string]any, len(schema.Required))
		for _, name := range sortedUniqueStrings(schema.Required) {
			property, ok := schema.Properties[name]
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
	case "array":
		return []any{}, nil
	case "string":
		return "", nil
	case "boolean":
		return false, nil
	case "number":
		return deterministicMockNumber(schema), nil
	case "integer":
		return deterministicMockInteger(schema), nil
	case "null":
		return nil, nil
	default:
		return nil, fmt.Errorf("%s: unsupported schema type %q", path, typeName)
	}
}

func deterministicMockNumber(schema runtimecontracts.ToolInputSchema) float64 {
	value := float64(0)
	if schema.Minimum != nil && value < *schema.Minimum {
		value = *schema.Minimum
	}
	if schema.Maximum != nil && value > *schema.Maximum {
		value = *schema.Maximum
	}
	return value
}

func deterministicMockInteger(schema runtimecontracts.ToolInputSchema) float64 {
	value := float64(0)
	if schema.Minimum != nil && value < *schema.Minimum {
		value = math.Ceil(*schema.Minimum)
	}
	if schema.Maximum != nil && value > *schema.Maximum {
		value = math.Floor(*schema.Maximum)
	}
	return value
}

func decodeMockSchemaLiteral(literal runtimecontracts.SchemaLiteral) (any, error) {
	var value any
	if err := literal.Node.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode enum literal: %w", err)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("enum literal is not JSON-compatible: %w", err)
	}
	var normalized any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return nil, fmt.Errorf("normalize enum literal: %w", err)
	}
	return normalized, nil
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
