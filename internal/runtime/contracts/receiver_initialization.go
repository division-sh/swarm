package contracts

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

type receiverVariable struct {
	name         string
	typeRef      string
	resolved     ResolvedCatalogType
	schema       map[string]any
	optional     bool
	hasDefault   bool
	defaultValue any
}

// ReceiverConfiguration owns receiver variable admission for every creation
// path. Its schema and defaults cannot be mutated by callers.
type ReceiverConfiguration struct{ variables []receiverVariable }

// MarshalYAML preserves authored presence when source projections are rendered.
func (v FlowVariable) MarshalYAML() (any, error) {
	typeRef := v.Type
	if v.IsOptional {
		typeRef += "?"
	}
	out := map[string]any{"type": typeRef}
	if v.HasDefault {
		out["default"] = v.Default
	}
	if v.Description != "" {
		out["description"] = v.Description
	}
	if v.Refinements.Pattern != "" {
		out["pattern"] = v.Refinements.Pattern
	}
	if v.Refinements.EqualTo != "" {
		out["equal_to"] = v.Refinements.EqualTo
	}
	if !v.Refinements.Length.Empty() {
		bounds := map[string]any{}
		if v.Refinements.Length.Min != nil {
			bounds["min"] = *v.Refinements.Length.Min
		}
		if v.Refinements.Length.Max != nil {
			bounds["max"] = *v.Refinements.Length.Max
		}
		out["length"] = bounds
	}
	if !v.Refinements.Range.Empty() {
		bounds := map[string]any{}
		if v.Refinements.Range.Min != nil {
			bounds["min"] = *v.Refinements.Range.Min
		}
		if v.Refinements.Range.Max != nil {
			bounds["max"] = *v.Refinements.Range.Max
		}
		out["range"] = bounds
	}
	return out, nil
}

func CompileReceiverConfiguration(declaration FlowInstanceVariables, catalog TypeCatalogDocument) (ReceiverConfiguration, error) {
	names := make([]string, 0, len(declaration.Variables))
	refinements := make(map[string]schemaRefinementField, len(declaration.Variables))
	for name, variable := range declaration.Variables {
		names = append(names, name)
		refinements[name] = schemaRefinementField{Name: name, TypeRef: variable.Type, Refinements: variable.Refinements}
	}
	if err := errors.Join(validateSchemaRefinementFields("receiver variables", catalog, refinements)...); err != nil {
		return ReceiverConfiguration{}, err
	}
	sort.Strings(names)
	out := ReceiverConfiguration{}
	for _, name := range names {
		if name == "" || name != strings.TrimSpace(name) || strings.Contains(name, ".") {
			return ReceiverConfiguration{}, fmt.Errorf("receiver variable %q must be an exact field name", name)
		}
		variable := declaration.Variables[name]
		if variable.Type == "" {
			return ReceiverConfiguration{}, fmt.Errorf("receiver variable %s requires a declared type", name)
		}
		if err := validateWave1TypeRef(variable.Type, "receiver variable "+name); err != nil {
			return ReceiverConfiguration{}, err
		}
		resolved, err := (CatalogTypeReference{Type: variable.Type, Catalog: catalog}).Resolve()
		if err != nil {
			return ReceiverConfiguration{}, fmt.Errorf("receiver variable %s: %w", name, err)
		}
		schema, _ := eventSchemaForTypeRef(variable.Type, catalog, map[string]struct{}{})
		applySchemaRefinements(schema, variable.Refinements)
		field := receiverVariable{name: name, typeRef: variable.Type, resolved: resolved, schema: schema, optional: variable.IsOptional, hasDefault: variable.HasDefault}
		if variable.HasDefault {
			value, err := admitReceiverVariable(field, variable.Default)
			if err != nil {
				return ReceiverConfiguration{}, fmt.Errorf("receiver variable %s default: %w", name, err)
			}
			field.defaultValue = value
		}
		out.variables = append(out.variables, field)
	}
	return out, nil
}

func admitReceiverVariable(field receiverVariable, value any) (any, error) {
	copy, err := canonicaljson.CloneRuntimeValue(value)
	if err != nil {
		return nil, err
	}
	if err := eventschema.ValidateValueAgainstSchema(field.schema, copy); err != nil {
		return nil, err
	}
	return copy, nil
}

// Admit applies defaults only to absent members. Explicit null and malformed
// supplied values are validated as supplied, never converted into defaults.
func (c ReceiverConfiguration) Admit(supplied map[string]any) (map[string]any, error) {
	known := make(map[string]bool, len(c.variables))
	for _, field := range c.variables {
		known[field.name] = true
	}
	for name := range supplied {
		if !known[name] {
			return nil, fmt.Errorf("receiver configuration contains undeclared variable %q", name)
		}
	}
	out := make(map[string]any, len(c.variables))
	for _, field := range c.variables {
		value, present := supplied[field.name]
		if !present {
			if field.hasDefault {
				value = field.defaultValue
			} else if field.optional {
				continue
			} else {
				return nil, fmt.Errorf("receiver variable %s is missing and has no default", field.name)
			}
		}
		admitted, err := admitReceiverVariable(field, value)
		if err != nil {
			return nil, fmt.Errorf("receiver variable %s: %w", field.name, err)
		}
		out[field.name] = admitted
	}
	properties := make(map[string]any, len(c.variables))
	for _, field := range c.variables {
		properties[field.name] = field.schema
	}
	if err := eventschema.ValidateValueAgainstSchema(map[string]any{"type": "object", "properties": properties, "additionalProperties": false}, out); err != nil {
		return nil, fmt.Errorf("receiver configuration: %w", err)
	}
	return out, nil
}

type receiverBinding struct {
	variable string
	path     []string
}

// ReceiverInitialization retains the exact payload bindings admitted for one
// input pin; runtime execution never interprets authored paths again.
type ReceiverInitialization struct {
	configuration ReceiverConfiguration
	bindings      []receiverBinding
	bound         bool
}

func CompileReceiverInitialization(configuration ReceiverConfiguration, bindings map[string]string, source CompiledEventSchema) (ReceiverInitialization, error) {
	root, hasSchema := source.StructuralType()
	out := ReceiverInitialization{configuration: configuration, bound: hasSchema || len(bindings) == 0}
	names := make([]string, 0, len(bindings))
	for name := range bindings {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		var target *receiverVariable
		for i := range configuration.variables {
			if configuration.variables[i].name == name {
				target = &configuration.variables[i]
				break
			}
		}
		if target == nil {
			return ReceiverInitialization{}, fmt.Errorf("initialize destination %q is not a declared receiver variable", name)
		}
		path, err := receiverPayloadPath(bindings[name])
		if err != nil {
			return ReceiverInitialization{}, fmt.Errorf("initialize %s: %w", name, err)
		}
		if !hasSchema {
			out.bindings = append(out.bindings, receiverBinding{variable: name, path: path})
			continue
		}
		field, found := root.FieldPath(strings.Join(path, "."))
		if !found {
			return ReceiverInitialization{}, fmt.Errorf("initialize %s source %s is not a declared payload field", name, bindings[name])
		}
		if !StructuralCatalogTypeAssignable(field.Type, target.resolved) {
			return ReceiverInitialization{}, fmt.Errorf("initialize %s source %s is not assignable to %s", name, bindings[name], target.typeRef)
		}
		out.bindings = append(out.bindings, receiverBinding{variable: name, path: path})
	}
	return out, nil
}

func (p ReceiverInitialization) Evaluate(payload map[string]any) (map[string]any, error) {
	return p.EvaluateWithIdentity(payload, nil)
}

// EvaluateWithIdentity admits the independently resolved instance key alongside
// bound values. A binding cannot override that key, even if types agree.
func (p ReceiverInitialization) EvaluateWithIdentity(payload, identity map[string]any) (map[string]any, error) {
	if !p.bound {
		return nil, fmt.Errorf("receiver initialization is not bound to an admitted producer schema")
	}
	values := make(map[string]any, len(p.bindings))
	for name, value := range identity {
		values[name] = value
	}
	for _, binding := range p.bindings {
		var value any = payload
		present := true
		for _, segment := range binding.path {
			object, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("initialize %s: payload path traverses a non-object", binding.variable)
			}
			value, present = object[segment]
			if !present {
				break
			}
		}
		if present {
			if key, exists := identity[binding.variable]; exists {
				left, err := canonicaljson.Hash(key)
				if err != nil {
					return nil, err
				}
				right, err := canonicaljson.Hash(value)
				if err != nil {
					return nil, err
				}
				if left != right {
					return nil, fmt.Errorf("initialize %s contradicts the resolved instance key", binding.variable)
				}
			}
			values[binding.variable] = value
		}
	}
	return p.configuration.Admit(values)
}

func receiverPayloadPath(raw string) ([]string, error) {
	if raw != strings.TrimSpace(raw) || !strings.HasPrefix(raw, "payload.") {
		return nil, fmt.Errorf("source must be one exact payload.field path")
	}
	parts := strings.Split(strings.TrimPrefix(raw, "payload."), ".")
	for _, part := range parts {
		if part == "" || strings.ContainsAny(part, " []{}()\t\r\n") {
			return nil, fmt.Errorf("source must be one exact payload.field path")
		}
	}
	return parts, nil
}

func decodeReceiverInitialize(node *yaml.Node) (map[string]string, error) {
	value := yamlsource.ValueFromNode(node)
	if value.Presence() != yamlsource.PresenceMapping {
		return nil, fmt.Errorf("initialize must be a non-empty variable-to-payload-path mapping")
	}
	fields, err := uniqueYAMLMappingFields(value, "initialize")
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, field := range fields {
		path, err := requiredLiteralString(field.Value, "initialize "+field.Name)
		if err != nil {
			return nil, err
		}
		if field.Name == "" || field.Name != strings.TrimSpace(field.Name) {
			return nil, fmt.Errorf("initialize destination %q must be exact", field.Name)
		}
		if _, err := receiverPayloadPath(path); err != nil {
			return nil, fmt.Errorf("initialize %s: %w", field.Name, err)
		}
		out[field.Name] = path
	}
	return out, nil
}

func decodeReceiverVariable(node *yaml.Node, out *FlowVariable) error {
	value := yamlsource.ValueFromNode(node)
	if value.Presence() != yamlsource.PresenceMapping {
		field, err := projectTypeFieldSpec(value)
		if err != nil {
			return err
		}
		*out = FlowVariable{Type: field.Type, IsOptional: field.IsOptional}
		return nil
	}
	fields, err := uniqueYAMLMappingFields(value, "receiver variable")
	if err != nil {
		return err
	}
	fieldNode := yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	var defaultValue yamlsource.Value
	hasDefault := false
	for _, member := range fields {
		if member.Name == "default" {
			defaultValue, hasDefault = member.Value, true
			continue
		}
		var child yaml.Node
		if err := member.Value.Project(&child); err != nil {
			return err
		}
		fieldNode.Content = append(fieldNode.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: member.Name}, &child)
	}
	var field TypeFieldSpec
	if err := fieldNode.Decode(&field); err != nil {
		return err
	}
	*out = FlowVariable{Type: field.Type, IsOptional: field.IsOptional, Description: field.Description, HasDefault: hasDefault, Refinements: field.Refinements}
	if hasDefault {
		if err := defaultValue.Project(&out.Default); err != nil {
			return err
		}
	}
	return nil
}

func (v *FlowInstanceVariables) UnmarshalYAML(node *yaml.Node) error {
	fields, err := uniqueYAMLMappingFields(yamlsource.ValueFromNode(node), "instance_variables")
	if err != nil {
		return err
	}
	out := FlowInstanceVariables{Variables: map[string]FlowVariable{}}
	for _, field := range fields {
		switch field.Name {
		case "description":
			if err := field.Value.Project(&out.Description); err != nil {
				return err
			}
		case "variables":
			variables, err := uniqueYAMLMappingFields(field.Value, "instance_variables.variables")
			if err != nil {
				return err
			}
			for _, variable := range variables {
				var declaration FlowVariable
				if err := variable.Value.Project(&declaration); err != nil {
					return err
				}
				out.Variables[variable.Name] = declaration
			}
		default:
			return fmt.Errorf("instance_variables field %q is unsupported; declare typed variables under variables", field.Name)
		}
	}
	*v = out
	return nil
}

// ReceiverConfigurationForFlow adds the instance key from its existing entity
// declaration, never from a path or an incoming configuration map.
func (b *WorkflowContractBundle) ReceiverConfigurationForFlow(flowID string) (ReceiverConfiguration, error) {
	if flowID == "" {
		flowID = "."
	}
	schema, ok := b.FlowSchemaByID(flowID)
	if !ok {
		return ReceiverConfiguration{}, fmt.Errorf("receiver configuration has no exact flow %q", flowID)
	}
	declaration := FlowInstanceVariables{Variables: map[string]FlowVariable{}}
	for name, variable := range schema.InstanceVariables.Variables {
		declaration.Variables[name] = variable
	}
	if !schema.Instance.Empty() {
		primary, err := b.ResolveFlowPrimaryEntity(flowID)
		if err != nil {
			return ReceiverConfiguration{}, err
		}
		field, ok := primary.Contract.Fields[schema.Instance.Path()]
		if !ok {
			return ReceiverConfiguration{}, fmt.Errorf("receiver instance field %s has no declaration", schema.Instance.Path())
		}
		if variable, exists := declaration.Variables[schema.Instance.Path()]; exists {
			if variable.Type != field.Type || variable.HasDefault || variable.IsOptional {
				return ReceiverConfiguration{}, fmt.Errorf("receiver variable %s contradicts the canonical instance declaration", schema.Instance.Path())
			}
		} else {
			declaration.Variables[schema.Instance.Path()] = FlowVariable{Type: field.Type}
		}
	}
	return CompileReceiverConfiguration(declaration, b.ResolvedTypeCatalogForFlow(flowID))
}

func (p ReceiverInitialization) semanticEvidence() any {
	fields := make([]map[string]any, 0, len(p.configuration.variables))
	for _, v := range p.configuration.variables {
		fields = append(fields, map[string]any{"name": v.name, "type": v.typeRef, "schema": v.schema, "optional": v.optional, "has_default": v.hasDefault, "default": v.defaultValue})
	}
	bindings := make(map[string]string, len(p.bindings))
	for _, binding := range p.bindings {
		bindings[binding.variable] = "payload." + strings.Join(binding.path, ".")
	}
	return struct {
		Fields   any
		Bindings any
		Bound    bool
	}{fields, bindings, p.bound}
}

func (p ReceiverInitialization) Bindings() map[string]string {
	out := make(map[string]string, len(p.bindings))
	for _, binding := range p.bindings {
		out[binding.variable] = "payload." + strings.Join(binding.path, ".")
	}
	return out
}

func (p CompiledFlowInputPin) Initialization() ReceiverInitialization {
	if p.value == nil {
		return ReceiverInitialization{}
	}
	return p.value.initialization
}
