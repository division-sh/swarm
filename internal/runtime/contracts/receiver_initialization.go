package contracts

import (
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

func CompileReceiverConfiguration(declaration FlowInstanceVariables, catalog TypeCatalogDocument) (ReceiverConfiguration, error) {
	names := make([]string, 0, len(declaration.Variables))
	for name := range declaration.Variables {
		names = append(names, name)
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
	if yamlsource.ValueFromNode(node).Presence() != yamlsource.PresenceMapping {
		return nil, fmt.Errorf("initialize must be a non-empty variable-to-payload-path mapping")
	}
	if err := validateExactW2MappingKeys(node, "initialize"); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		name, value := node.Content[i].Value, node.Content[i+1]
		if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
			return nil, fmt.Errorf("initialize %s must be one exact payload.field path", name)
		}
		if _, err := receiverPayloadPath(value.Value); err != nil {
			return nil, fmt.Errorf("initialize %s: %w", name, err)
		}
		out[name] = value.Value
	}
	return out, nil
}

func decodeReceiverVariable(node *yaml.Node, out *FlowVariable) error {
	if node.Kind == yaml.ScalarNode {
		var field TypeFieldSpec
		if err := node.Decode(&field); err != nil {
			return err
		}
		*out = FlowVariable{Type: field.Type, IsOptional: field.IsOptional}
		return nil
	}
	if err := validateExactW2MappingKeys(node, "receiver variable"); err != nil {
		return err
	}
	fieldNode := *node
	fieldNode.Content = nil
	var defaultNode *yaml.Node
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == "default" {
			defaultNode = node.Content[i+1]
			continue
		}
		fieldNode.Content = append(fieldNode.Content, node.Content[i], node.Content[i+1])
	}
	var field TypeFieldSpec
	if err := fieldNode.Decode(&field); err != nil {
		return err
	}
	*out = FlowVariable{Type: field.Type, IsOptional: field.IsOptional, Description: field.Description, HasDefault: defaultNode != nil, Refinements: field.Refinements}
	if defaultNode != nil {
		if err := defaultNode.Decode(&out.Default); err != nil {
			return err
		}
	}
	return nil
}

func (v *FlowInstanceVariables) UnmarshalYAML(node *yaml.Node) error {
	if err := validateKnownMappingFields(node, "instance_variables", map[string]struct{}{"description": {}, "variables": {}}); err != nil {
		return err
	}
	type raw FlowInstanceVariables
	var decoded raw
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*v = FlowInstanceVariables(decoded)
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
