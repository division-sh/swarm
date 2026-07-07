package flowmodel

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type PolicyDocument struct {
	Values     map[string]PolicyValue         `yaml:",inline"`
	Criteria   map[string]PolicyCriteriaSet   `yaml:"criteria,omitempty"`
	Validation map[string]PolicyValidationSet `yaml:"validation,omitempty"`
	Modules    map[string]PolicyModule        `yaml:"modules,omitempty"`
}

type PolicyValue struct {
	Value       any    `yaml:"value"`
	Description string `yaml:"description"`
	Override    bool   `yaml:"override"`
}

type PolicyCriteriaSet struct {
	Classes map[string]PolicyCriteriaClass `yaml:"classes"`
	Rules   []PolicyCriteriaRule           `yaml:"rules"`
}

type PolicyCriteriaClass struct {
	Disposition string `yaml:"disposition"`
}

type PolicyCriteriaRule struct {
	ID     string                         `yaml:"id"`
	Class  string                         `yaml:"class"`
	Text   string                         `yaml:"text"`
	Params map[string]PolicyCriteriaParam `yaml:"params"`
}

type PolicyCriteriaParam struct {
	Value any
}

type PolicyValidationSet struct {
	Classes map[string]PolicyValidationClass `yaml:"classes"`
	Inputs  map[string]string                `yaml:"inputs"`
	Rules   []PolicyValidationRule           `yaml:"rules"`
}

type PolicyValidationClass struct {
	Disposition string `yaml:"disposition"`
}

type PolicyValidationRule struct {
	ID           string                         `yaml:"id"`
	Class        string                         `yaml:"class"`
	Text         string                         `yaml:"text"`
	Params       map[string]PolicyCriteriaParam `yaml:"params"`
	PinCandidate *bool                          `yaml:"pin_candidate"`
	Check        PolicyValidationCheck          `yaml:"check"`
}

type PolicyValidationCheck struct {
	Equal *PolicyValidationEqualCheck `yaml:"equal"`
}

type PolicyValidationEqualCheck struct {
	Left  string `yaml:"left"`
	Right string `yaml:"right"`
}

type PolicyModule struct {
	Path         string              `yaml:"path"`
	Kind         string              `yaml:"kind"`
	ABI          string              `yaml:"abi"`
	Entry        string              `yaml:"entry"`
	Digest       string              `yaml:"digest"`
	SourcePath   string              `yaml:"source_path"`
	SourceHash   string              `yaml:"source_hash"`
	Runtime      PolicyModuleRuntime `yaml:"runtime"`
	InputSchema  map[string]any      `yaml:"input_schema"`
	OutputSchema map[string]any      `yaml:"output_schema"`
	Limits       PolicyModuleLimits  `yaml:"limits"`
}

type PolicyModuleRuntime struct {
	Interpreter       string `yaml:"interpreter"`
	InterpreterDigest string `yaml:"interpreter_digest"`
	SnapshotDigest    string `yaml:"snapshot_digest"`
	HarnessABI        string `yaml:"harness_abi"`
}

type PolicyModuleLimits struct {
	Gas         uint64 `yaml:"gas"`
	MemoryPages uint32 `yaml:"memory_pages"`
	OutputBytes int    `yaml:"output_bytes"`
}

type Tree[T any] struct {
	Root   *T
	ByPath map[string]*T
	ByID   map[string]*T
}

type URIRegistry struct {
	Scheme string
	Nodes  map[string]URIRef
	Agents map[string]URIRef
	Events map[string]URIRef
	ByURI  map[string]URIRef
}

type URIRef struct {
	Kind     string
	FlowID   string
	LocalID  string
	Path     string
	Absolute string
	Full     string
}

func (d *PolicyDocument) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	values := map[string]PolicyValue{}
	if node == nil || node.Kind == 0 {
		d.Values = values
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("policy document must be a mapping")
	}
	criteria := map[string]PolicyCriteriaSet{}
	validation := map[string]PolicyValidationSet{}
	modules := map[string]PolicyModule{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if key == "criteria" {
			if err := node.Content[i+1].Decode(&criteria); err != nil {
				return fmt.Errorf("policy criteria: %w", err)
			}
			continue
		}
		if key == "validation" {
			if err := node.Content[i+1].Decode(&validation); err != nil {
				return fmt.Errorf("policy validation: %w", err)
			}
			continue
		}
		if key == "modules" {
			if err := node.Content[i+1].Decode(&modules); err != nil {
				return fmt.Errorf("policy modules: %w", err)
			}
			continue
		}
		var value PolicyValue
		if err := node.Content[i+1].Decode(&value); err != nil {
			return err
		}
		values[key] = value
	}
	d.Values = values
	d.Criteria = criteria
	d.Validation = validation
	d.Modules = modules
	return nil
}

func (m *PolicyModule) UnmarshalYAML(node *yaml.Node) error {
	if m == nil {
		return nil
	}
	if err := validateYAMLMappingKeys(node, "policy module", map[string]struct{}{
		"path":          {},
		"kind":          {},
		"abi":           {},
		"entry":         {},
		"digest":        {},
		"source_path":   {},
		"source_hash":   {},
		"runtime":       {},
		"input_schema":  {},
		"output_schema": {},
		"limits":        {},
	}); err != nil {
		return err
	}
	type alias PolicyModule
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*m = PolicyModule(aux)
	return nil
}

func (r *PolicyModuleRuntime) UnmarshalYAML(node *yaml.Node) error {
	if r == nil {
		return nil
	}
	if err := validateYAMLMappingKeys(node, "policy module runtime", map[string]struct{}{
		"interpreter":        {},
		"interpreter_digest": {},
		"snapshot_digest":    {},
		"harness_abi":        {},
	}); err != nil {
		return err
	}
	type alias PolicyModuleRuntime
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*r = PolicyModuleRuntime(aux)
	return nil
}

func (l *PolicyModuleLimits) UnmarshalYAML(node *yaml.Node) error {
	if l == nil {
		return nil
	}
	if err := validateYAMLMappingKeys(node, "policy module limits", map[string]struct{}{
		"gas":          {},
		"memory_pages": {},
		"output_bytes": {},
	}); err != nil {
		return err
	}
	type alias PolicyModuleLimits
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*l = PolicyModuleLimits(aux)
	return nil
}

func (v *PolicyValue) UnmarshalYAML(node *yaml.Node) error {
	if v == nil {
		return nil
	}
	if node == nil {
		*v = PolicyValue{}
		return nil
	}
	if node.Kind == yaml.MappingNode && (hasYAMLMappingKey(node, "value") || hasYAMLMappingKey(node, "description") || hasYAMLMappingKey(node, "override")) {
		type alias PolicyValue
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*v = PolicyValue(aux)
		return nil
	}
	var raw any
	if err := node.Decode(&raw); err != nil {
		return err
	}
	*v = PolicyValue{Value: raw}
	return nil
}

func (p *PolicyCriteriaParam) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	var raw any
	if err := node.Decode(&raw); err != nil {
		return err
	}
	p.Value = raw
	return nil
}

func (s *PolicyValidationSet) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if err := validateYAMLMappingKeys(node, "policy validation set", map[string]struct{}{
		"classes": {},
		"inputs":  {},
		"rules":   {},
	}); err != nil {
		return err
	}
	type alias PolicyValidationSet
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = PolicyValidationSet(aux)
	return nil
}

func (c *PolicyValidationClass) UnmarshalYAML(node *yaml.Node) error {
	if c == nil {
		return nil
	}
	if err := validateYAMLMappingKeys(node, "policy validation class", map[string]struct{}{
		"disposition": {},
	}); err != nil {
		return err
	}
	type alias PolicyValidationClass
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*c = PolicyValidationClass(aux)
	return nil
}

func (r *PolicyValidationRule) UnmarshalYAML(node *yaml.Node) error {
	if r == nil {
		return nil
	}
	if err := validateYAMLMappingKeys(node, "policy validation rule", map[string]struct{}{
		"id":            {},
		"class":         {},
		"text":          {},
		"params":        {},
		"pin_candidate": {},
		"check":         {},
	}); err != nil {
		return err
	}
	type alias PolicyValidationRule
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*r = PolicyValidationRule(aux)
	return nil
}

func (c *PolicyValidationCheck) UnmarshalYAML(node *yaml.Node) error {
	if c == nil {
		return nil
	}
	if err := validateYAMLMappingKeys(node, "policy validation check", map[string]struct{}{
		"equal": {},
	}); err != nil {
		return err
	}
	type alias PolicyValidationCheck
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*c = PolicyValidationCheck(aux)
	return nil
}

func (c *PolicyValidationEqualCheck) UnmarshalYAML(node *yaml.Node) error {
	if c == nil {
		return nil
	}
	if err := validateYAMLMappingKeys(node, "policy validation equal check", map[string]struct{}{
		"left":  {},
		"right": {},
	}); err != nil {
		return err
	}
	type alias PolicyValidationEqualCheck
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*c = PolicyValidationEqualCheck(aux)
	return nil
}

func validateYAMLMappingKeys(node *yaml.Node, context string, allowed map[string]struct{}) error {
	if node == nil || node.Kind == 0 {
		return fmt.Errorf("%s must be a mapping", context)
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("%s must be a mapping", context)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("%s unsupported field %q", context, key)
		}
	}
	return nil
}

func hasYAMLMappingKey(node *yaml.Node, key string) bool {
	if node == nil || node.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == key {
			return true
		}
	}
	return false
}
