package flowmodel

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type PolicyDocument struct {
	Values   map[string]PolicyValue       `yaml:",inline"`
	Criteria map[string]PolicyCriteriaSet `yaml:"criteria,omitempty"`
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
		var value PolicyValue
		if err := node.Content[i+1].Decode(&value); err != nil {
			return err
		}
		values[key] = value
	}
	d.Values = values
	d.Criteria = criteria
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
