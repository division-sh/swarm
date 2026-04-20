package contracts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type AgentEntityWriteDecl struct {
	Create AgentEntityWriteRule `yaml:"create"`
	Save   AgentEntityWriteRule `yaml:"save"`
}

type AgentEntityWriteRule struct {
	All    bool
	Fields []string
}

func (r *AgentEntityWriteRule) UnmarshalYAML(node *yaml.Node) error {
	if node == nil {
		*r = AgentEntityWriteRule{}
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		value := strings.TrimSpace(node.Value)
		if value != "all" {
			return fmt.Errorf("entity write rule must be \"all\" or an explicit field list")
		}
		*r = AgentEntityWriteRule{All: true}
		return nil
	case yaml.SequenceNode:
		var fields []string
		if err := node.Decode(&fields); err != nil {
			return err
		}
		out := make([]string, 0, len(fields))
		seen := map[string]struct{}{}
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			if field == "all" {
				return fmt.Errorf("entity write rule cannot mix reserved keyword \"all\" with explicit fields")
			}
			if _, ok := seen[field]; ok {
				continue
			}
			seen[field] = struct{}{}
			out = append(out, field)
		}
		if len(out) == 0 {
			return fmt.Errorf("entity write rule explicit field list must not be empty")
		}
		*r = AgentEntityWriteRule{Fields: out}
		return nil
	default:
		return fmt.Errorf("entity write rule must be scalar \"all\" or sequence")
	}
}

func (r AgentEntityWriteRule) Declared() bool {
	return r.All || len(r.Fields) > 0
}

func (r AgentEntityWriteRule) AllowsField(field string) bool {
	field = strings.TrimSpace(field)
	if field == "" {
		return false
	}
	if r.All {
		return true
	}
	for _, candidate := range r.Fields {
		if strings.TrimSpace(candidate) == field {
			return true
		}
	}
	return false
}
