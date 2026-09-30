package contracts

import "strings"

type AgentEntityWriteDecl struct {
	Create AgentEntityWriteRule `yaml:"create"`
	Save   AgentEntityWriteRule `yaml:"save"`
}

type AgentEntityWriteRule struct {
	All    bool
	Fields []string
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
