package contracts

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
