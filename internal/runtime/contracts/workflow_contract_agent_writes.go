package contracts

type AgentWriteGrant struct {
	All    bool
	Fields []string
}

func (r AgentWriteGrant) Declared() bool {
	return r.All || len(r.Fields) > 0
}
