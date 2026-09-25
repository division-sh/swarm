package stagecatalogfixture

import (
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

// NewTerminalCatalog is only for tests whose subject is not stage admission.
// Stage identity tests must use the selected compiled topology instead.
func NewTerminalCatalog(workflow []string, flows map[string][]string) runlifecycle.TerminalCatalog {
	owner := terminalFixture{workflow: makeSet(workflow), flows: make(map[string]map[string]struct{}, len(flows))}
	for flow, states := range flows {
		owner.flows[flow] = makeSet(states)
	}
	catalog, err := runlifecycle.NewCompiledTerminalCatalog(owner)
	if err != nil {
		panic(err)
	}
	return catalog
}

type terminalFixture struct {
	workflow map[string]struct{}
	flows    map[string]map[string]struct{}
}

func (terminalFixture) Valid() bool { return true }

func (f terminalFixture) Terminal(flowTemplate, flowInstance, state string) (bool, bool) {
	state = strings.TrimSpace(state)
	if state == "" {
		return false, false
	}
	for _, key := range []string{flowTemplate, flowInstance} {
		if states, ok := f.flows[key]; ok && key != "" {
			_, terminal := states[state]
			return terminal, true
		}
	}
	if flowInstance != "" || len(f.workflow) == 0 {
		return false, false
	}
	_, terminal := f.workflow[state]
	return terminal, true
}

func makeSet(states []string) map[string]struct{} {
	out := make(map[string]struct{}, len(states))
	for _, state := range states {
		out[state] = struct{}{}
	}
	return out
}
