package stagecatalogfixture

import (
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

// NewFinalCatalog is only for tests whose subject is not stage admission.
// Stage identity tests must use the selected compiled topology instead.
func NewFinalCatalog(workflow []string, flows map[string][]string) runlifecycle.FinalCatalog {
	owner := terminalFixture{flows: make(map[string]map[string]struct{}, len(flows))}
	for flow, states := range flows {
		owner.flows[flow] = makeSet(states)
	}
	if len(workflow) != 0 {
		owner.flows["."] = makeSet(workflow)
	}
	catalog, err := runlifecycle.NewCompiledFinalCatalog(owner)
	if err != nil {
		panic(err)
	}
	return catalog
}

type terminalFixture struct {
	flows map[string]map[string]struct{}
}

func (terminalFixture) Valid() bool { return true }

func (f terminalFixture) Final(flowTemplate, flowInstance, state string) (bool, bool) {
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
	return false, false
}

func makeSet(states []string) map[string]struct{} {
	out := make(map[string]struct{}, len(states))
	for _, state := range states {
		out[state] = struct{}{}
	}
	return out
}
