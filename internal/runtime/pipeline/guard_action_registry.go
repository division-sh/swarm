package pipeline

import (
	"sort"
	"strings"
	"sync"

	runtimecontracts "empireai/internal/runtime/contracts"
)

type contractIDRegistry struct {
	ids map[string]struct{}
}

func (r contractIDRegistry) has(id string) bool {
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}
	_, ok := r.ids[id]
	return ok
}

func (r contractIDRegistry) sortedIDs() []string {
	out := make([]string, 0, len(r.ids))
	for id := range r.ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

type contractGuardRegistry struct {
	registry    contractIDRegistry
	definitions map[string]runtimecontracts.GuardActionEntry
}

func (r contractGuardRegistry) HasGuard(id string) bool { return r.registry.has(id) }
func (r contractGuardRegistry) GuardIDs() []string      { return r.registry.sortedIDs() }
func (r contractGuardRegistry) Guard(id string) (runtimecontracts.GuardActionEntry, bool) {
	id = strings.TrimSpace(id)
	def, ok := r.definitions[id]
	return def, ok
}

type contractActionRegistry struct {
	registry    contractIDRegistry
	definitions map[string]runtimecontracts.GuardActionEntry
}

func (r contractActionRegistry) HasAction(id string) bool { return r.registry.has(id) }
func (r contractActionRegistry) ActionIDs() []string      { return r.registry.sortedIDs() }
func (r contractActionRegistry) Action(id string) (runtimecontracts.GuardActionEntry, bool) {
	id = strings.TrimSpace(id)
	def, ok := r.definitions[id]
	return def, ok
}

var (
	workflowGuardRegistryOnce sync.Once
	workflowGuardRegistry     contractGuardRegistry
	workflowActionRegistry    contractActionRegistry
	workflowRegistryErr       error
)

func empireGuardRegistry() GuardRegistry {
	loadWorkflowGuardActionRegistries()
	if workflowRegistryErr != nil {
		panic(workflowRegistryErr)
	}
	return workflowGuardRegistry
}

func empireActionRegistry() ActionRegistry {
	loadWorkflowGuardActionRegistries()
	if workflowRegistryErr != nil {
		panic(workflowRegistryErr)
	}
	return workflowActionRegistry
}

func loadWorkflowGuardActionRegistries() {
	workflowGuardRegistryOnce.Do(func() {
		bundle := empireContractBundle()
		guards := make(map[string]struct{}, len(bundle.Hooks.Guards))
		guardDefs := make(map[string]runtimecontracts.GuardActionEntry, len(bundle.Hooks.Guards))
		for _, guard := range bundle.Hooks.Guards {
			id := strings.TrimSpace(guard.ID)
			if id != "" {
				guards[id] = struct{}{}
				guardDefs[id] = guard
			}
		}
		actions := make(map[string]struct{}, len(bundle.Hooks.Actions))
		actionDefs := make(map[string]runtimecontracts.GuardActionEntry, len(bundle.Hooks.Actions))
		for _, action := range bundle.Hooks.Actions {
			id := strings.TrimSpace(action.ID)
			if id != "" {
				actions[id] = struct{}{}
				actionDefs[id] = action
			}
		}
		workflowGuardRegistry = contractGuardRegistry{
			registry:    contractIDRegistry{ids: guards},
			definitions: guardDefs,
		}
		workflowActionRegistry = contractActionRegistry{
			registry:    contractIDRegistry{ids: actions},
			definitions: actionDefs,
		}
	})
}
