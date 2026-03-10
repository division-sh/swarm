package pipeline

import (
	"sort"
	"strings"

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
func (r contractGuardRegistry) IsExecutable(id string) bool {
	def, ok := r.Guard(id)
	return ok && isExecutableWorkflowGuardEntry(def)
}
func (r contractGuardRegistry) GuardIDs() []string { return r.registry.sortedIDs() }
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
func (r contractActionRegistry) IsExecutable(id string) bool {
	def, ok := r.Action(id)
	return ok && isExecutableWorkflowActionEntry(def)
}
func (r contractActionRegistry) ActionIDs() []string { return r.registry.sortedIDs() }
func (r contractActionRegistry) Action(id string) (runtimecontracts.GuardActionEntry, bool) {
	id = strings.TrimSpace(id)
	def, ok := r.definitions[id]
	return def, ok
}

func defaultGuardRegistry() GuardRegistry {
	return defaultWorkflowModule().GuardRegistry()
}

func defaultActionRegistry() ActionRegistry {
	return defaultWorkflowModule().ActionRegistry()
}

func NewContractGuardRegistry(bundle *runtimecontracts.WorkflowContractBundle) GuardRegistry {
	if bundle == nil {
		return contractGuardRegistry{}
	}
	entries := bundle.GuardEntries()
	guards := make(map[string]struct{}, len(entries))
	guardDefs := make(map[string]runtimecontracts.GuardActionEntry, len(entries))
	for _, guard := range entries {
		id := strings.TrimSpace(guard.ID)
		if id != "" {
			guards[id] = struct{}{}
			guardDefs[id] = guard
		}
	}
	return contractGuardRegistry{
		registry:    contractIDRegistry{ids: guards},
		definitions: guardDefs,
	}
}

func NewContractActionRegistry(bundle *runtimecontracts.WorkflowContractBundle) ActionRegistry {
	if bundle == nil {
		return contractActionRegistry{}
	}
	entries := bundle.ActionEntries()
	actions := make(map[string]struct{}, len(entries))
	actionDefs := make(map[string]runtimecontracts.GuardActionEntry, len(entries))
	for _, action := range entries {
		id := strings.TrimSpace(action.ID)
		if id != "" {
			actions[id] = struct{}{}
			actionDefs[id] = action
		}
	}
	return contractActionRegistry{
		registry:    contractIDRegistry{ids: actions},
		definitions: actionDefs,
	}
}
