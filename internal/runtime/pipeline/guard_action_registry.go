package pipeline

import (
	"sort"
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/core/identity"
	runtimeregistry "empireai/internal/runtime/registry"
	"empireai/internal/runtime/semanticview"
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

func workflowGuardExecutionKey(entry runtimecontracts.GuardActionEntry) string {
	if builtin := strings.TrimSpace(entry.PlatformBuiltin); builtin != "" {
		return normalizeWorkflowBuiltinGuardID(builtin)
	}
	return strings.TrimSpace(entry.ID)
}

func workflowActionExecutionKey(entry runtimecontracts.GuardActionEntry) string {
	if builtin := strings.TrimSpace(entry.PlatformBuiltin); builtin != "" {
		return normalizeWorkflowBuiltinActionID(builtin)
	}
	return strings.TrimSpace(entry.ID)
}

func isExecutableWorkflowGuardEntry(entry runtimecontracts.GuardActionEntry) bool {
	if strings.TrimSpace(entry.Check) != "" {
		return true
	}
	return isSupportedWorkflowGuardBuiltin(firstNonEmptyString(entry.PlatformBuiltin, entry.ID))
}

func isExecutableWorkflowActionEntry(entry runtimecontracts.GuardActionEntry) bool {
	if strings.TrimSpace(entry.Emits) != "" {
		return true
	}
	return isSupportedWorkflowHandlerActionID(firstNonEmptyString(entry.PlatformBuiltin, entry.ID))
}

func isSupportedWorkflowHandlerActionID(id string) bool {
	switch normalizeWorkflowBuiltinActionID(id) {
	case "create_flow_instance", "record_evidence":
		return true
	default:
		return isSupportedWorkflowActionBuiltin(id)
	}
}

type contractGuardRegistry struct {
	registry     contractIDRegistry
	instructions map[string]runtimeregistry.GuardInstruction
}

func (r contractGuardRegistry) HasGuard(id identity.GuardKey) bool {
	return r.registry.has(id.String())
}
func (r contractGuardRegistry) IsExecutable(id identity.GuardKey) bool {
	instruction, ok := r.instructions[id.String()]
	if !ok {
		return false
	}
	if instruction.Kind() == runtimeregistry.InstructionCEL {
		return true
	}
	return isSupportedWorkflowGuardBuiltin(firstNonEmptyString(instruction.Builtin, instruction.Key.String()))
}
func (r contractGuardRegistry) GuardIDs() []string { return r.registry.sortedIDs() }
func (r contractGuardRegistry) Guard(id identity.GuardKey) (runtimeregistry.GuardInstruction, bool) {
	instruction, ok := r.instructions[id.String()]
	return instruction, ok
}

type contractActionRegistry struct {
	registry     contractIDRegistry
	instructions map[string]runtimeregistry.ActionInstruction
}

func (r contractActionRegistry) HasAction(id identity.ActionKey) bool {
	return r.registry.has(id.String())
}
func (r contractActionRegistry) IsExecutable(id identity.ActionKey) bool {
	instruction, ok := r.instructions[id.String()]
	if !ok {
		return false
	}
	if instruction.Emits != "" {
		return true
	}
	return isSupportedWorkflowHandlerActionID(firstNonEmptyString(instruction.Builtin, instruction.Key.String()))
}
func (r contractActionRegistry) ActionIDs() []string { return r.registry.sortedIDs() }
func (r contractActionRegistry) Action(id identity.ActionKey) (runtimeregistry.ActionInstruction, bool) {
	instruction, ok := r.instructions[id.String()]
	return instruction, ok
}

func defaultGuardRegistry() GuardRegistry {
	return defaultWorkflowModule().GuardRegistry()
}

func defaultActionRegistry() ActionRegistry {
	return defaultWorkflowModule().ActionRegistry()
}

func NewContractGuardRegistry(source semanticview.Source) GuardRegistry {
	if source == nil {
		return contractGuardRegistry{}
	}
	instructions := source.GuardInstructions()
	guards := make(map[string]struct{}, len(instructions))
	guardInstructions := make(map[string]runtimeregistry.GuardInstruction, len(instructions))
	for _, instruction := range instructions {
		id := instruction.Key.String()
		if id != "" {
			guards[id] = struct{}{}
			guardInstructions[id] = instruction
		}
	}
	return contractGuardRegistry{
		registry:     contractIDRegistry{ids: guards},
		instructions: guardInstructions,
	}
}

func NewContractActionRegistry(source semanticview.Source) ActionRegistry {
	if source == nil {
		return contractActionRegistry{}
	}
	instructions := source.ActionInstructions()
	actions := make(map[string]struct{}, len(instructions))
	actionInstructions := make(map[string]runtimeregistry.ActionInstruction, len(instructions))
	for _, instruction := range instructions {
		id := instruction.Key.String()
		if id != "" {
			actions[id] = struct{}{}
			actionInstructions[id] = instruction
		}
	}
	return contractActionRegistry{
		registry:     contractIDRegistry{ids: actions},
		instructions: actionInstructions,
	}
}

func normalizeWorkflowBuiltinGuardID(id string) string {
	return strings.TrimSpace(strings.ToLower(id))
}

func normalizeWorkflowBuiltinActionID(id string) string {
	return strings.TrimSpace(strings.ToLower(id))
}

func isSupportedWorkflowGuardBuiltin(id string) bool {
	switch normalizeWorkflowBuiltinGuardID(id) {
	case "has_entity_id",
		"has_human_decision",
		"not_in_terminal_state",
		"not_in_terminal_stage",
		"not_in_operating_phase",
		"revision_count_below_limit",
		"inner_revision_count_below_limit",
		"state_in_phase":
		return true
	default:
		return false
	}
}

func isSupportedWorkflowActionBuiltin(id string) bool {
	switch normalizeWorkflowBuiltinActionID(id) {
	case "increment_revision_count",
		"record_state_change",
		"update_state",
		"cancel_state_timers",
		"start_state_timers",
		"record_evidence",
		"create_flow_instance":
		return true
	default:
		return false
	}
}
