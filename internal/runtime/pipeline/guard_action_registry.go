package pipeline

import (
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

func normalizeWorkflowBuiltinGuardID(id string) string {
	return strings.TrimSpace(strings.ToLower(id))
}

func isSupportedWorkflowGuardBuiltin(id string) bool {
	switch normalizeWorkflowBuiltinGuardID(id) {
	case "has_entity_id",
		"has_human_decision",
		"not_in_terminal_state",
		"not_in_terminal_stage",
		"state_in_phase":
		return true
	default:
		return false
	}
}
