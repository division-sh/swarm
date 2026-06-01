package registry

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

type InstructionKind uint8

const (
	InstructionUnknown InstructionKind = iota
	InstructionCEL
	InstructionBuiltin
)

type GuardInstruction struct {
	Key         identity.GuardKey
	Category    string
	Description string
	Check       string
	PolicyRef   string
	Builtin     string
	Effect      string
}

func GuardFromContract(entry runtimecontracts.GuardActionEntry) GuardInstruction {
	return GuardInstruction{
		Key:         identity.NormalizeGuardKey(entry.ID),
		Category:    strings.TrimSpace(entry.Category),
		Description: strings.TrimSpace(entry.Description),
		Check:       strings.TrimSpace(entry.Check),
		PolicyRef:   strings.TrimSpace(entry.PolicyRef),
		Builtin:     strings.TrimSpace(entry.PlatformBuiltin),
		Effect:      strings.TrimSpace(entry.Effect),
	}
}

func (g GuardInstruction) Kind() InstructionKind {
	switch {
	case g.Builtin != "":
		return InstructionBuiltin
	case g.Check != "":
		return InstructionCEL
	default:
		return InstructionUnknown
	}
}

func (g GuardInstruction) Executable() bool {
	return g.Kind() != InstructionUnknown
}

type ActionInstruction struct {
	Key         identity.ActionKey
	Category    string
	Description string
	PolicyRef   string
	Builtin     string
	Effect      string
	Emits       string
}

func ActionFromContract(entry runtimecontracts.GuardActionEntry) ActionInstruction {
	return ActionInstruction{
		Key:         identity.NormalizeActionKey(entry.ID),
		Category:    strings.TrimSpace(entry.Category),
		Description: strings.TrimSpace(entry.Description),
		PolicyRef:   strings.TrimSpace(entry.PolicyRef),
		Builtin:     strings.TrimSpace(entry.PlatformBuiltin),
		Effect:      strings.TrimSpace(entry.Effect),
		Emits:       strings.TrimSpace(entry.Emits),
	}
}

func (a ActionInstruction) Kind() InstructionKind {
	switch {
	case a.Builtin != "":
		return InstructionBuiltin
	case a.Emits != "" || a.Effect != "":
		return InstructionCEL
	default:
		return InstructionUnknown
	}
}

func (a ActionInstruction) Executable() bool {
	return a.Kind() != InstructionUnknown
}
