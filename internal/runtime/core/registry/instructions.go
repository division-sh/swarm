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
