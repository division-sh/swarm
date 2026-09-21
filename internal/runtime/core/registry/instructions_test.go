package registry

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestGuardFromContract_PreservesTypedKeyAndKind(t *testing.T) {
	entry := runtimecontracts.GuardActionEntry{
		ID:    "guard_a",
		Check: "payload.score > 5",
	}
	instruction := GuardFromContract(entry)

	if instruction.Key != identity.NormalizeGuardKey("guard_a") {
		t.Fatalf("guard key = %q", instruction.Key)
	}
	if instruction.Kind() != InstructionCEL {
		t.Fatalf("guard kind = %v", instruction.Kind())
	}
	if !instruction.Executable() {
		t.Fatalf("guard should be executable")
	}
}
