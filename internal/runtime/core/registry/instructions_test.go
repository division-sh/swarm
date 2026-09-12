package registry

import (
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestActionRegistryHasNoImplicitEmissionTransport(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeOf(ActionInstruction{}), reflect.TypeOf(runtimecontracts.GuardActionEntry{})} {
		if _, exists := typ.FieldByName("Emits"); exists {
			t.Fatalf("%s retains implicit action emission transport", typ)
		}
	}
	if ActionFromContract(runtimecontracts.GuardActionEntry{ID: "unimplemented"}).Executable() {
		t.Fatal("an action name is not executable implementation")
	}
}

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

func TestActionFromContract_PreservesBuiltinAndKind(t *testing.T) {
	entry := runtimecontracts.GuardActionEntry{
		ID:              "notify",
		PlatformBuiltin: "record_evidence",
	}
	instruction := ActionFromContract(entry)

	if instruction.Key != identity.NormalizeActionKey("notify") {
		t.Fatalf("action key = %q", instruction.Key)
	}
	if instruction.Kind() != InstructionBuiltin {
		t.Fatalf("action kind = %v", instruction.Kind())
	}
	if !instruction.Executable() {
		t.Fatalf("action should be executable")
	}
}
