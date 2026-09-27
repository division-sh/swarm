package channelnative

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/google/uuid"
)

func TestNativeInboxEntryContractUsesCompiledPlan(t *testing.T) {
	first, err := plangeneration.FromCanonicalValue(map[string]any{"connector": "a"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := plangeneration.FromCanonicalValue(map[string]any{"connector": "b"})
	if err != nil {
		t.Fatal(err)
	}
	a, err := EntryContractHash(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EntryContractHash(first)
	if err != nil || a != b {
		t.Fatalf("same compiled plan contract = %q, %v", b, err)
	}
	c, err := EntryContractHash(second)
	if err != nil || c == a {
		t.Fatalf("different compiled plan contract = %q, %v", c, err)
	}
}

func TestNativeInboxInstallOperationIsExactGeneration(t *testing.T) {
	setting := uuid.NewString()
	first, err := InstallOperationID(setting, 1)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := InstallOperationID(setting, 1)
	if err != nil || replay != first {
		t.Fatalf("same generation operation = %q, %v", replay, err)
	}
	next, err := InstallOperationID(setting, 2)
	if err != nil || next == first {
		t.Fatalf("next generation operation = %q, %v", next, err)
	}
	if _, err := InstallOperationID(setting, 0); err == nil {
		t.Fatal("zero generation accepted")
	}
	if _, err := InstallOperationID("not-uuid", 1); err == nil {
		t.Fatal("non-setting identity accepted")
	}
}
