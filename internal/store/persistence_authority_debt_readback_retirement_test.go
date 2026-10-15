package store_test

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"
)

// Ruling6174425427: these five existing pairs only; the historical 49-pair
// ledger and independent 34-pair retirement inventory remain unchanged.
func debtReadbackRetirementPairs() [5][2]authorityDebtSite {
	before := "func=func(t *testing.T, endpoint string, db *database/sql.DB, run string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|results=()"
	after := "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, run string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|results=()"
	callers := [5]struct {
		file, declaration string
		count             int
	}{
		{"semantic_numeric_activity_test.go", "proveActivitySemanticResultExecutionParity", 3},
		{"semantic_numeric_gate_test.go", "TestWorkflowGateSemanticNumericOutcomeExecutionParity", 2},
		{"semantic_numeric_ingress_test.go", "TestServedSemanticNumericEventSubscription", 1},
		{"semantic_numeric_ingress_test.go", "TestServedSemanticNumericIdempotencyReplay", 3},
		{"semantic_numeric_schedule_test.go", "TestGenericScheduleSemanticPayloadExecutionParity", 2},
	}
	var pairs [5][2]authorityDebtSite
	for i, caller := range callers {
		old := authorityDebtSite{
			Kind: "raw-operation", File: "internal/serveapp/" + caller.file, Declaration: caller.declaration,
			Operation: "call:github.com/division-sh/swarm/internal/serveapp.semanticNumericOutput", Resolved: before,
			Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: caller.count,
		}
		current := old
		current.Resolved = after
		pairs[i] = [2]authorityDebtSite{old, current}
		matches := 0
		for _, original := range debtServedDebugSignatureTransitions() {
			if original == pairs[i] {
				matches++
			}
		}
		if matches != 1 {
			panic("readback retirement is not its exact historical signature pair")
		}
	}
	return pairs
}

func debtReadbackRetirementComparisons(actual, head, base, source map[string]authorityDebtSite, proofs ...debtDeliveryRetirementProof) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	pairs := debtReadbackRetirementPairs()
	needed := false
	for _, pair := range pairs {
		if debtDeliveryRetirementPairAbsent(pair, actual) && !debtDeliveryRetirementPairAbsent(pair, head, base, source) {
			needed = true
		}
	}
	if !needed || len(proofs) == 0 {
		return head, base, source, nil
	}
	if len(proofs) != 1 || proofs[0].Root == "" || !debtSitesEqual(actual, proofs[0].Actual) {
		return head, base, source, []string{"readback retirement lacks exact actual census proof"}
	}
	if failures := authorityDebtSubset(actual, head, "readback actual -> retained ceiling"); len(failures) != 0 {
		return head, base, source, failures
	}
	if _, err := debtReadbackLiveSources(proofs[0].Root); err != nil {
		return head, base, source, []string{fmt.Sprintf("readback retirement changed authenticated native endpoints: %v", err)}
	}
	for _, pair := range pairs {
		if err := debtDeliveryRetirementState(pair, actual, head, base, source); err != nil {
			return head, base, source, []string{err.Error()}
		}
		if base[pair[0].identity()] == pair[0] {
			base = debtForwardQuiescenceUncertainty(base, pair[0], pair[1])
			source = debtForwardQuiescenceUncertainty(source, pair[0], pair[1])
		}
	}
	return head, base, source, nil
}

func debtReadbackRetirementControlSets() (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite) {
	old, retained, empty := debtControlSet(), debtControlSet(), debtControlSet()
	for _, pair := range debtReadbackRetirementPairs() {
		old[pair[0].identity()], retained[pair[1].identity()] = pair[0], pair[1]
	}
	return old, retained, empty
}

func TestPersistenceAuthorityDebtReadbackRetirementPreservesHistoricalInventories(t *testing.T) {
	occurrences := 0
	for _, pair := range debtReadbackRetirementPairs() {
		occurrences += pair[0].Multiplicity
	}
	if occurrences != 11 {
		t.Fatal("readback exact five-pair multiplicity changed", occurrences)
	}
	served, retired := 0, 0
	for _, pair := range debtServedDebugSignatureTransitions() {
		served += pair[0].Multiplicity
	}
	for _, pair := range debtServedObservationRetirementPairs() {
		retired += pair[0].Multiplicity
	}
	if served != 71 || retired != 44 {
		t.Fatal("pre-existing inventories changed", served, retired)
	}
}

func TestPersistenceAuthorityDebtReadbackRetirementIsExactAndRejectsResurrection(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	old, retained, empty := debtReadbackRetirementControlSets()
	proof := debtDeliveryRetirementProof{Root: root, Actual: empty}
	for _, ceiling := range []map[string]authorityDebtSite{retained, empty} {
		if failures := authorityDebtRatchet(empty, ceiling, old, old, proof); len(failures) != 0 {
			t.Fatal("exact retained/lowered readback retirement refused:", failures)
		}
	}
	if failures := authorityDebtRatchet(empty, empty, empty, empty, proof); len(failures) != 0 {
		t.Fatal("landed absence invented a donor:", failures)
	}
	for _, pair := range debtReadbackRetirementPairs() {
		assertDebtReadbackRetirementStateRefusals(t, root, pair, old, retained, empty)
	}
	for _, invalid := range [][]debtDeliveryRetirementProof{nil, {proof, proof}, {{Root: "", Actual: empty}}, {{Root: root, Actual: retained}}, {{Root: t.TempDir(), Actual: empty}}} {
		if failures := authorityDebtRatchet(empty, retained, old, old, invalid...); len(failures) == 0 {
			t.Fatal("readback retirement accepted missing/duplicate/foreign/fabricated proof")
		}
	}
	added := maps.Clone(empty)
	escape := debtControlSite("call:Exec", 1)
	added[escape.identity()] = escape
	if failures := authorityDebtRatchet(added, added, old, old, debtDeliveryRetirementProof{Root: root, Actual: added}); len(failures) == 0 {
		t.Fatal("readback retirement let unrelated deletion fund an escape")
	}
}

func assertDebtReadbackRetirementStateRefusals(t *testing.T, root string, pair [2]authorityDebtSite, old, retained, empty map[string]authorityDebtSite) {
	t.Helper()
	for slot := 0; slot < 4; slot++ {
		for _, field := range []string{"resurrection", "partial", "missing", "duplicate", "multiplicity", "owner", "resolved", "foreign"} {
			if field == "missing" && slot < 2 {
				continue
			}
			states := []map[string]authorityDebtSite{maps.Clone(empty), maps.Clone(retained), maps.Clone(old), maps.Clone(old)}
			mutateServedObservationRetirementState(states[slot], pair, slot, field)
			proof := debtDeliveryRetirementProof{Root: root, Actual: states[0]}
			if failures := authorityDebtRatchet(states[0], states[1], states[2], states[3], proof); len(failures) == 0 {
				t.Fatalf("readback retirement accepted slot=%d field=%s declaration=%s", slot, field, pair[0].Declaration)
			}
		}
	}
}

func TestPersistenceAuthorityDebtReadbackRetirementRequiresFreshCompleteNativeSources(t *testing.T) {
	root, copy := persistenceAuthorityRepoRoot(t), t.TempDir()
	debtReadbackCopySources(t, root, copy)
	old, retained, empty := debtReadbackRetirementControlSets()
	proof := debtDeliveryRetirementProof{Root: copy, Actual: empty}
	check := func() []string {
		_, _, _, failures := debtReadbackRetirementComparisons(empty, retained, old, old, proof)
		return failures
	}
	if failures := check(); len(failures) != 0 {
		t.Fatal("exact native source refused:", failures)
	}
	pins, err := debtReadbackLiveSources(copy)
	if err != nil {
		t.Fatal(err)
	}
	debtSourceBlobCopyDeclaration(t, copy, pins[0].File, pins[0].Function, pins[0].Before)
	if len(check()) == 0 {
		t.Fatal("warm retirement cache hid a stale actual sibling")
	}
	debtReadbackCopySources(t, root, copy)
	if err := os.Remove(filepath.Join(copy, pins[0].File)); err != nil {
		t.Fatal(err)
	}
	if len(check()) == 0 {
		t.Fatal("warm retirement cache hid a missing actual sibling")
	}
}
