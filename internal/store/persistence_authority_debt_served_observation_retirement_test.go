package store_test

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

//go:embed testdata/persistence_authority_served_observation_retirements.json
var debtServedObservationRetirementBytes []byte

// Ruling6162561533 fixes these complete pairs, not helper-wide permissions.
func debtServedObservationRetirementPairs() [][2]authorityDebtSite {
	if debtBaselineChecksum(debtServedObservationRetirementBytes) != "a27dc0490c99898a11f7fdcb905271c59a4cd9b76b9368f53a790db30ba475c6" {
		panic("exact served observation retirement inventory changed")
	}
	var rows []struct {
		Ledger, File, Enclosing, Operation, Kind, Family, Owner, Before, After string
		Multiplicity                                                           int
	}
	if err := json.Unmarshal(debtServedObservationRetirementBytes, &rows); err != nil || len(rows) != 34 {
		panic("served observation retirement inventory is incomplete")
	}
	served, writer := debtServedDebugSignatureTransitions(), debtWriterStageSignatureTransitions()
	owners := map[string][][2]authorityDebtSite{
		"served-debug": served[:],
		"writer-stage": writer[:],
	}
	counts, occurrences := map[string]int{}, map[string]int{}
	seen := map[string]bool{}
	var pairs [][2]authorityDebtSite
	for _, row := range rows {
		old := authorityDebtSite{row.Kind, row.File, row.Enclosing, row.Operation, row.Before, row.Family, row.Owner, row.Multiplicity}
		current := old
		current.Resolved = row.After
		pair := [2]authorityDebtSite{old, current}
		matches := 0
		for _, original := range owners[row.Ledger] {
			if original == pair {
				matches++
			}
		}
		if matches != 1 || seen[old.identity()] {
			panic("served observation retirement exceeded its historical owner")
		}
		seen[old.identity()] = true
		counts[row.Ledger]++
		occurrences[row.Ledger] += old.Multiplicity
		pairs = append(pairs, pair)
	}
	if counts["served-debug"] != 31 || occurrences["served-debug"] != 41 || counts["writer-stage"] != 3 || occurrences["writer-stage"] != 3 {
		panic("served observation historical owner partition changed")
	}
	return pairs
}

func debtServedObservationRetirementComparisons(actual, head, base, source map[string]authorityDebtSite, proofs ...debtDeliveryRetirementProof) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	pairs := debtServedObservationRetirementPairs()
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
		return head, base, source, []string{"served observation retirement lacks exact actual census proof"}
	}
	if failures := authorityDebtSubset(actual, head, "served retirement actual -> retained ceiling"); len(failures) != 0 {
		return head, base, source, failures
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
	if err := debtServedObservationRetirementSourcePins(proofs[0].Root); err != nil {
		return head, base, source, []string{err.Error()}
	}
	return head, base, source, nil
}

func debtServedObservationRetirementSourcePins(root string) error {
	raw, err := os.ReadFile(filepath.Join(root, "internal/store/testdata/persistence_authority_served_observation_retirements.json"))
	if err != nil || !bytes.Equal(raw, debtServedObservationRetirementBytes) {
		return fmt.Errorf("served observation exact retirement declarations changed: %v", err)
	}
	stages, err := debtServedObservationManifests(root)
	if err != nil {
		return err
	}
	keys := map[[2]string]bool{}
	for _, stage := range stages {
		for _, body := range stage {
			keys[[2]string{body.File, body.Function}] = true
		}
	}
	if len(keys) != 48 {
		return fmt.Errorf("served observation complete retirement source inventory changed")
	}
	files := map[string]map[string]string{}
	for key := range keys {
		pin, err := debtServedObservationComposedPin(stages, key)
		if err != nil {
			return err
		}
		if files[key[0]] == nil {
			files[key[0]], err = debtServedDebugDeclarations(filepath.Join(root, key[0]))
			if err != nil {
				return err
			}
		}
		current, err := debtServedSetupSourcePrevious(root, key[0], key[1], files[key[0]][key[1]])
		if err != nil || current != pin.After {
			return fmt.Errorf("served observation retirement changed actual final body: %s: %v", key[1], err)
		}
	}
	_, err = debtServedObservationLiveSources(root)
	return err
}

func debtServedObservationWriterRetirementSourcePrevious(root, function, current string) (string, error) {
	switch function {
	case "TestRunServeRuntimeEventPublishRunIDFollowUpServedPathDefaultSQLite", "runServedDynamicAutoEmitSQLiteProof", "runServedDynamicAutoEmitPostgresProof":
	default:
		return current, nil
	}
	if err := debtServedObservationRetirementSourcePins(root); err != nil {
		return "", err
	}
	stages, err := debtServedObservationManifests(root)
	if err != nil {
		return "", err
	}
	pin, err := debtServedObservationComposedPin(stages, [2]string{"internal/serveapp/main_runtime_test.go", function})
	if err != nil || current != pin.After {
		return "", fmt.Errorf("writer retirement supplied source differs from live endpoint: %s: %v", function, err)
	}
	return pin.Before, nil
}

func TestPersistenceAuthorityDebtServedObservationRetirementIsFiniteAndRejectsResurrection(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	pairs := debtServedObservationRetirementPairs()
	old, retained := debtControlSet(), debtControlSet()
	for _, pair := range pairs {
		old[pair[0].identity()], retained[pair[1].identity()] = pair[0], pair[1]
	}
	empty := debtControlSet()
	proof := debtDeliveryRetirementProof{Root: root, Actual: empty}
	for _, ceiling := range []map[string]authorityDebtSite{retained, empty} {
		if failures := authorityDebtRatchet(empty, ceiling, old, old, proof); len(failures) != 0 {
			t.Fatal("exact retained/lowered retirement refused:", failures)
		}
	}
	if failures := authorityDebtRatchet(empty, empty, empty, empty, proof); len(failures) != 0 {
		t.Fatal("landed absence invented donors:", failures)
	}
	for _, pair := range pairs {
		for slot := 0; slot < 4; slot++ {
			for _, field := range []string{"resurrection", "partial", "missing", "duplicate", "multiplicity", "owner", "resolved", "foreign"} {
				states := []map[string]authorityDebtSite{maps.Clone(empty), maps.Clone(retained), maps.Clone(old), maps.Clone(old)}
				mutateServedObservationRetirementState(states[slot], pair, slot, field)
				if field == "missing" && slot < 2 {
					continue // Legitimately lowered ceilings are covered above.
				}
				bound := debtDeliveryRetirementProof{Root: root, Actual: states[0]}
				if failures := authorityDebtRatchet(states[0], states[1], states[2], states[3], bound); len(failures) == 0 {
					t.Fatalf("retirement accepted slot=%d field=%s declaration=%s", slot, field, pair[0].Declaration)
				}
			}
		}
	}
	for _, bad := range [][]debtDeliveryRetirementProof{nil, {proof, proof}, {{Root: root, Actual: retained}}, {{Root: t.TempDir(), Actual: empty}}} {
		if failures := authorityDebtRatchet(empty, retained, old, old, bad...); len(failures) == 0 {
			t.Fatal("retirement accepted missing/duplicate/foreign/fabricated actual proof")
		}
	}
}

func mutateServedObservationRetirementState(state map[string]authorityDebtSite, pair [2]authorityDebtSite, slot int, field string) {
	index := 0
	if slot < 2 {
		index = 1
	}
	site := pair[index]
	switch field {
	case "missing":
		delete(state, site.identity())
	case "duplicate":
		state[pair[0].identity()], state[pair[1].identity()] = pair[0], pair[1]
	case "multiplicity":
		site.Multiplicity++
		state[site.identity()] = site
	case "partial":
		site.Multiplicity = 1
		if pair[index].Multiplicity == 1 {
			site.Multiplicity = 2
		}
		state[site.identity()] = site
	case "owner", "resolved", "foreign":
		delete(state, site.identity())
		if field == "owner" {
			site.Replacement = "foreign owner"
		} else if field == "resolved" {
			site.Resolved = "foreign signature"
		} else {
			site.File = "foreign.go"
		}
		state[site.identity()] = site
	default:
		state[pair[index].identity()] = pair[index]
		if slot == 1 {
			state[pair[0].identity()] = pair[0]
		} else if slot > 1 {
			state[pair[1].identity()] = pair[1]
		}
	}
}

func TestPersistenceAuthorityDebtServedObservationRetirementPinsAll48LiveBodies(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	if err := debtServedObservationRetirementSourcePins(root); err != nil {
		t.Fatal(err)
	}
	pairs := debtServedObservationRetirementPairs()
	old, retained := debtControlSet(pairs[0][0]), debtControlSet(pairs[0][1])
	empty := debtControlSet()
	copy := t.TempDir()
	debtServedObservationCopySourceEvidence(t, root, copy)
	stages, err := debtServedObservationManifests(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range stages {
		for _, body := range stage {
			key := [2]string{body.File, body.Function}
			pin, err := debtServedObservationComposedPin(stages, key)
			if err != nil {
				t.Fatal(err)
			}
			current, err := debtServedSetupDeclarations(filepath.Join(root, pin.File))
			if err != nil {
				t.Fatal(err)
			}
			debtSourceBlobCopyDeclaration(t, copy, pin.File, pin.Function, current[pin.Function])
		}
	}
	const path = "internal/store/testdata/persistence_authority_served_observation_retirements.json"
	debtWriteModuleSource(t, copy, path, string(debtServedObservationRetirementBytes))
	proof := debtDeliveryRetirementProof{Root: copy, Actual: empty}
	if failures := authorityDebtRatchet(empty, retained, old, old, proof); len(failures) != 0 {
		t.Fatal("complete live-source comparison image refused:", failures)
	}
	pin := stages[0][0]
	raw, err := os.ReadFile(filepath.Join(copy, pin.File))
	if err != nil {
		t.Fatal(err)
	}
	decls, err := debtServedDebugDeclarations(filepath.Join(copy, pin.File))
	if err != nil {
		t.Fatal(err)
	}
	current := decls[pin.Function]
	mutant := strings.Replace(current, "{", "{foreignRetirement();", 1)
	debtWriteModuleSource(t, copy, pin.File, strings.Replace(string(raw), current, mutant, 1))
	if failures := authorityDebtRatchet(empty, retained, old, old, proof); len(failures) == 0 {
		t.Fatal("changed live source received retirement credit")
	}
}
