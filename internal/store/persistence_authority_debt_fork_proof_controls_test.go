package store_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Excluded from the policy fingerprint to avoid a self-referential digest.
const debtForkProofCollectorTo = "3758daf3681922d019a670f17b7b00f28a667e7e521059009bf2a6a75bb22b0e"

func debtForkProofStates() (map[string]authorityDebtSite, map[string]authorityDebtSite) {
	old, current := debtControlSet(), debtControlSet()
	for _, site := range debtForkProofRows() {
		old[site.identity()] = site
		renamed := debtForkProofRenamed(site)
		current[renamed.identity()] = renamed
	}
	return old, current
}

func TestPersistenceAuthorityDebtForkProofTransferIsExact(t *testing.T) {
	old, current := debtForkProofStates()
	if len(old) != 9 || len(current) != 9 || authorityDebtCount(old) != 21 || authorityDebtCount(current) != 21 {
		t.Fatal("rename must preserve nine identities and all 21 occurrences")
	}
	var pinned strings.Builder
	for _, site := range debtForkProofRows() {
		fmt.Fprintf(&pinned, "%s\t%d\n", site.identity(), site.Multiplicity)
	}
	if debtBaselineChecksum([]byte(pinned.String())) != "b579ae1840469479148d49737f1c06c47241f730ba06be8da730f4b59ffda94d" {
		t.Fatal("reviewed nine-row donor changed")
	}
	for _, test := range []struct {
		name   string
		states [4]map[string]authorityDebtSite
		accept bool
	}{
		{"unchanged", [4]map[string]authorityDebtSite{old, old, old, old}, true},
		{"forward-unrefreshed", [4]map[string]authorityDebtSite{current, old, old, old}, true},
		{"forward-refreshed", [4]map[string]authorityDebtSite{current, current, old, old}, true},
		{"landed-inert", [4]map[string]authorityDebtSite{current, current, current, current}, true},
		{"reverse", [4]map[string]authorityDebtSite{old, old, current, current}, false},
		{"resurrection", [4]map[string]authorityDebtSite{current, current, debtControlSet(), debtControlSet()}, false},
		{"foreign-source", [4]map[string]authorityDebtSite{current, current, old, debtControlSet()}, false},
		{"later-removal", [4]map[string]authorityDebtSite{debtControlSet(), debtControlSet(), current, current}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			failures := authorityDebtRatchet(test.states[0], test.states[1], test.states[2], test.states[3])
			if (len(failures) == 0) != test.accept {
				t.Fatalf("accepted=%v failures=%v", test.accept, failures)
			}
		})
	}
	for slot := range 4 {
		t.Run([]string{"actual", "head", "trusted", "source"}[slot], func(t *testing.T) {
			states := [4]map[string]authorityDebtSite{current, current, old, old}
			states[slot] = maps.Clone(states[slot])
			for key, site := range old {
				states[slot][key] = site
			}
			for key, site := range current {
				states[slot][key] = site
			}
			if len(authorityDebtRatchet(states[0], states[1], states[2], states[3])) == 0 {
				t.Fatal("simultaneous old/new debt admitted")
			}
		})
	}
	for _, site := range debtForkProofRows() {
		debtForkProofHostility(t, site, old, current)
	}
	t.Run("unrelated-addition-despite-lower-total", func(t *testing.T) {
		trusted := maps.Clone(old)
		removed := debtControlSite("call:QueryRow", 3)
		trusted[removed.identity()] = removed
		actual := maps.Clone(current)
		added := debtControlSite("call:Exec", 1)
		actual[added.identity()] = added
		if authorityDebtCount(actual) >= authorityDebtCount(trusted) || len(authorityDebtRatchet(actual, actual, trusted, trusted)) == 0 {
			t.Fatal("deletion paid for unrelated new authority")
		}
	})
}

func debtForkProofHostility(t *testing.T, original authorityDebtSite, old, current map[string]authorityDebtSite) {
	t.Helper()
	for _, edit := range []struct {
		name   string
		change func(*authorityDebtSite)
	}{
		{"file", func(s *authorityDebtSite) { s.File += "foreign" }},
		{"kind", func(s *authorityDebtSite) { s.Kind += "foreign" }},
		{"operation", func(s *authorityDebtSite) { s.Operation += "foreign" }},
		{"resolved", func(s *authorityDebtSite) { s.Resolved += "foreign" }},
		{"family", func(s *authorityDebtSite) { s.Family += "foreign" }},
		{"owner", func(s *authorityDebtSite) { s.Replacement += "foreign" }},
		{"multiplicity", func(s *authorityDebtSite) { s.Multiplicity++ }},
		{"missing", func(s *authorityDebtSite) { s.Multiplicity = 0 }},
	} {
		t.Run(original.Operation+"/"+edit.name, func(t *testing.T) {
			changed := debtForkProofRenamed(original)
			actual := maps.Clone(current)
			delete(actual, changed.identity())
			edit.change(&changed)
			actual[changed.identity()] = changed
			if len(authorityDebtRatchet(actual, actual, old, old)) == 0 {
				t.Fatal("non-declaration edit or dropped occurrence admitted")
			}
		})
	}
	for slot := range 2 {
		t.Run(original.Operation+[]string{"/missing-trusted", "/missing-source"}[slot], func(t *testing.T) {
			states := [2]map[string]authorityDebtSite{maps.Clone(old), maps.Clone(old)}
			delete(states[slot], original.identity())
			if len(authorityDebtRatchet(current, current, states[0], states[1])) == 0 {
				t.Fatal("missing exact donor admitted")
			}
		})
	}
}

func TestPersistenceAuthorityDebtForkProofCollectorIsPinned(t *testing.T) {
	root := debtCatchUpHistoricalArchive(t, debtCatchUpMaster)
	digest, err := debtCollectorDigest(root)
	if err != nil || (digest != debtForkProofCollectorTo && digest != debtProcessLeaseCollectorTo && digest != debtProcessLeaseFreshnessCollectorTo) {
		t.Fatalf("fork/process/freshness collector=%s/%s/%s actual=%s error=%v", debtForkProofCollectorTo, debtProcessLeaseCollectorTo, debtProcessLeaseFreshnessCollectorTo, digest, err)
	}
	// The original fork-only transition is immutable. Test its exact historical
	// destination independently of the later process-owner policy composition.
	destination := debtForkProofCollectorTo
	old, current := debtForkProofStates()
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtForkProofCollectorFrom, Sites: old}
	for _, test := range []struct {
		name, base, destination string
		sites                   map[string]authorityDebtSite
		accept                  bool
	}{
		{"unrefreshed", trusted.Collector, destination, old, true},
		{"forward", trusted.Collector, destination, current, true},
		{"foreign-base", strings.Repeat("b", 64), destination, current, false},
		{"foreign-destination", trusted.Collector, strings.Repeat("c", 64), current, false},
		{"lost-debt", trusted.Collector, destination, debtControlSet(), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			head := trusted
			head.Collector, head.Sites = test.destination, test.sites
			if err := debtValidateCollectorIdentity(test.base, test.destination, trusted, head, ""); (err == nil) != test.accept {
				t.Fatalf("accepted=%v error=%v", test.accept, err)
			}
		})
	}
}

func TestPersistenceAuthorityDebtForkProofRequiresConservedSource(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	if err := debtForkProofSourcePins(root); err != nil {
		t.Fatal(err)
	}
	if err := debtForkProofSourcePins(t.TempDir()); err == nil {
		t.Fatal("missing source/recipe pins admitted")
	}
	data, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/recipes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatal(err)
	}
	var selected map[string]json.RawMessage
	for _, row := range rows {
		if string(row["Function"]) == `"`+debtForkProofOldName+`"` && string(row["File"]) == `"`+debtForkProofFile+`"` {
			selected = row
		}
	}
	if selected == nil {
		t.Fatal("missing historical recipe")
	}
	source, err := os.ReadFile(filepath.Join(root, debtForkProofFile))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"File", "Function", "Family", "Before", "After", "Successor"} {
		t.Run(field, func(t *testing.T) {
			copy := t.TempDir()
			row := maps.Clone(selected)
			row[field] = json.RawMessage(`"foreign"`)
			data, _ := json.Marshal([]map[string]json.RawMessage{row})
			debtWriteModuleSource(t, copy, "tools/fixture-codemod/pipeline-observations/recipes.json", string(data))
			debtWriteModuleSource(t, copy, debtForkProofFile, string(source))
			if err := debtForkProofSourcePins(copy); err == nil {
				t.Fatal("foreign/missing recipe pin admitted")
			}
		})
	}
	t.Run("changed-source", func(t *testing.T) {
		copy := t.TempDir()
		data, _ := json.Marshal([]map[string]json.RawMessage{selected})
		debtWriteModuleSource(t, copy, "tools/fixture-codemod/pipeline-observations/recipes.json", string(data))
		debtWriteModuleSource(t, copy, debtForkProofFile, strings.Replace(string(source), "forks != 1", "forks != 0", 1))
		if err := debtForkProofSourcePins(copy); err == nil {
			t.Fatal("changed business proof admitted")
		}
	})
}
