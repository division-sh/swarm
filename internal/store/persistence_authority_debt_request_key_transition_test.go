package store_test

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Excluded from the collector hash to avoid fingerprint self-reference.
const debtRequestKeyCollectorTo = "fd2d3745a56a6adaa06c3c434416d2cbb692e41f54bb0d259248e4931c2b59e8"

func TestPersistenceAuthorityDebtRequestKeyExactPairsRetainTwoOccurrences(t *testing.T) {
	for _, pair := range debtRequestKeySignatureTransitions() {
		old, current := pair[0], pair[1]
		before, after := debtControlSet(old), debtControlSet(current)
		for _, test := range []struct {
			name                       string
			actual, head, base, source map[string]authorityDebtSite
			accept                     bool
		}{
			{"unchanged", before, before, before, before, true},
			{"forward-unrefreshed", after, before, before, before, true},
			{"forward-refreshed", after, after, before, before, true},
			{"landed-inert", after, after, after, after, true},
			{"subsequent-deletion", debtControlSet(), debtControlSet(), after, after, true},
			{"reverse", before, before, after, after, false},
			{"resurrection", before, before, before, after, false},
			{"missing-head", after, debtControlSet(), before, before, false},
		} {
			t.Run(old.Declaration+"/"+test.name, func(t *testing.T) {
				failures := authorityDebtRatchet(test.actual, test.head, test.base, test.source)
				if (len(failures) == 0) != test.accept {
					t.Fatalf("accept=%v failures=%v", test.accept, failures)
				}
			})
		}
		for slot := range 4 {
			for _, mutation := range []string{"coexistence", "multiplicity", "kind", "file", "declaration", "operation", "resolved", "family", "owner"} {
				requestKeyDebtMutation(t, pair, slot, mutation)
			}
		}
		normalized, _, _, failures := debtRequestKeySignatureComparisons(after, before, before, before)
		if len(failures) != 0 || authorityDebtCount(normalized) != 1 || normalized[current.identity()] != current || normalized[old.identity()].Multiplicity != 0 {
			t.Fatal("retained raw call received deletion credit")
		}
	}
	pairs := debtRequestKeySignatureTransitions()
	before, after := debtControlSet(pairs[0][0], pairs[1][0]), debtControlSet(pairs[0][1], pairs[1][1])
	if authorityDebtCount(before) != 2 || authorityDebtCount(after) != 2 {
		t.Fatal("exact two-call transfer changed debt")
	}
	if len(authorityDebtRatchet(debtControlSet(pairs[0][1], debtControlSite("call:Exec", 1)), debtControlSet(pairs[0][1], debtControlSite("call:Exec", 1)), before, before)) == 0 {
		t.Fatal("one deleted escape paid for an unrelated addition")
	}
}

func requestKeyDebtMutation(t *testing.T, pair [2]authorityDebtSite, slot int, mutation string) {
	t.Helper()
	old, current := pair[0], pair[1]
	before, after := debtControlSet(old), debtControlSet(current)
	states := [4]map[string]authorityDebtSite{after, after, before, before}
	states[slot] = maps.Clone(states[slot])
	site := current
	if slot >= 2 {
		site = old
	}
	delete(states[slot], site.identity())
	if mutation == "coexistence" {
		states[slot][old.identity()], states[slot][current.identity()] = old, current
	} else {
		site = requestKeyChangedDebtSite(site, mutation)
		states[slot][site.identity()] = site
	}
	if failures := authorityDebtRatchet(states[0], states[1], states[2], states[3]); len(failures) == 0 {
		t.Fatalf("foreign/duplicate request-key transfer accepted: slot=%d mutation=%s", slot, mutation)
	}
}

func requestKeyChangedDebtSite(site authorityDebtSite, field string) authorityDebtSite {
	if field == "resolved" || field == "operation" {
		if field == "resolved" {
			site.Resolved += "foreign"
		} else {
			site.Operation += "foreign"
		}
		return site
	}
	return debtStageCompletionChangedSite(site, field)
}

func TestPersistenceAuthorityDebtRequestKeyCollectorTransitionKeepsExactDonorsAndDirection(t *testing.T) {
	pairs := debtRequestKeySignatureTransitions()
	old := debtControlSet(pairs[0][0], pairs[1][0], debtControlSite("call:QueryRow", 3))
	forward := debtControlSet(pairs[0][1], pairs[1][1], debtControlSite("call:QueryRow", 3))
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtReceiverReadbackCollectorTo, Sites: old}
	for _, mutation := range []string{"unrefreshed", "forward", "other-deletion", "dropped-donor", "duplicate", "coexistence", "unrelated-addition", "bootstrap", "foreign-base", "foreign-destination", "reverse"} {
		t.Run(mutation, func(t *testing.T) {
			head := trusted
			head.Collector, head.Sites = debtRequestKeyCollectorTo, maps.Clone(forward)
			base, collector := trusted.Collector, head.Collector
			switch mutation {
			case "unrefreshed":
				head.Sites = maps.Clone(old)
			case "other-deletion":
				delete(head.Sites, debtControlSite("call:QueryRow", 3).identity())
			case "dropped-donor":
				delete(head.Sites, pairs[1][1].identity())
			case "duplicate":
				site := pairs[0][1]
				site.Multiplicity = 2
				head.Sites[site.identity()] = site
			case "coexistence":
				head.Sites[pairs[0][0].identity()] = pairs[0][0]
			case "unrelated-addition":
				site := debtControlSite("call:Exec", 1)
				head.Sites[site.identity()] = site
			case "bootstrap":
				head.BootstrapSource = strings.Repeat("b", 40)
			case "foreign-base":
				base = strings.Repeat("b", 64)
			case "foreign-destination":
				collector, head.Collector = strings.Repeat("c", 64), strings.Repeat("c", 64)
			case "reverse":
				head.Collector = debtReceiverReadbackCollectorTo
			}
			accept := mutation == "unrefreshed" || mutation == "forward" || mutation == "other-deletion"
			if err := debtValidateCollectorIdentity(base, collector, trusted, head, ""); (err == nil) != accept {
				t.Fatalf("accept=%v error=%v", accept, err)
			}
		})
	}
}

func TestPersistenceAuthorityDebtRequestKeySourceAndCollectorPins(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	if err := debtRequestKeySourcePins(root); err != nil {
		t.Fatal(err)
	}
	if collector, err := debtCollectorDigest(root); err != nil || collector != debtActiveCollectorTo {
		t.Fatalf("request-key collector=%s expected=%s error=%v", collector, debtActiveCollectorTo, err)
	}
	copy := t.TempDir()
	paths := []string{"internal/serveapp/test_command_runtime_test.go", "tools/fixture-codemod/pipeline-observations/family145-request-key-preimages.json"}
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	bodies, err := debtRequestKeyBodies(root)
	if err != nil {
		t.Fatal(err)
	}
	var source strings.Builder
	source.WriteString("package proof\n")
	for _, body := range bodies {
		if body.File == paths[0] {
			source.WriteString(body.After + "\n")
		}
	}
	debtWriteModuleSource(t, copy, paths[0], source.String())
	if err := debtRequestKeySourcePins(copy); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(copy, paths[0])
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"captureSelectedRuntimePersistence", "servedEventPublishAPIIdempotencyCount", "idempotencyKey", "requirePublicInputRollbackNoResidue(t, db, backend, eventID, runID)", "db.QueryRowContext", "got != 0"} {
		mutant := strings.Replace(string(raw), cut, "foreignRequestKeyCut", 1)
		if mutant == string(raw) {
			t.Fatal("missing hostile cut:", cut)
		}
		if err := os.WriteFile(path, []byte(mutant), 0644); err != nil {
			t.Fatal(err)
		}
		if err := debtRequestKeySourcePins(copy); err == nil {
			t.Fatal("lost exact capture/observation/raw-cut accepted:", cut)
		}
	}
}
