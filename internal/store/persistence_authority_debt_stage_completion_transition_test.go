package store_test

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Excluded from the collector hash to avoid fingerprint self-reference.
const debtStageCompletionCollectorTo = "fc9b79436920f7984a42bafc04a40dd463568bdc056891d38dc1149e926af0b9"

func TestPersistenceAuthorityDebtStageCompletionRelocationIsExact(t *testing.T) {
	old, current := debtStageCompletionTransition()
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
		t.Run(test.name, func(t *testing.T) {
			failures := authorityDebtRatchet(test.actual, test.head, test.base, test.source)
			if (len(failures) == 0) != test.accept {
				t.Fatalf("accept=%v failures=%v", test.accept, failures)
			}
		})
	}
	debtStageCompletionOccurrenceControls(t, old, current)
	trusted := debtControlSet(old, debtControlSite("call:QueryRow", 3))
	added := debtControlSet(current, debtControlSite("call:Exec", 1))
	if authorityDebtCount(added) >= authorityDebtCount(trusted) || len(authorityDebtRatchet(added, added, trusted, trusted)) == 0 {
		t.Fatal("lower total paid for unrelated authority")
	}
	pgOld, pgCurrent := old, current
	pgOld.Operation, pgOld.Resolved = strings.ReplaceAll(old.Operation, "SQLiteRuntimeStore", "PostgresStore"), strings.ReplaceAll(old.Resolved, "SQLiteRuntimeStore", "PostgresStore")
	pgCurrent.Operation, pgCurrent.Resolved = pgOld.Operation, pgOld.Resolved
	pgOnly := debtControlSet(pgOld, pgCurrent)
	if len(authorityDebtRatchet(debtControlSet(current, pgCurrent), debtControlSet(current, pgCurrent), pgOnly, pgOnly)) == 0 {
		t.Fatal("a PostgreSQL donor paid for the SQLite relocation")
	}
	if authorityDebtCount(before) != 1 || authorityDebtCount(after) != 1 || before[old.identity()] != old {
		t.Fatal("relocation changed debt count or mutated the trusted predecessor")
	}
	normalized, _, _, _ := debtStageCompletionComparisons(after, before, before, before)
	if authorityDebtCount(normalized) != 1 || normalized[current.identity()] != current || normalized[old.identity()].Multiplicity != 0 {
		t.Fatal("refresh would credit the retained construction as deleted")
	}
}

func debtStageCompletionOccurrenceControls(t *testing.T, old, current authorityDebtSite) {
	t.Helper()
	before, after := debtControlSet(old), debtControlSet(current)
	for slot := range 4 {
		t.Run([]string{"actual", "head", "baseline", "source"}[slot], func(t *testing.T) {
			for _, mutation := range []string{"coexistence", "multiplicity"} {
				t.Run(mutation, func(t *testing.T) {
					debtStageCompletionOccurrenceMutation(t, [4]map[string]authorityDebtSite{after, after, before, before}, slot, mutation)
				})
			}
		})
	}
	for _, field := range []string{"kind", "file", "declaration", "operation", "resolved", "family", "owner"} {
		for slot := range 4 {
			t.Run(field+[]string{"-actual", "-head", "-baseline", "-source"}[slot], func(t *testing.T) {
				debtStageCompletionOccurrenceMutation(t, [4]map[string]authorityDebtSite{after, after, before, before}, slot, field)
			})
		}
	}
	for slot := range 2 {
		states := [2]map[string]authorityDebtSite{before, before}
		states[slot] = debtControlSet()
		if failures := authorityDebtRatchet(after, after, states[0], states[1]); len(failures) == 0 {
			t.Fatal("missing exact predecessor admitted")
		}
	}
}

func debtStageCompletionOccurrenceMutation(t *testing.T, states [4]map[string]authorityDebtSite, slot int, mutation string) {
	t.Helper()
	old, current := debtStageCompletionTransition()
	states[slot] = maps.Clone(states[slot])
	site := current
	if slot >= 2 {
		site = old
	}
	delete(states[slot], site.identity())
	if mutation == "coexistence" {
		states[slot][old.identity()], states[slot][current.identity()] = old, current
	} else {
		site = debtStageCompletionChangedSite(site, mutation)
		states[slot][site.identity()] = site
	}
	if failures := authorityDebtRatchet(states[0], states[1], states[2], states[3]); len(failures) == 0 {
		t.Fatal("foreign, duplicate or coexisting relocation admitted")
	}
}

func debtStageCompletionChangedSite(site authorityDebtSite, field string) authorityDebtSite {
	switch field {
	case "kind":
		site.Kind += "Foreign"
	case "file":
		site.File = "internal/apiv1/other_test.go"
	case "declaration":
		site.Declaration += "Other"
	case "operation":
		site.Operation = strings.ReplaceAll(site.Operation, "SQLiteRuntimeStore", "PostgresStore")
	case "resolved":
		site.Resolved = strings.ReplaceAll(site.Resolved, "SQLiteRuntimeStore", "PostgresStore")
	case "family":
		site.Family += "Other"
	case "owner":
		site.Replacement += "Other"
	case "multiplicity":
		site.Multiplicity = 2
	}
	return site
}

func TestPersistenceAuthorityDebtStageCompletionCollectorTransitionIsPinned(t *testing.T) {
	old, current := debtStageCompletionTransition()
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtFixtureRoleCollectorTo, Sites: debtControlSet(old, debtControlSite("call:QueryRow", 3))}
	for _, mutation := range []string{"unrefreshed", "forward", "other-deletion", "dropped-debt", "duplicate", "coexistence", "unrelated-addition", "bootstrap", "foreign-base", "foreign-destination", "old-metadata"} {
		t.Run(mutation, func(t *testing.T) {
			head := trusted
			head.Collector = debtStageCompletionCollectorTo
			head.Sites = debtControlSet(current, debtControlSite("call:QueryRow", 3))
			base, collector := trusted.Collector, head.Collector
			switch mutation {
			case "unrefreshed":
				// Rotate only fingerprint metadata before the supported downward scan.
				head.Sites = maps.Clone(trusted.Sites)
			case "other-deletion":
				head.Sites = debtControlSet(current)
			case "dropped-debt":
				head.Sites = debtControlSet()
			case "duplicate":
				site := current
				site.Multiplicity = 2
				head.Sites[site.identity()] = site
			case "coexistence":
				head.Sites[old.identity()] = old
			case "unrelated-addition":
				head.Sites = debtControlSet(current, debtControlSite("call:Exec", 1))
			case "bootstrap":
				head.BootstrapSource = strings.Repeat("b", 40)
			case "foreign-base":
				base = strings.Repeat("b", 64)
			case "foreign-destination":
				collector, head.Collector = strings.Repeat("c", 64), strings.Repeat("c", 64)
			case "old-metadata":
				head.Collector = trusted.Collector
			}
			accept := mutation == "unrefreshed" || mutation == "forward" || mutation == "other-deletion"
			if err := debtValidateCollectorIdentity(base, collector, trusted, head, ""); (err == nil) != accept {
				t.Fatalf("accept=%v error=%v", accept, err)
			}
		})
	}
	landed := trusted
	landed.Collector, landed.Sites = debtStageCompletionCollectorTo, debtControlSet(current)
	if err := debtValidateCollectorIdentity(landed.Collector, landed.Collector, landed, landed, ""); err != nil {
		t.Fatal("landed metadata rejected:", err)
	}
	if err := debtValidateCollectorIdentity(landed.Collector, trusted.Collector, landed, trusted, ""); err == nil {
		t.Fatal("reverse policy transition admitted")
	}
}

func TestPersistenceAuthorityDebtStageCompletionSourceAndFactoryPins(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	if err := debtStageCompletionSourcePins(root, true); err != nil {
		t.Fatal(err)
	}
	collector, err := debtCollectorDigest(root)
	if err != nil || collector != debtActiveCollectorTo {
		t.Fatalf("source-pinned collector=%s expected=%s error=%v", collector, debtActiveCollectorTo, err)
	}
	prior := t.TempDir()
	for _, path := range []string{
		"internal/store/persistence_authority_debt_census_test.go", "internal/store/persistence_authority_debt_test.go",
		"internal/store/persistence_authority_debt_cache_test.go", "internal/store/persistence_authority_debt_sidecar_test.go",
		"internal/checkoutsource/fixture_role.go",
	} {
		data, err := debtGit(root, "show", "2b2079761d163370c6d7f7c7c01f834878f67e36:"+path)
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, prior, path, string(data))
	}
	if collector, err := debtCollectorDigest(prior); err != nil || collector != debtFixtureRoleCollectorTo {
		t.Fatalf("immutable predecessor policy changed: %s %v", collector, err)
	}
	paths := []string{"internal/apiv1/operator_stage_completion_journey_test.go", "internal/store/storetest/runtime_reopen.go"}
	for _, direction := range []bool{false, true} {
		copy := t.TempDir()
		for _, path := range paths {
			data, err := os.ReadFile(filepath.Join(root, path))
			if !direction {
				data, err = debtGit(root, "show", "2b2079761d163370c6d7f7c7c01f834878f67e36:"+path)
			}
			if err != nil {
				t.Fatal(err)
			}
			debtWriteModuleSource(t, copy, path, string(data))
		}
		if err := debtStageCompletionSourcePins(copy, direction); err != nil {
			t.Fatal("reviewed source rejected:", err)
		}
		for _, path := range paths {
			original, err := os.ReadFile(filepath.Join(copy, path))
			if err != nil {
				t.Fatal(err)
			}
			for _, mutation := range []string{"body", "duplicate", "missing", "factory-provenance"} {
				debtWriteModuleSource(t, copy, path, string(original))
				switch mutation {
				case "body":
					debtWriteModuleSource(t, copy, path, strings.ReplaceAll(string(original), "t.Helper()", "t.Helper(); panic(\"drift\")"))
				case "duplicate":
					debtWriteModuleSource(t, copy, path, string(original)+string(original))
				case "missing":
					if err := os.Remove(filepath.Join(copy, path)); err != nil {
						t.Fatal(err)
					}
				case "factory-provenance":
					importPath := "github.com/division-sh/swarm/internal/store/storetest"
					if path == paths[1] {
						importPath = "github.com/division-sh/swarm/internal/store"
					}
					debtWriteModuleSource(t, copy, path, strings.ReplaceAll(string(original), importPath+"\"", importPath+"/foreign\""))
				}
				if err := debtStageCompletionSourcePins(copy, direction); err == nil {
					t.Fatalf("source drift admitted: %s %s", path, mutation)
				}
			}
			debtWriteModuleSource(t, copy, path, string(original))
		}
		if err := debtStageCompletionSourcePins(copy, !direction); err == nil {
			t.Fatal("source direction ignored")
		}
	}
}
