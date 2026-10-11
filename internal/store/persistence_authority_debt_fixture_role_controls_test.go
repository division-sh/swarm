package store_test

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The destination is excluded from the fingerprint to avoid self-reference.
const debtFixtureRoleCollectorTo = "3958381013996586ea697cd868abcacec29e9316706276f603f32523f74b5cb6"

func TestPersistenceAuthorityDebtFixtureRoleTransitionIsExactAndMetadataOnly(t *testing.T) {
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtFixtureRoleCollectorFrom, Sites: debtControlSet(debtControlSite("call:QueryRow", 2))}
	head := trusted
	head.Collector = debtFixtureRoleCollectorTo
	for _, test := range []struct {
		name, base, current string
		trusted, head       authorityDebtBaseline
		accept              bool
	}{
		{"exact", trusted.Collector, head.Collector, trusted, head, true},
		{"landed-inert", head.Collector, head.Collector, head, head, true},
		{"reverse", head.Collector, trusted.Collector, head, trusted, false},
		{"foreign-base", strings.Repeat("c", 64), head.Collector, trusted, head, false},
		{"foreign-destination", trusted.Collector, strings.Repeat("d", 64), trusted, head, false},
		{"unrotated-metadata", trusted.Collector, head.Collector, trusted, trusted, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := debtValidateCollectorIdentity(test.base, test.current, test.trusted, test.head, ""); (err == nil) != test.accept {
				t.Fatalf("accept=%v error=%v", test.accept, err)
			}
		})
	}
	for _, mutation := range []string{"bootstrap", "removed", "added", "multiplicity", "uncertainty"} {
		t.Run(mutation, func(t *testing.T) {
			changed := head
			changed.Sites = maps.Clone(head.Sites)
			switch mutation {
			case "bootstrap":
				changed.BootstrapSource = strings.Repeat("b", 40)
			case "removed":
				changed.Sites = debtControlSet()
			case "added":
				added := debtControlSite("call:Exec", 1)
				changed.Sites[added.identity()] = added
			case "multiplicity":
				changed.Sites = debtControlSet(debtControlSite("call:QueryRow", 3))
			case "uncertainty":
				changed.Sites = debtControlSet(authorityDebtSite{Kind: "unresolved-excluded-source", File: "internal/runtime/probe_windows.go", Declaration: "probe", Operation: "unknown", Resolved: "unknown", Family: "unclassified", Replacement: "classify", Multiplicity: 2})
			}
			if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, changed, ""); err == nil {
				t.Fatal("role extraction changed finding identity or count")
			}
		})
	}
	old, current := debtRunFixtureTransitionStates()
	before, after := debtQuiescenceUncertaintyTransition()
	old[before.identity()] = before
	current[after.identity()] = after
	chained := trusted
	chained.Collector, chained.Sites = debtQuiescenceCollectorFrom, old
	chainedHead := head
	chainedHead.Sites = current
	if err := debtValidateCollectorIdentity(chained.Collector, chainedHead.Collector, chained, chainedHead, ""); err != nil {
		t.Fatal("already-approved exact chain rejected:", err)
	}
	delete(chained.Sites, before.identity())
	if err := debtValidateCollectorIdentity(chained.Collector, chainedHead.Collector, chained, chainedHead, ""); err == nil {
		t.Fatal("missing historical uncertainty accepted through composition")
	}
}

func TestPersistenceAuthorityDebtFixtureRoleFingerprintIsRequired(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	digest, err := debtCollectorDigest(root)
	if err != nil || digest != debtFixtureRoleCollectorTo {
		t.Fatalf("source-pinned role destination=%s actual=%s error=%v", debtFixtureRoleCollectorTo, digest, err)
	}
	copy := t.TempDir()
	for _, path := range []string{
		"internal/store/persistence_authority_debt_census_test.go",
		"internal/store/persistence_authority_debt_test.go",
		"internal/store/persistence_authority_debt_cache_test.go",
		"internal/store/persistence_authority_debt_sidecar_test.go",
		"internal/checkoutsource/fixture_role.go",
	} {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(data))
	}
	const role = "internal/checkoutsource/fixture_role.go"
	data, err := os.ReadFile(filepath.Join(copy, role))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, role, strings.Replace(string(data), "return internal &&", "return !internal &&", 1))
	if changed, err := debtCollectorDigest(copy); err != nil || changed == digest {
		t.Fatalf("shared role policy was not fingerprinted: %s %v", changed, err)
	}
	if err := os.Remove(filepath.Join(copy, role)); err != nil {
		t.Fatal(err)
	}
	if _, err := debtCollectorDigest(copy); err == nil {
		t.Fatal("current collector accepted missing role owner")
	}
	debtWriteModuleSource(t, copy, role, "package checkoutsource;func broken(")
	if _, err := debtCollectorDigest(copy); err == nil {
		t.Fatal("current collector accepted corrupt role owner")
	}
}
