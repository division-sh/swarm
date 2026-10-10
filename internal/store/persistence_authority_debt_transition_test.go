package store_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Only the destination is outside the hashed policy to avoid a self-referential
// digest. The origin and permission checks are hashed, so the transition cannot
// be reactivated after landing by editing this destination alone.
const debtG01CollectorTo = "b42ea974646e7b666459091174645baa501d91da16871813568db23858def7b7"
const debtQuiescenceCollectorTo = "4ab44a86253d3b09474c20463ca1a4e5c6296f4a4694e4d80d904832757ae047"
const debtRunFixtureCollectorTo = "dd36814075a0641f1085eb8fe8d74ae1cd61d8fba3b6047a8514b175ddbfa53e"

func TestPersistenceAuthorityDebtG01TransitionIsExactAndMetadataOnly(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, debtBaselinePath))
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := parseAuthorityDebtBaseline(data)
	if err != nil {
		t.Fatal(err)
	}
	if baseline.Collector != debtG01CollectorTo {
		t.Fatalf("G01's retained exact historical destination changed: %s", baseline.Collector)
	}
	old := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtG01CollectorFrom, Sites: debtControlSet(debtControlSite("call:QueryRow", 2))}
	current := old
	current.Collector = debtG01CollectorTo
	for _, tc := range []struct {
		name, base, collector string
		trusted, head         authorityDebtBaseline
		accept                bool
	}{
		{"normal-old", debtG01CollectorFrom, debtG01CollectorFrom, old, old, true},
		{"exact-transition", debtG01CollectorFrom, debtG01CollectorTo, old, current, true},
		{"inert-after-landing", debtG01CollectorTo, debtG01CollectorTo, current, current, true},
		{"foreign-base", strings.Repeat("b", 64), debtG01CollectorTo, old, current, false},
		{"foreign-head", debtG01CollectorFrom, strings.Repeat("c", 64), old, current, false},
		{"reverse", debtG01CollectorTo, debtG01CollectorFrom, current, old, false},
		{"unrotated-metadata", debtG01CollectorFrom, debtG01CollectorTo, old, old, false},
		{"future-collector", debtG01CollectorTo, strings.Repeat("c", 64), current, current, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := debtValidateCollectorIdentity(tc.base, tc.collector, tc.trusted, tc.head, ""); (err == nil) != tc.accept {
				t.Fatalf("identity acceptance=%v error=%v", tc.accept, err)
			}
		})
	}
	for _, mutation := range []string{"bootstrap", "remove-site", "add-site", "multiplicity", "trusted-metadata", "head-metadata"} {
		t.Run(mutation, func(t *testing.T) {
			trusted, head := old, current
			switch mutation {
			case "bootstrap":
				head.BootstrapSource = strings.Repeat("b", 40)
			case "remove-site":
				head.Sites = debtControlSet()
			case "add-site":
				head.Sites = debtControlSet(debtControlSite("call:QueryRow", 2), debtControlSite("call:Exec", 1))
			case "multiplicity":
				head.Sites = debtControlSet(debtControlSite("call:QueryRow", 1))
			case "trusted-metadata":
				trusted.Collector = strings.Repeat("d", 64)
			case "head-metadata":
				head.Collector = strings.Repeat("e", 64)
			}
			if err := debtValidateCollectorIdentity(debtG01CollectorFrom, debtG01CollectorTo, trusted, head, ""); err == nil {
				t.Fatal("transition admitted a non-metadata baseline change")
			}
		})
	}
}

func TestPersistenceAuthorityDebtQuiescenceTransitionPreservesExactDebtAndDirection(t *testing.T) {
	old, current := debtQuiescenceUncertaintyTransition()
	unrelated := debtControlSite("call:QueryRow", 3)
	oldState := debtControlSet(old, unrelated)
	newState := debtControlSet(current, unrelated)
	for _, test := range []struct {
		name                       string
		actual, head, base, source map[string]authorityDebtSite
		accept                     bool
	}{
		{"unchanged-before", oldState, oldState, oldState, oldState, true},
		{"forward-refresh", newState, oldState, oldState, oldState, true},
		{"forward-refreshed", newState, newState, oldState, oldState, true},
		{"inert-after-landing", newState, newState, newState, newState, true},
		{"both-actual", debtControlSet(old, current, unrelated), newState, oldState, oldState, false},
		{"both-head", newState, debtControlSet(old, current, unrelated), oldState, oldState, false},
		{"both-trusted", newState, newState, debtControlSet(old, current, unrelated), oldState, false},
		{"both-source", newState, newState, oldState, debtControlSet(old, current, unrelated), false},
		{"reverse-landed", oldState, oldState, newState, newState, false},
		{"old-source-resurrection", oldState, oldState, oldState, newState, false},
		{"independent-source-does-not-match", newState, newState, oldState, debtControlSet(unrelated), false},
		{"unrelated-addition-despite-lower-count", debtControlSet(current, debtControlSite("call:Exec", 1)), debtControlSet(current, debtControlSite("call:Exec", 1)), oldState, oldState, false},
		{"unrelated-multiplicity-increase", debtControlSet(current, debtControlSite("call:QueryRow", 4)), debtControlSet(current, debtControlSite("call:QueryRow", 4)), oldState, oldState, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			failures := authorityDebtRatchet(test.actual, test.head, test.base, test.source)
			if (len(failures) == 0) != test.accept {
				t.Fatalf("accepted=%v,failures=%v", test.accept, failures)
			}
		})
	}
	for _, field := range []string{"file", "declaration", "kind", "hash", "context", "family", "owner", "multiplicity"} {
		t.Run(field, func(t *testing.T) {
			changed := current
			switch field {
			case "file":
				changed.File += ".other"
			case "declaration":
				changed.Declaration += "Other"
			case "kind":
				changed.Kind = "raw-operation"
			case "hash":
				changed.Operation = "declaration:" + strings.Repeat("e", 64)
			case "context":
				changed.Resolved += "other"
			case "family":
				changed.Family += "other"
			case "owner":
				changed.Replacement += "other"
			case "multiplicity":
				changed.Multiplicity = 2
			}
			state := debtControlSet(changed, unrelated)
			if failures := authorityDebtRatchet(state, state, oldState, oldState); len(failures) == 0 {
				t.Fatal("reviewed transition admitted a different site or count")
			}
		})
	}
	if oldState[old.identity()] != old || oldState[current.identity()].Multiplicity != 0 {
		t.Fatal("comparison mutated its original trusted reference")
	}
}

func TestPersistenceAuthorityDebtQuiescenceCollectorTransitionIsPinnedAndDownward(t *testing.T) {
	old, forward := debtQuiescenceUncertaintyTransition()
	legacy := debtControlSite("call:QueryRow", 3)
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtQuiescenceCollectorFrom, Sites: debtControlSet(old, legacy)}
	for _, test := range []struct {
		name   string
		sites  map[string]authorityDebtSite
		accept bool
	}{
		{"before-refresh", trusted.Sites, true},
		{"exact-forward", debtControlSet(forward, legacy), true},
		{"forward-and-genuine-removal", debtControlSet(forward), true},
		{"uncertainty-dropped", debtControlSet(legacy), false},
		{"both-identities", debtControlSet(old, forward), false},
		{"increased-uncertainty", debtControlSet(authorityDebtSite{forward.Kind, forward.File, forward.Declaration, forward.Operation, forward.Resolved, forward.Family, forward.Replacement, 2}), false},
		{"new-raw-despite-lower-count", debtControlSet(forward, debtControlSite("call:Exec", 1)), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			head := trusted
			head.Collector, head.Sites = debtQuiescenceCollectorTo, test.sites
			if err := debtValidateCollectorIdentity(debtQuiescenceCollectorFrom, debtQuiescenceCollectorTo, trusted, head, ""); (err == nil) != test.accept {
				t.Fatalf("accepted=%v,error=%v", test.accept, err)
			}
		})
	}
	landed := trusted
	landed.Collector, landed.Sites = debtQuiescenceCollectorTo, debtControlSet(forward)
	if err := debtValidateCollectorIdentity(debtQuiescenceCollectorTo, debtQuiescenceCollectorTo, landed, landed, ""); err != nil {
		t.Fatal(err)
	}
	if err := debtValidateCollectorIdentity(debtQuiescenceCollectorTo, debtQuiescenceCollectorFrom, landed, trusted, ""); err == nil {
		t.Fatal("reverse collector transition accepted")
	}
	wrong := landed
	wrong.BootstrapSource = strings.Repeat("b", 40)
	if err := debtValidateCollectorIdentity(debtQuiescenceCollectorFrom, debtQuiescenceCollectorTo, trusted, wrong, ""); err == nil {
		t.Fatal("bootstrap origin changed")
	}
	if err := debtValidateCollectorIdentity(debtQuiescenceCollectorFrom, strings.Repeat("c", 64), trusted, landed, ""); err == nil {
		t.Fatal("unreviewed collector digest accepted")
	}
}
