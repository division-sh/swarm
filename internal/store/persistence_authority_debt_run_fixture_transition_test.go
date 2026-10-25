package store_test

import (
	"maps"
	"strings"
	"testing"
)

const debtSelectedFixtureOldSource = "d47682d5c0bf8c921b4468668cefda60d2ac0cbe"
const debtSelectedFixtureCurrentSource = "ea11297ceb7a0213f2c113c0c17f93e88be8a529"

// #2788/6375760610: the same five correspondences composed onto the landed source.
const debtSelectedFixtureComposedOldSource = "97305834e727bab34ea5d2faf4e19d9192cad0df"
const debtSelectedFixtureComposedCurrentSource = "d3fd1308470056da34276aaf08e212f1f4f19faa"
const debtSelectedFixtureComposedOldSHA256 = "2abd9424b866dbda517bcd44726bf04179a5a53ca75443588c86742370285681"
const debtSelectedFixtureComposedCurrentSHA256 = "cdb196a9aeb5f87d51e656249b6bb533003d0af1ed7322cc858a2d9bd9d81fae"
const debtSelectedFixtureComposedCollectorTo = "db0de72a381af1ab7ee26a4e0748565cde6c124a25a02ac3f79e2bbbc3464801"
const debtCompletedSelectedFixturePinCollectorTo = "ff1f6eee6151ee17d5a40d1dc211dcecb51ba12577b6365a4fdc25b4c04ea1f6"

// #2727/6388897450: source-pin validation for an already-landed composed endpoint.
const debtSelectedFixtureLandedCollectorTo = "1b48c6bd1c1b6d6100ee6df74bb9fe3d239372befa4c8f96f02243602fa9f646"

func debtRunFixtureTransitionStates() (map[string]authorityDebtSite, map[string]authorityDebtSite) {
	old, current := debtControlSet(), debtControlSet()
	for _, pair := range debtRunFixtureSignatureTransitions() {
		old[pair[0].identity()] = pair[0]
		current[pair[1].identity()] = pair[1]
	}
	return old, current
}

func TestPersistenceAuthorityDebtRunFixtureSignatureTransitionIsExact(t *testing.T) {
	old, current := debtRunFixtureTransitionStates()
	t.Run("selected-original-owner-threading", func(t *testing.T) {
		before, after := debtControlSet(), debtControlSet()
		for _, pair := range debtSelectedFixtureSignatureTransitions() {
			before[pair[0].identity()] = pair[0]
			after[pair[1].identity()] = pair[1]
		}
		if authorityDebtCount(before) != 5 || authorityDebtCount(after) != 5 {
			t.Fatal("original-owner threading must preserve five distinct occurrences")
		}
		if failures := authorityDebtRatchet(after, before, before, before); len(failures) != 0 {
			t.Fatal(failures)
		}
		if failures := authorityDebtRatchet(after, before, before, after); len(failures) != 0 {
			t.Fatal("landed selected source must preserve all five baseline occurrences:", failures)
		}
		if failures := authorityDebtRatchet(before, before, after, after); len(failures) == 0 {
			t.Fatal("reverse original-owner signature continuation admitted")
		}
		if failures := authorityDebtRatchet(after, after, before, debtControlSet()); len(failures) == 0 {
			t.Fatal("original-owner signature continuation admitted without its source")
		}
		for _, pair := range debtSelectedFixtureSignatureTransitions() {
			debtRunFixtureSignatureHostileControls(t, pair, before, after)
		}
	})
	if authorityDebtCount(old) != 9 || authorityDebtCount(current) != 9 {
		t.Fatal("signature accounting must preserve all nine distinct debt occurrences")
	}
	for _, test := range []struct {
		name                       string
		actual, head, base, source map[string]authorityDebtSite
		accept                     bool
	}{
		{"unchanged", old, old, old, old, true},
		{"forward-refresh", current, old, old, old, true},
		{"forward-refreshed", current, current, old, old, true},
		{"landed-inert", current, current, current, current, true},
		{"unreviewed-landed-unrefreshed", current, old, old, current, false},
		{"reverse", old, old, current, current, false},
		{"old-source-resurrection", old, old, old, current, false},
		{"foreign-source", current, current, old, debtControlSet(), false},
		{"genuine-deletion-after-landing", debtControlSet(), debtControlSet(), current, current, true},
		{"addition-despite-deletion", debtControlSet(debtControlSite("call:Exec", 1)), debtControlSet(debtControlSite("call:Exec", 1)), current, current, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			failures := authorityDebtRatchet(test.actual, test.head, test.base, test.source)
			if (len(failures) == 0) != test.accept {
				t.Fatalf("accepted=%v failures=%v", test.accept, failures)
			}
		})
	}
	for _, pair := range debtRunFixtureSignatureTransitions() {
		t.Run(pair[0].Declaration, func(t *testing.T) {
			debtRunFixtureSignatureHostileControls(t, pair, old, current)
		})
	}
	t.Run("forward-cannot-pay-for-unrelated-addition", func(t *testing.T) {
		trusted := maps.Clone(old)
		prior := debtControlSite("call:QueryRow", 3)
		trusted[prior.identity()] = prior
		actual := maps.Clone(current)
		added := debtControlSite("call:Exec", 1)
		actual[added.identity()] = added
		if authorityDebtCount(actual) >= authorityDebtCount(trusted) {
			t.Fatal("hostile control must have lower total debt")
		}
		if failures := authorityDebtRatchet(actual, actual, trusted, trusted); len(failures) == 0 {
			t.Fatal("forward signature transition paid for unrelated new authority")
		}
	})
	for _, pair := range debtRunFixtureSignatureTransitions() {
		if old[pair[0].identity()] != pair[0] || old[pair[1].identity()].Multiplicity != 0 {
			t.Fatal("original trusted maps were mutated")
		}
	}
}

func debtRunFixtureSignatureHostileControls(t *testing.T, pair [2]authorityDebtSite, old, current map[string]authorityDebtSite) {
	t.Helper()
	for slot := range 4 {
		t.Run([]string{"coexists-actual", "coexists-head", "coexists-baseline", "coexists-source"}[slot], func(t *testing.T) {
			states := [4]map[string]authorityDebtSite{current, current, old, old}
			states[slot] = maps.Clone(states[slot])
			states[slot][pair[0].identity()] = pair[0]
			states[slot][pair[1].identity()] = pair[1]
			if failures := authorityDebtRatchet(states[0], states[1], states[2], states[3]); len(failures) == 0 {
				t.Fatal("old/new coexistence admitted")
			}
		})
	}
	for _, field := range []string{"kind", "path", "declaration", "operation", "argument", "receiver", "result", "family", "owner", "multiplicity"} {
		t.Run(field, func(t *testing.T) {
			changed := pair[1]
			switch field {
			case "kind":
				changed.Kind = "raw-type"
			case "path":
				changed.File = "internal/serveapp/other_test.go"
			case "declaration":
				changed.Declaration += "Other"
			case "operation":
				changed.Operation += "Other"
			case "argument":
				changed.Resolved = strings.Replace(changed.Resolved, "|arg=string|", "|arg=int|", 1)
			case "receiver":
				changed.Resolved = strings.Replace(changed.Resolved, "|recv=<nil>|", "|recv=*database/sql.DB|", 1)
			case "result":
				changed.Resolved += ",error"
			case "family":
				changed.Family += "Other"
			case "owner":
				changed.Replacement += "Other"
			case "multiplicity":
				changed.Multiplicity++
			}
			actual := maps.Clone(current)
			delete(actual, pair[1].identity())
			actual[changed.identity()] = changed
			if failures := authorityDebtRatchet(actual, actual, old, old); len(failures) == 0 {
				t.Fatal("non-reviewed identity or duplicate call admitted")
			}
		})
	}
	for slot := range 2 {
		for _, mutation := range []string{"missing", "duplicate", "changed"} {
			t.Run(mutation+[]string{"-baseline-predecessor", "-source-predecessor"}[slot], func(t *testing.T) {
				states := [2]map[string]authorityDebtSite{maps.Clone(old), maps.Clone(old)}
				delete(states[slot], pair[0].identity())
				if mutation != "missing" {
					changed := pair[0]
					if mutation == "duplicate" {
						changed.Multiplicity++
					} else {
						changed.Operation += "Other"
					}
					states[slot][changed.identity()] = changed
				}
				if failures := authorityDebtRatchet(current, current, states[0], states[1]); len(failures) == 0 {
					t.Fatal("unmatched predecessor admitted")
				}
			})
		}
	}
}

func TestPersistenceAuthorityDebtRunFixtureCollectorTransitionIsPinned(t *testing.T) {
	old, current := debtRunFixtureTransitionStates()
	legacy := debtControlSite("call:QueryRow", 3)
	old[legacy.identity()] = legacy
	current[legacy.identity()] = legacy
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtRunFixtureCollectorFrom, Sites: old}
	added := maps.Clone(current)
	delete(added, legacy.identity())
	escape := debtControlSite("call:Exec", 1)
	added[escape.identity()] = escape
	for _, test := range []struct {
		name   string
		sites  map[string]authorityDebtSite
		accept bool
	}{
		{"unrefreshed", old, true},
		{"exact-forward", current, true},
		{"genuine-other-removal", func() map[string]authorityDebtSite { _, sites := debtRunFixtureTransitionStates(); return sites }(), true},
		{"nine-debt-units-dropped", debtControlSet(legacy), false},
		{"new-site-despite-lower-count", added, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			head := trusted
			head.Collector, head.Sites = debtRunFixtureCollectorTo, test.sites
			if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, ""); (err == nil) != test.accept {
				t.Fatalf("accepted=%v error=%v", test.accept, err)
			}
		})
	}
	landed := trusted
	landed.Collector, landed.Sites = debtRunFixtureCollectorTo, current
	composed := landed
	composed.Collector = debtSelectedFixtureComposedCollectorTo
	successor := composed
	successor.Collector = debtSelectedFixtureLandedCollectorTo
	changedSites := successor
	changedSites.Sites = maps.Clone(successor.Sites)
	delete(changedSites.Sites, legacy.identity())
	replacement := debtControlSite("call:Exec", legacy.Multiplicity)
	changedSites.Sites[replacement.identity()] = replacement
	changedBootstrap := successor
	changedBootstrap.BootstrapSource = strings.Repeat("d", 40)
	for _, test := range []struct {
		name, base, collector string
		trusted, head         authorityDebtBaseline
		accept                bool
	}{
		{"landed-inert", landed.Collector, landed.Collector, landed, landed, true},
		{"reverse", landed.Collector, trusted.Collector, landed, trusted, false},
		{"foreign-base", strings.Repeat("b", 64), landed.Collector, trusted, landed, false},
		{"foreign-destination", trusted.Collector, strings.Repeat("c", 64), trusted, landed, false},
		{"unrotated-metadata", trusted.Collector, landed.Collector, trusted, trusted, false},
		{"selected-forward", composed.Collector, successor.Collector, composed, successor, true},
		{"selected-landed-inert", successor.Collector, successor.Collector, successor, successor, true},
		{"selected-reverse", successor.Collector, composed.Collector, successor, composed, false},
		{"selected-foreign-base", strings.Repeat("b", 64), successor.Collector, composed, successor, false},
		{"selected-foreign-destination", composed.Collector, strings.Repeat("c", 64), composed, successor, false},
		{"selected-changed-sites", composed.Collector, successor.Collector, composed, changedSites, false},
		{"selected-changed-bootstrap", composed.Collector, successor.Collector, composed, changedBootstrap, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := debtValidateCollectorIdentity(test.base, test.collector, test.trusted, test.head, ""); (err == nil) != test.accept {
				t.Fatalf("accepted=%v error=%v", test.accept, err)
			}
		})
	}
	quiescenceOld, quiescenceNew := debtQuiescenceUncertaintyTransition()
	chained := trusted
	chained.Collector, chained.Sites = debtQuiescenceCollectorFrom, maps.Clone(old)
	chained.Sites[quiescenceOld.identity()] = quiescenceOld
	chainedHead := landed
	chainedHead.Sites = maps.Clone(current)
	chainedHead.Sites[quiescenceNew.identity()] = quiescenceNew
	if err := debtValidateCollectorIdentity(chained.Collector, chainedHead.Collector, chained, chainedHead, ""); err != nil {
		t.Fatal("exact already-reviewed predecessor chain rejected:", err)
	}
	delete(chained.Sites, quiescenceOld.identity())
	if err := debtValidateCollectorIdentity(chained.Collector, chainedHead.Collector, chained, chainedHead, ""); err == nil {
		t.Fatal("unmatched quiescence source admitted through chained transition")
	}
}
