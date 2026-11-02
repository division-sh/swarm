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
const debtCompletedSelectedFixturePinCollectorTo = "f28178924493fb28094b3bd1fb4814564680387608f43984fcd532afb01636a6"
const debtW3LandedFixtureCollectorTo = "895137c088b18e51143008b7c47e32b76a573054322d87c0072781adcf1c9626"
const debtW1ComparisonCollectorTo = "e15497e25a763bd9e57c71221b045640ca92517d809095ed618eb93c741f8127"
const debtW1RebasedComparisonCollectorTo = "58217e03fdd7a5276ef4ac1b0a763fb2b2508423763415bc875ddcb6469c78d0"
const debtSourceLoaderCollectorTo = "2b1b5a1147e3733dbc57049a2515c05f0b0103e595a666c524232a7ff9012e5a"
const debtProviderSourceCollectorTo = "e30b8d1d122008c82a16d1b85f04d24e72c125f44fba0fa4a0d6b8de3f8eeacb"
const debtDeploymentHandlerLandedCollectorTo = "7e349138dacce1f6893971fe8e012c21ac27710b95b721dbbd5afe720b792b72"
const debtRetiredSourceFreezeCollectorTo = "ae3603d66950e48dcee170a6a962982f6420cb8e5d2f40362dc210af029162ec"
const debtW4ComparisonCollectorTo = "d29c2f75bc329c11bf7ba4107081c55376ca71cee03e02e041d5e371df0009d1"

const debtS2OriginalContextCollectorTo = "b9cc07d820372e373f0971aa9961c8e9a233ee52908b17f865efbf0fdb1b6656"

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
	t.Run("w1-exact-run-observations", func(t *testing.T) {
		before, after := debtControlSet(), debtControlSet()
		pairs := debtW1RunObservationSignatureTransitions()
		for _, pair := range pairs {
			before[pair[0].identity()], after[pair[1].identity()] = pair[0], pair[1]
		}
		if len(before) != 10 || len(after) != 10 || authorityDebtCount(before) != 13 || authorityDebtCount(after) != 13 {
			t.Fatal("W1 comparison must preserve ten identities and thirteen occurrences")
		}
		originalBefore, originalAfter := maps.Clone(before), maps.Clone(after)
		for _, source := range []map[string]authorityDebtSite{before, after} {
			for _, head := range []map[string]authorityDebtSite{before, after} {
				if failures := authorityDebtRatchet(after, head, before, source); len(failures) != 0 {
					t.Fatal(failures)
				}
			}
		}
		if !maps.Equal(before, originalBefore) || !maps.Equal(after, originalAfter) {
			t.Fatal("W1 comparison mutated source or baseline")
		}
		if failures := authorityDebtRatchet(before, before, after, after); len(failures) == 0 {
			t.Fatal("reverse W1 transition admitted")
		}
		for _, pair := range pairs {
			for _, landed := range []bool{false, true} {
				t.Run(pair[0].Declaration+"/"+pair[0].Operation+map[bool]string{false: "/forward", true: "/landed"}[landed], func(t *testing.T) {
					debtRunFixtureSignatureHostileControls(t, pair, before, after, landed)
				})
			}
		}
	})
	t.Run("w4-landed-source", func(t *testing.T) {
		before, after := debtControlSet(), debtControlSet()
		for _, pair := range debtW4LiveFixtureTransfers() {
			before[pair[0].identity()], after[pair[1].identity()] = pair[0], pair[1]
		}
		composition := debtW4LiveFixtureComposition()
		mockReturn := composition[1]
		mockReturn.Declaration = "startServedTestSetupEntitiesProofRuntimeFromSource"
		before[mockReturn.identity()], after[mockReturn.identity()] = mockReturn, mockReturn
		for _, site := range composition {
			after[site.identity()] = site
		}
		originalBefore, originalAfter := maps.Clone(before), maps.Clone(after)
		for _, source := range []map[string]authorityDebtSite{before, after} {
			for _, head := range []map[string]authorityDebtSite{before, after} {
				if failures := authorityDebtRatchet(after, head, before, source); len(failures) != 0 {
					t.Fatal(failures)
				}
			}
		}
		if !maps.Equal(before, originalBefore) || !maps.Equal(after, originalAfter) {
			t.Fatal("W4 comparison mutated source or baseline")
		}
		for _, mutation := range []string{"partial-source", "conflicting-head", "missing-mock", "foreign-composition", "coexists-actual", "coexists-source", "count-actual", "count-source", "reverse"} {
			t.Run(mutation, func(t *testing.T) {
				actual, head, base, source := maps.Clone(after), maps.Clone(after), maps.Clone(before), maps.Clone(after)
				switch mutation {
				case "partial-source":
					delete(source, composition[0].identity())
				case "conflicting-head":
					changed := composition[0]
					changed.Multiplicity++
					head[changed.identity()] = changed
				case "missing-mock":
					delete(source, mockReturn.identity())
				case "foreign-composition":
					changed := composition[0]
					changed.Resolved += "Other"
					delete(actual, composition[0].identity())
					actual[changed.identity()] = changed
				case "coexists-actual", "coexists-source":
					pair := debtW4LiveFixtureTransfers()[0]
					if mutation == "coexists-actual" {
						actual[pair[0].identity()] = pair[0]
					} else {
						source[pair[0].identity()] = pair[0]
					}
				case "count-actual", "count-source":
					changed := debtW4LiveFixtureTransfers()[0][1]
					changed.Multiplicity++
					if mutation == "count-actual" {
						actual[changed.identity()] = changed
					} else {
						source[changed.identity()] = changed
					}
				case "reverse":
					actual, head, base, source = before, before, after, after
				}
				if failures := authorityDebtRatchet(actual, head, base, source); len(failures) == 0 {
					t.Fatal("non-exact W4 source composition admitted")
				}
			})
		}
	})
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
		originalBefore, originalAfter := maps.Clone(before), maps.Clone(after)
		for _, head := range []map[string]authorityDebtSite{before, after} {
			if failures := authorityDebtRatchet(after, head, before, after); len(failures) != 0 {
				t.Fatal("exact landed selected source rejected: ", failures)
			}
		}
		if !maps.Equal(before, originalBefore) || !maps.Equal(after, originalAfter) {
			t.Fatal("comparison normalization mutated baseline or source evidence")
		}
		if failures := authorityDebtRatchet(before, before, after, after); len(failures) == 0 {
			t.Fatal("reverse original-owner signature continuation admitted")
		}
		if failures := authorityDebtRatchet(after, after, before, debtControlSet()); len(failures) == 0 {
			t.Fatal("original-owner signature continuation admitted without its source")
		}
		if failures := authorityDebtRatchet(before, before, before, after); len(failures) == 0 {
			t.Fatal("landed selected source admitted old-call resurrection")
		}
		added := maps.Clone(after)
		unrelated := debtControlSite("call:Exec", 1)
		added[unrelated.identity()] = unrelated
		if failures := authorityDebtRatchet(added, added, before, after); len(failures) == 0 {
			t.Fatal("landed selected source paid for an unrelated addition")
		}
		for _, pair := range debtSelectedFixtureSignatureTransitions() {
			for _, landed := range []bool{false, true} {
				name := "forward/" + pair[0].Declaration
				if landed {
					name = "landed/" + pair[0].Declaration
				}
				t.Run(name, func(t *testing.T) { debtRunFixtureSignatureHostileControls(t, pair, before, after, landed) })
			}
		}
	})
	t.Run("selected-binding-root-arguments", func(t *testing.T) {
		pair := debtSelectedBindingRootArgumentTransition()
		before, after := debtControlSet(pair[0]), debtControlSet(pair[1])
		if failures := authorityDebtRatchet(after, before, before, before); len(failures) != 0 {
			t.Fatal(failures)
		}
		if failures := authorityDebtRatchet(before, before, after, after); len(failures) == 0 {
			t.Fatal("reverse rooted INSERT correspondence admitted")
		}
		if failures := authorityDebtRatchet(after, after, before, after); len(failures) != 0 {
			t.Fatal("exact landed rooted INSERT source was not recognized", failures)
		}
		debtRunFixtureSignatureHostileControls(t, pair, before, after, false)
		debtRunFixtureSignatureHostileControls(t, pair, before, after, true)
	})
	t.Run("deployment-handler-source-signature", func(t *testing.T) {
		pair := debtDeploymentHandlerSignatureTransition()
		before, after := debtControlSet(pair[0]), debtControlSet(pair[1])
		if failures := authorityDebtRatchet(after, before, before, before); len(failures) != 0 {
			t.Fatal("exact counted stub correspondence was not recognized", failures)
		}
		if failures := authorityDebtRatchet(after, before, before, after); len(failures) != 0 {
			t.Fatal("exact landed counted stub correspondence was not recognized", failures)
		}
		debtRunFixtureSignatureHostileControls(t, pair, before, after, false)
		debtRunFixtureSignatureHostileControls(t, pair, before, after, true)
	})
	t.Run("catalog-original-native-reader-threading", func(t *testing.T) {
		prior, next := debtCatalogRuntimeRowsSignatureTransition()
		before, after := debtControlSet(prior), debtControlSet(next)
		originalBefore, originalAfter := maps.Clone(before), maps.Clone(after)
		for _, test := range []struct {
			name                       string
			actual, head, base, source map[string]authorityDebtSite
			accept                     bool
		}{
			{"forward-unrefreshed", after, before, before, before, true},
			{"forward-refreshed", after, after, before, before, true},
			{"landed-inert", after, after, after, after, true},
			{"landed-source-cannot-extend-pair", after, before, before, after, false},
			{"landed-source-refreshed-head-cannot-extend-pair", after, after, before, after, false},
			{"reverse", before, before, after, after, false},
			{"old-source-resurrection", before, before, before, after, false},
			{"foreign-source", after, after, before, debtControlSet(), false},
			{"unrelated-addition", debtControlSet(next, debtControlSite("call:Exec", 1)), after, before, before, false},
		} {
			t.Run(test.name, func(t *testing.T) {
				if failures := authorityDebtRatchet(test.actual, test.head, test.base, test.source); (len(failures) == 0) != test.accept {
					t.Fatalf("accepted=%v failures=%v", test.accept, failures)
				}
			})
		}
		debtRunFixtureSignatureHostileControls(t, [2]authorityDebtSite{prior, next}, before, after, false)
		if !maps.Equal(before, originalBefore) || !maps.Equal(after, originalAfter) {
			t.Fatal("catalog comparison normalization mutated baseline or source evidence")
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
		{"landed-source-does-not-extend-nine-pair-family", current, old, old, current, false},
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
			debtRunFixtureSignatureHostileControls(t, pair, old, current, false)
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

func debtRunFixtureSignatureHostileControls(t *testing.T, pair [2]authorityDebtSite, old, current map[string]authorityDebtSite, landed bool) {
	t.Helper()
	source := old
	if landed {
		source = current
	}
	for slot := range 4 {
		t.Run([]string{"coexists-actual", "coexists-head", "coexists-baseline", "coexists-source"}[slot], func(t *testing.T) {
			states := [4]map[string]authorityDebtSite{current, current, old, source}
			states[slot] = maps.Clone(states[slot])
			states[slot][pair[0].identity()] = pair[0]
			states[slot][pair[1].identity()] = pair[1]
			if landed && slot == 2 {
				for _, reviewed := range debtW4LiveFixtureTransfers() {
					if pair != reviewed {
						continue
					}
					// #2791/6453734119: removing already-counted OLD debt is
					// downward progress, not a coexistence normalization.
					original := [4]map[string]authorityDebtSite{maps.Clone(states[0]), maps.Clone(states[1]), maps.Clone(states[2]), maps.Clone(states[3])}
					head, base, source := debtW4LiveFixtureComparisons(states[0], states[1], states[2], states[3])
					if !maps.Equal(head, original[1]) || !maps.Equal(base, original[2]) || !maps.Equal(source, original[3]) {
						t.Fatal("W4 coexistence normalized comparison inputs")
					}
					for index := range states {
						if !maps.Equal(states[index], original[index]) {
							t.Fatal("W4 coexistence mutated trusted input")
						}
					}
					if failures := authorityDebtRatchet(states[0], states[1], states[2], states[3]); len(failures) != 0 {
						t.Fatal("downward OLD deletion refused: ", failures)
					}
					return
				}
			}
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
				if strings.Contains(changed.Resolved, "|recv=*database/sql.DB|") {
					changed.Resolved = strings.Replace(changed.Resolved, "|recv=*database/sql.DB|", "|recv=*database/sql.Conn|", 1)
				} else {
					changed.Resolved = strings.Replace(changed.Resolved, "|recv=<nil>|", "|recv=*database/sql.DB|", 1)
				}
			case "result":
				changed.Resolved += ",error"
			case "family":
				changed.Family += "Other"
			case "owner":
				changed.Replacement += "Other"
			case "multiplicity":
				changed.Multiplicity++
			}
			if changed == pair[1] {
				changed.Resolved += "Other"
			}
			actual := maps.Clone(current)
			delete(actual, pair[1].identity())
			actual[changed.identity()] = changed
			if failures := authorityDebtRatchet(actual, actual, old, source); len(failures) == 0 {
				t.Fatal("non-reviewed identity or duplicate call admitted")
			}
		})
	}
	for slot := range 2 {
		for _, mutation := range []string{"missing", "duplicate", "changed"} {
			t.Run(mutation+[]string{"-baseline-predecessor", "-source-predecessor"}[slot], func(t *testing.T) {
				states := [2]map[string]authorityDebtSite{maps.Clone(old), maps.Clone(source)}
				predecessor := pair[0]
				if slot == 1 && landed {
					predecessor = pair[1]
				}
				delete(states[slot], predecessor.identity())
				if mutation != "missing" {
					changed := predecessor
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
	t.Run("w1-comparison-metadata-only", func(t *testing.T) {
		for _, edge := range [][2]string{{debtCompletedSelectedFixturePinCollectorTo, debtW1ComparisonCollectorTo}, {debtW3LandedFixtureCollectorTo, debtW1RebasedComparisonCollectorTo}, {debtW1RebasedComparisonCollectorTo, debtSourceLoaderCollectorTo}, {debtS1CollectorTo, debtProviderSourceCollectorTo}, {debtProviderSourceCollectorTo, debtS3AccountCollectorTo}, {debtS3AccountCollectorTo, debtDeploymentHandlerLandedCollectorTo}, {debtDeploymentHandlerLandedCollectorTo, debtRetiredSourceFreezeCollectorTo}} {
			t.Run(edge[0], func(t *testing.T) {
				for _, mutation := range []string{"exact", "foreign-base", "trusted-collector", "foreign-current", "head-collector", "unreviewed", "foreign-review", "reverse", "bootstrap", "remove-site", "add-site", "multiplicity", "replace-site"} {
					t.Run(mutation, func(t *testing.T) {
						before := authorityDebtBaseline{BootstrapSource: trusted.BootstrapSource, Collector: edge[0], Sites: debtControlSet(legacy)}
						after := before
						after.Collector, after.Sites = edge[1], maps.Clone(before.Sites)
						base, collector, reviewed := before.Collector, after.Collector, after.Collector
						switch mutation {
						case "foreign-base":
							base = strings.Repeat("b", 64)
						case "trusted-collector":
							before.Collector = strings.Repeat("b", 64)
						case "foreign-current":
							collector, after.Collector, reviewed = strings.Repeat("b", 64), strings.Repeat("b", 64), strings.Repeat("b", 64)
						case "head-collector":
							after.Collector = before.Collector
						case "unreviewed":
							reviewed = ""
						case "foreign-review":
							reviewed = strings.Repeat("b", 64)
						case "reverse":
							before, after = after, before
							base, collector, reviewed = before.Collector, after.Collector, after.Collector
						case "bootstrap":
							after.BootstrapSource = strings.Repeat("b", 40)
						case "remove-site":
							delete(after.Sites, legacy.identity())
						case "add-site":
							after.Sites[escape.identity()] = escape
						case "multiplicity":
							changed := legacy
							changed.Multiplicity--
							after.Sites[changed.identity()] = changed
						case "replace-site":
							delete(after.Sites, legacy.identity())
							after.Sites[replacement.identity()] = replacement
						}
						if err := debtValidateCollectorIdentity(base, collector, before, after, reviewed); (err == nil) != (mutation == "exact") {
							t.Fatalf("metadata-only edge: %v", err)
						}
					})
				}
			})
		}
	})
	t.Run("w4-comparison-metadata-only", func(t *testing.T) {
		for _, mutation := range []string{"exact", "foreign-base", "trusted-collector", "foreign-current", "head-collector", "unreviewed", "foreign-review", "reverse", "bootstrap", "remove-site", "add-site", "multiplicity", "replace-site"} {
			t.Run(mutation, func(t *testing.T) {
				before := authorityDebtBaseline{BootstrapSource: trusted.BootstrapSource, Collector: debtRetiredSourceFreezeCollectorTo, Sites: debtControlSet(legacy)}
				after := before
				after.Collector, after.Sites = debtW4ComparisonCollectorTo, maps.Clone(before.Sites)
				base, collector, reviewed := before.Collector, after.Collector, after.Collector
				switch mutation {
				case "foreign-base":
					base = strings.Repeat("b", 64)
				case "trusted-collector":
					before.Collector = strings.Repeat("b", 64)
				case "foreign-current":
					collector, after.Collector, reviewed = strings.Repeat("b", 64), strings.Repeat("b", 64), strings.Repeat("b", 64)
				case "head-collector":
					after.Collector = before.Collector
				case "unreviewed":
					reviewed = ""
				case "foreign-review":
					reviewed = strings.Repeat("b", 64)
				case "reverse":
					before, after = after, before
					base, collector, reviewed = before.Collector, after.Collector, after.Collector
				case "bootstrap":
					after.BootstrapSource = strings.Repeat("b", 40)
				case "remove-site":
					delete(after.Sites, legacy.identity())
				case "add-site":
					after.Sites[escape.identity()] = escape
				case "multiplicity":
					changed := legacy
					changed.Multiplicity--
					after.Sites[changed.identity()] = changed
				case "replace-site":
					delete(after.Sites, legacy.identity())
					after.Sites[replacement.identity()] = replacement
				}
				if err := debtValidateCollectorIdentity(base, collector, before, after, reviewed); (err == nil) != (mutation == "exact") {
					t.Fatalf("metadata-only edge: %v", err)
				}
			})
		}
	})
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
