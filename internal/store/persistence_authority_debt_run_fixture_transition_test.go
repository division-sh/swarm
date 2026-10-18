package store_test

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const debtSelectedFixtureSignatureCollectorTo2738 = "d28210ea744ffdb3eaffd17163f357c6c08da48340692826b373cd988bfbea13"

func debtRunFixtureTransitionStates() (map[string]authorityDebtSite, map[string]authorityDebtSite) {
	old, current := debtControlSet(), debtControlSet()
	for _, pair := range debtRunFixtureSignatureTransitions() {
		old[pair[0].identity()] = pair[0]
		current[pair[1].identity()] = pair[1]
	}
	return old, current
}

func TestPersistenceAuthorityDebtRunFixtureSignatureTransitionIsExact(t *testing.T) {
	t.Run("selected-context-threading-2738", proveSelectedFixtureSignatureContinuation2738)
	old, current := debtRunFixtureTransitionStates()
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

func proveSelectedFixtureSignatureContinuation2738(t *testing.T) {
	old, current := debtControlSet(), debtControlSet()
	for _, pair := range debtSelectedFixtureSignatureTransitions2738() {
		old[pair[0].identity()] = pair[0]
		current[pair[1].identity()] = pair[1]
	}
	if len(old) != 6 || len(current) != 6 || authorityDebtCount(old) != 6 || authorityDebtCount(current) != 6 {
		t.Fatal("selected fixture signature map changed its six distinct occurrences")
	}
	for _, states := range [][4]map[string]authorityDebtSite{{old, old, old, old}, {current, old, old, old}, {current, current, old, old}, {current, current, current, current}} {
		if failures := authorityDebtRatchet(states[0], states[1], states[2], states[3]); len(failures) != 0 {
			t.Fatal(failures)
		}
	}
	for _, states := range [][4]map[string]authorityDebtSite{{old, old, current, current}, {current, current, old, debtControlSet()}} {
		if len(authorityDebtRatchet(states[0], states[1], states[2], states[3])) == 0 {
			t.Fatal("selected signature reverse/foreign source admitted")
		}
	}
	for _, pair := range debtSelectedFixtureSignatureTransitions2738() {
		t.Run(pair[0].Declaration, func(t *testing.T) {
			debtRunFixtureSignatureHostileControls(t, pair, old, current)
			missing := maps.Clone(current)
			delete(missing, pair[1].identity())
			if len(authorityDebtRatchet(missing, current, old, old)) == 0 {
				t.Fatal("missing original signature donor was treated as deletion credit")
			}
		})
	}
	root := persistenceAuthorityRepoRoot(t)
	before, err := debtGit(root, "show", "b74c4c58b3ef772f11faed17d78324b8210b2a7a:internal/runtime/runforkexecution/execution_test.go")
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(root, "internal/runtime/runforkexecution/execution_test.go"))
	if err != nil || debtSelectedFixtureSignatureImages2738(before, after) != nil {
		t.Fatalf("selected signature complete source is not current: %v", err)
	}
	for _, image := range []string{"warm-current", "foreign-prior", "copied-current"} {
		prior, current := before, after
		if image == "foreign-prior" {
			prior = append(append([]byte(nil), before...), ' ')
		} else {
			current = append(append([]byte(nil), after...), ' ')
		}
		if debtSelectedFixtureSignatureImages2738(prior, current) == nil {
			t.Fatal("signature image changed after successful warm validation: " + image)
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
				if changed.Resolved == pair[1].Resolved {
					changed.Resolved = strings.Replace(changed.Resolved, "|arg=context.Context|", "|arg=int|", 1)
				}
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
	t.Run("selected-original-signature-successor-2738", func(t *testing.T) {
		inherited := debtControlSet(debtControlSite("call:QueryRow", 1))
		for _, mutation := range []string{"exact", "old-chain", "foreign", "reverse", "missing", "duplicate", "rebootstrap", "stale"} {
			t.Run(mutation, func(t *testing.T) {
				base := authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: debtProcessFenceCollectorTo, Sites: maps.Clone(inherited)}
				head := base
				head.Collector, head.Sites = debtSelectedFixtureSignatureCollectorTo2738, maps.Clone(inherited)
				switch mutation {
				case "old-chain":
					base.Collector = debtProcessLeaseCollectorTo
				case "foreign":
					base.Collector = strings.Repeat("f", 64)
				case "reverse":
					base, head = head, base
				case "missing":
					head.Sites = debtControlSet()
				case "duplicate":
					head.Sites = debtControlSet(debtControlSite("call:QueryRow", 2))
				case "rebootstrap":
					head.BootstrapSource = strings.Repeat("a", 40)
				case "stale":
					head.Collector = strings.Repeat("b", 64)
				}
				err := debtValidateCollectorIdentity(base.Collector, head.Collector, base, head, "")
				if (err == nil) != (mutation == "exact" || mutation == "old-chain") {
					t.Fatalf("collector successor %s: %v", mutation, err)
				}
			})
		}
	})
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

func TestPersistenceAuthorityDebtRunFixturePolicyDestinationMatchesSource(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	digest, err := debtCollectorDigest(root)
	if err != nil || (digest != debtForkProofCollectorTo && digest != debtProcessLeaseCollectorTo && digest != debtProcessFenceCollectorTo && digest != debtSelectedFixtureSignatureCollectorTo2738) {
		t.Fatalf("reviewed fork/process successor=%s/%s actual=%s error=%v", debtForkProofCollectorTo, debtProcessLeaseCollectorTo, digest, err)
	}
	prior := materializeDebtBase(t, root, "40c0b18bf315a949959417688adb32e358413b78")
	if digest, err := debtCollectorDigest(prior); err != nil || digest != debtRunFixtureCollectorTo {
		t.Fatalf("immutable run-fixture policy destination changed: digest=%s error=%v", digest, err)
	}
	// This immutable master ancestor contains the identical nine predecessor
	// calls; unlike the old pre-rebase branch commit it survives fresh CI clones.
	data, err := debtGit(root, "show", "44c4047f0ac6866f6f4475c12d8a0f69e16b062c:"+debtBaselinePath)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := parseAuthorityDebtBaseline(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range debtRunFixtureSignatureTransitions() {
		if baseline.Sites[pair[0].identity()] != pair[0] || baseline.Sites[pair[1].identity()].Multiplicity != 0 {
			t.Fatalf("reviewed immutable predecessor did not own the exact call: %s", pair[0].Declaration)
		}
	}
}
