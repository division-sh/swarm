package store_test

import (
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The destination is outside the hashed policy to avoid self-reference.
const debtQ10CollectorTo = "b79822ba2b94a8df51f34ade3eefe3092e83453df3c4f68945d5577cc70795a7"

func TestPersistenceAuthorityDebtQ10NullableTransitionIsExact(t *testing.T) {
	old, current := debtQ10NullableUncertaintyTransition()
	unrelated := debtControlSite("call:QueryRow", 3)
	before, after := debtControlSet(old, unrelated), debtControlSet(current, unrelated)
	for _, test := range []struct {
		name                       string
		actual, head, base, source map[string]authorityDebtSite
		accept                     bool
	}{
		{"unchanged", before, before, before, before, true},
		{"forward-unrefreshed", after, before, before, before, true},
		{"forward-refreshed", after, after, before, before, true},
		{"landed-inert", after, after, after, after, true},
		{"reverse", before, before, after, after, false},
		{"old-source-resurrection", before, before, before, after, false},
		{"missing-trusted-predecessor", after, after, debtControlSet(unrelated), before, false},
		{"missing-source-predecessor", after, after, before, debtControlSet(unrelated), false},
		{"unrelated-addition-despite-lower-debt", debtControlSet(current, debtControlSite("call:Exec", 1)), debtControlSet(current, debtControlSite("call:Exec", 1)), before, before, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			failures := authorityDebtRatchet(test.actual, test.head, test.base, test.source)
			if (len(failures) == 0) != test.accept {
				t.Fatalf("accepted=%v failures=%v", test.accept, failures)
			}
		})
	}
	for slot := range 4 {
		states := [4]map[string]authorityDebtSite{after, after, before, before}
		states[slot] = debtControlSet(old, current, unrelated)
		if failures := authorityDebtRatchet(states[0], states[1], states[2], states[3]); len(failures) == 0 {
			t.Fatalf("old/new coexistence accepted in slot %d", slot)
		}
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
				changed.Operation += "other"
			case "context":
				changed.Resolved += "other"
			case "family":
				changed.Family += "other"
			case "owner":
				changed.Replacement += "other"
			case "multiplicity":
				changed.Multiplicity++
			}
			actual := debtControlSet(changed, unrelated)
			if failures := authorityDebtRatchet(actual, actual, before, before); len(failures) == 0 {
				t.Fatal("non-reviewed identity/count accepted")
			}
		})
	}
	if before[old.identity()] != old || before[current.identity()].Multiplicity != 0 {
		t.Fatal("comparison mutated the trusted maps")
	}
}

func TestPersistenceAuthorityDebtQ10NullableCollectorTransitionIsPinned(t *testing.T) {
	old, current := debtQ10NullableUncertaintyTransition()
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtQ10CollectorFrom, Sites: debtControlSet(old)}
	head := trusted
	head.Collector = debtQ10CollectorTo
	for _, test := range []struct {
		name   string
		sites  map[string]authorityDebtSite
		accept bool
	}{
		{"unrefreshed", trusted.Sites, true},
		{"exact-forward", debtControlSet(current), true},
		{"uncertainty-dropped", debtControlSet(), false},
		{"both-identities", debtControlSet(old, current), false},
		{"added-authority", debtControlSet(current, debtControlSite("call:Exec", 1)), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := head
			candidate.Sites = test.sites
			if err := debtValidateCollectorIdentity(trusted.Collector, candidate.Collector, trusted, candidate, ""); (err == nil) != test.accept {
				t.Fatalf("accepted=%v error=%v", test.accept, err)
			}
		})
	}
	if err := debtValidateCollectorIdentity(strings.Repeat("b", 64), head.Collector, trusted, head, ""); err == nil {
		t.Fatal("foreign collector predecessor accepted")
	}
	if err := debtValidateCollectorIdentity(trusted.Collector, strings.Repeat("b", 64), trusted, head, ""); err == nil {
		t.Fatal("unreviewed collector accepted")
	}
	root := persistenceAuthorityRepoRoot(t)
	if digest, err := debtCollectorDigest(root); err != nil || digest != debtQ10CollectorTo {
		t.Fatalf("source-pinned Q10 collector=%s actual=%s error=%v", debtQ10CollectorTo, digest, err)
	}
}

func TestPersistenceAuthorityDebtQ10NullableSourcePairIsExact(t *testing.T) {
	old, current := debtQ10NullableUncertaintyTransition()
	raw, err := os.ReadFile(filepath.Join(persistenceAuthorityRepoRoot(t), current.File))
	if err != nil {
		t.Fatal(err)
	}
	const from = "periods[index].Entity.CurrentState == nil || *periods[index].Entity.CurrentState"
	if strings.Count(string(raw), from) != 1 {
		t.Fatal("nullable assertion correction must have exactly one source occurrence")
	}
	for _, test := range []struct {
		source string
		want   authorityDebtSite
	}{
		{string(raw), current},
		{strings.Replace(string(raw), from, "periods[index].Entity.CurrentState", 1), old},
	} {
		file, err := parser.ParseFile(token.NewFileSet(), current.File, test.source, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		actual := authorityDebtSites(debtExcludedSourceFindings(t, current.File, file, &types.Info{}))
		if actual[test.want.identity()] != test.want {
			t.Fatalf("live source does not reproduce the exact reviewed identity: want=%+v actual=%+v", test.want, actual)
		}
	}
}
