package store_test

import (
	"strings"
	"testing"
)

// Only the reviewed identity pair is separate from the two hashed policy files,
// avoiding a self-referential digest. Permission checks remain hashed policy.
const debtG01CollectorFrom = "494fd3b6300c4163241395ef9e3aa59ce58eb32f45e9f5d8bc5a5078401303d5"
const debtG01CollectorTo = "de36becf443af2b3da7724f72e7a664e6f3f83f1a84b9ec02a365536629ce749"

func TestPersistenceAuthorityDebtG01TransitionIsExactAndMetadataOnly(t *testing.T) {
	digest, err := debtCollectorDigest(persistenceAuthorityRepoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("collector=%s", digest)
	if digest != debtG01CollectorTo {
		t.Fatalf("G01's exact reviewed destination does not match actual policy: %s", digest)
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
			if err := debtValidateCollectorIdentity(tc.base, tc.collector, tc.trusted, tc.head); (err == nil) != tc.accept {
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
			if err := debtValidateCollectorIdentity(debtG01CollectorFrom, debtG01CollectorTo, trusted, head); err == nil {
				t.Fatal("transition admitted a non-metadata baseline change")
			}
		})
	}
}
