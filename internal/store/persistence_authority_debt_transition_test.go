package store_test

import (
	"strings"
	"testing"
)

// Only the destination is outside the hashed policy to avoid a self-referential
// digest. The origin and permission checks are hashed, so the transition cannot
// be reactivated after landing by editing this destination alone.
const debtG01CollectorTo = "b42ea974646e7b666459091174645baa501d91da16871813568db23858def7b7"
const debtCacheCollectorTo = "9838ebeda35431046f2c855e7b941997a846b1abc7ccbcf6a1f1fd7243c3e669"

func TestPersistenceAuthorityDebtG01TransitionIsExactAndMetadataOnly(t *testing.T) {
	digest, err := debtCollectorDigest(persistenceAuthorityRepoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("collector=%s", digest)
	if digest != debtCacheCollectorTo {
		t.Fatalf("the cache's exact reviewed destination does not match actual policy: %s", digest)
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

func TestPersistenceAuthorityDebtCacheTransitionIsExactAndMetadataOnly(t *testing.T) {
	old := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtCacheCollectorFrom, Sites: debtControlSet(debtControlSite("call:QueryRow", 2))}
	current := old
	current.Collector = debtCacheCollectorTo
	for _, tc := range []struct {
		name, base, collector string
		trusted, head         authorityDebtBaseline
		accept                bool
	}{
		{"normal-predecessor", debtCacheCollectorFrom, debtCacheCollectorFrom, old, old, true},
		{"exact-transition", debtCacheCollectorFrom, debtCacheCollectorTo, old, current, true},
		{"inert-after-landing", debtCacheCollectorTo, debtCacheCollectorTo, current, current, true},
		{"foreign-base", strings.Repeat("b", 64), debtCacheCollectorTo, old, current, false},
		{"foreign-head", debtCacheCollectorFrom, strings.Repeat("c", 64), old, current, false},
		{"reverse", debtCacheCollectorTo, debtCacheCollectorFrom, current, old, false},
		{"unrotated-metadata", debtCacheCollectorFrom, debtCacheCollectorTo, old, old, false},
		{"future-collector", debtCacheCollectorTo, strings.Repeat("c", 64), current, current, false},
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
			if err := debtValidateCollectorIdentity(debtCacheCollectorFrom, debtCacheCollectorTo, trusted, head); err == nil {
				t.Fatal("cache transition admitted a non-metadata baseline change")
			}
		})
	}
}
