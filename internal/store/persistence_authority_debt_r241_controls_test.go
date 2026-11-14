package store_test

import (
	"strings"
	"testing"
)

// Only the destination is outside the hashed permission source, as for S1.
// Derived with the standalone canonical AST hash; no census or test was run.
const debtR241CollectorTo = "be31d2831c50e2894d8362acd092395216c9ce0f537fc4f0825216f0db706cd9"

func debtR241ControlPrior() map[string]authorityDebtSite {
	sites := debtControlSet(debtControlSite("call:Unrelated", 2))
	for _, entry := range debtR241NamedSites() {
		if entry.Prior != 0 {
			prior := entry.Site
			prior.Multiplicity = entry.Prior
			sites[prior.identity()] = prior
		}
	}
	return sites
}

func debtR241ControlClone(sites map[string]authorityDebtSite) map[string]authorityDebtSite {
	out := make(map[string]authorityDebtSite, len(sites))
	for key, site := range sites {
		out[key] = site
	}
	return out
}

func TestPersistenceAuthorityDebtR241ExactAddition(t *testing.T) {
	entries := debtR241NamedSites()
	seen := map[string]bool{}
	priorCount, currentCount := 0, 0
	for _, entry := range entries {
		key := entry.Site.identity()
		if seen[key] || entry.Site.Multiplicity <= entry.Prior || entry.Prior < 0 {
			t.Fatalf("duplicate or non-increasing R241 entry: %+v", entry)
		}
		seen[key] = true
		priorCount += entry.Prior
		currentCount += entry.Site.Multiplicity
	}
	if len(seen) != 23 || priorCount != 2 || currentCount != 34 {
		t.Fatalf("R241 counts keys=%d prior=%d current=%d", len(seen), priorCount, currentCount)
	}
	base := debtR241ControlPrior()
	head := debtWithR241NamedSites(base)
	actual, source := debtR241ControlClone(head), debtR241ControlClone(base)
	snapshots := []map[string]authorityDebtSite{debtR241ControlClone(actual), debtR241ControlClone(head), debtR241ControlClone(base), debtR241ControlClone(source)}
	if len(authorityDebtRatchet(actual, head, base, source)) == 0 {
		t.Fatal("DATA alone bypassed the original comparisons")
	}
	forwardBase, forwardSource, err := debtR241Comparisons(actual, head, base, source, debtR241CollectorFrom, debtR241CollectorTo)
	if err != nil {
		t.Fatal(err)
	}
	if failures := authorityDebtRatchet(actual, head, forwardBase, forwardSource); len(failures) != 0 {
		t.Fatal(failures)
	}
	for i, original := range []map[string]authorityDebtSite{actual, head, base, source} {
		if !debtSitesEqual(original, snapshots[i]) {
			t.Fatalf("comparison changed original map %d", i)
		}
	}
	delete(forwardBase, entries[0].Site.identity())
	if !debtSitesEqual(head, forwardSource) || !debtSitesEqual(base, snapshots[2]) || !debtSitesEqual(source, snapshots[3]) {
		t.Fatal("forward maps alias each other or the original maps")
	}

	fields := []struct {
		name   string
		change func(*authorityDebtSite)
	}{
		{"kind", func(s *authorityDebtSite) { s.Kind += "Other" }},
		{"file", func(s *authorityDebtSite) { s.File += "Other" }},
		{"declaration", func(s *authorityDebtSite) { s.Declaration += "Other" }},
		{"operation", func(s *authorityDebtSite) { s.Operation += "Other" }},
		{"resolved", func(s *authorityDebtSite) { s.Resolved += "Other" }},
		{"family", func(s *authorityDebtSite) { s.Family += "Other" }},
		{"replacement", func(s *authorityDebtSite) { s.Replacement += "Other" }},
		{"overcount", func(s *authorityDebtSite) { s.Multiplicity++ }},
		{"undercount", func(s *authorityDebtSite) { s.Multiplicity-- }},
	}
	for _, entry := range entries {
		key := entry.Site.identity()
		for _, target := range []string{"actual", "head"} {
			t.Run(target+"/missing/"+key, func(t *testing.T) {
				a, h := debtR241ControlClone(actual), debtR241ControlClone(head)
				if target == "actual" {
					delete(a, key)
				} else {
					delete(h, key)
				}
				if _, _, err := debtR241Comparisons(a, h, base, source, debtR241CollectorFrom, debtR241CollectorTo); err == nil {
					t.Fatal("missing exact initial site accepted")
				}
			})
			for _, field := range fields {
				t.Run(target+"/"+field.name+"/"+key, func(t *testing.T) {
					a, h := debtR241ControlClone(actual), debtR241ControlClone(head)
					changed := entry.Site
					field.change(&changed)
					if target == "actual" {
						delete(a, key)
						a[changed.identity()] = changed
					} else {
						delete(h, key)
						h[changed.identity()] = changed
					}
					if _, _, err := debtR241Comparisons(a, h, base, source, debtR241CollectorFrom, debtR241CollectorTo); err == nil {
						t.Fatal("changed initial full tuple accepted")
					}
				})
			}
		}
		for _, target := range []string{"base", "source"} {
			t.Run(target+"/wrong-prior/"+key, func(t *testing.T) {
				b, s := debtR241ControlClone(base), debtR241ControlClone(source)
				wrong := entry.Site
				if target == "base" {
					b[key] = wrong
				} else {
					s[key] = wrong
				}
				if _, _, err := debtR241Comparisons(actual, head, b, s, debtR241CollectorFrom, debtR241CollectorTo); err == nil {
					t.Fatal("changed original multiplicity accepted")
				}
			})
		}
	}
	for _, target := range []string{"actual", "head"} {
		t.Run("foreign/"+target, func(t *testing.T) {
			a, h := debtR241ControlClone(actual), debtR241ControlClone(head)
			foreign := debtControlSite("call:Foreign", 1)
			if target == "actual" {
				a[foreign.identity()] = foreign
			} else {
				h[foreign.identity()] = foreign
			}
			b, s, err := debtR241Comparisons(a, h, base, source, debtR241CollectorFrom, debtR241CollectorTo)
			if err == nil && len(authorityDebtRatchet(a, h, b, s)) == 0 {
				t.Fatal("foreign key bypassed the ordinary comparisons")
			}
		})
	}
}

func TestPersistenceAuthorityDebtR241LandedRemovalCannotResurrect(t *testing.T) {
	prior := debtR241ControlPrior()
	landed := debtWithR241NamedSites(prior)
	for _, entry := range debtR241NamedSites() {
		key := entry.Site.identity()
		t.Run(key, func(t *testing.T) {
			removed := debtR241ControlClone(landed)
			delete(removed, key)
			b, s, err := debtR241Comparisons(removed, landed, landed, landed, debtR241CollectorTo, debtR241CollectorTo)
			if err != nil || !debtSitesEqual(b, landed) || !debtSitesEqual(s, landed) || len(authorityDebtRatchet(removed, landed, b, s)) != 0 {
				t.Fatal("landed bridge did not stay inert for removal")
			}
			// Even a retained historical head ceiling cannot resurrect a removed source site.
			b, s, err = debtR241Comparisons(landed, landed, landed, removed, debtR241CollectorTo, debtR241CollectorTo)
			if err != nil || !debtSitesEqual(s, removed) || len(authorityDebtRatchet(landed, landed, b, s)) == 0 {
				t.Fatal("landed exception resurrected a retired site")
			}
			b, s, err = debtR241Comparisons(landed, landed, removed, removed, debtR241CollectorTo, debtR241CollectorTo)
			if err != nil || len(authorityDebtRatchet(landed, landed, b, s)) == 0 {
				t.Fatal("retired baseline/source accepted resurrection")
			}
			if entry.Site.Multiplicity > 1 {
				reduced := debtR241ControlClone(landed)
				partial := entry.Site
				partial.Multiplicity--
				reduced[key] = partial
				b, s, err = debtR241Comparisons(landed, landed, landed, reduced, debtR241CollectorTo, debtR241CollectorTo)
				if err != nil || len(authorityDebtRatchet(landed, landed, b, s)) == 0 {
					t.Fatal("landed exception restored a retired occurrence")
				}
			}
		})
	}
}

func TestPersistenceAuthorityDebtR241MetadataIsExact(t *testing.T) {
	if !authorityDebtHex(debtR241CollectorTo, 64) || debtR241CollectorTo == debtR241CollectorFrom {
		t.Fatal("R241 destination must be a distinct exact fingerprint")
	}
	prior := authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: debtR241CollectorFrom, Sites: debtR241ControlPrior()}
	next := prior
	next.Collector, next.Sites = debtR241CollectorTo, debtWithR241NamedSites(prior.Sites)
	for _, tc := range []struct {
		name          string
		base, current string
		trusted, head authorityDebtBaseline
		accept        bool
	}{
		{"exact", debtR241CollectorFrom, debtR241CollectorTo, prior, next, true},
		{"landed", debtR241CollectorTo, debtR241CollectorTo, next, next, true},
		{"foreign-base", strings.Repeat("a", 64), debtR241CollectorTo, prior, next, false},
		{"foreign-current", debtR241CollectorFrom, strings.Repeat("a", 64), prior, next, false},
		{"reverse", debtR241CollectorTo, debtR241CollectorFrom, next, prior, false},
		{"unrotated", debtR241CollectorFrom, debtR241CollectorTo, prior, prior, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := debtValidateCollectorIdentity(tc.base, tc.current, tc.trusted, tc.head, tc.current); (err == nil) != tc.accept {
				t.Fatalf("metadata acceptance=%v err=%v", tc.accept, err)
			}
		})
	}
	for _, target := range []string{"bootstrap", "trusted-collector", "head-collector", "foreign-site"} {
		t.Run(target, func(t *testing.T) {
			trusted, head := prior, next
			head.Sites = debtR241ControlClone(next.Sites)
			switch target {
			case "bootstrap":
				head.BootstrapSource = strings.Repeat("a", 40)
			case "trusted-collector":
				trusted.Collector = strings.Repeat("a", 64)
			case "head-collector":
				head.Collector = strings.Repeat("a", 64)
			case "foreign-site":
				s := debtControlSite("call:Foreign", 1)
				head.Sites[s.identity()] = s
			}
			if err := debtValidateCollectorIdentity(debtR241CollectorFrom, debtR241CollectorTo, trusted, head, debtR241CollectorTo); err == nil {
				t.Fatal("changed metadata or baseline accepted")
			}
		})
	}
	if err := debtValidateCollectorIdentity(debtR241CollectorFrom, debtR241CollectorTo, prior, next, ""); err == nil {
		t.Fatal("unreviewed destination token accepted")
	}
	encoded := next
	encoded.Collector = debtCacheCollectorFrom
	data := marshalAuthorityDebtBaseline(encoded)
	baseline, err := parseAuthorityDebtBaseline(data)
	if err != nil {
		t.Fatal(err)
	}
	if baseline.BootstrapSource != debtBootstrapSource || baseline.Collector != debtCacheCollectorFrom {
		t.Fatal("historical baseline headers changed")
	}
	for _, entry := range debtR241NamedSites() {
		if baseline.Sites[entry.Site.identity()] != entry.Site {
			t.Fatal("R241 baseline row differs from exact named debt")
		}
	}
	line := entryR241ControlLine(debtR241NamedSites()[0].Site)
	if _, err := parseAuthorityDebtBaseline(append(append([]byte(nil), data...), []byte(line)...)); err == nil {
		t.Fatal("duplicate R241 baseline row accepted")
	}
}

func entryR241ControlLine(site authorityDebtSite) string {
	data := marshalAuthorityDebtBaseline(authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: debtCacheCollectorFrom, Sites: debtControlSet(site)})
	for _, line := range strings.Split(string(data), "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			return line + "\n"
		}
	}
	return ""
}
