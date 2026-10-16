package store_test

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const debtPublicA2EndpointCollectorTo = "4f0e55f7ea16bcc6af6eb9fcf1ca94fb441a54fcdcc9736389ac6f3c208d9ebe"
const debtPublicA2EndpointManifest = "tools/fixture-codemod/pipeline-observations/internal/publicendpoint/preimages.json"

func TestPersistenceAuthorityDebtPublicA2AccountingConservesAncestorIntermediateAndCurrent(t *testing.T) {
	ancestor, intermediate := debtQuiescenceUncertaintyTransition()
	before, current := debtPublicA2EndpointTransition()
	if before != intermediate || before.Multiplicity != 1 || current.Multiplicity != 1 {
		t.Fatal("exact A2 append changed its recorded predecessor or counted occurrence")
	}
	old, mid, now := debtControlSet(ancestor), debtControlSet(intermediate), debtControlSet(current)
	for _, test := range []struct {
		name                       string
		actual, head, base, source map[string]authorityDebtSite
		accept                     bool
	}{
		{"ancestor-unchanged", old, old, old, old, true},
		{"intermediate-unchanged", mid, mid, mid, mid, true},
		{"ancestor-forward-unrefreshed", now, old, old, old, true},
		{"ancestor-forward-refreshed", now, now, old, old, true},
		{"intermediate-forward-unrefreshed", now, mid, mid, mid, true},
		{"intermediate-forward-refreshed", now, now, mid, mid, true},
		{"composed-baseline-ancestor-source-intermediate", now, now, old, mid, true},
		{"landed-inert", now, now, now, now, true},
		{"missing-head-donor", now, debtControlSet(), mid, mid, false},
		{"missing-base-donor", now, now, debtControlSet(), mid, false},
		{"missing-source-donor", now, now, mid, debtControlSet(), false},
		{"reverse-intermediate", mid, mid, now, now, false},
		{"reverse-ancestor", old, old, now, now, false},
		{"ancestor-resurrection", old, old, old, now, false},
		{"intermediate-resurrection", mid, mid, mid, now, false},
		{"genuine-subsequent-deletion", debtControlSet(), debtControlSet(), now, now, true},
		{"deletion-no-reusable-credit", debtControlSet(current), debtControlSet(current), now, debtControlSet(), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			failures := authorityDebtRatchet(test.actual, test.head, test.base, test.source)
			if (len(failures) == 0) != test.accept {
				t.Fatalf("accepted=%v failures=%v", test.accept, failures)
			}
		})
	}
	debtPublicA2EndpointDescriptorHostiles(t, before, current)
	for _, previous := range []authorityDebtSite{ancestor, intermediate} {
		for position := range 4 {
			states := []map[string]authorityDebtSite{now, now, old, old}
			states[position] = debtControlSet(previous, current)
			if len(authorityDebtRatchet(states[0], states[1], states[2], states[3])) == 0 {
				t.Fatal("coexisting ancestor/intermediate/new receipt accepted")
			}
		}
	}
	if old[ancestor.identity()] != ancestor || mid[intermediate.identity()] != intermediate || now[current.identity()] != current {
		t.Fatal("comparison altered its immutable input evidence")
	}
}

func debtPublicA2EndpointDescriptorHostiles(t *testing.T, before, current authorityDebtSite) {
	t.Helper()
	for _, test := range []struct {
		name   string
		mutate func(*authorityDebtSite)
	}{
		{"file", func(site *authorityDebtSite) { site.File += ".foreign" }},
		{"declaration", func(site *authorityDebtSite) { site.Declaration += "Foreign" }},
		{"kind", func(site *authorityDebtSite) { site.Kind = "raw-operation" }},
		{"body", func(site *authorityDebtSite) { site.Operation += "foreign" }},
		{"context", func(site *authorityDebtSite) { site.Resolved += "foreign" }},
		{"family", func(site *authorityDebtSite) { site.Family += "foreign" }},
		{"owner", func(site *authorityDebtSite) { site.Replacement += "foreign" }},
		{"multiplicity", func(site *authorityDebtSite) { site.Multiplicity = 2 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := current
			test.mutate(&changed)
			if len(authorityDebtRatchet(debtControlSet(changed), debtControlSet(changed), debtControlSet(before), debtControlSet(before))) == 0 {
				t.Fatal("different current uncertainty descriptor accepted")
			}
			changed = before
			test.mutate(&changed)
			for position := 1; position < 4; position++ {
				states := []map[string]authorityDebtSite{debtControlSet(current), debtControlSet(before), debtControlSet(before), debtControlSet(before)}
				states[position] = debtControlSet(changed)
				if len(authorityDebtRatchet(states[0], states[1], states[2], states[3])) == 0 {
					t.Fatal("mismatched comparison donor accepted")
				}
			}
		})
	}
}

func debtPublicA2EndpointSourceCopy(t *testing.T) string {
	t.Helper()
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	copyRepair2698CustodySources(t, root, copy)
	_, current := debtPublicA2EndpointTransition()
	for _, path := range []string{current.File, debtPublicA2EndpointManifest} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	return copy
}

func TestPersistenceAuthorityDebtPublicA2SourceAndManifestRejectUnknownBodies(t *testing.T) {
	copy := debtPublicA2EndpointSourceCopy(t)
	if err := debtPublicA2EndpointSourcePins(copy); err != nil {
		t.Fatal(err)
	}
	_, current := debtPublicA2EndpointTransition()
	source, err := os.ReadFile(filepath.Join(copy, current.File))
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"rt.Endpoint", "operating.report.triggered", "reflect.DeepEqual", "join.OutcomePending"} {
		if !strings.Contains(string(source), cut) {
			t.Fatal("hostile source control lost its original cut:", cut)
		}
		mutant := strings.Replace(string(source), cut, "foreignA2Cut", 1)
		debtWriteModuleSource(t, copy, current.File, mutant)
		if err := debtPublicA2EndpointSourcePins(copy); err == nil {
			t.Fatal("changed complete excluded-source body accepted:", cut)
		}
	}
	debtWriteModuleSource(t, copy, current.File, string(source))
	manifest, err := os.ReadFile(filepath.Join(copy, debtPublicA2EndpointManifest))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtPublicA2EndpointManifest, string(manifest)+" ")
	if err := debtPublicA2EndpointSourcePins(copy); err == nil {
		t.Fatal("changed reviewed source manifest accepted")
	}
	debtWriteModuleSource(t, copy, debtPublicA2EndpointManifest, string(manifest))
	if err := os.Remove(filepath.Join(copy, current.File)); err != nil {
		t.Fatal(err)
	}
	if err := debtPublicA2EndpointSourcePins(copy); err == nil {
		t.Fatal("missing owned source accepted")
	}
}

func TestPersistenceAuthorityDebtPublicA2CollectorIsPinnedAndConservesDebt(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	digest, err := debtCollectorDigest(root)
	if err != nil || digest != debtActiveCollectorTo {
		t.Fatalf("current collector=%s pinned=%s err=%v", digest, debtActiveCollectorTo, err)
	}
	ancestor, _ := debtQuiescenceUncertaintyTransition()
	old, current := debtPublicA2EndpointTransition()
	for _, predecessor := range []authorityDebtSite{ancestor, old} {
		trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtServedDebugCollectorTo, Sites: debtControlSet(predecessor)}
		head := trusted
		head.Collector, head.Sites = debtPublicA2EndpointCollectorTo, debtControlSet(current)
		if !debtPublicA2EndpointCollectorTransition(trusted.Collector, head.Collector, trusted, head) {
			t.Fatal("exact counted predecessor rotation refused")
		}
		head.Sites = debtControlSet()
		if debtPublicA2EndpointCollectorTransition(trusted.Collector, head.Collector, trusted, head) {
			t.Fatal("dropping uncertainty admitted as collector metadata migration")
		}
	}
}

func TestPersistenceAuthorityDebtPublicA2UnrefreshedMetadataIsLocalAndSourceBound(t *testing.T) {
	copy := debtPublicA2EndpointSourceCopy(t)
	old, current := debtPublicA2EndpointTransition()
	baseline := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtCacheCollectorFrom, Sites: debtControlSet(old)}
	data := marshalAuthorityDebtBaseline(baseline)
	metadata, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: debtServedDebugCollectorTo, BaselineSHA256: debtBaselineChecksum(data), Predecessor: debtCacheCollectorFrom})
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata))
	t.Setenv(debtRefreshEnv, "downward")
	t.Setenv("CI", "")
	t.Setenv("GITHUB_ACTIONS", "")
	got, err := debtBaselineWithCollector(copy, data, debtPublicA2EndpointCollectorTo, false)
	if err != nil || got.Collector != debtPublicA2EndpointCollectorTo || !debtSitesEqual(got.Sites, baseline.Sites) {
		t.Fatal("exact local metadata rotation changed counted uncertainty:", err)
	}
	for _, mode := range []string{"read-only", "CI", "GITHUB_ACTIONS", "historical", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "read-only" {
				t.Setenv(debtRefreshEnv, "")
			}
			if mode == "CI" || mode == "GITHUB_ACTIONS" {
				t.Setenv(mode, "true")
			}
			if mode == "malformed" {
				debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata)+" ")
				defer debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata))
			}
			if _, err := debtBaselineWithCollector(copy, data, debtPublicA2EndpointCollectorTo, mode == "historical"); err == nil {
				t.Fatal("unrefreshed metadata escaped exact local downward rotation")
			}
		})
	}
	for _, changed := range []map[string]authorityDebtSite{debtControlSet(), debtControlSet(old, current), debtControlSet(current)} {
		broken := baseline
		broken.Sites = maps.Clone(changed)
		bytes := marshalAuthorityDebtBaseline(broken)
		metadata, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: debtServedDebugCollectorTo, BaselineSHA256: debtBaselineChecksum(bytes), Predecessor: debtCacheCollectorFrom})
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata))
		if _, err := debtBaselineWithCollector(copy, bytes, debtPublicA2EndpointCollectorTo, false); err == nil {
			t.Fatal("missing/coexisting/nonpredecessor donor accepted")
		}
	}
}
