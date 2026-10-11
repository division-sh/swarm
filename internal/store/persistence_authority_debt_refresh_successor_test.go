package store_test

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPersistenceAuthorityDebtRefreshedSnapshotAcceptsUnchangedSuccessor(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	collector, err := debtCollectorDigest(root)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, debtBaselinePath))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := debtBaselineWithCollector(root, data, collector, false)
	if err != nil {
		t.Fatal(err)
	}
	findings := debtLoadPersistenceAuthorityFindings(t, root)
	findings = append(findings, debtSelectedBoundaryFindings(t, root)...)
	actual := authorityDebtSites(findings)
	if !debtSitesEqual(actual, snapshot.Sites) {
		t.Fatal("refreshed snapshot does not equal the independently measured current tree")
	}
	// After merge this exact bound and source become trusted. An unchanged
	// successor must pass that same comparison, without a source-pair exception.
	if failures := authorityDebtRatchet(actual, snapshot.Sites, snapshot.Sites, actual); len(failures) != 0 {
		t.Fatalf("unchanged successor rejected by refreshed bound: %v", failures)
	}
	t.Logf("exact refreshed snapshot and unchanged successor accepted: collector=%s debt=%d", collector, authorityDebtCount(actual))
}
