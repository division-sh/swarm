package store_test

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const debtServedObservationCollectorTo = "51f998119a27523e0d4ae44aae8184a844745af6d8d0f66219c0d847978513f5"

// Ruling6161771753: source-entry placement only, never additional debt credit.
func debtServedObservationCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtServedObservationCollectorTo || head.Collector != current || len(proofs) != 1 || trusted.BootstrapSource != head.BootstrapSource {
		return false
	}
	root := proofs[0].Root
	actual, err := debtCollectorDigest(root)
	if err != nil || actual != current {
		return false
	}
	if _, err := debtServedObservationLiveSources(root); err != nil || debtServedDebugSourcePins(root) != nil {
		return false
	}
	if base == debtPublicRollbackCollectorTo {
		if trusted.Collector != base {
			return false
		}
	} else {
		intermediate := head
		intermediate.Collector = debtPublicRollbackCollectorTo
		if err := debtValidateCollectorIdentity(base, intermediate.Collector, trusted, intermediate, "", proofs...); err != nil {
			return false
		}
	}
	return len(authorityDebtRatchet(proofs[0].Actual, head.Sites, trusted.Sites, trusted.Sites, proofs...)) == 0
}

func TestPersistenceAuthorityDebtServedObservationCollectorRetainsTrustedPredecessorCeiling(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	base, err := debtTrustedBase(root)
	if err != nil {
		t.Fatal(err)
	}
	baseRoot := materializeDebtBase(t, root, base)
	baseCollector, err := debtCollectorDigest(baseRoot)
	if err != nil {
		t.Fatal(err)
	}
	baseRaw, err := os.ReadFile(filepath.Join(baseRoot, debtBaselinePath))
	if err != nil {
		t.Fatal(err)
	}
	trusted, err := debtBaselineWithCollector(baseRoot, baseRaw, baseCollector, true)
	if err != nil {
		t.Fatal(err)
	}
	headRaw, err := os.ReadFile(filepath.Join(root, debtBaselinePath))
	if err != nil {
		t.Fatal(err)
	}
	head, err := debtBaselineWithCollector(root, headRaw, debtServedObservationCollectorTo, false)
	if err != nil {
		t.Fatal(err)
	}
	// This is retained-ceiling metadata proof, not a substitute for the census.
	proof := debtDeliveryRetirementProof{Root: root, Actual: head.Sites}
	prior := head
	prior.Collector = debtPublicRollbackCollectorTo
	if err := debtValidateCollectorIdentity(baseCollector, prior.Collector, trusted, prior, "", proof); err != nil {
		t.Fatal("unchanged predecessor chain refused retained ceiling:", baseCollector, err)
	}
	if err := debtValidateCollectorIdentity(baseCollector, head.Collector, trusted, head, "", proof); err != nil {
		t.Fatal("new entry-only collector refused retained ceiling:", err, authorityDebtRatchet(head.Sites, head.Sites, trusted.Sites, trusted.Sites, proof))
	}
}

func TestPersistenceAuthorityDebtServedObservationCollectorFingerprintIsExact(t *testing.T) {
	actual, err := debtCollectorDigest(persistenceAuthorityRepoRoot(t))
	t.Logf("served observation actual collector=%s", actual)
	if err != nil || actual != debtServedObservationCollectorTo {
		t.Fatal("served observation exact collector identity changed:", actual, err)
	}
}

func TestPersistenceAuthorityDebtServedObservationCollectorTransitionIsOneWayAndAddsNoCredit(t *testing.T) {
	root, sites := persistenceAuthorityRepoRoot(t), debtControlSet()
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtPublicRollbackCollectorTo, Sites: sites}
	head := trusted
	head.Collector = debtServedObservationCollectorTo
	proof := debtDeliveryRetirementProof{Root: root, Actual: sites}
	if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, "", proof); err != nil {
		t.Fatal("exact zero-credit source-only transition refused:", err)
	}
	withDonor := trusted
	withDonor.Sites = maps.Clone(sites)
	donor := debtControlSite("call:QueryRow", 2)
	withDonor.Sites[donor.identity()] = donor
	added := maps.Clone(sites)
	escape := debtControlSite("call:Exec", 1)
	added[escape.identity()] = escape
	wrongBootstrap := head
	wrongBootstrap.BootstrapSource = strings.Repeat("b", 40)
	wrongMetadata := head
	wrongMetadata.Collector = strings.Repeat("b", 64)
	addedHead := head
	addedHead.Sites = added
	for _, state := range []struct {
		base, current string
		trusted, head authorityDebtBaseline
		proofs        []debtDeliveryRetirementProof
	}{
		{head.Collector, trusted.Collector, head, trusted, []debtDeliveryRetirementProof{proof}},
		{trusted.Collector, strings.Repeat("b", 64), trusted, head, []debtDeliveryRetirementProof{proof}},
		{trusted.Collector, head.Collector, trusted, wrongMetadata, []debtDeliveryRetirementProof{proof}},
		{trusted.Collector, head.Collector, trusted, wrongBootstrap, []debtDeliveryRetirementProof{proof}},
		{trusted.Collector, head.Collector, trusted, head, nil},
		{trusted.Collector, head.Collector, trusted, head, []debtDeliveryRetirementProof{proof, proof}},
		{trusted.Collector, head.Collector, trusted, head, []debtDeliveryRetirementProof{{Root: t.TempDir(), Actual: sites}}},
		{withDonor.Collector, head.Collector, withDonor, addedHead, []debtDeliveryRetirementProof{{Root: root, Actual: added}}},
	} {
		if err := debtValidateCollectorIdentity(state.base, state.current, state.trusted, state.head, "", state.proofs...); err == nil {
			t.Fatal("source entry repair admitted reverse/foreign/unbound metadata or payment by deletion")
		}
	}
}
