package store_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var debtPublicRollbackPins = [3]debtRunIdentitySourcePin{
	{"internal/serveapp/test_command_runtime_test.go", "requirePublicInputRollbackNoResidue", "204a0f50aaed0325143aed87714aef533524d1e40119717131c55a0050716680", "ed144aa595810f7300549ec28e44f856f858ebdc1b549846761522e67737538b"},
	{"internal/serveapp/test_command_runtime_test.go", "TestRootConnectedTemplatePublicationRollbackIsAtomicAcrossSelectedStores", "89e32e53bd62d0e8059bada4a2a80929ab69634620ed5ea1062fb52d4aee74da", "db862eff5e55f1e01179caf1b5b13ff459a7908103b3967482886873ab5e3eea"},
	{"internal/serveapp/test_command_runtime_test.go", "TestRootConnectedTemplatePublicationRollsBackAfterDeliveryAndCompletionAcrossSelectedStores", "eea8dd9dc9c5844ac1910db7eaac14fb03e8a7edb606aa9453f8281c33307395", "15953ed967b4bcccefe591da71113ecfc174cc9a7ecbd5567421e469e0ba769a"},
}

const debtPublicRollbackCollectorFrom = debtRunIdentityCollectorTo
const debtPublicRollbackCollectorTo = "4e3e2cb0469e008c3b2e7c516c1152a60b00589934e7a1a54ef3628a27eb2b68"

func debtPublicRollbackSourcePrevious(root, file, function, current string) (string, error) {
	for _, pin := range debtPublicRollbackPins {
		if pin.File != file || pin.Function != function || debtBaselineChecksum([]byte(current)) != pin.CurrentSHA256 {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/family145-public-rollback-preimages.json"))
		if err != nil {
			return "", err
		}
		previous, err := debtPublicRollbackExactSourceInverse(raw, pin, current)
		if err != nil {
			return "", err
		}
		if err := debtPublicRollbackNativePins(root); err != nil {
			return "", err
		}
		return previous, nil
	}
	return current, nil
}

func debtPublicRollbackExactSourceInverse(raw []byte, pin debtRunIdentitySourcePin, current string) (string, error) {
	if debtBaselineChecksum(raw) != "0806a88943bacd967a72a39fb5c92f078443af44dd4d444ac62c3745a8deceb5" {
		return "", fmt.Errorf("rollback exact manifest changed")
	}
	var bodies []debtRunIdentitySourceBody
	if err := json.Unmarshal(raw, &bodies); err != nil || len(bodies) != 3 {
		return "", fmt.Errorf("rollback complete source inventory changed: %v", err)
	}
	return debtRunIdentityUniqueBodyInverse(bodies, pin, current)
}

func debtPublicRollbackNativePins(root string) error {
	for _, pin := range [][2]string{
		{"internal/store/internal/backend/pipelinepersistence/template_storage_observation.go", "9710f979a5d8ac8f552f4f9b8bd1000447fd164aa69c1719879aad723a37323a"},
		{"internal/store/internal/runtimepersistence/test_served_delivery_observation.go", "bb5b2b2ec0c32fc6153b712be59cf827c2e858e02b54a7c5cf541b60aac96be3"},
		{"internal/store/storetest/template_delivery_storage.go", "2c9f647c25d159eaf7a8a7a4827e49fcc0dea4c00bd5222649fafdfec7fdca42"},
	} {
		raw, err := os.ReadFile(filepath.Join(root, pin[0]))
		if err != nil || debtBaselineChecksum(raw) != pin[1] {
			return fmt.Errorf("rollback native endpoint changed: %s: %v", pin[0], err)
		}
	}
	return nil
}

func debtPublicRollbackCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtPublicRollbackCollectorTo || head.Collector != current || len(proofs) != 1 || trusted.BootstrapSource != head.BootstrapSource || debtPublicRollbackNativePins(proofs[0].Root) != nil || debtRequestKeySourcePins(proofs[0].Root) != nil || debtServedDebugSourcePins(proofs[0].Root) != nil {
		return false
	}
	if base == debtPublicRollbackCollectorFrom {
		if trusted.Collector != base {
			return false
		}
	} else {
		intermediate := head
		intermediate.Collector = debtPublicRollbackCollectorFrom
		if debtValidateCollectorIdentity(base, intermediate.Collector, trusted, intermediate, "", proofs...) != nil {
			return false
		}
	}
	return len(authorityDebtRatchet(proofs[0].Actual, head.Sites, trusted.Sites, trusted.Sites, proofs...)) == 0
}

func TestPersistenceAuthorityDebtPublicRollbackSourceCompositionIsFinite(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/family145-public-rollback-preimages.json"))
	if err != nil {
		t.Fatal(err)
	}
	var bodies []debtRunIdentitySourceBody
	if err := json.Unmarshal(raw, &bodies); err != nil {
		t.Fatal(err)
	}
	decls, err := debtServedDebugDeclarations(filepath.Join(root, debtPublicRollbackPins[0].File))
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range debtPublicRollbackPins {
		current := decls[pin.Function]
		previous, err := debtPublicRollbackSourcePrevious(root, pin.File, pin.Function, current)
		if err != nil || debtBaselineChecksum([]byte(previous)) != pin.BeforeSHA256 {
			t.Fatal("rollback exact inverse refused:", pin.Function, err)
		}
		for _, key := range [][2]string{{"foreign.go", pin.Function}, {pin.File, "foreignFunction"}} {
			if got, err := debtPublicRollbackSourcePrevious(root, key[0], key[1], current); err != nil || got != current {
				t.Fatal("foreign source received inverse:", err)
			}
		}
		for _, cut := range []string{"reader", "InspectSnapshot", "runID", "eventID", "count != 0", "t.Fatalf", "query.label", "context.Background()"} {
			if strings.Contains(current, cut) {
				mutant := strings.Replace(current, cut, "foreignRollbackCut", 1)
				if _, err := debtPublicRollbackExactSourceInverse(raw, pin, mutant); err == nil {
					t.Fatal("changed exact rollback cut accepted:", cut)
				}
			}
		}
		if _, err := debtPublicRollbackExactSourceInverse(append(append([]byte{}, raw...), ' '), pin, current); err == nil {
			t.Fatal("changed manifest digest accepted")
		}
		debtRunIdentityHostileRows(t, bodies, pin, current)
	}
	if err := debtRequestKeySourcePins(root); err != nil {
		t.Fatal(err)
	}
}

func TestPersistenceAuthorityDebtPublicRollbackCollectorAddsNoInverseCredit(t *testing.T) {
	root, current := persistenceAuthorityRepoRoot(t), debtControlSet()
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtPublicRollbackCollectorFrom, Sites: current}
	head := trusted
	head.Collector = debtPublicRollbackCollectorTo
	proof := debtDeliveryRetirementProof{Root: root, Actual: current}
	if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, "", proof); err != nil {
		t.Fatal("exact one-way collector recomputation refused:", err)
	}
	withDonor := trusted
	withDonor.Sites = maps.Clone(current)
	donor := debtControlSite("call:QueryRow", 1)
	withDonor.Sites[donor.identity()] = donor
	added := maps.Clone(current)
	escape := debtControlSite("call:Exec", 1)
	added[escape.identity()] = escape
	for _, state := range []struct {
		base, current string
		trusted, head authorityDebtBaseline
		proofs        []debtDeliveryRetirementProof
	}{
		{head.Collector, trusted.Collector, head, trusted, []debtDeliveryRetirementProof{proof}},
		{trusted.Collector, strings.Repeat("b", 64), trusted, head, []debtDeliveryRetirementProof{proof}},
		{trusted.Collector, head.Collector, trusted, head, nil},
		{trusted.Collector, head.Collector, trusted, head, []debtDeliveryRetirementProof{proof, proof}},
		{trusted.Collector, head.Collector, trusted, head, []debtDeliveryRetirementProof{{Root: t.TempDir(), Actual: current}}},
		{withDonor.Collector, head.Collector, withDonor, head, []debtDeliveryRetirementProof{{Root: root, Actual: added}}},
	} {
		if err := debtValidateCollectorIdentity(state.base, state.current, state.trusted, state.head, "", state.proofs...); err == nil {
			t.Fatal("inverse admitted reverse/foreign/missing source or deletion paying for added escape")
		}
	}
}

func TestPersistenceAuthorityDebtPublicRollbackHistoricalCallerDeletionStillNeedsDisposition(t *testing.T) {
	pairs := debtRequestKeySignatureTransitions()
	before := debtControlSet(pairs[0][0], pairs[1][0])
	after := debtControlSet(pairs[0][1], pairs[1][1])
	actual := debtControlSet()
	if failures := authorityDebtRatchet(after, after, before, before); len(failures) != 0 {
		t.Fatal("original exact signature transfer stopped working:", failures)
	}
	failures := authorityDebtRatchet(actual, after, before, before)
	if len(failures) != 2 {
		t.Fatal("expected precisely the two historical head-to-base identities, not added authority:", failures)
	}
	for _, failure := range failures {
		if !strings.HasPrefix(failure, "head -> trusted baseline new/increased site: internal/serveapp/test_command_runtime_test.go") || !strings.Contains(failure, "requirePublicInputRollbackNoResidue") {
			t.Fatal("unexpected historical deletion refusal:", failure)
		}
		t.Log(failure)
	}
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtPublicRollbackCollectorFrom, Sites: before}
	head := trusted
	head.Collector, head.Sites = debtPublicRollbackCollectorTo, after
	if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, "", debtDeliveryRetirementProof{Root: persistenceAuthorityRepoRoot(t), Actual: actual}); err == nil {
		t.Fatal("source-only inverse silently permitted historical pair retirement")
	}
}
