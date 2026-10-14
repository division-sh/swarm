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

type debtRunIdentitySourcePin struct {
	File, Function, BeforeSHA256, CurrentSHA256 string
}

var debtRunIdentitySourcePins = [3]debtRunIdentitySourcePin{
	{"internal/serveapp/reset_served_parity_test.go", "TestServedResetRetainClearAndHistoricalReplayBothStores", "2e67b209141456eee87b9db4c954477fee9465b35fa2fe6e1fbe1b32c9a3c6f6", "c822417546a88e925daa1c3c5f687a94197332dcd2d156cde3004a4882666813"},
	{"internal/serveapp/reset_convergence_receipt_test.go", "proveServedResetFinalReceiptFailure", "92710ce05ec379e7b1a590fa43226eef3d26852fba8737a54612129bd5041531", "0fd2f10af111ec3b06c32412e26e5aac9aed23024b113e171bb949f6ed967e2c"},
	{"internal/serveapp/selected_reset_admission_test.go", "TestSelectedResetPreparationAdmissionOrderBothStores", "3734f550bb0dd76a1d3b72eba09acfcd903d7cb412b48a5efc160776b272e6cc", "75aa93875bd113e42929f271bd7cbbadb06a67fae2371009db5a3d2a3256a04f"},
}

const debtRunIdentityCollectorFrom = "64b8aced371482a1b0ef4e7e9552d39a6190f122b8ae231e8c51a898b6fe7d44"
const debtRunIdentityCollectorTo = "f9326c34325de36fa3fe8e12ea658f37e7d5540288a198e3cbd7242adc89685b"

func debtRunIdentityCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtRunIdentityCollectorTo || head.Collector != current || len(proofs) != 1 ||
		trusted.BootstrapSource != head.BootstrapSource || debtRunIdentityNativeSourcePins(proofs[0].Root) != nil || debtServedDebugSourcePins(proofs[0].Root) != nil {
		return false
	}
	if base == debtRunIdentityCollectorFrom {
		if trusted.Collector != base {
			return false
		}
	} else {
		intermediate := head
		intermediate.Collector = debtRunIdentityCollectorFrom
		if debtValidateCollectorIdentity(base, intermediate.Collector, trusted, intermediate, "", proofs...) != nil {
			return false
		}
	}
	return len(authorityDebtRatchet(proofs[0].Actual, head.Sites, trusted.Sites, trusted.Sites, proofs...)) == 0
}

type debtRunIdentitySourceBody struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

// Exact source evidence only: no census occurrence or signature is rewritten.
func debtRunIdentitySourcePrevious(root, file, function, current string) (string, error) {
	var err error
	current, err = debtSourceBlobResetPrevious(root, file, function, current)
	if err != nil {
		return "", err
	}
	for _, pin := range debtRunIdentitySourcePins {
		if pin.File != file || pin.Function != function {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/family145-run-identity-preimages.json"))
		if err != nil {
			return "", err
		}
		previous, err := debtRunIdentityExactSourceInverse(raw, pin, current)
		if err != nil {
			return "", err
		}
		if err := debtRunIdentityNativeSourcePins(root); err != nil {
			return "", err
		}
		return previous, nil
	}
	return current, nil
}

func debtRunIdentityExactSourceInverse(raw []byte, pin debtRunIdentitySourcePin, current string) (string, error) {
	if debtBaselineChecksum(raw) != "23c82ec70c3225c2d92dd639d51ce592b0655cb9caa18856240810474dabef1f" {
		return "", fmt.Errorf("run identity complete source manifest changed")
	}
	var bodies []debtRunIdentitySourceBody
	if err := json.Unmarshal(raw, &bodies); err != nil || len(bodies) != 5 {
		return "", fmt.Errorf("run identity complete source inventory changed: %v", err)
	}
	return debtRunIdentityUniqueBodyInverse(bodies, pin, current)
}

func debtRunIdentityUniqueBodyInverse(bodies []debtRunIdentitySourceBody, pin debtRunIdentitySourcePin, current string) (string, error) {
	previous, matches := "", 0
	for _, body := range bodies {
		if body.File != pin.File || body.Function != pin.Function {
			continue
		}
		matches++
		if body.SHA256 != pin.BeforeSHA256 || debtBaselineChecksum([]byte(body.Before)) != pin.BeforeSHA256 ||
			debtBaselineChecksum([]byte(body.After)) != pin.CurrentSHA256 || body.After != current {
			return "", fmt.Errorf("run identity exact current/predecessor provenance changed")
		}
		previous = body.Before
	}
	if matches != 1 {
		return "", fmt.Errorf("run identity complete source requires one unique row, got%d", matches)
	}
	return previous, nil
}

func debtRunIdentityNativeSourcePins(root string) error {
	for _, pin := range [][2]string{
		{"internal/store/storetest/api_event_storage.go", "e6d83f45223bb623cc2253e72466e5d89fce3c723630b3d28fbbae18c54166ad"},
		{"internal/store/internal/runtimepersistence/test_api_event_storage.go", "1543aecebbdef7f12b5d56561bf0780bce22ec6b6994a34e44e1cea0096d344f"},
		{"internal/store/internal/backend/runlifecycle/run_identity_cardinality.go", "e3562a9f6ce92544e1cf10f84efb433b086435698ac4386012236cc3c7178dd7"},
	} {
		raw, err := os.ReadFile(filepath.Join(root, pin[0]))
		if err != nil || debtBaselineChecksum(raw) != pin[1] {
			return fmt.Errorf("run identity native owner source changed: %s: %v", pin[0], err)
		}
	}
	decls, err := debtServedDebugDeclarations(filepath.Join(root, "internal/serveapp/run_identity_observation_test.go"))
	if err != nil || debtBaselineChecksum([]byte(decls["readServedRunPhysicalIdentity"])) != "2e30e20d7ff00b4f7b6ca9c1df90cc8ce481fff8a79a0de7da352f3d90a8e49a" {
		return fmt.Errorf("run identity borrowed snapshot owner changed: %v", err)
	}
	return nil
}

func TestPersistenceAuthorityDebtRunIdentitySourceCompositionIsFinite(t *testing.T) {
	root := "../.."
	raw, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/family145-run-identity-preimages.json"))
	if err != nil {
		t.Fatal(err)
	}
	var bodies []debtRunIdentitySourceBody
	if err := json.Unmarshal(raw, &bodies); err != nil {
		t.Fatal(err)
	}
	for _, pin := range debtRunIdentitySourcePins {
		decls, err := debtServedDebugDeclarations(filepath.Join(root, pin.File))
		if err != nil {
			t.Fatal(err)
		}
		current := decls[pin.Function]
		previous, err := debtRunIdentitySourcePrevious(root, pin.File, pin.Function, current)
		if err != nil || debtBaselineChecksum([]byte(previous)) != pin.BeforeSHA256 {
			t.Fatal("run identity finite source composition refused:", pin.Function, err)
		}
		for _, cut := range []string{"readServedRunPhysicalIdentity", "ReceiverStateReader", "initial.RunID", "later.RunID", "seed.RunID", "context.Background()", "err != nil", " != ", "lease.Done()", "release.Do"} {
			if strings.Contains(current, cut) {
				mutant := strings.Replace(current, cut, "foreignRunIdentityCut", 1)
				if _, err := debtRunIdentitySourcePrevious(root, pin.File, pin.Function, mutant); err == nil {
					t.Fatal("run identity accepted changed owner/identity/context/assertion/cut:", cut)
				}
			}
		}
		for _, key := range [][2]string{{"foreign.go", pin.Function}, {pin.File, "foreignFunction"}} {
			if got, err := debtRunIdentitySourcePrevious(root, key[0], key[1], current); err != nil || got != current {
				t.Fatal("run identity inverse consumed a foreign key:", key, err)
			}
		}
		if _, err := debtRunIdentityExactSourceInverse(append(append([]byte{}, raw...), ' '), pin, current); err == nil {
			t.Fatal("run identity accepted a changed manifest digest")
		}
		debtRunIdentityHostileRows(t, bodies, pin, current)
	}
	if err := debtServedDebugSourcePins(root); err != nil {
		t.Fatal("run identity broke the existing49-pair/71-occurrence source evidence:", err)
	}
}

func debtRunIdentityHostileRows(t *testing.T, bodies []debtRunIdentitySourceBody, pin debtRunIdentitySourcePin, current string) {
	t.Helper()
	var exact debtRunIdentitySourceBody
	for _, body := range bodies {
		if body.File == pin.File && body.Function == pin.Function {
			exact = body
		}
	}
	wrong := exact
	wrong.Before += "foreign predecessor"
	for _, rows := range [][]debtRunIdentitySourceBody{nil, {exact, exact}, {wrong}} {
		if _, err := debtRunIdentityUniqueBodyInverse(rows, pin, current); err == nil {
			t.Fatal("missing/duplicate/wrong predecessor row accepted")
		}
	}
}

func debtRunIdentityCopySourceEvidence(t *testing.T, root, copy string) {
	t.Helper()
	debtSourceForkCopySourceEvidence(t, root, copy)
	for _, path := range []string{
		"tools/fixture-codemod/pipeline-observations/family145-run-identity-preimages.json",
		"internal/store/storetest/api_event_storage.go",
		"internal/store/internal/runtimepersistence/test_api_event_storage.go",
		"internal/store/internal/backend/runlifecycle/run_identity_cardinality.go",
		"internal/serveapp/run_identity_observation_test.go",
	} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	for _, pin := range debtRunIdentitySourcePins {
		current, err := debtServedDebugDeclarations(filepath.Join(root, pin.File))
		if err != nil {
			t.Fatal(err)
		}
		previous, err := debtRunIdentitySourcePrevious(root, pin.File, pin.Function, current[pin.Function])
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(copy, pin.File))
		if err != nil || strings.Count(string(raw), previous) != 1 {
			t.Fatal("copied historical source lacks one exact predecessor:", pin.Function, err)
		}
		debtWriteModuleSource(t, copy, pin.File, strings.Replace(string(raw), previous, current[pin.Function], 1))
	}
	debtSourceBlobCopySourceEvidence(t, root, copy)
}

func TestPersistenceAuthorityDebtRunIdentityCollectorAddsNoCredit(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	current := debtControlSet()
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtRunIdentityCollectorFrom, Sites: current}
	head := trusted
	head.Collector = debtRunIdentityCollectorTo
	proof := debtDeliveryRetirementProof{Root: root, Actual: current}
	if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, "", proof); err != nil {
		t.Fatal("exact metadata-only change refused:", err)
	}
	donor := debtControlSite("call:QueryRow", 1)
	withDonor := trusted
	withDonor.Sites = maps.Clone(current)
	withDonor.Sites[donor.identity()] = donor
	if err := debtValidateCollectorIdentity(withDonor.Collector, head.Collector, withDonor, head, "", proof); err != nil {
		t.Fatal("genuine old-site removal refused:", err)
	}
	added := maps.Clone(current)
	escape := debtControlSite("call:Exec", 1)
	added[escape.identity()] = escape
	foreignHead := head
	foreignHead.BootstrapSource = strings.Repeat("b", 40)
	for _, state := range []struct {
		name          string
		base, current string
		trusted, head authorityDebtBaseline
		proofs        []debtDeliveryRetirementProof
	}{
		{"reverse", head.Collector, trusted.Collector, head, trusted, []debtDeliveryRetirementProof{proof}},
		{"foreign-current", trusted.Collector, strings.Repeat("b", 64), trusted, head, []debtDeliveryRetirementProof{proof}},
		{"foreign-base", strings.Repeat("b", 64), trusted.Collector, trusted, head, []debtDeliveryRetirementProof{proof}},
		{"foreign-bootstrap", trusted.Collector, head.Collector, trusted, foreignHead, []debtDeliveryRetirementProof{proof}},
		{"missing-proof", trusted.Collector, head.Collector, trusted, head, nil},
		{"duplicate-proof", trusted.Collector, head.Collector, trusted, head, []debtDeliveryRetirementProof{proof, proof}},
		{"missing-source", trusted.Collector, head.Collector, trusted, head, []debtDeliveryRetirementProof{{Root: t.TempDir(), Actual: current}}},
		{"deletion-cannot-pay-for-addition", withDonor.Collector, head.Collector, withDonor, head, []debtDeliveryRetirementProof{{Root: root, Actual: added}}},
	} {
		t.Run(state.name, func(t *testing.T) {
			if err := debtValidateCollectorIdentity(state.base, state.current, state.trusted, state.head, "", state.proofs...); err == nil {
				t.Fatal("source metadata change accepted foreign provenance or new raw authority")
			}
		})
	}
}
