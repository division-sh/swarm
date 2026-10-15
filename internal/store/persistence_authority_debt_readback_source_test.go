package store_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Ruling6173853859: exact source evidence only, never debt or classifier credit.
const debtReadbackSourcePredecessor = "19de2c2e1fc57bcf4b9264c738db22de51742195"
const debtReadbackSourceManifest = "tools/fixture-codemod/pipeline-observations/family145-receiver-publication-readback-preimages.json"
const debtReadbackSourceDigest = "6e8a242bbac3092e8885c8792b899a693c4495d24cc5ccac53f93b2debadffee"

// Only a dispatch filter: every inverse still authenticates the full manifest
// and every actual sibling. The signed-membership control also checks this set.
var debtReadbackSourceNames = func() map[string]bool {
	names := map[string]bool{}
	for _, name := range strings.Fields(`TestEdgeOwnedConnectionPoliciesSharingInputBothStores TestEdgeOwnedMixedConnectionProjectionsBothStores requireMixedConnectionReadback requireConnectionPolicyRoutes TestRunForkHistoricalIdentityPublicExecutionBothStores TestServedForkAccumulatorRetainedGenerationBothStores TestServedForkLoopGenerationStateEffectBothStores requireForkLoopStateEffect requireSupplementalForkAcquisitionSettlement TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores requireForkReceiverExecution requireForkReceiverDelivery requireForkReceiverEffect runForkReceiverOwnershipJourney requireForkReceiverBusinessMutation TestSelectedForkIndependentReceiverBusinessMutationBothStores TestServedGeneratedActivityPublicationBothStores requireActivityPublicationReadback TestServedPublicationDirectRestartBothStores proveServedPublicationSites waitPublicationSiteFanOut requirePublicationSiteReadback TestServedReceiverAgentBusinessNamespaceBothStores requireServedReceiverAgentCollision proveActivitySemanticResultExecutionParity requireNumericMockForkOutput TestWorkflowGateSemanticNumericOutcomeExecutionParity TestServedSemanticNumericIdempotencyReplay TestServedSemanticNumericEventSubscription semanticNumericOutput startSemanticNumericRuntime TestGenericScheduleSemanticPayloadExecutionParity`) {
		names[name] = true
	}
	return names
}()

func debtReadbackSourcePins(root string) ([]debtRunIdentitySourceBody, error) {
	raw, err := os.ReadFile(filepath.Join(root, debtReadbackSourceManifest))
	if err != nil || debtBaselineChecksum(raw) != debtReadbackSourceDigest {
		return nil, fmt.Errorf("readback immutable complete source inventory changed: %v", err)
	}
	if cached, ok := debtSelectedReadRolePinCache.Load(debtReadbackSourceDigest); ok {
		return slices.Clone(cached.([]debtRunIdentitySourceBody)), nil
	}
	var pins []debtRunIdentitySourceBody
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 32 {
		return nil, fmt.Errorf("readback exact declaration count changed: %v", err)
	}
	seen := map[string]bool{}
	for _, pin := range pins {
		key := pin.File + "/" + pin.Function
		if !debtReadbackSourceNames[pin.Function] || seen[key] || pin.Before == "" || pin.After == "" || pin.Before == pin.After || debtBaselineChecksum([]byte(pin.Before)) != pin.SHA256 {
			return nil, fmt.Errorf("readback duplicate/changed signed predecessor: %s", key)
		}
		seen[key] = true
	}
	debtSelectedReadRolePinCache.Store(debtReadbackSourceDigest, slices.Clone(pins))
	return pins, nil
}

func debtReadbackLiveSources(root string) ([]debtRunIdentitySourceBody, error) {
	pins, err := debtReadbackSourcePins(root)
	if err != nil {
		return nil, err
	}
	files := map[string]map[string]string{}
	for _, pin := range pins {
		if files[pin.File] == nil {
			files[pin.File], err = debtCatchUpLaneDeclarations(root, pin.File)
			if err != nil {
				return nil, err
			}
		}
		if files[pin.File][pin.Function] != pin.After {
			return nil, fmt.Errorf("readback actual current source changed: %s", pin.Function)
		}
	}
	return pins, nil
}

func debtReadbackSourcePrevious(root, file, function, current string) (string, error) {
	var err error
	current, err = debtCatchUpLaneDeclaration(root, file, function, current)
	if err != nil {
		return "", err
	}
	if !debtReadbackSourceNames[function] {
		return current, nil
	}
	pins, err := debtReadbackSourcePins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if _, err := debtReadbackLiveSources(root); err != nil {
			return "", err
		}
		return debtReadbackSourceInverse(pins, file, function, current)
	}
	return current, nil
}

func debtReadbackSourceInverse(pins []debtRunIdentitySourceBody, file, function, current string) (string, error) {
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if current != pin.After {
			return "", fmt.Errorf("readback supplied evidence is not actual current source: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func debtReadbackCopySources(t *testing.T, root, copy string) {
	t.Helper()
	pins, err := debtReadbackLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, debtReadbackSourceManifest))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtReadbackSourceManifest, string(raw))
	for _, pin := range pins {
		debtSourceBlobCopyDeclaration(t, copy, pin.File, pin.Function, pin.After)
	}
}

func TestPersistenceAuthorityDebtReadbackSourceIsSignedFiniteAndCurrent(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	pins, err := debtReadbackLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	prior := materializeDebtBase(t, root, debtReadbackSourcePredecessor)
	for _, pin := range pins {
		bodies, err := debtServedSetupDeclarations(filepath.Join(prior, pin.File))
		if err != nil || bodies[pin.Function] != pin.Before {
			t.Fatalf("readback Before is not its exact signed declaration: %s: %v", pin.Function, err)
		}
		before, err := debtReadbackSourcePrevious(root, pin.File, pin.Function, pin.After)
		if err != nil || before != pin.Before {
			t.Fatalf("readback exact inverse changed: %s: %v", pin.Function, err)
		}
		for _, foreign := range []string{pin.Before, pin.After + "foreign"} {
			if _, err := debtReadbackSourcePrevious(root, pin.File, pin.Function, foreign); err == nil {
				t.Fatal("readback accepted historical/foreign supplied source", pin.Function)
			}
		}
		if got, err := debtReadbackSourcePrevious(root, "foreign.go", pin.Function, pin.After); err != nil || got != pin.After {
			t.Fatal("unlisted declaration received inverse authority:", err)
		}
	}
}

func TestPersistenceAuthorityDebtReadbackWarmSourceAndManifestControls(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	copy := t.TempDir()
	debtReadbackCopySources(t, root, copy)
	pins, err := debtReadbackLiveSources(copy)
	if err != nil {
		t.Fatal(err)
	}
	pins[0].Before = "foreign returned pin"
	again, err := debtReadbackSourcePins(copy)
	if err != nil || again[0].Before == pins[0].Before {
		t.Fatal("readback returned pins poisoned immutable cache:", err)
	}
	pins = again
	debtSourceBlobCopyDeclaration(t, copy, pins[0].File, pins[0].Function, pins[0].Before)
	if _, err := debtReadbackSourcePrevious(copy, pins[1].File, pins[1].Function, pins[1].After); err == nil {
		t.Fatal("readback warm cache hid stale actual sibling")
	}
	debtReadbackCopySources(t, root, copy)
	path := filepath.Join(copy, pins[0].File)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, pins[0].File, string(raw)+"\n"+pins[0].After)
	if _, err := debtReadbackLiveSources(copy); err == nil {
		t.Fatal("readback duplicate actual declaration passed")
	}
	debtReadbackCopySources(t, root, copy)
	if err := os.Remove(filepath.Join(copy, pins[0].File)); err != nil {
		t.Fatal(err)
	}
	if _, err := debtReadbackSourcePrevious(copy, pins[1].File, pins[1].Function, pins[1].After); err == nil {
		t.Fatal("readback missing actual sibling passed")
	}
	assertDebtReadbackManifestRefusal(t, root)
}

func assertDebtReadbackManifestRefusal(t *testing.T, root string) {
	t.Helper()
	copy := t.TempDir()
	debtReadbackCopySources(t, root, copy)
	path := filepath.Join(copy, debtReadbackSourceManifest)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var pins []debtRunIdentitySourceBody
	if err := json.Unmarshal(raw, &pins); err != nil {
		t.Fatal(err)
	}
	pins[1] = pins[0]
	duplicate, err := json.Marshal(pins)
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []string{string(raw) + "\n", string(duplicate)} {
		debtWriteModuleSource(t, copy, debtReadbackSourceManifest, changed)
		if _, err := debtReadbackSourcePins(copy); err == nil {
			t.Fatal("readback altered/duplicate manifest inherited a warm-cache pass")
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := debtReadbackSourcePins(copy); err == nil {
		t.Fatal("readback missing manifest inherited a warm-cache pass")
	}
}
