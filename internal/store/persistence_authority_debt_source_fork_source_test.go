package store_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var debtSourceForkPin = debtRunIdentitySourcePin{"internal/serveapp/main_runtime_test.go", "TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation", "89f5dd1b3bc9e48b823be7a697ec25354ea0265bdc1007cbcfd4d04b81386c4b", "3eccce2f3034abc34dc6af25adae10cc146ae7f19f20c6905493c75534423e11"}

func debtSourceForkSourcePrevious(root, file, function, current string) (string, error) {
	if file != debtSourceForkPin.File || function != debtSourceForkPin.Function || debtBaselineChecksum([]byte(current)) != debtSourceForkPin.CurrentSHA256 {
		return current, nil
	}
	raw, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/family145-source-fork-cardinality-preimages.json"))
	if err != nil {
		return "", err
	}
	if debtBaselineChecksum(raw) != "46e028ffb16b29264fe757efb4bb67aa2e98cdda9236fb3398e589dfc96740e8" {
		return "", fmt.Errorf("source fork complete manifest changed")
	}
	var bodies []debtRunIdentitySourceBody
	if err := json.Unmarshal(raw, &bodies); err != nil || len(bodies) != 6 {
		return "", fmt.Errorf("source fork complete inventory changed: %v", err)
	}
	previous, err := debtRunIdentityUniqueBodyInverse(bodies, debtSourceForkPin, current)
	if err != nil {
		return "", err
	}
	if err := debtSourceForkNativePins(root); err != nil {
		return "", err
	}
	return previous, nil
}

func debtSourceForkNativePins(root string) error {
	for _, pin := range [][2]string{
		{"internal/store/storetest/run_fork.go", "f2ce1fbd901ce87aefda8e70c6d65bfa75f4c150efb932c5a6b0cc62eec5e142"},
		{"internal/store/internal/runtimepersistence/test_selected_runtime_outcome_observation.go", "eb91b19e8ecc5c1736088bd905b1be072bd461e39a6e3234d5e0cb54641e5263"},
		{"internal/store/internal/backend/runforkpersistence/selected_refusal_observation.go", "87811533c349dc979c2f2024adc6858d0da8cf3ad6a0d8e85292048daba7c8cf"},
		{"internal/serveapp/source_fork_cardinality_observation_test.go", "2a88d6cecde248f5d9d2801fa6648ba0eac8429affa0c658ba219f363300b357"},
	} {
		raw, err := os.ReadFile(filepath.Join(root, pin[0]))
		if err == nil {
			raw, err = debtSourceForkDeploymentNativePrevious(pin[0], raw)
		}
		if err != nil || debtBaselineChecksum(raw) != pin[1] {
			return fmt.Errorf("source fork native owner/borrowed helper changed: %s: %v", pin[0], err)
		}
	}
	return nil
}

// Exact additive owner bridge from3fed7372e1; no source or accounting credit.
func debtSourceForkDeploymentNativePrevious(path string, raw []byte) ([]byte, error) {
	if path != "internal/store/internal/runtimepersistence/test_selected_runtime_outcome_observation.go" {
		return raw, nil
	}
	if debtBaselineChecksum(raw) != "807878f2988e11be92912786f11f8447d77a5b511de6e91f1708f68859152e12" {
		return nil, fmt.Errorf("source fork exact deployment observation endpoint changed")
	}
	const addition = "func CountSelectedSourceRuntimeExecutionsStorageForTest(ctx context.Context, selected any, sourceRunID string) (int, error) {\n\treturn readDeploymentStorageForTest(ctx, selected, sourceRunID, runforkpersistence.CountSelectedSourceRuntimeExecutionsStorageTx)\n}\n\n"
	if strings.Count(string(raw), addition) != 1 {
		return nil, fmt.Errorf("source fork exact additive declaration missing or duplicated")
	}
	previous := []byte(strings.Replace(string(raw), addition, "", 1))
	if debtBaselineChecksum(previous) != "eb91b19e8ecc5c1736088bd905b1be072bd461e39a6e3234d5e0cb54641e5263" {
		return nil, fmt.Errorf("source fork retained deployment observation source changed")
	}
	return previous, nil
}

func TestPersistenceAuthorityDebtSourceForkDeploymentInversePinsBothCompleteFiles(t *testing.T) {
	const path = "internal/store/internal/runtimepersistence/test_selected_runtime_outcome_observation.go"
	raw, err := os.ReadFile(filepath.Join(persistenceAuthorityRepoRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := debtSourceForkDeploymentNativePrevious(path, raw)
	if err != nil || debtBaselineChecksum(previous) != "eb91b19e8ecc5c1736088bd905b1be072bd461e39a6e3234d5e0cb54641e5263" {
		t.Fatal("exact additive bridge did not preserve its complete predecessor:", err)
	}
	for _, cut := range []string{"CountSelectedSourceRuntimeExecutionsStorageForTest", "readDeploymentStorageForTest", "CountSelectedSourceRuntimeExecutionsStorageTx", "sourceRunID string", "validateStandaloneStorageEvent"} {
		mutant := strings.Replace(string(raw), cut, "foreignSourceForkBridge", 1)
		if mutant == string(raw) {
			t.Fatal("hostile source cut is absent:", cut)
		}
		if _, err := debtSourceForkDeploymentNativePrevious(path, []byte(mutant)); err == nil {
			t.Fatal("changed added/retained bridge source was accepted:", cut)
		}
	}
	for _, stale := range [][]byte{previous, append(append([]byte{}, raw...), '\n')} {
		if _, err := debtSourceForkDeploymentNativePrevious(path, stale); err == nil {
			t.Fatal("stale/noncanonical complete file was accepted")
		}
	}
	if untouched, err := debtSourceForkDeploymentNativePrevious("foreign.go", raw); err != nil || string(untouched) != string(raw) {
		t.Fatal("unrelated path received a historical inverse:", err)
	}
}

func debtSourceForkCopySourceEvidence(t *testing.T, root, copy string) {
	t.Helper()
	for _, path := range []string{
		"tools/fixture-codemod/pipeline-observations/family145-source-fork-cardinality-preimages.json",
		"internal/store/storetest/run_fork.go",
		"internal/store/internal/runtimepersistence/test_selected_runtime_outcome_observation.go",
		"internal/store/internal/backend/runforkpersistence/selected_refusal_observation.go",
		"internal/serveapp/source_fork_cardinality_observation_test.go",
	} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
}

func TestPersistenceAuthorityDebtSourceForkCompositionPreservesExactHistory(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	decls, err := debtServedDebugDeclarations(filepath.Join(root, debtSourceForkPin.File))
	if err != nil {
		t.Fatal(err)
	}
	current := decls[debtSourceForkPin.Function]
	entry, err := debtServedObservationSourcePrevious(root, debtSourceForkPin.File, debtSourceForkPin.Function, current)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := debtSourceForkSourcePrevious(root, debtSourceForkPin.File, debtSourceForkPin.Function, entry)
	if err != nil || debtBaselineChecksum([]byte(previous)) != debtSourceForkPin.BeforeSHA256 {
		t.Fatal("exact source fork inverse refused:", err)
	}
	for _, key := range [][2]string{{"foreign.go", debtSourceForkPin.Function}, {debtSourceForkPin.File, "foreignFunction"}} {
		if got, err := debtSourceForkSourcePrevious(root, key[0], key[1], current); err != nil || got != current {
			t.Fatal("foreign key received inverse:", err)
		}
	}
	for _, cut := range []string{"readServedSourceForkCardinality", "reader", "initial.RunID", "forkRuns != 0", "t.Fatalf", "context.Background()", "waitServedRunDeliveryQuiescence", "forkEventID"} {
		mutant := strings.Replace(current, cut, "foreignSourceForkCut", 1)
		if mutant == current {
			t.Fatal("missing source fork hostile cut:", cut)
		}
		if _, err := debtServedObservationSourcePrevious(root, debtSourceForkPin.File, debtSourceForkPin.Function, mutant); err == nil {
			t.Fatal("changed actual current body entered the historical comparison:", cut)
		}
		if got, err := debtSourceForkSourcePrevious(root, debtSourceForkPin.File, debtSourceForkPin.Function, mutant); err != nil || got != mutant {
			t.Fatal("changed current body received historical inverse:", cut, err)
		}
	}
	if err := debtServedDebugSourcePins(root); err != nil {
		t.Fatal(err)
	}
}
