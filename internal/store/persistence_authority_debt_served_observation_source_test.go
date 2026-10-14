package store_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const debtServedSettlementManifest = "tools/fixture-codemod/pipeline-observations/family145-served-settlement-preimages.json"
const debtServedGlobalNameManifest = "tools/fixture-codemod/pipeline-observations/family145-global-event-name-preimages.json"

var debtServedObservationResetSourceKeys = [4][2]string{
	{"internal/serveapp/reset_candidate_fence_test.go", "TestResetCandidateSetFencesConsumersWhileSecondPreparesBothStores"},
	{"internal/serveapp/reset_served_parity_test.go", "TestServedResetRetainClearAndHistoricalReplayBothStores"},
	{"internal/serveapp/reset_convergence_receipt_test.go", "proveServedResetFinalReceiptFailure"},
	{"internal/serveapp/selected_reset_admission_test.go", "TestSelectedResetPreparationAdmissionOrderBothStores"},
}

var debtServedObservationOlderSourceKeys = [25][2]string{
	{"internal/serveapp/issue2277_audit_probe_test.go", "TestAudit2277StoppedRunReadinessRestart"},
	{"internal/serveapp/mailbox_entity_filter_test.go", "startServedMailboxEntityFilterProof"},
	{"internal/serveapp/main_runtime_test.go", "TestRunServeRuntimeJoinFailureReachesAPIAndCLI"},
	{"internal/serveapp/main_runtime_test.go", "TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation"},
	{"internal/serveapp/main_runtime_test.go", "TestRunServeRuntimeNukeQuiescesSessionWriterBeforeCleanupPostgres"},
	{"internal/serveapp/main_runtime_test.go", "startServedSessionCleanupProof"},
	{"internal/serveapp/main_runtime_test.go", "runServedRunForkBackendProof"},
	{"internal/serveapp/main_runtime_test.go", "runServedRuntimeIngressControlLifecycleProof"},
	{"internal/serveapp/main_runtime_test.go", "runServedCreateCarryProjectionProof"},
	{"internal/serveapp/main_runtime_test.go", "runServedDynamicAutoEmitProof"},
	{"internal/serveapp/main_runtime_test.go", "createServedControlWaitingRun"},
	{"internal/serveapp/main_runtime_test.go", "publishServedLiveAgentHoldEvent"},
	{"internal/serveapp/main_runtime_test.go", "requireServedStoppedPendingDelivery"},
	{"internal/serveapp/main_runtime_test.go", "waitForServedEventPublishNodeDeliveryLifecycleForNode"},
	{"internal/serveapp/main_runtime_test.go", "waitForServedEventPublishNodeDeliveryLifecycle"},
	{"internal/serveapp/main_runtime_test.go", "runServedEventPublishFollowUpProof"},
	{"internal/serveapp/main_runtime_test.go", "runServedEventPublishTargetRouteProof"},
	{"internal/serveapp/main_runtime_test.go", "runServedEventPublishActiveLoadProof"},
	{"internal/serveapp/main_runtime_test.go", "requireServedEventPublishCommittedReplayScope"},
	{"internal/serveapp/main_runtime_test.go", "waitServedEventPublishReceiptOutcomeCount"},
	{"internal/serveapp/main_runtime_test.go", "waitServedDeliveryOutcomeCount"},
	{"internal/serveapp/main_runtime_test.go", "waitServedEventPublishEventID"},
	{"internal/serveapp/receiver_failure_settlement_test.go", "TestReceiverCompositionFailureSettlementBothStores"},
	{"internal/serveapp/reset_served_parity_test.go", "TestServedResetCLIDryRunApplyAndReplayBothStores"},
	{"internal/serveapp/selected_fork_pending_input_mixed_test.go", "TestSelectedForkPendingInputMixedCompletionBothStores"},
}

func debtServedObservationManifests(root string) ([][]debtRunIdentitySourceBody, error) {
	out := make([][]debtRunIdentitySourceBody, 3)
	for index, pin := range [][3]string{
		{debtServedGlobalNameManifest, "9faffbd6ffd5e33f4f1dd3d92cec698def49a3923b81f5144bfdca0b9859d565", "7"},
		{debtServedSettlementManifest, "dbf467316e449a09a2670d61cfe437bd6f492b32aaf97d9897e7dbda613ba217", "34"},
		{"tools/fixture-codemod/pipeline-observations/family145-served-publication-lookup-preimages.json", "d826a092d7b7da7843cd43ff1a462563cc5752857a8c7ba7f3884eacc07f6e69", "11"},
	} {
		raw, err := os.ReadFile(filepath.Join(root, pin[0]))
		if err != nil || debtBaselineChecksum(raw) != pin[1] {
			return nil, fmt.Errorf("served observation immutable stage changed: %s: %v", pin[0], err)
		}
		if err := json.Unmarshal(raw, &out[index]); err != nil || strconv.Itoa(len(out[index])) != pin[2] {
			return nil, fmt.Errorf("served observation complete stage inventory changed: %s: %v", pin[0], err)
		}
	}
	return out, nil
}

func debtServedObservationPin(bodies []debtRunIdentitySourceBody, key [2]string) (debtRunIdentitySourceBody, error) {
	var found debtRunIdentitySourceBody
	count := 0
	for _, body := range bodies {
		if body.File == key[0] && body.Function == key[1] {
			found, count = body, count+1
		}
	}
	if count != 1 || debtBaselineChecksum([]byte(found.Before)) != found.SHA256 {
		return found, fmt.Errorf("served observation requires one exact complete pin: %s", key[1])
	}
	return found, nil
}

func debtServedObservationLiveSources(root string) ([]debtRunIdentitySourceBody, error) {
	stages, err := debtServedObservationManifests(root)
	if err != nil {
		return nil, err
	}
	var pins []debtRunIdentitySourceBody
	keys := append(append([][2]string{}, debtServedObservationResetSourceKeys[:]...), debtServedObservationOlderSourceKeys[:]...)
	files := map[string]map[string]string{}
	for _, key := range keys {
		pin, err := debtServedObservationComposedPin(stages, key)
		if err != nil {
			return nil, err
		}
		if files[key[0]] == nil {
			files[key[0]], err = debtServedDebugDeclarations(filepath.Join(root, key[0]))
			if err != nil {
				return nil, err
			}
		}
		current, err := debtServedSetupSourcePrevious(root, key[0], key[1], files[key[0]][key[1]])
		if err != nil || current != pin.After {
			return nil, fmt.Errorf("served observation actual final unique body changed: %s: %v", key[1], err)
		}
		pins = append(pins, pin)
	}
	if err := debtServedObservationNativeSource(root); err != nil {
		return nil, err
	}
	return pins, nil
}

func debtServedObservationComposedPin(stages [][]debtRunIdentitySourceBody, key [2]string) (debtRunIdentitySourceBody, error) {
	var composed debtRunIdentitySourceBody
	for _, stage := range stages {
		for _, body := range stage {
			if body.File != key[0] || body.Function != key[1] {
				continue
			}
			pin, err := debtServedObservationPin(stage, key)
			if err != nil {
				return composed, err
			}
			if composed.File == "" {
				composed = pin
			} else if composed.After == pin.Before {
				composed.After = pin.After
			} else {
				return composed, fmt.Errorf("served observation intermediate link changed: %s", key[1])
			}
		}
	}
	if composed.File == "" {
		return composed, fmt.Errorf("served observation missing composed declaration: %s", key[1])
	}
	return composed, nil
}

// Rulings6161646396/6161771753: 29 finite source-evidence copies at live ingress.
func debtServedObservationSourcePrevious(root, file, function, current string) (string, error) {
	var err error
	current, err = debtServedSetupSourcePrevious(root, file, function, current)
	if err != nil {
		return "", err
	}
	keys := append(append([][2]string{}, debtServedObservationResetSourceKeys[:]...), debtServedObservationOlderSourceKeys[:]...)
	return debtServedObservationPreviousForKeys(root, file, function, current, keys)
}

func debtServedObservationPreviousForKeys(root, file, function, current string, keys [][2]string) (string, error) {
	for _, key := range keys {
		if file != key[0] || function != key[1] {
			continue
		}
		pins, err := debtServedObservationLiveSources(root)
		if err != nil {
			return "", err
		}
		for _, pin := range pins {
			if pin.File == file && pin.Function == function {
				if pin.After != current {
					return "", fmt.Errorf("served observation supplied old source instead of actual final source")
				}
				return pin.Before, nil
			}
		}
	}
	return current, nil
}

func debtServedObservationNativeSource(root string) error {
	for _, path := range []string{
		"internal/store/internal/backend/pipelinepersistence/template_storage_observation.go",
		"internal/store/internal/runtimepersistence/test_served_delivery_observation.go",
	} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			return err
		}
		if _, err := debtServedObservationNativePrevious(path, raw); err != nil {
			return err
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal/serveapp/served_settlement_observation_test.go"))
	if err != nil || debtBaselineChecksum(raw) != "1da0a810e056b34f252538c8af1091b19cb8b8c567a7441fdacdcf9f4c32e083" {
		return fmt.Errorf("served observation original snapshot/context/reader bridge changed: %v", err)
	}
	return nil
}

func debtServedObservationNativePrevious(path string, raw []byte) ([]byte, error) {
	current, previous, functions, imports := debtServedObservationNativeCut(path)
	if current == "" {
		return raw, nil
	}
	if debtBaselineChecksum(raw) != current {
		return nil, fmt.Errorf("served observation exact additive native source changed: %s", path)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, raw, parser.ParseComments|parser.AllErrors)
	if err != nil {
		return nil, err
	}
	var kept []ast.Decl
	seen := map[string]int{}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && functions[fn.Name.Name] {
			seen[fn.Name.Name]++
			continue
		}
		if group, ok := declaration.(*ast.GenDecl); ok && group.Tok == token.IMPORT {
			var specs []ast.Spec
			for _, spec := range group.Specs {
				item := spec.(*ast.ImportSpec)
				if imports[item.Path.Value] {
					seen[item.Path.Value]++
					continue
				}
				specs = append(specs, spec)
			}
			group.Specs = specs
		}
		kept = append(kept, declaration)
	}
	for name := range functions {
		if seen[name] != 1 {
			return nil, fmt.Errorf("served native comparison missing/duplicate exact declaration: %s", name)
		}
	}
	for name := range imports {
		if seen[name] != 1 {
			return nil, fmt.Errorf("served native comparison missing/duplicate exact import: %s", name)
		}
	}
	file.Decls = kept
	var out bytes.Buffer
	if err := format.Node(&out, set, file); err != nil {
		return nil, err
	}
	if debtBaselineChecksum(out.Bytes()) != previous {
		return nil, fmt.Errorf("served native comparison changed retained source: %s", path)
	}
	return out.Bytes(), nil
}

func debtServedObservationNativeCut(path string) (string, string, map[string]bool, map[string]bool) {
	switch path {
	case "internal/store/internal/backend/pipelinepersistence/template_storage_observation.go":
		return "91a17b161389b88bbdcc056131c4de757579b73aa6b6c59a7236d767ae83d592", "9710f979a5d8ac8f552f4f9b8bd1000447fd164aa69c1719879aad723a37323a", map[string]bool{
			"CountSubscriberEventReceiptOutcomeStorageTx": true,
			"CountExactRunEventReplayScopeStorageTx":      true,
			"CountEventSubscriberReceiptStorageTx":        true,
		}, map[string]bool{`"fmt"`: true, `"strings"`: true}
	case "internal/store/internal/runtimepersistence/test_served_delivery_observation.go":
		return "256de06dd68f2c2fe45eac7c02ae8726e022696e6d977db3ca74f981d3e865a7", "bb5b2b2ec0c32fc6153b712be59cf827c2e858e02b54a7c5cf541b60aac96be3", map[string]bool{
			"CountExactRunNodeEventDeliveryStorageForTest":     true,
			"CountCurrentEventDeliveryOutcomeStorageForTest":   true,
			"CountSubscriberEventReceiptOutcomeStorageForTest": true,
			"CountExactRunEventReplayScopeStorageForTest":      true,
			"CountEventSubscriberReceiptStorageForTest":        true,
		}, nil
	default:
		return "", "", nil, nil
	}
}

func debtServedObservationCopySourceEvidence(t *testing.T, root, copy string) {
	t.Helper()
	for _, path := range []string{debtServedSettlementManifest, debtServedGlobalNameManifest,
		"internal/store/testdata/persistence_authority_served_observation_retirements.json",
		"tools/fixture-codemod/pipeline-observations/family145-served-publication-lookup-preimages.json",
		"tools/fixture-codemod/pipeline-observations/family145-delivery-status-preimages.json",
		"tools/fixture-codemod/pipeline-observations/family145-event-name-preimages.json",
		"tools/fixture-codemod/pipeline-observations/family145-run-scalar-preimages.json",
		"tools/fixture-codemod/pipeline-observations/family145-request-key-preimages.json",
		"tools/fixture-codemod/pipeline-observations/family145-receiver-public-readback-preimages.json",
		debtWorkflowStateManifest,
		"internal/store/internal/backend/pipelinepersistence/workflow_state_entity_observation.go",
		"internal/store/internal/backend/entityruntime/run_state_cardinality.go",
		"internal/store/internal/runtimepersistence/test_workflow_state_observation.go",
		"internal/store/internal/backend/pipelinepersistence/template_storage_observation.go",
		"internal/store/internal/runtimepersistence/test_served_delivery_observation.go",
		"internal/serveapp/served_settlement_observation_test.go"} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	pins, err := debtServedObservationLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		debtSourceBlobCopyDeclaration(t, copy, pin.File, pin.Function, pin.After)
	}
	stages, err := debtServedObservationManifests(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range stages {
		for _, body := range stage {
			pin, err := debtServedObservationComposedPin(stages, [2]string{body.File, body.Function})
			if err != nil {
				t.Fatal(err)
			}
			debtSourceBlobCopyDeclaration(t, copy, pin.File, pin.Function, pin.After)
		}
	}
	debtServedSetupCopySourceEvidence(t, root, copy)
}

func TestPersistenceAuthorityDebtServedObservationSourceInversesAreExactAndActualSourceBound(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	pins, err := debtServedObservationLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	keys := append(append([][2]string{}, debtServedObservationResetSourceKeys[:]...), debtServedObservationOlderSourceKeys[:]...)
	for _, pin := range pins {
		previous, err := debtServedObservationPreviousForKeys(root, pin.File, pin.Function, pin.After, keys)
		if err != nil || previous != pin.Before {
			t.Fatal("complete served observation inverse refused:", pin.Function, err)
		}
		for _, source := range []string{pin.Before, strings.Replace(pin.After, "{", "{foreignCut();", 1)} {
			if _, err := debtServedObservationPreviousForKeys(root, pin.File, pin.Function, source, keys); err == nil {
				t.Fatal("historical/mutated supplied source substituted for live final source")
			}
		}
		if got, err := debtServedObservationPreviousForKeys(root, "foreign.go", pin.Function, pin.After, keys); err != nil || got != pin.After {
			t.Fatal("foreign declaration received inverse:", err)
		}
	}
	for _, pin := range pins {
		copy := t.TempDir()
		debtServedObservationCopySourceEvidence(t, root, copy)
		raw, err := os.ReadFile(filepath.Join(copy, pin.File))
		if err != nil {
			t.Fatal(err)
		}
		mutant := strings.Replace(pin.After, "{", "{foreignLiveCut();", 1)
		debtWriteModuleSource(t, copy, pin.File, strings.Replace(string(raw), pin.After, mutant, 1))
		if _, err := debtServedObservationPreviousForKeys(copy, pins[0].File, pins[0].Function, pins[0].After, keys); err == nil {
			t.Fatal("unchanged supplied string bypassed changed actual live source:", pin.Function)
		}
	}
}

func TestPersistenceAuthorityDebtServedObservationNativeComparisonPreservesAddedAndRetainedCode(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	for _, path := range []string{
		"internal/store/internal/backend/pipelinepersistence/template_storage_observation.go",
		"internal/store/internal/runtimepersistence/test_served_delivery_observation.go",
	} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := debtServedObservationNativePrevious(path, raw); err != nil {
			t.Fatal(err)
		}
		_, _, functions, imports := debtServedObservationNativeCut(path)
		cuts := []string{"context.Context", "COUNT", "readFanOutSemanticStorage", "return", "eventID"}
		for name := range functions {
			cuts = append(cuts, name)
		}
		for name := range imports {
			cuts = append(cuts, name)
		}
		for _, cut := range cuts {
			if !strings.Contains(string(raw), cut) {
				continue
			}
			if _, err := debtServedObservationNativePrevious(path, []byte(strings.Replace(string(raw), cut, "foreignNativeCut", 1))); err == nil {
				t.Fatal("changed added/retained native code was hidden by comparison:", path, cut)
			}
		}
		if _, err := debtServedObservationNativePrevious(path, append(append([]byte{}, raw...), '\n')); err == nil {
			t.Fatal("stale/noncanonical native source accepted")
		}
		if got, err := debtServedObservationNativePrevious("foreign.go", raw); err != nil || !bytes.Equal(got, raw) {
			t.Fatal("foreign file received a native-file inverse:", err)
		}
	}
}
