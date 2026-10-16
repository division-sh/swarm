package store_test

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Excluded from the collector hash to avoid fingerprint self-reference.
const debtWorkflowStateCollectorTo = "64b8aced371482a1b0ef4e7e9552d39a6190f122b8ae231e8c51a898b6fe7d44"
const debtWorkflowStatePredecessor = "b44b2ff3711b3dc8e65ce278edaeea49594153be"

func debtWorkflowStateSourceCopy(t *testing.T) string {
	t.Helper()
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	copyRepair2698CustodySources(t, root, copy)
	for _, path := range []string{
		debtWorkflowStateFile, debtWorkflowStateManifest,
		"internal/store/internal/backend/pipelinepersistence/workflow_state_entity_observation.go",
		"internal/store/internal/backend/entityruntime/run_state_cardinality.go",
		"internal/store/internal/runtimepersistence/test_workflow_state_observation.go",
	} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	return copy
}

func TestPersistenceAuthorityDebtWorkflowStateComposesExactlyOneHistoricalBody(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	body, err := debtWorkflowStateBody(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := debtWorkflowStateSourcePins(root); err != nil {
		t.Fatal(err)
	}
	if err := debtServedDebugSourcePins(root); err != nil {
		t.Fatal("original served-debug evidence did not compose:", err)
	}
	collector, err := debtCollectorDigest(root)
	if err != nil || collector != debtActiveCollectorTo {
		t.Fatalf("workflow-state collector=%s expected=%s error=%v", collector, debtActiveCollectorTo, err)
	}
	raw, err := debtGit(root, "show", debtWorkflowStatePredecessor+":"+debtWorkflowStateFile)
	if err != nil {
		t.Fatal(err)
	}
	copy := t.TempDir()
	debtWriteModuleSource(t, copy, debtWorkflowStateFile, string(raw))
	predecessor, err := debtServedDebugDeclarations(filepath.Join(copy, debtWorkflowStateFile))
	if err != nil || predecessor[debtWorkflowStateFunction] != body.Before {
		t.Fatal("immutable predecessor disagrees with complete preimage:", err)
	}
	manifest, err := os.ReadFile(filepath.Join(root, debtWorkflowStateManifest))
	if err != nil {
		t.Fatal(err)
	}
	var bodies []debtRequestKeyBody
	if err := json.Unmarshal(manifest, &bodies); err != nil {
		t.Fatal(err)
	}
	inverses := 0
	for _, entry := range bodies {
		previous, err := debtWorkflowStateSourcePrevious(root, entry.File, entry.Function, entry.After)
		if err != nil {
			t.Fatal(err)
		}
		if previous != entry.After {
			inverses++
			if entry.File != debtWorkflowStateFile || entry.Function != debtWorkflowStateFunction || previous != body.Before {
				t.Fatal("accounting inverse exceeded the single approved declaration")
			}
		}
	}
	if inverses != 1 {
		t.Fatalf("composed %d declarations, want one", inverses)
	}
}

func TestPersistenceAuthorityDebtWorkflowStateRejectsForeignCutsAndKeys(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	body, err := debtWorkflowStateBody(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{
		{"rt.ReceiverStateReader", "foreignSameTypedReader"}, {"seed.RunID", "foreignRun"},
		{`"done", 2`, `"done", 3`}, {`"archived", 1`, `"archived", 2`},
		{`"done", 2`, `"other", 2`}, {"close(release)", "foreignRelease()"},
		{`response.Error == nil`, `response.Error != nil`},
		{`refusal == nil`, `refusal != nil`},
	} {
		t.Run(pair[0], func(t *testing.T) {
			if !strings.Contains(body.After, pair[0]) {
				t.Fatal("hostile control has no exact cut")
			}
			mutant := strings.Replace(body.After, pair[0], pair[1], 1)
			previous, err := debtWorkflowStateSourcePrevious(root, body.File, body.Function, mutant)
			if err != nil || previous != mutant {
				t.Fatal("foreign complete source received accounting credit:", err)
			}
		})
	}
	for _, key := range [][2]string{{body.File + "foreign", body.Function}, {body.File, body.Function + "Foreign"}} {
		previous, err := debtWorkflowStateSourcePrevious(root, key[0], key[1], body.After)
		if err != nil || previous != body.After {
			t.Fatal("foreign file/function received inverse credit:", err)
		}
	}
}

func TestPersistenceAuthorityDebtWorkflowStateManifestAndOwnersFailClosed(t *testing.T) {
	copy := debtWorkflowStateSourceCopy(t)
	manifest, err := os.ReadFile(filepath.Join(copy, debtWorkflowStateManifest))
	if err != nil {
		t.Fatal(err)
	}
	var bodies []debtRequestKeyBody
	if err := json.Unmarshal(manifest, &bodies); err != nil {
		t.Fatal(err)
	}
	body, err := debtWorkflowStateBody(copy)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"missing", "duplicate", "preimage", "manifest"} {
		t.Run(mode, func(t *testing.T) {
			mutant := workflowStateManifestMutation(t, bodies, body, manifest, mode)
			debtWriteModuleSource(t, copy, debtWorkflowStateManifest, string(mutant))
			if _, err := debtWorkflowStateSourcePrevious(copy, body.File, body.Function, body.After); err == nil {
				t.Fatal("changed complete immutable source accepted:", mode)
			}
			debtWriteModuleSource(t, copy, debtWorkflowStateManifest, string(manifest))
		})
	}
	for _, path := range []string{
		debtWorkflowStateFile,
		"internal/store/internal/backend/pipelinepersistence/workflow_state_entity_observation.go",
		"internal/store/internal/backend/entityruntime/run_state_cardinality.go",
		"internal/store/internal/runtimepersistence/test_workflow_state_observation.go",
	} {
		t.Run(path, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(copy, path))
			if err != nil {
				t.Fatal(err)
			}
			mutant := strings.Replace(string(raw), "{", "{ foreignOwner();", 1)
			debtWriteModuleSource(t, copy, path, mutant)
			if err := debtWorkflowStateSourcePins(copy); err == nil {
				t.Fatal("changed current/native owner body accepted")
			}
			debtWriteModuleSource(t, copy, path, string(raw))
		})
	}
}

func workflowStateManifestMutation(t *testing.T, bodies []debtRequestKeyBody, body debtRequestKeyBody, manifest []byte, mode string) []byte {
	t.Helper()
	if mode == "manifest" {
		return append(append([]byte{}, manifest...), ' ')
	}
	changed := append([]debtRequestKeyBody{}, bodies...)
	for index, entry := range changed {
		if entry.Function != body.Function || entry.File != body.File {
			continue
		}
		switch mode {
		case "missing":
			changed = append(changed[:index], changed[index+1:]...)
		case "duplicate":
			changed = append(changed, entry)
		case "preimage":
			changed[index].Before += " "
		}
		break
	}
	mutant, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	return mutant
}

func TestPersistenceAuthorityDebtWorkflowStateMetadataIsLocalAndSourceBound(t *testing.T) {
	copy := debtWorkflowStateSourceCopy(t)
	_, sites := debtServedDebugTransitionStates()
	baseline := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtCacheCollectorFrom, Sites: sites}
	data := marshalAuthorityDebtBaseline(baseline)
	metadata, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: debtEventNameCollectorTo, BaselineSHA256: debtBaselineChecksum(data), Predecessor: debtCacheCollectorFrom})
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata))
	t.Setenv(debtRefreshEnv, "downward")
	t.Setenv("CI", "")
	t.Setenv("GITHUB_ACTIONS", "")
	got, err := debtBaselineWithCollector(copy, data, debtWorkflowStateCollectorTo, false)
	if err != nil || got.Collector != debtWorkflowStateCollectorTo || !debtSitesEqual(got.Sites, baseline.Sites) {
		t.Fatal("local workflow source transition changed debt:", err)
	}
	for _, mode := range []string{"read-only", "CI", "GITHUB_ACTIONS", "historical", "foreign-collector", "checksum", "source", "header"} {
		t.Run(mode, func(t *testing.T) {
			input := workflowStateMetadataMutation(t, copy, mode, data, metadata)
			if _, err := debtBaselineWithCollector(copy, input, debtWorkflowStateCollectorTo, mode == "historical"); err == nil {
				t.Fatal("unqualified metadata transition admitted:", mode)
			}
		})
	}
	stored, err := os.ReadFile(filepath.Join(copy, debtCollectorSidecarPath))
	if err != nil || string(stored) != string(metadata) {
		t.Fatal("validation mutated sidecar:", err)
	}
}

func workflowStateMetadataMutation(t *testing.T, copy, mode string, data, metadata []byte) []byte {
	t.Helper()
	switch mode {
	case "read-only":
		t.Setenv(debtRefreshEnv, "")
	case "CI", "GITHUB_ACTIONS":
		t.Setenv(mode, "true")
	case "foreign-collector":
		debtWriteModuleSource(t, copy, debtCollectorSidecarPath, strings.Replace(string(metadata), debtEventNameCollectorTo, debtRunScalarCollectorTo, 1))
		t.Cleanup(func() { debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata)) })
	case "checksum":
		return append(append([]byte{}, data...), '\n')
	case "source":
		raw, err := os.ReadFile(filepath.Join(copy, debtWorkflowStateManifest))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, debtWorkflowStateManifest, string(raw)+" ")
		t.Cleanup(func() { debtWriteModuleSource(t, copy, debtWorkflowStateManifest, string(raw)) })
	case "header":
		changed := strings.Replace(string(data), debtCacheCollectorFrom, debtEventNameCollectorTo, 1)
		matching, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: debtEventNameCollectorTo, BaselineSHA256: debtBaselineChecksum([]byte(changed)), Predecessor: debtCacheCollectorFrom})
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(matching))
		t.Cleanup(func() { debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata)) })
		return []byte(changed)
	}
	return data
}

func TestPersistenceAuthorityDebtWorkflowStateCollectorPreservesOldChainAndDebt(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	before, current := debtServedDebugTransitionStates()
	for _, pair := range debtRequestKeySignatureTransitions() {
		before[pair[0].identity()], current[pair[1].identity()] = pair[0], pair[1]
	}
	_, uncertainty := debtPublicA2EndpointTransition()
	before[uncertainty.identity()], current[uncertainty.identity()] = uncertainty, uncertainty
	for _, pair := range debtDeliveryRetirementPairs() {
		delete(current, pair[1].identity())
	}
	for _, origin := range []string{debtReceiverReadbackCollectorTo, debtEventNameCollectorTo} {
		trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: origin, Sites: before}
		if origin == debtEventNameCollectorTo {
			trusted.Sites = current
		}
		head := trusted
		head.Collector, head.Sites = debtWorkflowStateCollectorTo, current
		proof := debtDeliveryRetirementProof{Root: root, Actual: current}
		if err := debtValidateCollectorIdentity(origin, head.Collector, trusted, head, "", proof); err != nil {
			t.Fatal("exact predecessor chain refused:", err)
		}
		added := debtControlSite("call:Exec", 1)
		head.Sites = maps.Clone(current)
		head.Sites[added.identity()] = added
		if err := debtValidateCollectorIdentity(origin, head.Collector, trusted, head, "", proof); err == nil {
			t.Fatal("source inverse paid for unrelated raw authority")
		}
		if err := debtValidateCollectorIdentity(origin, strings.Repeat("b", 64), trusted, head, "", proof); err == nil {
			t.Fatal("arbitrary resulting collector admitted")
		}
	}
}
