package store_test

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Excluded from the collector hash to avoid fingerprint self-reference.
const debtEventNameCollectorTo = "88b0724a6221e16b93c92943cb9090ab9178318f906edb65cc1e02fc2dcd1b66"

func TestPersistenceAuthorityDebtEventNameMetadataIsLocalAndSourceBound(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	copyRepair2698CustodySources(t, root, copy)
	_, current := debtServedDebugTransitionStates()
	baseline := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtCacheCollectorFrom, Sites: current}
	data := marshalAuthorityDebtBaseline(baseline)
	metadata, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: debtDeliveryRetirementCollectorTo, BaselineSHA256: debtBaselineChecksum(data), Predecessor: debtCacheCollectorFrom})
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata))
	t.Setenv(debtRefreshEnv, "downward")
	t.Setenv("CI", "")
	t.Setenv("GITHUB_ACTIONS", "")
	got, err := debtBaselineWithCollector(copy, data, debtEventNameCollectorTo, false)
	if err != nil || got.Collector != debtEventNameCollectorTo || !debtSitesEqual(got.Sites, baseline.Sites) {
		t.Fatal("local metadata rotation changed retained occurrences:", err)
	}
	for _, mode := range []string{"read-only", "CI", "GITHUB_ACTIONS", "historical", "malformed", "foreign-collector", "checksum", "source"} {
		t.Run(mode, func(t *testing.T) {
			input := eventNameMetadataMutation(t, copy, mode, data, metadata)
			if _, err := debtBaselineWithCollector(copy, input, debtEventNameCollectorTo, mode == "historical"); err == nil {
				t.Fatal("unqualified local metadata transition admitted:", mode)
			}
		})
	}
	if stored, err := os.ReadFile(filepath.Join(copy, debtCollectorSidecarPath)); err != nil || string(stored) != string(metadata) {
		t.Fatal("validation rewrote the sidecar:", err)
	}
}

func eventNameMetadataMutation(t *testing.T, copy, mode string, data, metadata []byte) []byte {
	t.Helper()
	switch mode {
	case "read-only":
		t.Setenv(debtRefreshEnv, "")
	case "CI", "GITHUB_ACTIONS":
		t.Setenv(mode, "true")
	case "malformed", "foreign-collector":
		changed := string(metadata) + " "
		if mode == "foreign-collector" {
			changed = strings.Replace(string(metadata), debtDeliveryRetirementCollectorTo, debtRunScalarCollectorTo, 1)
		}
		debtWriteModuleSource(t, copy, debtCollectorSidecarPath, changed)
		t.Cleanup(func() { debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata)) })
	case "checksum":
		return append(append([]byte{}, data...), '\n')
	case "source":
		path := "tools/fixture-codemod/pipeline-observations/family145-event-name-preimages.json"
		raw, err := os.ReadFile(filepath.Join(copy, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw)+" ")
		t.Cleanup(func() { debtWriteModuleSource(t, copy, path, string(raw)) })
	}
	return data
}

func TestPersistenceAuthorityDebtEventNameSourceCompositionIsExact(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	if err := debtEventNameSourcePins(root); err != nil {
		t.Fatal(err)
	}
	if collector, err := debtCollectorDigest(root); err != nil || collector != debtActiveCollectorTo {
		t.Fatalf("event name collector=%s expected=%s error=%v", collector, debtActiveCollectorTo, err)
	}
	bodies, err := debtEventNameBodies(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range bodies {
		previous, err := debtEventNameSourcePrevious(root, body.File, body.Function, body.After)
		if err != nil || previous != body.Before {
			t.Fatal("exact complete event name inverse failed:", body.Function, err)
		}
		for _, cut := range []string{"InspectSnapshot", "CountEventNameStorage", "CountRunNamedEventStorage", "reader", "ReceiverStateReader", "eventName", "runID", "t.Fatal", " != ", "servedProofPollDeadline", "25 * time.Millisecond"} {
			if !strings.Contains(body.After, cut) {
				continue
			}
			mutant := strings.Replace(body.After, cut, "foreignEventNameCut", 1)
			previous, err := debtEventNameSourcePrevious(root, body.File, body.Function, mutant)
			if err != nil || previous != mutant {
				t.Fatal("changed source/assertion/polling received inverse credit:", cut, err)
			}
		}
	}
}

func TestPersistenceAuthorityDebtEventNameCollectorPreservesOldChainAndAllDebt(t *testing.T) {
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
	for _, origin := range []string{debtReceiverReadbackCollectorTo, debtDeliveryRetirementCollectorTo} {
		trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: origin, Sites: before}
		if origin == debtDeliveryRetirementCollectorTo {
			trusted.Sites = current
		}
		head := trusted
		head.Collector, head.Sites = debtEventNameCollectorTo, current
		proof := debtDeliveryRetirementProof{Root: root, Actual: current}
		if err := debtValidateCollectorIdentity(origin, head.Collector, trusted, head, "", proof); err != nil {
			t.Fatal("exact native/source old collector chain refused:", err)
		}
		added := debtControlSite("call:Exec", 1)
		head.Sites = maps.Clone(current)
		head.Sites[added.identity()] = added
		if err := debtValidateCollectorIdentity(origin, head.Collector, trusted, head, "", proof); err == nil {
			t.Fatal("native source composition paid for unrelated new raw authority")
		}
		if err := debtValidateCollectorIdentity(origin, strings.Repeat("b", 64), trusted, head, "", proof); err == nil {
			t.Fatal("unknown collector accepted")
		}
	}
}

func TestPersistenceAuthorityDebtEventNameSourceManifestAndNativeBodyFailClosed(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	copyRepair2698CustodySources(t, root, copy)
	const file = "internal/serveapp/main_runtime_test.go"
	const manifest = "tools/fixture-codemod/pipeline-observations/family145-event-name-preimages.json"
	if err := debtEventNameSourcePins(copy); err != nil {
		t.Fatal(err)
	}
	bodies, err := debtEventNameBodies(root)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(copy, file))
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range bodies {
		declarations, err := debtServedDebugDeclarations(filepath.Join(copy, file))
		if err != nil {
			t.Fatal(err)
		}
		current := declarations[body.Function]
		mutant := strings.Replace(current, "{", "{ foreignEventNameSource();", 1)
		if current == "" || strings.Count(string(raw), current) != 1 || current == mutant {
			t.Fatal("source hostile cut not unique:", body.Function)
		}
		debtWriteModuleSource(t, copy, file, strings.Replace(string(raw), current, mutant, 1))
		if err := debtEventNameSourcePins(copy); err == nil {
			t.Fatal("changed whole native source accepted:", body.Function)
		}
		debtWriteModuleSource(t, copy, file, string(raw))
	}
	debtWriteModuleSource(t, copy, file, string(raw))
	debtWriteModuleSource(t, copy, manifest, "[]\n")
	if err := debtEventNameSourcePins(copy); err == nil {
		t.Fatal("changed/missing complete immutable manifest accepted")
	}
}
