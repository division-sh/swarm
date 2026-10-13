package store_test

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const debtReceiverReadbackCollectorTo = "84f9fb91bc1ec1d9f0da71ddd541905b995ada9cf6bae14960c642dca9d5596a"
const debtActiveCollectorTo = debtRunIdentityCollectorTo
const debtReceiverReadbackManifest = "tools/fixture-codemod/pipeline-observations/family145-receiver-public-readback-preimages.json"

func TestPersistenceAuthorityDebtReceiverReadbackCompositionPreservesOriginalSource(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	if err := debtServedDebugSourcePins(root); err != nil {
		t.Fatal(err)
	}
	const file = "internal/serveapp/receiver_failure_settlement_test.go"
	const function = "TestReceiverCompositionFailureSettlementBothStores"
	bodies, err := debtServedDebugDeclarations(filepath.Join(root, file))
	if err != nil {
		t.Fatal(err)
	}
	current := bodies[function]
	previous, err := debtReceiverReadbackSourcePrevious(root, file, function, current)
	if err != nil || debtBaselineChecksum([]byte(previous)) != "2f7cfdc7e2c181c70f1eea10c631613f3dcb6b63d9d3ba3b8b9e79e58461d27c" {
		t.Fatal("exact new body did not preserve its original served-debug receipt:", err)
	}
	if same, err := debtReceiverReadbackSourcePrevious(root, file, function, previous); err != nil || same != previous {
		t.Fatal("original receipt changed")
	}
	for _, cut := range []string{"rt.Endpoint", "rt.ReceiverStateReader", "seed.RunID", "servedInspectionRunDebugSummary", "t.Fatal", "t.Errorf", "waitServedRunDeliveryQuiescence"} {
		if !strings.Contains(current, cut) {
			continue
		}
		mutant := strings.Replace(current, cut, "foreignReceiverSource", 1)
		if _, err := debtReceiverReadbackSourcePrevious(root, file, function, mutant); err == nil {
			t.Fatal("changed owner/identity/assertion/wait body normalized:", cut)
		}
	}
	for _, location := range [][2]string{{file + ".foreign", function}, {file, function + "Foreign"}} {
		if same, err := debtReceiverReadbackSourcePrevious(root, location[0], location[1], current); err != nil || same != current {
			t.Fatal("foreign file/declaration entered the one-body inverse")
		}
	}
	copy := debtReceiverReadbackSourceCopy(t, root, file)
	manifest, err := os.ReadFile(filepath.Join(copy, debtReceiverReadbackManifest))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtReceiverReadbackManifest, string(manifest)+" ")
	if _, err := debtReceiverReadbackSourcePrevious(copy, file, function, current); err == nil {
		t.Fatal("changed manifest admitted")
	}
	if err := os.Remove(filepath.Join(copy, debtReceiverReadbackManifest)); err != nil {
		t.Fatal(err)
	}
	if _, err := debtReceiverReadbackSourcePrevious(copy, file, function, current); err == nil {
		t.Fatal("missing source manifest admitted")
	}
	if err := os.Remove(filepath.Join(copy, file)); err != nil {
		t.Fatal(err)
	}
	if err := debtServedDebugSourcePins(copy); err == nil {
		t.Fatal("missing current source admitted")
	}
}

func debtReceiverReadbackSourceCopy(t *testing.T, root, file string) string {
	t.Helper()
	copy := debtServedDebugSourceCopy(t)
	for _, path := range []string{file, debtReceiverReadbackManifest} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	return copy
}

func TestPersistenceAuthorityDebtReceiverReadbackCollectorIsExactAndAddsNoCredit(t *testing.T) {
	digest, err := debtCollectorDigest(persistenceAuthorityRepoRoot(t))
	if err != nil || digest != debtActiveCollectorTo {
		t.Fatalf("receiver collector=%s pinned=%s err=%v", digest, debtActiveCollectorTo, err)
	}
	_, current := debtServedDebugTransitionStates()
	_, uncertainty := debtPublicA2EndpointTransition()
	current[uncertainty.identity()] = uncertainty
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtPublicA2EndpointCollectorTo, Sites: current}
	head := trusted
	head.Collector = debtReceiverReadbackCollectorTo
	if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, ""); err != nil {
		t.Fatal("exact code-only preservation refused:", err)
	}
	escape := debtControlSite("call:Exec", 1)
	head.Sites = maps.Clone(current)
	head.Sites[escape.identity()] = escape
	if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, ""); err == nil {
		t.Fatal("collector source repair paid for new raw authority")
	}
	if err := debtValidateCollectorIdentity(trusted.Collector, strings.Repeat("b", 64), trusted, head, ""); err == nil {
		t.Fatal("unknown collector accepted")
	}
}

func TestPersistenceAuthorityDebtReceiverReadbackMetadataIsLocalAndSourceBound(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	file := "internal/serveapp/receiver_failure_settlement_test.go"
	copy := debtReceiverReadbackSourceCopy(t, root, file)
	_, current := debtServedDebugTransitionStates()
	baseline := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtCacheCollectorFrom, Sites: current}
	data := marshalAuthorityDebtBaseline(baseline)
	metadata, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: debtPublicA2EndpointCollectorTo, BaselineSHA256: debtBaselineChecksum(data), Predecessor: debtCacheCollectorFrom})
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata))
	t.Setenv(debtRefreshEnv, "downward")
	t.Setenv("CI", "")
	t.Setenv("GITHUB_ACTIONS", "")
	got, err := debtBaselineWithCollector(copy, data, debtReceiverReadbackCollectorTo, false)
	if err != nil || got.Collector != debtReceiverReadbackCollectorTo || !debtSitesEqual(got.Sites, baseline.Sites) {
		t.Fatal("exact local metadata rotation changed retained occurrences:", err)
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
			if _, err := debtBaselineWithCollector(copy, data, debtReceiverReadbackCollectorTo, mode == "historical"); err == nil {
				t.Fatal("stale metadata escaped explicit local downward refresh")
			}
		})
	}
}
