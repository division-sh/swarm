package contracts_test

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func TestBundleCatalogReconstructionRetainsExactPredecessorDevelopmentBase(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	embedded := packfixture.EmbeddedBase(t)
	telegram, ok := embedded.Lookup("provider.telegram")
	if !ok {
		t.Fatal("embedded Telegram pack is missing")
	}
	firstBody := []byte(strings.Replace(string(telegram.ManifestBody()), "telegram update object is required", "catalog predecessor telegram update object is required", 1))
	secondBody := []byte(strings.Replace(string(telegram.ManifestBody()), "telegram update object is required", "catalog successor telegram update object is required", 1))
	firstBase, _ := packfixture.DevelopmentBase(t, map[string][]byte{"provider.telegram": firstBody})
	secondBase, _ := packfixture.DevelopmentBase(t, map[string][]byte{"provider.telegram": secondBody})
	owner, err := packartifact.NewPlatformPackBaseGenerationOwner(firstBase)
	if err != nil {
		t.Fatal(err)
	}

	root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	platform := contracts.DefaultPlatformSpecFile(repo)
	bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, platform, contracts.WorkflowContractLoadOptions{
		PlatformPackBase: firstBase, AdmitPackInventory: packadmission.AdmitInventory,
	})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := contracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Select(secondBase); err != nil {
		t.Fatal(err)
	}
	reconstructed, err := contracts.LoadBundleCatalogRuntimeSource(repo, contracts.BundleCatalogRuntimeLoadRequest{
		BundleHash: projection.BundleHash, ContentYAML: projection.ContentYAML, DataBlob: projection.DataBlob,
		RunningPlatformSpecPath: platform, PlatformPackBases: owner, AdmitPackInventory: packadmission.AdmitInventory,
	})
	if err != nil {
		t.Fatalf("reconstruct predecessor development generation: %v", err)
	}
	defer reconstructed.Cleanup()
	if reconstructed.Bundle.PackInventory.BaseDigest() != firstBase.Digest() {
		t.Fatalf("reconstructed base = %s, want predecessor %s (current=%s)", reconstructed.Bundle.PackInventory.BaseDigest(), firstBase.Digest(), secondBase.Digest())
	}
}
