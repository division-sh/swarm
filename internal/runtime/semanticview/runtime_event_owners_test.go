package semanticview

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
)

func TestRuntimeEventOwners_UsesScopedAuthoritativeOwners(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-sibling-both-instantiated-isolated")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := Wrap(bundle)

	if owners := source.RuntimeEventOwners("work.begin"); len(owners) != 0 {
		t.Fatalf("expected no semanticview authoritative owners for root work.begin, got %v", owners)
	}
	if owners := source.RuntimeEventOwners("flow-a/work.begin"); !testHasAll(owners, "alpha-intake") || testHasAny(owners, "beta-intake") {
		t.Fatalf("expected only alpha-intake for flow-a/work.begin, got %v", owners)
	}
	if owners := source.RuntimeEventOwners("flow-b/work.begin"); !testHasAll(owners, "beta-intake") || testHasAny(owners, "alpha-intake") {
		t.Fatalf("expected only beta-intake for flow-b/work.begin, got %v", owners)
	}
}

func testHasAll(values []string, wants ...string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	for _, want := range wants {
		if _, ok := seen[want]; !ok {
			return false
		}
	}
	return true
}

func testHasAny(values []string, wants ...string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	for _, want := range wants {
		if _, ok := seen[want]; ok {
			return true
		}
	}
	return false
}
