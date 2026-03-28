package bootverify

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

func TestRun_MapsMissingToolToToolResolutionWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-tool-missing")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "tool_resolution", "nonexistent_tool") {
		t.Fatalf("expected tool_resolution warning, got %#v", report.Warnings())
	}
}

func TestRun_MapsReservedPlatformNamespaceToNamedCheck(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	bundle.Events["platform.forbidden"] = runtimecontracts.EventCatalogEntry{}
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "platform_namespace_violation", "reserved platform.* namespace") {
		t.Fatalf("expected platform_namespace_violation error, got %#v", report.Errors())
	}
}

func TestBootCheckRegistry_HasSpecCheckCount(t *testing.T) {
	if got := len(bootCheckRegistry); got != 24 {
		t.Fatalf("bootCheckRegistry count = %d, want 24", got)
	}
	if got := len(supplementalChecks); got != 1 {
		t.Fatalf("supplementalChecks count = %d, want 1", got)
	}
}

func loadTier8Fixture(t *testing.T, fixture string) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadTier8FixtureBundle(t, fixture))
}

func loadTier8FixtureBundle(t *testing.T, fixture string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", fixture)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixture, err)
	}
	return bundle
}

func reportContains(items []Finding, checkID, contains string) bool {
	for _, item := range items {
		if item.CheckID == checkID && strings.Contains(item.Message, contains) {
			return true
		}
	}
	return false
}
