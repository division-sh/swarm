package cataloge2e

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCatalogRejectsStaticCreateEntityHandlerFixture(t *testing.T) {
	fixtureRoot := writeCreateEntityExactOnceFixture(t)
	repoRoot := canonicalrouting.RepoRoot(t)
	_, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), `handler field "create_entity" is not supported`) || !strings.Contains(err.Error(), "Valid fields:") {
		t.Fatalf("expected strict handler-creation retirement, got %v", err)
	}
}

func writeCreateEntityExactOnceFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyStaticMultiEntityRetirement(t, canonicalrouting.StaticRetirementCreate)
}
