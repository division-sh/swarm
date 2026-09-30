package cataloge2e

import (
	"context"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCatalogRejectsStaticCreateEntityHandlerFixture(t *testing.T) {
	fixtureRoot := writeCreateEntityExactOnceFixture(t)
	bundle := loadFixtureBundle(t, fixtureRoot)
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), runtimebootverify.Options{})

	if !catalogCreateEntityFindingContains(report.Errors(), "flow_boundary_create_entity_validation", "caller-selected entity_id") {
		t.Fatalf("expected caller-selected static create_entity validation error, got %#v", report.Errors())
	}
}

func writeCreateEntityExactOnceFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyStaticMultiEntityRetirement(t, canonicalrouting.StaticRetirementCreate)
}

func catalogCreateEntityFindingContains(findings []runtimebootverify.Finding, checkID, substr string) bool {
	for _, finding := range findings {
		if finding.CheckID != checkID {
			continue
		}
		if substr == "" || strings.Contains(finding.Message, substr) {
			return true
		}
	}
	return false
}
