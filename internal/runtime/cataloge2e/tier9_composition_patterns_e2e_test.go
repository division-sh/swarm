package cataloge2e

import (
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier9CompositionPatternCatalogFixtures_RealRuntime(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-accumulate-compute-branch"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-clear-gates-reenter"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-create-instance-config"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-gate-chain-three"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-gate-data-advance-emit"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-guard-counter-escalate"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-guard-multi-source"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-guard-query-capacity"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-lifecycle-seven-states"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-multi-emit-cross-flow"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-rules-fanout-data"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-rules-per-rule-data"),
	)
	for _, fixture := range catalogRuntimeFixtures(t, "catalog.runtime.composition_patterns") {
		fixtureName, fixtureRoot := fixture.Name, fixture.Root
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			h := newRuntimeHarness(t, fixtureRoot, fixtureName == "test-compose-clear-gates-reenter")
			h.seedEntityFields(expected)
			for index, step := range expected.triggerSequence() {
				if index > 0 && fixtureName == "test-compose-clear-gates-reenter" {
					h.waitForRunTerminal(catalogRuntimePublishTimeout)
				}
				h.publishAndWait(step, catalogRuntimePublishTimeout)
			}
			assertCatalogRuntimeOutcome(t, h, expected)
		})
	}
}
