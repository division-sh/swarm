package cataloge2e

import (
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier10PolicyPatternCatalogFixtures_RealRuntime(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier10-policy-patterns/test-policy-capacity-query"),
		canonicalrouting.ArtifactID("tests/tier10-policy-patterns/test-policy-counter-escalate"),
		canonicalrouting.ArtifactID("tests/tier10-policy-patterns/test-policy-hard-gate-override"),
		canonicalrouting.ArtifactID("tests/tier10-policy-patterns/test-policy-multi-guard-partial"),
		canonicalrouting.ArtifactID("tests/tier10-policy-patterns/test-policy-threshold-three-way"),
		canonicalrouting.ArtifactID("tests/tier10-policy-patterns/test-policy-timeout-elapsed"),
	)
	for _, fixture := range catalogRuntimeFixtures(t, "catalog.runtime.policy_patterns") {
		fixtureName, fixtureRoot := fixture.Name, fixture.Root
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			h := newRuntimeHarness(t, fixtureRoot, false)
			h.seedEntityFields(expected)
			for _, step := range expected.triggerSequence() {
				h.publishAndWait(step, catalogRuntimePublishTimeout)
			}
			assertCatalogRuntimeOutcome(t, h, expected)
		})
	}
}
