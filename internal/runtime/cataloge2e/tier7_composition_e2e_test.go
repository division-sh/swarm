package cataloge2e

import (
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier7CompositionCatalogFixtures_RealRuntime(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier7-composition/test-agent-emits-to-node"),
		canonicalrouting.ArtifactID("tests/tier7-composition/test-dual-delivery"),
		canonicalrouting.ArtifactID("tests/tier7-composition/test-full-lifecycle"),
		canonicalrouting.ArtifactID("tests/tier7-composition/test-multi-gate-pipeline"),
		canonicalrouting.ArtifactID("tests/tier7-composition/test-two-node-chain"),
		canonicalrouting.ArtifactID("tests/tier7-composition/test-wildcard-cross-flow"),
	)
	for _, fixture := range catalogRuntimeFixtures(t, "catalog.runtime.composition") {
		fixtureName, fixtureRoot := fixture.Name, fixture.Root
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			h := newRuntimeHarness(t, fixtureRoot, fixtureName == "test-agent-emits-to-node")
			h.seedEntityFields(expected)
			for _, step := range expected.triggerSequence() {
				h.publishAndWait(step, catalogRuntimePublishTimeout)
			}
			assertCatalogRuntimeOutcome(t, h, expected)
		})
	}
}
