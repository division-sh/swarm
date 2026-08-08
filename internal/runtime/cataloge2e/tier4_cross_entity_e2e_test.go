package cataloge2e

import (
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier4CrossEntityCatalogFixtures_RealRuntime(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier4-cross-entity/test-clear-multiple-targets"),
		canonicalrouting.ArtifactID("tests/tier4-cross-entity/test-clear-state"),
		canonicalrouting.ArtifactID("tests/tier4-cross-entity/test-create-entity"),
		canonicalrouting.ArtifactID("tests/tier4-cross-entity/test-query-filter"),
		canonicalrouting.ArtifactID("tests/tier4-cross-entity/test-query-group-by"),
	)
	for _, fixture := range catalogRuntimeFixtures(t, "catalog.runtime.cross_entity") {
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
