package cataloge2e

import (
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier1PrimitiveCatalogFixtures_RealRuntime(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-advances-to"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-advances-to-terminal"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-clear-gates"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-compute-standalone"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-data-accumulation-direct"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-data-accumulation-literal"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-data-accumulation-mapped"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-emits-multiple"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-emits-payload-transform"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-from-filter"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-guard-compound-condition"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-guard-discard"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-guard-entity-ref"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-guard-escalate"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-guard-kill"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-guard-multi"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-guard-multi-fail"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-guard-pass"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-guard-policy-ref"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-guard-reject"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-on-complete-first-match"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-on-complete-second-match"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-on-complete-with-state"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-payload-transform-multi-source"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-record-evidence"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-rules-advances-to"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-rules-data-accumulation"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-rules-else"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-rules-match"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-rules-no-match"),
		canonicalrouting.ArtifactID("tests/tier1-primitives/test-sets-gate"),
	)
	for _, fixture := range catalogRuntimeFixtures(t, "catalog.runtime.primitives") {
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
