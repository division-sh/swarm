package cataloge2e

import (
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier6EventLoopCatalogFixtures_RealRuntime(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier6-event-loop/test-atomicity-commit"),
		canonicalrouting.ArtifactID("tests/tier6-event-loop/test-atomicity-guard-rollback"),
		canonicalrouting.ArtifactID("tests/tier6-event-loop/test-atomicity-rollback"),
		canonicalrouting.ArtifactID("tests/tier6-event-loop/test-chain-depth-limit"),
		canonicalrouting.ArtifactID("tests/tier6-event-loop/test-cross-entity-concurrent"),
		canonicalrouting.ArtifactID("tests/tier6-event-loop/test-dead-letter"),
		canonicalrouting.ArtifactID("tests/tier6-event-loop/test-entity-serialization"),
		canonicalrouting.ArtifactID("tests/tier6-event-loop/test-event-persisted-before-delivery"),
		canonicalrouting.ArtifactID("tests/tier6-event-loop/test-event-validation"),
		canonicalrouting.ArtifactID("tests/tier6-event-loop/test-guards-pre-handler-state"),
		canonicalrouting.ArtifactID("tests/tier6-event-loop/test-on-complete-atomicity-chain"),
	)
	for _, fixture := range catalogRuntimeFixtures(t, "catalog.runtime.event_loop") {
		fixtureName, fixtureRoot := fixture.Name, fixture.Root
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			h := newRuntimeHarness(t, fixtureRoot, false)
			h.seedEntityFields(expected)
			if len(expected.Trigger.Concurrent) > 0 {
				h.publishConcurrentAndWait(expected.Trigger.Concurrent, catalogRuntimePublishTimeout)
			} else {
				for _, step := range expected.triggerSequence() {
					h.publishAndWait(step, catalogRuntimePublishTimeout)
				}
			}
			assertCatalogRuntimeOutcome(t, h, expected)
		})
	}
}
