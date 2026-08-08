package cataloge2e

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier5LifecycleCatalogFixtures_RealRuntime(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-auto-emit-on-create"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-create-flow-instance"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-create-flow-instance-config"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-create-flow-instance-duplicate"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-template-no-boot-instance"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-terminal-state-preserves"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-terminal-state-rejects"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-timer-cancel"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-timer-fire"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-timer-recurring"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-timer-start-on"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-wildcard-subscription"),
	)
	for _, fixture := range catalogRuntimeFixtures(t, "catalog.runtime.flow_lifecycle") {
		fixtureName, fixtureRoot := fixture.Name, fixture.Root
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			startRuntime := fixtureName == "test-auto-emit-on-create" ||
				fixtureName == "test-create-flow-instance" ||
				fixtureName == "test-create-flow-instance-config" ||
				fixtureName == "test-create-flow-instance-duplicate" ||
				fixtureName == "test-terminal-state-preserves" ||
				fixtureName == "test-terminal-state-rejects" ||
				fixtureName == "test-timer-fire" ||
				fixtureName == "test-timer-recurring"
			h := newRuntimeHarness(t, fixtureRoot, startRuntime)
			h.seedEntityFields(expected)
			if expected.Trigger.Boot || strings.TrimSpace(expected.Expected.BootResult) != "" {
				assertCatalogRuntimeOutcome(t, h, expected)
				return
			}
			for index, step := range expected.triggerSequence() {
				if index > 0 &&
					(fixtureName == "test-terminal-state-preserves" ||
						fixtureName == "test-terminal-state-rejects") {
					h.waitForRunTerminal(catalogRuntimePublishTimeout)
				}
				h.publishAndWait(step, catalogRuntimePublishTimeout)
			}
			h.waitForExpectedEmittedEvents(expected, catalogRuntimePublishTimeout)
			assertCatalogRuntimeOutcome(t, h, expected)
		})
	}
}
