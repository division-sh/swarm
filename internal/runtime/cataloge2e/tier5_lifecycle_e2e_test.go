package cataloge2e

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier5LifecycleCanonicalRoutingOwnership(t *testing.T) {
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
}

func TestCatalogTemplateNoBootInstance_RealRuntimeBoot(t *testing.T) {
	fixture := catalogRuntimeFixture(t, "catalog.runtime.flow_lifecycle", "test-template-no-boot-instance")
	var expected catalogExpectedDocument
	loadYAML(t, catalogExpectedPath(fixture.Root), &expected)
	h := newRuntimeHarness(t, fixture.Root, false)
	assertCatalogRuntimeOutcome(t, h, expected)
}
