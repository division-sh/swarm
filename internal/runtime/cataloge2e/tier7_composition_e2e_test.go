package cataloge2e

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier7CompositionCanonicalRoutingOwnership(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier7-composition/test-agent-emits-to-node"),
		canonicalrouting.ArtifactID("tests/tier7-composition/test-dual-delivery"),
		canonicalrouting.ArtifactID("tests/tier7-composition/test-full-lifecycle"),
		canonicalrouting.ArtifactID("tests/tier7-composition/test-multi-gate-pipeline"),
		canonicalrouting.ArtifactID("tests/tier7-composition/test-two-node-chain"),
		canonicalrouting.ArtifactID("tests/tier7-composition/test-wildcard-cross-flow"),
	)
}
