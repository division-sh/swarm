package cataloge2e

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier9CompositionPatternCanonicalRoutingOwnership(t *testing.T) {
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
}
