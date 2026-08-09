package cataloge2e

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier3ListProcessingCanonicalRoutingOwnership(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-fan-out-basic"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-fan-out-count"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-fan-out-emit-mapping"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-fan-out-empty"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-filter-basic"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-filter-empty"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-group-by-standalone"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-reduce-count"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-reduce-max"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-reduce-min"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-reduce-operation-count"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-reduce-pick-or-average"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-reduce-sum"),
		canonicalrouting.ArtifactID("tests/tier3-list-processing/test-reduce-weighted-average"),
	)
}
