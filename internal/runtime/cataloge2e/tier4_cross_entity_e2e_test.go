package cataloge2e

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier4CrossEntityCanonicalRoutingOwnership(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier4-cross-entity/test-clear-multiple-targets"),
		canonicalrouting.ArtifactID("tests/tier4-cross-entity/test-clear-state"),
		canonicalrouting.ArtifactID("tests/tier4-cross-entity/test-create-entity"),
		canonicalrouting.ArtifactID("tests/tier4-cross-entity/test-query-filter"),
		canonicalrouting.ArtifactID("tests/tier4-cross-entity/test-query-group-by"),
	)
}
