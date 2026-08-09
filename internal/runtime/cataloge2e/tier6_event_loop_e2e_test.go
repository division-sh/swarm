package cataloge2e

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier6EventLoopCanonicalRoutingOwnership(t *testing.T) {
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
}
