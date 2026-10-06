package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"testing"
)

type SemanticEventFixtureEvidence = private.SemanticEventFixtureEvidence

func ReadSemanticEventFixtureEvidence(t testing.TB, ctx context.Context, selected any, runID, eventID string) private.SemanticEventFixtureEvidence {
	t.Helper()
	evidence, err := private.ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID)
	if err != nil {
		t.Fatalf("read semantic event fixture evidence %s/%s: %v", runID, eventID, err)
	}
	return evidence
}
