package storetest

import (
	"context"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"testing"
)

type SemanticEventFixtureEvidence = private.SemanticEventFixtureEvidence

func ObserveSemanticEventFixtureEvidence(ctx context.Context, selected any, runID, eventID string) (SemanticEventFixtureEvidence, error) {
	return private.ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID)
}

func ReadCommittedPipelineScope(ctx context.Context, selected any, eventID string) (runtimepipelineobligation.CommittedScope, error) {
	return private.ReadCommittedPipelineScopeForTest(ctx, selected, eventID)
}

func ReadSemanticEventFixtureEvidence(t testing.TB, ctx context.Context, selected any, runID, eventID string) private.SemanticEventFixtureEvidence {
	t.Helper()
	evidence, err := ObserveSemanticEventFixtureEvidence(ctx, selected, runID, eventID)
	if err != nil {
		t.Fatalf("read semantic event fixture evidence %s/%s: %v", runID, eventID, err)
	}
	return evidence
}
