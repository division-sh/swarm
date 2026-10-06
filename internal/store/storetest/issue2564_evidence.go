package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type EntityMutationEvidence = private.EntityMutationEvidence

func ObserveEntityMutationHistory(t testing.TB, ctx context.Context, selected any, runID string) []EntityMutationEvidence {
	t.Helper()
	value, err := private.ObserveEntityMutationHistoryForTest(ctx, selected, runID)
	if err != nil {
		t.Fatalf("observe exact mutation evidence: %v", err)
	}
	return value
}

type CardContentionEvidence = private.CardContentionEvidence

func ObserveCardContention(t testing.TB, ctx context.Context, selected any, key, cardID string) CardContentionEvidence {
	t.Helper()
	value, err := private.ObserveCardContentionForTest(ctx, selected, key, cardID)
	if err != nil {
		t.Fatalf("observe exact card contention: %v", err)
	}
	return value
}

func AdvanceGateHeaderRevision(ctx context.Context, selected any, state pipeline.WorkflowEngineStateRecord) error {
	return private.AdvanceGateHeaderRevisionForTest(ctx, selected, state)
}
