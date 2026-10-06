package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"testing"
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
