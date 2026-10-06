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

type WriterStageEvidence = private.WriterStageEvidence
type WriterFlowEvidence = private.WriterFlowEvidence
type DeliveryEventEvidence = private.DeliveryEventEvidence
type WriterRunDeliveryEvidence = private.WriterRunDeliveryEvidence

func ObserveWriterStage(t testing.TB, ctx context.Context, selected any, runID, entityID, stage string) WriterStageEvidence {
	t.Helper()
	value, err := private.ObserveWriterStageForTest(ctx, selected, runID, entityID, stage)
	if err != nil {
		t.Fatalf("observe exact writer stage: %v", err)
	}
	return value
}

func ObservePendingFixtureCard(t testing.TB, ctx context.Context, selected any, runID string) string {
	t.Helper()
	value, err := private.ObservePendingFixtureCardForTest(ctx, selected, runID)
	if err != nil {
		t.Fatalf("observe exact pending card: %v", err)
	}
	return value
}

func ObserveFixturePrincipal(t testing.TB, ctx context.Context, selected any) string {
	t.Helper()
	value, err := private.ObserveFixturePrincipalForTest(ctx, selected)
	if err != nil {
		t.Fatalf("observe exact fixture principal: %v", err)
	}
	return value
}

func ObserveWriterRunDelivery(t testing.TB, ctx context.Context, selected any, runID string) WriterRunDeliveryEvidence {
	t.Helper()
	value, err := private.ObserveWriterRunDeliveryForTest(ctx, selected, runID)
	if err != nil {
		t.Fatalf("observe writer delivery evidence: %v", err)
	}
	return value
}

func ObserveLiveWriterCount(t testing.TB, ctx context.Context, selected any, runID string) int {
	t.Helper()
	value, err := private.ObserveLiveWriterCountForTest(ctx, selected, runID)
	if err != nil {
		t.Fatalf("observe live writer evidence: %v", err)
	}
	return value
}

func ObserveWriterFlow(t testing.TB, ctx context.Context, selected any, runID, entityID string) WriterFlowEvidence {
	t.Helper()
	value, err := private.ObserveWriterFlowForTest(ctx, selected, runID, entityID)
	if err != nil {
		t.Fatalf("observe writer flow evidence: %v", err)
	}
	return value
}

func ObserveDeliveryEventEvidence(t testing.TB, ctx context.Context, selected any, eventID string) DeliveryEventEvidence {
	t.Helper()
	value, err := private.ObserveDeliveryEventEvidenceForTest(ctx, selected, eventID)
	if err != nil {
		t.Fatalf("observe exact delivery evidence: %v", err)
	}
	return value
}
