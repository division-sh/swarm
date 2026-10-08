package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type DeliveryEventEvidence = private.DeliveryEventEvidence
type EntityMutationEvidence = private.EntityMutationEvidence
type PipelineReceiptEvidence = private.PipelineReceiptEvidence
type SelectedPoolEvidence = private.SelectedPoolEvidence
type ActivityResultMutationEvidence = private.ActivityResultMutationEvidence
type WriterRunDeliveryEvidence = private.WriterRunDeliveryEvidence
type WriterFlowEvidence = private.WriterFlowEvidence
type WriterStageEvidence = private.WriterStageEvidence
type CardContentionEvidence = private.CardContentionEvidence

func ObserveWriterStage(t testing.TB, ctx context.Context, selected any, runID, entityID, stage string) WriterStageEvidence {
	t.Helper()
	value, err := private.ObserveWriterStageForTest(ctx, selected, runID, entityID, stage)
	if err != nil {
		t.Fatalf("observe exact writer stage: %v", err)
	}
	return value
}

func ObserveCardContention(t testing.TB, ctx context.Context, selected any, key, cardID string) CardContentionEvidence {
	t.Helper()
	value, err := private.ObserveCardContentionForTest(ctx, selected, key, cardID)
	if err != nil {
		t.Fatalf("observe exact card contention: %v", err)
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

func AdvanceGateHeaderRevision(ctx context.Context, selected any, state pipeline.WorkflowEngineStateRecord) error {
	return private.AdvanceGateHeaderRevisionForTest(ctx, selected, state)
}

func PostgresFixtureLocation(t *testing.T) string {
	t.Helper()
	return private.PostgresFixtureLocationForTest(t)
}

func ObserveActivityResultMutations(t testing.TB, ctx context.Context, selected any, eventID string) ActivityResultMutationEvidence {
	t.Helper()
	value, err := private.ObserveActivityResultMutationsForTest(ctx, selected, eventID)
	if err != nil {
		t.Fatalf("observe exact activity result mutations: %v", err)
	}
	return value
}

func ObserveEventCardinality(t testing.TB, ctx context.Context, selected any, eventID string) int {
	t.Helper()
	value, err := private.ObserveEventCardinalityForTest(ctx, selected, eventID)
	if err != nil {
		t.Fatalf("observe exact event cardinality: %v", err)
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

func ObserveEntityMutationHistory(t testing.TB, ctx context.Context, selected any, runID string) []EntityMutationEvidence {
	t.Helper()
	value, err := private.ObserveEntityMutationHistoryForTest(ctx, selected, runID)
	if err != nil {
		t.Fatalf("observe exact mutation evidence: %v", err)
	}
	return value
}

func ObservePipelineReceipt(t testing.TB, ctx context.Context, selected any, eventID string) PipelineReceiptEvidence {
	t.Helper()
	value, err := private.ObservePipelineReceiptForTest(ctx, selected, eventID)
	if err != nil {
		t.Fatalf("observe exact pipeline receipt: %v", err)
	}
	return value
}

func RetirePlannedReadiness(t testing.TB, ctx context.Context, selected any, plan pipeline.DynamicFlowRuntimeReadinessPlan, ordinal uint64, at time.Time) {
	t.Helper()
	if err := private.RetirePlannedReadinessForTest(ctx, selected, plan, ordinal, at); err != nil {
		t.Fatalf("retire exact planned readiness: %v", err)
	}
}

func ObserveSelectedPool(t testing.TB, selected any) SelectedPoolEvidence {
	t.Helper()
	value, err := private.ObserveSelectedPoolForTest(selected)
	if err != nil {
		t.Fatalf("observe original selected pool: %v", err)
	}
	return value
}
