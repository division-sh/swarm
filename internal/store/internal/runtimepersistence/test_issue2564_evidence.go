package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	storepipeline "github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type EntityMutationEvidence = storepipeline.EntityMutationEvidence

func requireIssue2564EvidenceOwner(selected any) error {
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner != nil && owner.backend != nil && owner.pipelinePostgresOwner != nil && owner.deliveryPostgresOwner != nil {
			return owner.requireCurrentSchema()
		}
	case *SQLiteRuntimeStore:
		if owner != nil && owner.backend != nil && owner.pipelineSQLiteOwner != nil && owner.deliverySQLiteOwner != nil {
			return owner.requireCurrentSchema()
		}
	}
	return fmt.Errorf("closed evidence requires the original initialized selected owner, got %T", selected)
}

func ObserveEntityMutationHistoryForTest(ctx context.Context, selected any, runID string) ([]EntityMutationEvidence, error) {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return nil, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ObserveMutationHistoryForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ObserveMutationHistoryForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("mutation evidence requires original selected owner, got %T", selected)
	}
}

type CardContentionEvidence = storepipeline.CardContentionEvidence

func ObserveCardContentionForTest(ctx context.Context, selected any, key, cardID string) (CardContentionEvidence, error) {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return CardContentionEvidence{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ObserveCardContentionForTest(ctx, key, cardID)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ObserveCardContentionForTest(ctx, key, cardID)
	default:
		return CardContentionEvidence{}, fmt.Errorf("card evidence requires selected owner")
	}
}

func AdvanceGateHeaderRevisionForTest(ctx context.Context, selected any, state pipeline.WorkflowEngineStateRecord) error {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.AdvanceGateHeaderRevisionForTest(ctx, state)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.AdvanceGateHeaderRevisionForTest(ctx, state)
	default:
		return fmt.Errorf("gate contention fixture requires selected owner")
	}
}

type WriterStageEvidence = storepipeline.WriterStageEvidence
type WriterFlowEvidence = storepipeline.WriterFlowEvidence
type DeliveryEventEvidence = storedelivery.DeliveryEventEvidence
type WriterRunDeliveryEvidence = storedelivery.WriterRunDeliveryEvidence

func ObserveWriterStageForTest(ctx context.Context, selected any, runID, entityID, stage string) (WriterStageEvidence, error) {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return WriterStageEvidence{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ObserveWriterStageForTest(ctx, runID, entityID, stage)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ObserveWriterStageForTest(ctx, runID, entityID, stage)
	default:
		return WriterStageEvidence{}, fmt.Errorf("writer stage requires selected owner")
	}
}

func ObservePendingFixtureCardForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return "", err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ObservePendingFixtureCardForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ObservePendingFixtureCardForTest(ctx, runID)
	default:
		return "", fmt.Errorf("pending card requires selected owner")
	}
}

func ObserveFixturePrincipalForTest(ctx context.Context, selected any) (string, error) {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return "", err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.operatorChannelPostgresOwner.ObserveFixturePrincipalForTest(ctx)
	case *SQLiteRuntimeStore:
		return owner.operatorChannelSQLiteOwner.ObserveFixturePrincipalForTest(ctx)
	default:
		return "", fmt.Errorf("fixture principal requires selected owner")
	}
}

func ObserveWriterRunDeliveryForTest(ctx context.Context, selected any, runID string) (WriterRunDeliveryEvidence, error) {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return WriterRunDeliveryEvidence{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.deliveryPostgresOwner.ObserveWriterRunForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return owner.deliverySQLiteOwner.ObserveWriterRunForTest(ctx, runID)
	default:
		return WriterRunDeliveryEvidence{}, fmt.Errorf("writer delivery evidence requires selected owner")
	}
}

func ObserveLiveWriterCountForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return 0, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.lLMPostgresOwner.ObserveLiveWriterCountForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return owner.lLMSQLiteOwner.ObserveLiveWriterCountForTest(ctx, runID)
	default:
		return 0, fmt.Errorf("live writer evidence requires selected owner")
	}
}

func ObserveWriterFlowForTest(ctx context.Context, selected any, runID, entityID string) (WriterFlowEvidence, error) {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return WriterFlowEvidence{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ObserveWriterFlowForTest(ctx, runID, entityID)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ObserveWriterFlowForTest(ctx, runID, entityID)
	default:
		return WriterFlowEvidence{}, fmt.Errorf("writer flow evidence requires selected owner")
	}
}

func ObserveDeliveryEventEvidenceForTest(ctx context.Context, selected any, eventID string) (DeliveryEventEvidence, error) {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return DeliveryEventEvidence{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.deliveryPostgresOwner.ObserveEventEvidenceForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return owner.deliverySQLiteOwner.ObserveEventEvidenceForTest(ctx, eventID)
	default:
		return DeliveryEventEvidence{}, fmt.Errorf("delivery evidence requires original selected owner, got %T", selected)
	}
}
