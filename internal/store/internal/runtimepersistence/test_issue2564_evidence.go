package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
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
