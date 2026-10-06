package runtimepersistence

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	storepipeline "github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type DeliveryEventEvidence = storedelivery.DeliveryEventEvidence
type EntityMutationEvidence = storepipeline.EntityMutationEvidence
type PipelineReceiptEvidence = storepipeline.PipelineReceiptEvidence
type ActivityResultMutationEvidence = storepipeline.ActivityResultMutationEvidence
type WriterRunDeliveryEvidence = storedelivery.WriterRunDeliveryEvidence
type WriterFlowEvidence = storepipeline.WriterFlowEvidence
type WriterStageEvidence = storepipeline.WriterStageEvidence
type CardContentionEvidence = storepipeline.CardContentionEvidence

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

func ObserveActivityResultMutationsForTest(ctx context.Context, selected any, eventID string) (ActivityResultMutationEvidence, error) {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return ActivityResultMutationEvidence{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ObserveActivityResultMutationsForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ObserveActivityResultMutationsForTest(ctx, eventID)
	default:
		return ActivityResultMutationEvidence{}, fmt.Errorf("activity result mutation evidence requires original selected owner, got %T", selected)
	}
}

func ObserveEventCardinalityForTest(ctx context.Context, selected any, eventID string) (int, error) {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return 0, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.eventPostgresOwner.ObserveEventCardinalityForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return owner.eventSQLiteOwner.ObserveEventCardinalityForTest(ctx, eventID)
	default:
		return 0, fmt.Errorf("event cardinality requires original selected owner, got %T", selected)
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

func ObservePipelineReceiptForTest(ctx context.Context, selected any, eventID string) (PipelineReceiptEvidence, error) {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return PipelineReceiptEvidence{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ObserveReceiptForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ObserveReceiptForTest(ctx, eventID)
	default:
		return PipelineReceiptEvidence{}, fmt.Errorf("pipeline receipt requires original selected owner, got %T", selected)
	}
}

func RetirePlannedReadinessForTest(ctx context.Context, selected any, plan pipeline.DynamicFlowRuntimeReadinessPlan, ordinal uint64, at time.Time) error {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.RetirePlannedReadinessForTest(ctx, plan, ordinal, at)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.RetirePlannedReadinessForTest(ctx, plan, ordinal, at)
	default:
		return fmt.Errorf("readiness fixture requires original selected owner, got %T", selected)
	}
}

type SelectedPoolEvidence struct{ InUse, Idle, OpenConnections, MaxOpenConnections int }

// ObserveSelectedPoolForTest observes only the original construction's pool.
// Neither the handle nor connection admission escapes this owner.
func ObserveSelectedPoolForTest(selected any) (SelectedPoolEvidence, error) {
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner != nil && owner.backend != nil {
			stats := owner.backend.ConstructionHandle().Stats()
			return SelectedPoolEvidence{stats.InUse, stats.Idle, stats.OpenConnections, stats.MaxOpenConnections}, nil
		}
	case *SQLiteRuntimeStore:
		if owner != nil && owner.backend != nil {
			stats := owner.backend.ConstructionHandle().Stats()
			return SelectedPoolEvidence{stats.InUse, stats.Idle, stats.OpenConnections, stats.MaxOpenConnections}, nil
		}
	}
	return SelectedPoolEvidence{}, fmt.Errorf("pool evidence requires original selected construction, got %T", selected)
}
