package operatorsurface

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func (s *EntityPostgres) LoadOperatorEntityAtLife(ctx context.Context, entityID, runID string, life int64) (operatorread.OperatorEntityFull, error) {
	current, err := s.LoadOperatorEntity(ctx, entityID, runID)
	if err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	if err := validateOperatorEntityLife(current, life); err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	if *current.Entity.Life == life {
		return current, nil
	}
	metadata, projection, found, err := runforkrevision.ReadEntityLife(ctx, s.backend, current.Entity.RunID, current.Entity.EntityID, life, false)
	if err != nil || !found {
		if err == nil {
			err = operatorread.ErrEntityNotFound
		}
		return operatorread.OperatorEntityFull{}, err
	}
	return operatorEntityLifeProjection(current, metadata, projection.Fields, projection.Bookkeeping, projection.Accumulator)
}

func (s *EntitySQLite) LoadOperatorEntityAtLife(ctx context.Context, entityID, runID string, life int64) (operatorread.OperatorEntityFull, error) {
	current, err := s.LoadOperatorEntity(ctx, entityID, runID)
	if err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	if err := validateOperatorEntityLife(current, life); err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	if *current.Entity.Life == life {
		return current, nil
	}
	metadata, projection, found, err := runforkrevision.ReadEntityLife(ctx, s.backend, current.Entity.RunID, current.Entity.EntityID, life, false)
	if err != nil || !found {
		if err == nil {
			err = operatorread.ErrEntityNotFound
		}
		return operatorread.OperatorEntityFull{}, err
	}
	return operatorEntityLifeProjection(current, metadata, projection.Fields, projection.Bookkeeping, projection.Accumulator)
}

func validateOperatorEntityLife(current operatorread.OperatorEntityFull, life int64) error {
	if life <= 0 {
		return &operatorread.EntityReadParamError{Field: "iter", Reason: "must be a positive integer"}
	}
	if current.Entity.Life == nil {
		return &operatorread.EntityReadParamError{Field: "iter", Reason: "requires a constructed instance"}
	}
	if life > *current.Entity.Life {
		return operatorread.ErrEntityNotFound
	}
	return nil
}

func operatorEntityLifeProjection(current operatorread.OperatorEntityFull, metadata runforkrevision.EntityMetadataFact, fields, bookkeeping, accumulator map[string]any) (operatorread.OperatorEntityFull, error) {
	if metadata.EntityID != current.Entity.EntityID || metadata.FlowInstance != current.Entity.FlowInstance ||
		metadata.EntityType != current.Entity.EntityType || metadata.InstanceRevision <= 0 ||
		metadata.DataCreatedAt == nil || metadata.DataUpdatedAt == nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("prior entity life requires exact retained state-row and header evidence")
	}
	loops, err := loopruntime.PublicActivations(accumulator)
	if err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	summary := operatorread.OperatorEntitySummary{
		EntityID: metadata.EntityID, RunID: current.Entity.RunID, FlowInstance: metadata.FlowInstance, EntityType: metadata.EntityType,
		StageDefined: &metadata.StageDefined, Life: &metadata.Life, Revision: &metadata.InstanceRevision,
		LifecycleCreatedAt: &metadata.CreatedAt, LifecycleUpdatedAt: &metadata.UpdatedAt,
		CreatedAt: *metadata.DataCreatedAt, UpdatedAt: *metadata.DataUpdatedAt, Slug: metadata.Slug, Name: metadata.Name,
	}
	if metadata.StageDefined {
		summary.CurrentState, summary.EnteredStateAt = &metadata.CurrentState, &metadata.EnteredStateAt
	}
	return operatorread.OperatorEntityFull{Entity: summary, Fields: fields, Bookkeeping: bookkeeping,
		Accumulated: loopruntime.PublicStateBuckets(accumulator), Loops: loops}, nil
}
