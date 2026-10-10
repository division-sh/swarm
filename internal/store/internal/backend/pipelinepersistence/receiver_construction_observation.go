package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func (s *PipelinePostgresOwner) LoadFlowConstructionPublication(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entityID string) (pipeline.FlowConstructionPublicationEvidence, error) {
	if s == nil || s.backend == nil {
		return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("postgres construction receipt owner is required")
	}
	var evidence pipeline.FlowConstructionPublicationEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		evidence, err = ReadFlowConstructionPublicationTx(ctx, tx, owner, entityID)
		return err
	})
	if err != nil {
		return pipeline.FlowConstructionPublicationEvidence{}, err
	}
	return evidence, nil
}

func (s *PipelineSQLiteOwner) LoadFlowConstructionPublication(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entityID string) (pipeline.FlowConstructionPublicationEvidence, error) {
	if s == nil || s.backend == nil {
		return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("sqlite construction receipt owner is required")
	}
	var evidence pipeline.FlowConstructionPublicationEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		evidence, err = ReadFlowConstructionPublicationTx(ctx, tx, owner, entityID)
		return err
	})
	if err != nil {
		return pipeline.FlowConstructionPublicationEvidence{}, err
	}
	return evidence, nil
}

// ReadFlowConstructionPublicationTx reads the immutable construction
// receipt, not either current half of the receiver persistence aggregate.
func ReadFlowConstructionPublicationTx(ctx context.Context, tx *sql.Tx, owner flowidentity.RunScopedFlowInstance, entityID string) (pipeline.FlowConstructionPublicationEvidence, error) {
	if tx == nil {
		return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("construction evidence requires the selected read transaction")
	}
	if err := owner.Validate(); err != nil {
		return pipeline.FlowConstructionPublicationEvidence{}, err
	}
	entity, entityErr := uuid.Parse(entityID)
	if entityErr != nil || entity == uuid.Nil || entity.String() != entityID {
		return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("construction evidence requires exact entity identity")
	}
	var raw, readiness []byte
	var templateID, headerEntityID, planHash string
	var parentInstance, instanceKey sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT m.projection, fi.flow_template, fi.entity_id, r.plan, r.plan_hash, fi.parent_instance, fi.instance_key
		FROM workflow_instance_initial_materializations m
		JOIN flow_instances fi ON fi.run_id=m.run_id AND fi.instance_path=m.instance_path AND fi.entity_id=m.entity_id
		JOIN flow_instance_runtime_readiness r ON r.run_id=fi.run_id AND r.instance_path=fi.instance_path
		WHERE m.run_id=$1 AND m.entity_id=$2 AND m.instance_path=$3`, owner.RunID, entityID, owner.Route.InstancePath).Scan(&raw, &templateID, &headerEntityID, &readiness, &planHash, &parentInstance, &instanceKey); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return pipeline.FlowConstructionPublicationEvidence{}, corruptFlowConstructionEvidence(owner, err)
		}
		return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("read immutable construction evidence: %w", err)
	}
	evidence, err := pipeline.ProjectFlowConstructionPublication(raw, owner, entityID)
	if err != nil {
		return pipeline.FlowConstructionPublicationEvidence{}, corruptFlowConstructionEvidence(owner, err)
	}
	if evidence.CreatingInput.EventID == uuid.Nil.String() {
		return pipeline.FlowConstructionPublicationEvidence{}, corruptFlowConstructionEvidence(owner, fmt.Errorf("construction evidence requires exact creating-event identity"))
	}
	plan, err := pipeline.DecodeFlowReadinessPlan(readiness, planHash)
	if err != nil {
		return pipeline.FlowConstructionPublicationEvidence{}, corruptFlowConstructionEvidence(owner, err)
	}
	if evidence.Identity.TemplateID != templateID || evidence.Identity.EntityID != headerEntityID || plan.Identity != evidence.Identity || plan.RunID != owner.RunID ||
		evidence.Identity.ParentRoute.FlowInstance != parentInstance.String || evidence.InstanceKey != instanceKey.String {
		return pipeline.FlowConstructionPublicationEvidence{}, corruptFlowConstructionEvidence(owner, fmt.Errorf("construction evidence contradicts its native header or attachment owner"))
	}
	return evidence, nil
}

func corruptFlowConstructionEvidence(owner flowidentity.RunScopedFlowInstance, err error) error {
	return &pipeline.FlowInstanceConstructionCorruption{RunID: owner.RunID, FlowID: owner.Route.ScopeKey, InstancePath: owner.Route.InstancePath, Cause: err}
}

func ReadFlowConstructionPublicationFieldsTx(ctx context.Context, tx *sql.Tx, owner flowidentity.RunScopedFlowInstance, entityID, eventID string) (map[string]any, error) {
	if tx == nil {
		return nil, fmt.Errorf("construction evidence requires the selected read transaction")
	}
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	entity, entityErr := uuid.Parse(entityID)
	event, eventErr := uuid.Parse(eventID)
	if entityErr != nil || eventErr != nil || entity == uuid.Nil || event == uuid.Nil || entity.String() != entityID || event.String() != eventID {
		return nil, fmt.Errorf("construction evidence requires exact entity and creating-event identities")
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT projection FROM workflow_instance_initial_materializations
		WHERE run_id=$1 AND entity_id=$2 AND instance_path=$3`, owner.RunID, entityID, owner.Route.InstancePath).Scan(&raw); err != nil {
		return nil, fmt.Errorf("read immutable construction evidence: %w", err)
	}
	return pipeline.FlowConstructionPublicationFields(raw, owner, entityID, eventID)
}
