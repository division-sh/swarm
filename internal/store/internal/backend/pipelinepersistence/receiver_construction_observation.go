package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

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
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT projection FROM workflow_instance_initial_materializations
		WHERE run_id=$1 AND entity_id=$2 AND instance_path=$3`, owner.RunID, entityID, owner.Route.InstancePath).Scan(&raw); err != nil {
		return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("read immutable construction evidence: %w", err)
	}
	evidence, err := pipeline.ProjectFlowConstructionPublication(raw, owner, entityID)
	if err != nil {
		return pipeline.FlowConstructionPublicationEvidence{}, err
	}
	if evidence.CreatingInput.EventID == uuid.Nil.String() {
		return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("construction evidence requires exact creating-event identity")
	}
	return evidence, nil
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
