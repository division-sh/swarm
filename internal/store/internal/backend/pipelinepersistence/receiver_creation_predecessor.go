package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

func (s *PipelinePostgresOwner) ReceiverCreatingEventTx(ctx context.Context, tx *sql.Tx, runID string, route events.DeliveryRoute) (string, error) {
	return receiverCreatingEventTx(ctx, tx, runID, route)
}

func (s *PipelineSQLiteOwner) ReceiverCreatingEventTx(ctx context.Context, tx *sql.Tx, runID string, route events.DeliveryRoute) (string, error) {
	return receiverCreatingEventTx(ctx, tx, runID, route)
}

// Construction supplies the immutable predecessor identity, never its outcome.
// Delivery admission consumes that identity inside its original claim transaction.
func receiverCreatingEventTx(ctx context.Context, tx *sql.Tx, runID string, route events.DeliveryRoute) (string, error) {
	if tx == nil || route.Target.Empty() {
		return "", fmt.Errorf("creating-turn observation requires a transaction and exact receiver")
	}
	if err := events.ValidateDeliveryRoutes([]events.DeliveryRoute{route}); err != nil {
		return "", err
	}
	target := route.Target.Route()
	owner, err := flowidentity.NewRunScopedFlowInstance(runID,
		flowidentity.StoredRoute(target.FlowID, flowidentity.LogicalInstanceID(target.FlowInstance), target.FlowInstance))
	if err != nil {
		return "", err
	}
	var entityID string
	err = tx.QueryRowContext(ctx, `SELECT entity_id FROM flow_instances
		WHERE run_id=$1 AND instance_path=$2 AND flow_template=$3`,
		owner.RunID, owner.Route.InstancePath, owner.Route.ScopeKey).Scan(&entityID)
	if errors.Is(err, sql.ErrNoRows) {
		// Absence supplies no predecessor; it does not admit construction or bypass
		// the independent exact-target validation before handler execution.
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read creating-turn receiver identity: %w", err)
	}
	if target.EntityID != "" && target.EntityID != entityID {
		return "", fmt.Errorf("creating-turn target contradicts constructed receiver identity")
	}
	evidence, err := ReadFlowConstructionPublicationTx(ctx, tx, owner, entityID)
	if err != nil {
		return "", err
	}
	return evidence.CreatingInput.EventID, nil
}
