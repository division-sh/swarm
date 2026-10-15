package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func (s *PipelinePostgresOwner) ReceiverCreatingEventTx(ctx context.Context, tx *sql.Tx, runID string, route events.DeliveryRoute) (string, error) {
	run, err := s.LoadSnapshotTx(ctx, tx, runID, false)
	if err != nil {
		return "", err
	}
	return ordinaryReceiverCreatingEventTx(ctx, tx, run, route, s.historicalInstances, false)
}

func (s *PipelineSQLiteOwner) ReceiverCreatingEventTx(ctx context.Context, tx *sql.Tx, runID string, route events.DeliveryRoute) (string, error) {
	run, err := s.LoadSnapshotTx(ctx, tx, runID)
	if err != nil {
		return "", err
	}
	return ordinaryReceiverCreatingEventTx(ctx, tx, run, route, s.historicalInstances, true)
}

func ordinaryReceiverCreatingEventTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, route events.DeliveryRoute, history HistoricalFlowInstanceReader, sqlite bool) (string, error) {
	if err := run.Validate(); err != nil {
		return "", err
	}
	if run.Origin.Kind() != runlifecycle.OriginForkMaterialization {
		return receiverCreatingEventTx(ctx, tx, run.RunID, route)
	}
	return historicalReceiverCreatingEventTx(ctx, tx, run, route, history, sqlite)
}

func (s *PipelinePostgresOwner) SelectedReceiverCreatingEventTx(ctx context.Context, tx *sql.Tx, authority deliverylifecycle.ExecutionAuthority, route events.DeliveryRoute) (string, error) {
	run, err := s.LoadSnapshotTx(ctx, tx, authority.ForkRunID(), false)
	if err != nil {
		return "", err
	}
	return selectedReceiverCreatingEventTx(ctx, tx, authority, route, run, s.historicalInstances, false)
}

func (s *PipelineSQLiteOwner) SelectedReceiverCreatingEventTx(ctx context.Context, tx *sql.Tx, authority deliverylifecycle.ExecutionAuthority, route events.DeliveryRoute) (string, error) {
	run, err := s.LoadSnapshotTx(ctx, tx, authority.ForkRunID())
	if err != nil {
		return "", err
	}
	return selectedReceiverCreatingEventTx(ctx, tx, authority, route, run, s.historicalInstances, true)
}

// The delivery owner has checked the exact selected-execution fence. A missing
// native receipt is never historical authority: the fixed cut must prove it.
func selectedReceiverCreatingEventTx(ctx context.Context, tx *sql.Tx, authority deliverylifecycle.ExecutionAuthority, route events.DeliveryRoute, run runlifecycle.Snapshot, history HistoricalFlowInstanceReader, sqlite bool) (string, error) {
	if tx == nil || authority.Kind() != deliverylifecycle.ExecutionAuthoritySelectedContractFork || authority.Validate() != nil || route.Target.Empty() {
		return "", fmt.Errorf("selected creating-turn identity requires its admitted fork authority and transaction")
	}
	if err := events.ValidateDeliveryRoutes([]events.DeliveryRoute{route}); err != nil {
		return "", err
	}
	if history == nil || run.RunID != authority.ForkRunID() || run.Validate() != nil {
		return "", fmt.Errorf("selected creating-turn observation requires its bound run and fixed-snapshot owner")
	}
	return historicalReceiverCreatingEventTx(ctx, tx, run, route, history, sqlite)
}

func historicalReceiverCreatingEventTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, route events.DeliveryRoute, history HistoricalFlowInstanceReader, sqlite bool) (string, error) {
	if tx == nil || history == nil || route.Target.Empty() || run.Validate() != nil {
		return "", fmt.Errorf("creating-turn history requires its bound run, exact receiver and original transaction")
	}
	if err := events.ValidateDeliveryRoutes([]events.DeliveryRoute{route}); err != nil {
		return "", err
	}
	header, err := constructedCreatingReceiverHeader(ctx, tx, run.RunID, route.Target.Route(), sqlite)
	if err != nil {
		return "", err
	}
	_, historical, err := history.ReadHistoricalFlowInstanceConstructionEvidenceTx(ctx, tx, run, header)
	if err != nil {
		return "", err
	}
	if historical {
		if _, present, err := ReadStoredFlowConstructionReceiptTx(ctx, tx, run.RunID, header); err != nil {
			return "", err
		} else if present {
			if err := requireProjectedReceiverReceiptTx(ctx, tx, run, header, history); err != nil {
				return "", err
			}
		}
		return "", nil
	}
	return receiverCreatingEventTx(ctx, tx, run.RunID, route)
}

func constructedCreatingReceiverHeader(ctx context.Context, tx *sql.Tx, runID string, target events.RouteIdentity, sqlite bool) (pipeline.WorkflowInstance, error) {
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(target.FlowID, flowidentity.LogicalInstanceID(target.FlowInstance), target.FlowInstance))
	if err != nil {
		return pipeline.WorkflowInstance{}, err
	}
	var entityID string
	if err := tx.QueryRowContext(ctx, `SELECT entity_id FROM flow_instances WHERE run_id=$1 AND instance_path=$2 AND flow_template=$3`, runID, target.FlowInstance, target.FlowID).Scan(&entityID); err != nil {
		return pipeline.WorkflowInstance{}, fmt.Errorf("read constructed receiver: %w", err)
	}
	if target.EntityID != "" && target.EntityID != entityID {
		return pipeline.WorkflowInstance{}, fmt.Errorf("creating-turn target contradicts constructed receiver identity")
	}
	paired, err := loadWorkflowTargetPersistence(ctx, tx, owner, identity.NormalizeEntityID(entityID), sqlite)
	if err != nil {
		return pipeline.WorkflowInstance{}, err
	}
	return paired.DecodeComplete(owner.Route, identity.NormalizeEntityID(entityID))
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
