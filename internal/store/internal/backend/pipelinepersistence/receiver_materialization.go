package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func (s *PipelinePostgresOwner) ReceiverMaterializedTx(ctx context.Context, tx *sql.Tx, route events.DeliveryRoute) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("receiver construction requires its original transaction")
	}
	run, err := s.LoadSnapshotTx(ctx, tx, route.AgentIdentity.RunID, false)
	if err != nil {
		return false, err
	}
	return receiverMaterializedTx(ctx, tx, route, false, run, s.historicalInstances)
}

func (s *PipelineSQLiteOwner) ReceiverMaterializedTx(ctx context.Context, tx *sql.Tx, route events.DeliveryRoute) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("receiver construction requires its original transaction")
	}
	run, err := s.LoadSnapshotTx(ctx, tx, route.AgentIdentity.RunID)
	if err != nil {
		return false, err
	}
	return receiverMaterializedTx(ctx, tx, route, true, run, s.historicalInstances)
}

// Delivery settlement is not state ownership. The existing target persistence
// owner admits the exact persisted halves before dependent execution is ready.
func receiverMaterializedTx(ctx context.Context, tx *sql.Tx, route events.DeliveryRoute, sqlite bool, run runlifecycle.Snapshot, historical HistoricalFlowInstanceReader) (bool, error) {
	if tx == nil || !route.Recipient.IsAgent() || route.Initialization.Empty() {
		return false, fmt.Errorf("receiver materialization requires a transaction and exact construction dependency")
	}
	if err := events.ValidateDeliveryRoutes([]events.DeliveryRoute{route}); err != nil {
		return false, err
	}
	if err := route.Initialization.ValidateRoute(route); err != nil {
		return false, err
	}
	target := route.Initialization.ConstructionTarget().Route()
	receiver := route.Initialization.ReceiverRoute()
	scope, logical := receiver.ScopeKey, receiver.InstanceID
	if receiver.Presence == agentidentity.RouteRoot {
		scope, logical = ".", route.AgentIdentity.RunID
	}
	flow, err := flowidentity.NewRunScopedFlowInstance(route.AgentIdentity.RunID,
		flowidentity.StoredRoute(scope, logical, target.FlowInstance))
	if err != nil {
		return false, err
	}
	if !flow.MatchesAgentRoute(route.AgentIdentity) {
		return false, fmt.Errorf("receiver materialization target contradicts exact agent coordinates")
	}
	entity := identity.NormalizeEntityID(target.EntityID)
	record, err := loadWorkflowTargetPersistence(ctx, tx, flow, entity, sqlite)
	if err != nil {
		return false, err
	}
	if record.Presence == pipeline.WorkflowTargetPersistenceAbsent {
		return false, nil
	}
	if !record.Presence.Constructed() {
		return false, fmt.Errorf("receiver construction persistence is incomplete")
	}
	header, err := record.DecodeComplete(flow.Route, entity)
	if err != nil {
		return false, err
	}
	if header.WorkflowName != target.FlowID {
		return false, fmt.Errorf("receiver dependency contradicts constructed flow declaration")
	}
	if record.Lifecycle.Status == "terminated" {
		return false, fmt.Errorf("materialized receiver lifecycle has terminated")
	}
	return receiverConstructionEvidenceTx(ctx, tx, flow, route.Initialization, run, header, historical)
}

func receiverConstructionEvidenceTx(ctx context.Context, tx *sql.Tx, flow flowidentity.RunScopedFlowInstance, initialization events.ReceiverInitialization, run runlifecycle.Snapshot, header pipeline.WorkflowInstance, historical HistoricalFlowInstanceReader) (bool, error) {
	target := initialization.ConstructionTarget().Route()
	if historical == nil || run.RunID != flow.RunID {
		return false, fmt.Errorf("receiver construction requires its exact native/historical owner")
	}
	fixed, projected, err := historical.ReadHistoricalFlowInstanceConstructionEvidenceTx(ctx, tx, run, header)
	if err != nil {
		return false, err
	}
	original, recorded, err := ReadStoredFlowConstructionReceiptTx(ctx, tx, run.RunID, header)
	if err != nil {
		return false, err
	}
	if projected && recorded && (fixed.Identity != original.Identity || fixed.InstanceKey != original.InstanceKey) {
		return false, fmt.Errorf("receiver native receipt contradicts fixed historical construction")
	}
	if recorded {
		if err := original.ValidateStoredMode(header.Mode); err != nil {
			return false, err
		}
	}
	if dependency, isHistorical := initialization.HistoricalConstruction(); isHistorical {
		if !projected || fixed.Identity.TemplateID != target.FlowID || fixed.Identity.EntityID != target.EntityID ||
			fixed.Identity.InstancePath != target.FlowInstance || fixed.SourceOwner.RunID != dependency.SourceRunID || fixed.SourceRevision != dependency.SourceRevision {
			return false, fmt.Errorf("receiver dependency contradicts exact historical construction")
		}
		sourceReceiver, err := fixed.SourceOwner.Route.AgentIdentityRoute()
		if err != nil || sourceReceiver != dependency.SourceReceiver {
			return false, fmt.Errorf("receiver dependency contradicts captured historical coordinates")
		}
		return true, nil
	}
	if !recorded {
		return false, fmt.Errorf("native receiver construction lacks its original receipt")
	}
	if original.Identity.TemplateID != target.FlowID {
		return false, fmt.Errorf("receiver dependency contradicts original construction declaration")
	}
	if original.CreatingInput.EventID != initialization.CreatingEventID() {
		return false, fmt.Errorf("receiver dependency contradicts original construction publication")
	}
	return true, nil
}
