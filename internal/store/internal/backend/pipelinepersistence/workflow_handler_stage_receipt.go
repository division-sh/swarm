package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func persistWorkflowHandlerStageReceiptTx(ctx context.Context, tx *sql.Tx, claim deliverylifecycle.Claim, settled deliverylifecycle.Snapshot, stage engine.CommittedStage) error {
	if claim.SubscriberClass() != deliverylifecycle.SubscriberNode || !settled.MatchesSettlementClaim(claim) || settled.Status != deliverylifecycle.StatusDelivered || claim.RunID() != stage.Instance.RunID {
		return fmt.Errorf("handler stage receipt requires its exact settled node claim")
	}
	target := settled.Route.Target.Route()
	if target.FlowID != stage.Instance.Route.ScopeKey || target.FlowInstance != stage.Instance.Route.InstancePath || target.EntityID != stage.EntityID {
		return fmt.Errorf("handler stage receipt contradicts its settled target")
	}
	receipt, err := pipelineobligation.StageReceiptEvidence(settled.EventID, stage)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	query := `INSERT INTO workflow_handler_stage_receipts(delivery_id,claim_version,event_id,run_id,instance_path,receipt) VALUES($1,$2,$3,$4,$5,$6)`
	if _, err := tx.ExecContext(ctx, query, claim.DeliveryID(), claim.Version(), settled.EventID, stage.Instance.RunID, stage.Instance.Route.InstancePath, string(raw)); err != nil {
		return fmt.Errorf("commit exact handler stage receipt: %w", err)
	}
	return nil
}

// This snapshot consumes immutable handler receipts, never the current header.
// A caller cannot replace the event's own-instance result with another receiver.
func readWorkflowHandlerStageReceipts(ctx context.Context, tx *sql.Tx, eventID string, instance flowidentity.RunScopedFlowInstance) (result []pipelineobligation.CommittedStageReceipt, err error) {
	if err := instance.Validate(); err != nil {
		return nil, err
	}
	if err := validateWorkflowStageReceiptEventID(eventID); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT CAST(receipt AS TEXT) FROM workflow_handler_stage_receipts WHERE event_id=$1 AND run_id=$2 AND instance_path=$3 ORDER BY claim_version,delivery_id`, eventID, instance.RunID, instance.Route.InstancePath)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var receipt pipelineobligation.CommittedStageReceipt
		if err := json.Unmarshal([]byte(raw), &receipt); err != nil {
			return nil, err
		}
		if receipt.EventID() != eventID || receipt.Stage().Instance != instance {
			return nil, fmt.Errorf("persisted handler stage receipt contradicts its exact index")
		}
		result = append(result, receipt)
	}
	return result, rows.Err()
}

func validateWorkflowStageReceiptEventID(eventID string) error {
	if id, err := uuid.Parse(eventID); err != nil || id == uuid.Nil || id.String() != eventID {
		return fmt.Errorf("stage receipt lookup requires its canonical event UUID")
	}
	return nil
}

func (s *PipelinePostgresOwner) ReadWorkflowHandlerStageReceipts(ctx context.Context, eventID string, instance flowidentity.RunScopedFlowInstance) (receipts []pipelineobligation.CommittedStageReceipt, err error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		receipts, err = readWorkflowHandlerStageReceipts(ctx, tx, eventID, instance)
		return err
	})
	return receipts, err
}

func (s *PipelineSQLiteOwner) ReadWorkflowHandlerStageReceipts(ctx context.Context, eventID string, instance flowidentity.RunScopedFlowInstance) (receipts []pipelineobligation.CommittedStageReceipt, err error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		receipts, err = readWorkflowHandlerStageReceipts(ctx, tx, eventID, instance)
		return err
	})
	return receipts, err
}
