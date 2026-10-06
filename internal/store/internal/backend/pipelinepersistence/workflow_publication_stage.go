package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func captureWorkflowPublicationStageTx(ctx context.Context, tx *sql.Tx, postgres bool, event events.Event, request pipeline.WorkflowPublicationStageRequest, duplicate bool) (pipelineobligation.CommittedStageReceipt, error) {
	if err := request.ValidateEvent(event); err != nil {
		return pipelineobligation.CommittedStageReceipt{}, err
	}
	if duplicate {
		receipt, found, err := readWorkflowPublicationAcceptanceTx(ctx, tx, event.ID(), request.Instance)
		if err != nil {
			return pipelineobligation.CommittedStageReceipt{}, fmt.Errorf("load exact publication acceptance stage: %w", err)
		}
		if !found {
			return pipelineobligation.CommittedStageReceipt{}, fmt.Errorf("exact duplicate publication lacks its acceptance stage receipt")
		}
		stage := receipt.Stage()
		if receipt.EventID() != event.ID() || stage.Instance != request.Instance || stage.EntityID != request.EntityID {
			return pipelineobligation.CommittedStageReceipt{}, fmt.Errorf("publication acceptance receipt contradicts its exact source")
		}
		return receipt, nil
	}
	query := `SELECT CAST(entity_id AS TEXT),flow_template,current_state,revision,updated_at,stage_defined FROM flow_instances WHERE run_id=$1 AND instance_path=$2`
	if postgres {
		query += ` FOR SHARE`
	}
	stage := engine.CommittedStage{Instance: request.Instance}
	var scope string
	var updatedAt any
	if err := tx.QueryRowContext(ctx, query, request.Instance.RunID, request.Instance.Route.InstancePath).Scan(&stage.EntityID, &scope, &stage.Stage, &stage.Revision, &updatedAt, &stage.StageDefined); err != nil {
		return pipelineobligation.CommittedStageReceipt{}, err
	}
	if postgres {
		value, ok := updatedAt.(time.Time)
		if !ok {
			return pipelineobligation.CommittedStageReceipt{}, fmt.Errorf("publication stage timestamp is invalid")
		}
		stage.UpdatedAt = value.UTC()
	} else {
		value, found, err := sqliteTimeValue(updatedAt)
		if err != nil || !found {
			return pipelineobligation.CommittedStageReceipt{}, fmt.Errorf("publication stage timestamp is invalid: %v", err)
		}
		stage.UpdatedAt = value.UTC()
	}
	if scope != request.Instance.Route.ScopeKey || stage.EntityID != request.EntityID {
		return pipelineobligation.CommittedStageReceipt{}, fmt.Errorf("publication stage source contradicts its constructed header")
	}
	receipt, err := pipelineobligation.StageReceiptEvidence(event.ID(), stage)
	if err != nil {
		return receipt, err
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return receipt, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO workflow_publication_stage_receipts(event_id,run_id,instance_path,receipt) VALUES($1,$2,$3,$4)`, event.ID(), request.Instance.RunID, request.Instance.Route.InstancePath, string(raw))
	return receipt, err
}

func readWorkflowPublicationAcceptanceTx(ctx context.Context, tx *sql.Tx, eventID string, instance flowidentity.RunScopedFlowInstance) (pipelineobligation.CommittedStageReceipt, bool, error) {
	if err := instance.Validate(); err != nil {
		return pipelineobligation.CommittedStageReceipt{}, false, err
	}
	if err := validateWorkflowStageReceiptEventID(eventID); err != nil {
		return pipelineobligation.CommittedStageReceipt{}, false, err
	}
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT CAST(receipt AS TEXT) FROM workflow_publication_stage_receipts WHERE event_id=$1 AND run_id=$2 AND instance_path=$3`, eventID, instance.RunID, instance.Route.InstancePath).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return pipelineobligation.CommittedStageReceipt{}, false, nil
		}
		return pipelineobligation.CommittedStageReceipt{}, false, err
	}
	var receipt pipelineobligation.CommittedStageReceipt
	if err := json.Unmarshal([]byte(raw), &receipt); err != nil {
		return pipelineobligation.CommittedStageReceipt{}, false, err
	}
	if receipt.EventID() != eventID || receipt.Stage().Instance != instance {
		return pipelineobligation.CommittedStageReceipt{}, false, fmt.Errorf("publication acceptance contradicts its exact index")
	}
	return receipt, true, nil
}

func (s *PipelinePostgresOwner) CaptureWorkflowPublicationStageTx(ctx context.Context, tx *sql.Tx, event events.Event, request pipeline.WorkflowPublicationStageRequest, duplicate bool) (pipelineobligation.CommittedStageReceipt, error) {
	return captureWorkflowPublicationStageTx(ctx, tx, true, event, request, duplicate)
}

func (s *PipelineSQLiteOwner) CaptureWorkflowPublicationStageTx(ctx context.Context, tx *sql.Tx, event events.Event, request pipeline.WorkflowPublicationStageRequest, duplicate bool) (pipelineobligation.CommittedStageReceipt, error) {
	return captureWorkflowPublicationStageTx(ctx, tx, false, event, request, duplicate)
}

func readWorkflowPublicationStages(ctx context.Context, tx *sql.Tx, eventID string, instance flowidentity.RunScopedFlowInstance) (pipeline.WorkflowPublicationStageEvidence, bool, error) {
	acceptance, found, err := readWorkflowPublicationAcceptanceTx(ctx, tx, eventID, instance)
	if err != nil || !found {
		return pipeline.WorkflowPublicationStageEvidence{}, found, err
	}
	result := pipeline.WorkflowPublicationStageEvidence{Acceptance: acceptance}
	result.Handlers, err = readWorkflowHandlerStageReceipts(ctx, tx, eventID, instance)
	if err != nil {
		return pipeline.WorkflowPublicationStageEvidence{}, false, err
	}
	return result, true, result.Validate()
}

func (s *PipelinePostgresOwner) ReadWorkflowPublicationStages(ctx context.Context, eventID string, instance flowidentity.RunScopedFlowInstance) (result pipeline.WorkflowPublicationStageEvidence, found bool, err error) {
	if err := s.requireCurrentSchema(); err != nil {
		return result, false, err
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, found, err = readWorkflowPublicationStages(ctx, tx, eventID, instance)
		return err
	})
	return result, found, err
}

func (s *PipelineSQLiteOwner) ReadWorkflowPublicationStages(ctx context.Context, eventID string, instance flowidentity.RunScopedFlowInstance) (result pipeline.WorkflowPublicationStageEvidence, found bool, err error) {
	if err := s.requireCurrentSchema(); err != nil {
		return result, false, err
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, found, err = readWorkflowPublicationStages(ctx, tx, eventID, instance)
		return err
	})
	return result, found, err
}
