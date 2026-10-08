package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func readWorkflowEmitFeedbackTx(ctx context.Context, tx *sql.Tx, eventID string, instance flowidentity.RunScopedFlowInstance) (pipeline.WorkflowEmitFeedback, bool, error) {
	if err := instance.Validate(); err != nil {
		return pipeline.WorkflowEmitFeedback{}, false, err
	}
	if err := validateWorkflowStageReceiptEventID(eventID); err != nil {
		return pipeline.WorkflowEmitFeedback{}, false, err
	}
	var raw sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT CAST(feedback AS TEXT) FROM workflow_publication_stage_receipts WHERE event_id=$1 AND run_id=$2 AND instance_path=$3`, eventID, instance.RunID, instance.Route.InstancePath).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) || err == nil && !raw.Valid {
		return pipeline.WorkflowEmitFeedback{}, false, nil
	}
	if err != nil {
		return pipeline.WorkflowEmitFeedback{}, false, err
	}
	var feedback pipeline.WorkflowEmitFeedback
	if err := json.Unmarshal([]byte(raw.String), &feedback); err != nil {
		return feedback, false, err
	}
	if feedback.Receipt.EventID() != eventID || feedback.Receipt.Stage().Instance != instance {
		return pipeline.WorkflowEmitFeedback{}, false, fmt.Errorf("emit feedback contradicts its exact occurrence index")
	}
	evidence, found, err := readWorkflowPublicationStages(ctx, tx, eventID, instance)
	if err != nil || !found {
		return pipeline.WorkflowEmitFeedback{}, false, errors.Join(err, fmt.Errorf("stored emit feedback lacks exact occurrence evidence"))
	}
	if err := validateWorkflowEmitFeedbackEvidence(feedback, evidence); err != nil {
		return pipeline.WorkflowEmitFeedback{}, false, err
	}
	return feedback, true, nil
}

func validateWorkflowEmitFeedbackEvidence(candidate pipeline.WorkflowEmitFeedback, evidence pipeline.WorkflowPublicationStageEvidence) error {
	valid := candidate.StageOrigin == pipeline.EmitStageAcceptance && candidate.Receipt == evidence.Acceptance
	if candidate.StageOrigin == pipeline.EmitStageHandler {
		for _, receipt := range evidence.Handlers {
			valid = valid || candidate.Receipt == receipt
		}
	}
	if !valid {
		return fmt.Errorf("emit feedback stage is not a committed fact of its own occurrence")
	}
	return nil
}

func commitWorkflowEmitFeedbackTx(ctx context.Context, tx *sql.Tx, postgres bool, candidate pipeline.WorkflowEmitFeedback) (pipeline.WorkflowEmitFeedbackCommit, error) {
	if err := candidate.Validate(); err != nil {
		return pipeline.WorkflowEmitFeedbackCommit{}, err
	}
	eventID, instance := candidate.Receipt.EventID(), candidate.Receipt.Stage().Instance
	query := `SELECT CAST(event_id AS TEXT) FROM workflow_publication_stage_receipts WHERE event_id=$1 AND run_id=$2 AND instance_path=$3`
	if postgres {
		query += ` FOR UPDATE`
	}
	var lockedEventID string
	if err := tx.QueryRowContext(ctx, query, eventID, instance.RunID, instance.Route.InstancePath).Scan(&lockedEventID); err != nil {
		return pipeline.WorkflowEmitFeedbackCommit{}, fmt.Errorf("emit feedback requires its committed publication: %w", err)
	}
	evidence, found, err := readWorkflowPublicationStages(ctx, tx, eventID, instance)
	if err != nil || !found {
		return pipeline.WorkflowEmitFeedbackCommit{}, errors.Join(err, fmt.Errorf("emit feedback lacks exact occurrence evidence"))
	}
	if err := validateWorkflowEmitFeedbackEvidence(candidate, evidence); err != nil {
		return pipeline.WorkflowEmitFeedbackCommit{}, err
	}
	existing, found, err := readWorkflowEmitFeedbackTx(ctx, tx, eventID, instance)
	if err != nil {
		return pipeline.WorkflowEmitFeedbackCommit{}, err
	}
	if found {
		return pipeline.WorkflowEmitFeedbackCommit{Feedback: existing, Replay: true}, nil
	}
	raw, err := json.Marshal(candidate)
	if err != nil {
		return pipeline.WorkflowEmitFeedbackCommit{}, err
	}
	updated, err := tx.ExecContext(ctx, `UPDATE workflow_publication_stage_receipts SET feedback=$1 WHERE event_id=$2 AND run_id=$3 AND instance_path=$4 AND feedback IS NULL`, string(raw), eventID, instance.RunID, instance.Route.InstancePath)
	if err != nil {
		return pipeline.WorkflowEmitFeedbackCommit{}, err
	}
	count, err := updated.RowsAffected()
	if err != nil || count != 1 {
		return pipeline.WorkflowEmitFeedbackCommit{}, errors.Join(err, fmt.Errorf("emit feedback lost its exact publication row"))
	}
	return pipeline.WorkflowEmitFeedbackCommit{Feedback: candidate}, nil
}

func (s *PipelinePostgresOwner) CommitWorkflowEmitFeedback(ctx context.Context, candidate pipeline.WorkflowEmitFeedback) (result pipeline.WorkflowEmitFeedbackCommit, err error) {
	if err := s.requireCurrentSchema(); err != nil {
		return result, err
	}
	ack, err := s.backend.RunTransactionOutcome(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, err = commitWorkflowEmitFeedbackTx(ctx, tx, true, candidate)
		return err
	})
	if !ack {
		return pipeline.WorkflowEmitFeedbackCommit{}, err
	}
	result.Acknowledged = true
	return result, err
}

func (s *PipelineSQLiteOwner) CommitWorkflowEmitFeedback(ctx context.Context, candidate pipeline.WorkflowEmitFeedback) (result pipeline.WorkflowEmitFeedbackCommit, err error) {
	if err := s.requireCurrentSchema(); err != nil {
		return result, err
	}
	ack, err := s.backend.RunTransactionOutcome(ctx, "sqlite workflow emit feedback", func(ctx context.Context, tx *sql.Tx) error {
		result, err = commitWorkflowEmitFeedbackTx(ctx, tx, false, candidate)
		return err
	})
	if !ack {
		return pipeline.WorkflowEmitFeedbackCommit{}, err
	}
	result.Acknowledged = true
	return result, err
}

func (s *PipelinePostgresOwner) ReadWorkflowEmitFeedback(ctx context.Context, eventID string, instance flowidentity.RunScopedFlowInstance) (result pipeline.WorkflowEmitFeedback, found bool, err error) {
	if err := s.requireCurrentSchema(); err != nil {
		return result, false, err
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, found, err = readWorkflowEmitFeedbackTx(ctx, tx, eventID, instance)
		return err
	})
	return result, found, err
}

func (s *PipelineSQLiteOwner) ReadWorkflowEmitFeedback(ctx context.Context, eventID string, instance flowidentity.RunScopedFlowInstance) (result pipeline.WorkflowEmitFeedback, found bool, err error) {
	if err := s.requireCurrentSchema(); err != nil {
		return result, false, err
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, found, err = readWorkflowEmitFeedbackTx(ctx, tx, eventID, instance)
		return err
	})
	return result, found, err
}

var _ pipeline.WorkflowEmitFeedbackOwner = (*PipelinePostgresOwner)(nil)
var _ pipeline.WorkflowEmitFeedbackOwner = (*PipelineSQLiteOwner)(nil)
