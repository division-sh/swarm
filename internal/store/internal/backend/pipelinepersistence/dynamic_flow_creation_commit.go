package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func prepareDynamicFlowCreationOccurrenceCommit(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	req runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest,
) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("dynamic flow creation occurrence requires private transaction ownership")
	}
	if err := req.Validate(); err != nil {
		return false, err
	}
	state, err := authorizeCurrentFlowActivationAttemptTx(ctx, tx, postgres, req.Attempt)
	if err != nil {
		return false, fmt.Errorf("authorize dynamic flow creation occurrence attempt: %w", err)
	}
	if state != "accepted" {
		return false, fmt.Errorf("dynamic flow creation occurrence requires completed activation attempt")
	}
	expected, err := req.Plan.Normalized()
	if err != nil {
		return false, err
	}
	expectedHash, err := expected.Hash()
	if err != nil {
		return false, fmt.Errorf("encode expected dynamic flow readiness %s: %w", req.InstancePath, err)
	}
	current, found, err := loadDynamicFlowRuntimeReadiness(ctx, tx, postgres, req.RunID, runtimeflowidentity.RouteForInstancePath(req.InstancePath), true)
	if err != nil {
		return false, err
	}
	if !found || !current.Eligible() {
		return false, fmt.Errorf("dynamic flow runtime creation occurrence requires one active eligible record: %s", req.InstancePath)
	}
	if current.Phase != runtimepipeline.FlowAttachmentReady {
		return false, fmt.Errorf("dynamic flow runtime creation occurrence requires topology readiness: %s", req.InstancePath)
	}
	if current.PlanHash != expectedHash {
		return false, fmt.Errorf("dynamic flow runtime creation occurrence readiness plan changed for %s", req.InstancePath)
	}
	return !current.CreationEventEmittedAt.IsZero(), nil
}

func (s *PipelinePostgresOwner) PrepareDynamicFlowCreationOccurrenceCommitTx(ctx context.Context, tx *sql.Tx, req runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) (bool, error) {
	return prepareDynamicFlowCreationOccurrenceCommit(ctx, tx, true, req)
}

func (s *PipelineSQLiteOwner) PrepareDynamicFlowCreationOccurrenceCommitTx(ctx context.Context, tx *sql.Tx, req runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) (bool, error) {
	return prepareDynamicFlowCreationOccurrenceCommit(ctx, tx, false, req)
}

func markDynamicFlowCreationOccurrenceCommitted(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	req runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest,
) error {
	query := `
		UPDATE flow_instance_runtime_readiness
		SET creation_event_emitted_at = $1, updated_at = $1
		WHERE run_id = $2::uuid
		  AND instance_path = $3
		  AND activation_attempt_id = $4
		  AND activation_attempt_id = $5
		  AND activation_attempt_grant_id = $6::uuid
		  AND activation_attempt_state = 'accepted'
		  AND phase = 'ready'
		  AND creation_event_emitted_at IS NULL
	`
	if !postgres {
		query = `
			UPDATE flow_instance_runtime_readiness
			SET creation_event_emitted_at = ?, updated_at = ?
			WHERE run_id = ?
			  AND instance_path = ?
			  AND activation_attempt_id = ?
			  AND activation_attempt_id = ?
			  AND activation_attempt_grant_id = ?
			  AND activation_attempt_state = 'accepted'
			  AND phase = 'ready'
			  AND creation_event_emitted_at IS NULL
		`
	}
	var result sql.Result
	var err error
	if postgres {
		result, err = tx.ExecContext(ctx, query, req.OccurredAt.UTC(), req.RunID, req.InstancePath, req.Attempt.Ordinal(), req.Attempt.ID(), req.Attempt.ProcessBinding().GenerationGrantID)
	} else {
		result, err = tx.ExecContext(ctx, query, req.OccurredAt.UTC(), req.OccurredAt.UTC(), req.RunID, req.InstancePath, req.Attempt.Ordinal(), req.Attempt.ID(), req.Attempt.ProcessBinding().GenerationGrantID)
	}
	if err != nil {
		return fmt.Errorf("mark dynamic flow runtime creation occurrence complete: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count dynamic flow runtime creation occurrence completion: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("dynamic flow runtime creation occurrence completion changed %d rows for %s", rows, req.InstancePath)
	}
	return nil
}

func (s *PipelinePostgresOwner) MarkDynamicFlowCreationOccurrenceCommittedTx(ctx context.Context, tx *sql.Tx, req runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) error {
	return markDynamicFlowCreationOccurrenceCommitted(ctx, tx, true, req)
}

func (s *PipelineSQLiteOwner) MarkDynamicFlowCreationOccurrenceCommittedTx(ctx context.Context, tx *sql.Tx, req runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) error {
	return markDynamicFlowCreationOccurrenceCommitted(ctx, tx, false, req)
}
