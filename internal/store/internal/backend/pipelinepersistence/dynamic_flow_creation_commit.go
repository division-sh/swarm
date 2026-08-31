package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
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
	expected, err := req.Plan.Normalized()
	if err != nil {
		return false, err
	}
	expectedJSON, err := runtimecanonicaljson.Bytes(expected)
	if err != nil {
		return false, fmt.Errorf("encode expected dynamic flow readiness %s: %w", req.InstancePath, err)
	}
	query := `
		SELECT readiness.plan,
		       readiness.topology_ready_at IS NOT NULL,
		       readiness.creation_event_emitted_at IS NOT NULL,
		       run.status,
		       instance.status,
		       instance.terminated_at IS NULL
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.run_id = readiness.run_id AND instance.instance_path = readiness.instance_path
		JOIN runs AS run ON run.run_id = readiness.run_id
		WHERE readiness.run_id = $1::uuid AND readiness.instance_path = $2
		FOR UPDATE OF readiness, instance, run
	`
	if !postgres {
		query = `
			SELECT readiness.plan,
			       readiness.topology_ready_at IS NOT NULL,
			       readiness.creation_event_emitted_at IS NOT NULL,
			       run.status,
			       instance.status,
			       instance.terminated_at IS NULL
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.run_id = readiness.run_id AND instance.instance_path = readiness.instance_path
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE readiness.run_id = ? AND readiness.instance_path = ?
		`
	}
	var (
		actualJSON                    []byte
		topologyReady, alreadyEmitted bool
		runStatus, instanceStatus     string
		unterminated                  bool
	)
	if err := tx.QueryRowContext(ctx, query, req.RunID, req.InstancePath).Scan(
		&actualJSON,
		&topologyReady,
		&alreadyEmitted,
		&runStatus,
		&instanceStatus,
		&unterminated,
	); err != nil {
		if err == sql.ErrNoRows {
			return false, fmt.Errorf("dynamic flow runtime creation occurrence requires one readiness record: %s", req.InstancePath)
		}
		return false, fmt.Errorf("lock dynamic flow runtime creation occurrence %s: %w", req.InstancePath, err)
	}
	if !dynamicFlowCreationRunActive(runStatus) || !strings.EqualFold(strings.TrimSpace(instanceStatus), "active") || !unterminated {
		return false, fmt.Errorf("dynamic flow runtime creation occurrence requires one active eligible record: %s", req.InstancePath)
	}
	if !topologyReady {
		return false, fmt.Errorf("dynamic flow runtime creation occurrence requires topology readiness: %s", req.InstancePath)
	}
	actual, err := runtimecanonicaljson.Decode(actualJSON)
	if err != nil {
		return false, fmt.Errorf("decode persisted dynamic flow readiness %s: %w", req.InstancePath, err)
	}
	actualJSON, err = runtimecanonicaljson.Encode(actual)
	if err != nil {
		return false, fmt.Errorf("canonicalize persisted dynamic flow readiness %s: %w", req.InstancePath, err)
	}
	if string(actualJSON) != string(expectedJSON) {
		return false, fmt.Errorf("dynamic flow runtime creation occurrence readiness plan changed for %s", req.InstancePath)
	}
	return alreadyEmitted, nil
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
		  AND creation_event_emitted_at IS NULL
	`
	if !postgres {
		query = `
			UPDATE flow_instance_runtime_readiness
			SET creation_event_emitted_at = ?, updated_at = ?
			WHERE run_id = ?
			  AND instance_path = ?
			  AND creation_event_emitted_at IS NULL
		`
	}
	var result sql.Result
	var err error
	if postgres {
		result, err = tx.ExecContext(ctx, query, req.OccurredAt.UTC(), req.RunID, req.InstancePath)
	} else {
		result, err = tx.ExecContext(ctx, query, req.OccurredAt.UTC(), req.OccurredAt.UTC(), req.RunID, req.InstancePath)
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

func dynamicFlowCreationRunActive(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "running", "paused":
		return true
	default:
		return false
	}
}
