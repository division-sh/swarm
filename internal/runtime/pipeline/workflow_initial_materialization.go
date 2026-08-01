package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
)

const workflowInitialMaterializationProjectionVersion = 1

// workflowInitialMaterializationProjection is immutable creation identity.
// Mutable workflow progress is deliberately absent from replay comparison.
type workflowInitialMaterializationProjection struct {
	Version         int                                 `json:"version"`
	RunID           string                              `json:"run_id"`
	EntityID        string                              `json:"entity_id"`
	FlowInstance    string                              `json:"flow_instance"`
	WorkflowName    string                              `json:"workflow_name"`
	WorkflowVersion string                              `json:"workflow_version"`
	InitialState    string                              `json:"initial_state"`
	OccurredAt      time.Time                           `json:"occurred_at"`
	Persisted       workflowInstancePersistedProjection `json:"persisted"`
}

func newWorkflowInitialMaterializationProjection(
	ctx context.Context,
	identity runtimeflowidentity.Persisted,
	instance WorkflowInstance,
	occurredAt time.Time,
) (workflowInitialMaterializationProjection, error) {
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return workflowInitialMaterializationProjection{}, err
	}
	persisted, err := workflowInstancePersistedProjectionFromInstance(instance, identity.StorageRef)
	if err != nil {
		return workflowInitialMaterializationProjection{}, err
	}
	projection := workflowInitialMaterializationProjection{
		Version:         workflowInitialMaterializationProjectionVersion,
		RunID:           strings.TrimSpace(runID),
		EntityID:        strings.TrimSpace(identity.RowID()),
		FlowInstance:    strings.Trim(strings.TrimSpace(identity.StorageRef), "/"),
		WorkflowName:    strings.TrimSpace(instance.WorkflowName),
		WorkflowVersion: strings.TrimSpace(instance.WorkflowVersion),
		InitialState:    strings.TrimSpace(instance.CurrentState),
		OccurredAt:      canonicalWorkflowInstancePersistedTime(occurredAt),
		Persisted:       persisted,
	}
	if projection.RunID == "" || projection.EntityID == "" || projection.FlowInstance == "" ||
		projection.WorkflowName == "" || projection.WorkflowVersion == "" ||
		projection.InitialState == "" || projection.OccurredAt.IsZero() {
		return workflowInitialMaterializationProjection{}, fmt.Errorf("workflow initial materialization projection requires exact identity, workflow, state, and occurrence")
	}
	return projection, nil
}

func (s *workflowInstanceStore) insertWorkflowInitialMaterializationProjection(
	ctx context.Context,
	projection workflowInitialMaterializationProjection,
) error {
	tx, ok := sqlTxFromContext(ctx)
	if !ok || tx == nil || !authoractivity.InMutation(ctx, tx) {
		return fmt.Errorf("workflow initial materialization creation requires selected mutation")
	}
	encoded, err := canonicaljson.Bytes(projection)
	if err != nil {
		return fmt.Errorf("encode workflow initial materialization: %w", err)
	}
	if s.isSQLite() {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO workflow_instance_initial_materializations (
				run_id, entity_id, instance_id, projection_version, projection, occurred_at
			) VALUES (?, ?, ?, ?, ?, ?)
		`, projection.RunID, projection.EntityID, projection.FlowInstance, projection.Version, encoded, projection.OccurredAt)
	} else {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO workflow_instance_initial_materializations (
				run_id, entity_id, instance_id, projection_version, projection, occurred_at
			) VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, $6)
		`, projection.RunID, projection.EntityID, projection.FlowInstance, projection.Version, encoded, projection.OccurredAt)
	}
	if err != nil {
		return fmt.Errorf("persist workflow initial materialization %s: %w", projection.FlowInstance, err)
	}
	return nil
}

func (s *workflowInstanceStore) workflowInitialMaterializationProjectionEqual(
	ctx context.Context,
	expected workflowInitialMaterializationProjection,
) (bool, error) {
	query := `
		SELECT entity_id::text, instance_id, projection_version, projection, occurred_at
		FROM workflow_instance_initial_materializations
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`
	if s.isSQLite() {
		query = `
			SELECT entity_id, instance_id, projection_version, projection, occurred_at
			FROM workflow_instance_initial_materializations
			WHERE run_id = ? AND entity_id = ?
		`
	}
	var (
		entityID          string
		instanceID        string
		projectionVersion int
		raw               []byte
		occurredAt        time.Time
		occurredAtRaw     any
	)
	tx, ok := sqlTxFromContext(ctx)
	if !ok || tx == nil {
		return false, fmt.Errorf("workflow initial materialization comparison requires selected mutation")
	}
	occurredAtDestination := any(&occurredAt)
	if s.isSQLite() {
		occurredAtDestination = &occurredAtRaw
	}
	err := tx.QueryRowContext(ctx, query, expected.RunID, expected.EntityID).Scan(
		&entityID,
		&instanceID,
		&projectionVersion,
		&raw,
		occurredAtDestination,
	)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load workflow initial materialization %s: %w", expected.FlowInstance, err)
	}
	if s.isSQLite() {
		var ok bool
		occurredAt, ok, err = sqliteWorkflowTimeValue(occurredAtRaw)
		if err != nil {
			return false, fmt.Errorf("decode workflow initial materialization occurrence %s: %w", expected.FlowInstance, err)
		}
		if !ok {
			return false, fmt.Errorf("workflow initial materialization %s has no occurrence time", expected.FlowInstance)
		}
	}
	admitted, err := canonicaljson.Decode(raw)
	if err != nil {
		return false, fmt.Errorf("decode workflow initial materialization %s: %w", expected.FlowInstance, err)
	}
	actualJSON, err := canonicaljson.Encode(admitted)
	if err != nil {
		return false, fmt.Errorf("encode persisted workflow initial materialization %s: %w", expected.FlowInstance, err)
	}
	expectedJSON, err := canonicaljson.Bytes(expected)
	if err != nil {
		return false, fmt.Errorf("encode expected workflow initial materialization %s: %w", expected.FlowInstance, err)
	}
	return projectionVersion == workflowInitialMaterializationProjectionVersion &&
		strings.TrimSpace(entityID) == expected.EntityID &&
		strings.Trim(strings.TrimSpace(instanceID), "/") == expected.FlowInstance &&
		canonicalWorkflowInstancePersistedTime(occurredAt).Equal(expected.OccurredAt) &&
		bytes.Equal(actualJSON, expectedJSON), nil
}
