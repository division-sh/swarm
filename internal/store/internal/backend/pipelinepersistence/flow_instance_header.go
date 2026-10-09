package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

// InstallActiveForkReceiverHeaderDoneFaultTx deliberately invalidates only the
// live header, without manufacturing a workflow transition or its revisions.
func InstallActiveForkReceiverHeaderDoneFaultTx(ctx context.Context, tx *sql.Tx, runID, entityID string) error {
	if tx == nil {
		return fmt.Errorf("fork receiver header fault requires the selected transaction")
	}
	result, err := tx.ExecContext(ctx, `UPDATE flow_instances SET current_state='done'
		WHERE run_id=$1 AND entity_id=$2 AND current_state='active'`, runID, entityID)
	if err != nil {
		return fmt.Errorf("install active fork receiver header fault: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return fmt.Errorf("fork receiver header fault requires exactly one active header: count=%d err=%v", count, err)
	}
	return nil
}

// The constructed header owns lifecycle identity and compare-and-write progress,
// independently of whether the flow declares an entity field contract.
func commitWorkflowInstanceHeader(ctx context.Context, tx *sql.Tx, postgres bool, record pipeline.WorkflowEngineStateRecord, create bool) (workflowEngineStateFact, error) {
	var query string
	var args []any
	if create {
		query = `INSERT INTO flow_instances (
			run_id, instance_path, entity_id, entity_type, slug, name, flow_template, mode,
			config, status, stage_defined, current_state, gates, bookkeeping, accumulator,
			revision, entered_state_at, created_at, updated_at, terminated_at
		) VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)`
		if postgres {
			query = `INSERT INTO flow_instances (
				run_id, instance_path, entity_id, entity_type, slug, name, flow_template, mode,
				config, status, stage_defined, current_state, gates, bookkeeping, accumulator,
				revision, entered_state_at, created_at, updated_at, terminated_at
			) VALUES ($1::uuid, $2, $3::uuid, NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''),
				$7, $8, $9::jsonb, $10, $11, $12, $13::jsonb, $14::jsonb, $15::jsonb, 1, $16, $17, $18, $19)`
		}
		args = []any{record.Identity.RunID, record.Identity.Route.InstancePath, record.EntityID, record.EntityType,
			record.Slug, record.Name, record.WorkflowName, record.Mode, string(record.Config), record.Status,
			record.StageDefined, record.CurrentState, string(record.Gates), string(record.Bookkeeping), string(record.Accumulator),
			record.EnteredStageAt, record.CreatedAt, record.UpdatedAt, nullableWorkflowTerminationTime(record.TerminatedAt)}
	} else {
		query = `UPDATE flow_instances SET slug = NULLIF(?, ''), name = NULLIF(?, ''),
			config = ?, status = ?, current_state = ?, gates = ?, bookkeeping = ?, accumulator = ?,
			revision = revision + 1, entered_state_at = ?, updated_at = ?, terminated_at = ?
			WHERE run_id = ? AND instance_path = ? AND entity_id = ? AND revision = ? AND current_state = ?
			AND flow_template = ? AND mode = ? AND entity_type IS NULLIF(?, '')`
		if postgres {
			query = `UPDATE flow_instances SET slug = NULLIF($1, ''), name = NULLIF($2, ''),
				config = $3::jsonb, status = $4, current_state = $5, gates = $6::jsonb,
				bookkeeping = $7::jsonb, accumulator = $8::jsonb, revision = revision + 1,
				entered_state_at = $9, updated_at = $10, terminated_at = $11
				WHERE run_id = $12::uuid AND instance_path = $13 AND entity_id = $14::uuid
				AND revision = $15 AND current_state = $16 AND flow_template = $17 AND mode = $18
				AND entity_type IS NOT DISTINCT FROM NULLIF($19, '')`
		}
		args = []any{record.Slug, record.Name, string(record.Config), record.Status, record.CurrentState,
			string(record.Gates), string(record.Bookkeeping), string(record.Accumulator), record.EnteredStageAt,
			record.UpdatedAt, nullableWorkflowTerminationTime(record.TerminatedAt), record.Identity.RunID,
			record.Identity.Route.InstancePath, record.EntityID, record.ExpectedRevision, record.ExpectedState,
			record.WorkflowName, record.Mode, record.EntityType}
	}
	var fact workflowEngineStateFact
	write := transactiontest.BeginWorkflowHeaderJSON(ctx, len(record.Config))
	err := tx.QueryRowContext(ctx, query+` RETURNING CAST(run_id AS TEXT), CAST(entity_id AS TEXT)`, args...).Scan(&fact.runID, &fact.entityID)
	write.End(err)
	if err == sql.ErrNoRows {
		return workflowEngineStateFact{}, workflowEngineStateRevisionConflict(record)
	}
	if err != nil {
		return workflowEngineStateFact{}, fmt.Errorf("commit constructed workflow header: %w", err)
	}
	return fact, nil
}

// Historical selected execution projects admitted lifecycle evidence; it does
// not rerun construction, creating delivery, or initial-entry contributions.
func CommitSelectedHistoricalWorkflowHeader(ctx context.Context, tx *sql.Tx, postgres bool, record pipeline.WorkflowEngineStateRecord, receipt pipeline.FlowConstructionReceipt) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if !record.Transition.CreatesState() {
		return fmt.Errorf("selected historical header requires a fresh projected identity")
	}
	raw, err := pipeline.EncodeFlowConstructionReceipt(receipt)
	if err != nil {
		return err
	}
	if _, err := pipeline.DecodeFlowConstructionReceipt(raw, record.Identity, record.EntityID); err != nil {
		return err
	}
	if receipt.WorkflowName != record.WorkflowName || !receipt.OccurredAt.Equal(record.CreatedAt) || receipt.Persisted.Control.EntityType != record.EntityType {
		return fmt.Errorf("historical construction contradicts projected header")
	}
	if record.EntityType == "" {
		if err := requireFieldlessWorkflowStateAbsent(ctx, tx, postgres, record); err != nil {
			return err
		}
	}
	if _, err := commitWorkflowInstanceHeader(ctx, tx, postgres, record, true); err != nil {
		return err
	}
	return insertFlowConstructionReceipt(ctx, tx, postgres, receipt.RunID, receipt.EntityID, receipt.FlowInstance, raw, receipt.OccurredAt)
}

// Materialize-only reuse consumes the same paired reader and immutable record
// as the historical writer. It cannot repair or accept changed snapshot facts.
func RequireSelectedHistoricalWorkflowHeader(ctx context.Context, tx *sql.Tx, postgres bool, expected pipeline.WorkflowEngineStateRecord) error {
	if err := requireWorkflowHeaderProjection(ctx, tx, postgres, expected, 1); err != nil {
		return err
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT projection FROM workflow_instance_initial_materializations
		WHERE run_id=$1 AND instance_path=$2 AND entity_id=$3`, expected.Identity.RunID, expected.Identity.Route.InstancePath, expected.EntityID).Scan(&raw); err != nil {
		return fmt.Errorf("read historical construction receipt: %w", err)
	}
	receipt, err := pipeline.DecodeFlowConstructionReceipt(raw, expected.Identity, expected.EntityID)
	if err != nil {
		return err
	}
	if receipt.WorkflowName != expected.WorkflowName || !receipt.OccurredAt.Equal(expected.CreatedAt) || receipt.Persisted.Control.EntityType != expected.EntityType {
		return fmt.Errorf("historical construction contradicts persisted header")
	}
	return nil
}

// Exact materialization reuse also compares immutable initialization. Header
// equality alone cannot prove that later fields have not replaced the receipt.
func RequireSelectedHistoricalWorkflowReceipt(ctx context.Context, tx *sql.Tx, expected pipeline.FlowConstructionReceipt) error {
	raw, err := pipeline.EncodeFlowConstructionReceipt(expected)
	if err != nil {
		return err
	}
	var persisted []byte
	if err := tx.QueryRowContext(ctx, `SELECT projection FROM workflow_instance_initial_materializations
		WHERE run_id=$1 AND instance_path=$2 AND entity_id=$3`, expected.RunID, expected.FlowInstance, expected.EntityID).Scan(&persisted); err != nil {
		return fmt.Errorf("read immutable historical construction: %w", err)
	}
	if _, err := pipeline.DecodeStoredFlowConstructionReceipt(persisted, expected.RunID, expected.EntityID, expected.FlowInstance, expected.WorkflowName); err != nil {
		return err
	}
	if !workflowCommitJSONEqual(persisted, raw) {
		return fmt.Errorf("historical construction receipt disagrees with admitted initialization")
	}
	return nil
}

func requireWorkflowHeaderProjection(ctx context.Context, tx *sql.Tx, postgres bool, expected pipeline.WorkflowEngineStateRecord, revision int64) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	target, err := loadWorkflowTargetPersistence(ctx, tx, expected.Identity, identity.NormalizeEntityID(expected.EntityID), !postgres)
	if err != nil {
		return err
	}
	if _, err := target.DecodeComplete(expected.Identity.Route, identity.NormalizeEntityID(expected.EntityID)); err != nil {
		return err
	}
	lifecycle, header := target.Lifecycle, target.Lifecycle.State
	if expected.Transition.PreservesState() && (header.Revision != revision || header.CurrentState != expected.ExpectedState) {
		return workflowEngineStateRevisionConflict(expected)
	}
	if lifecycle.WorkflowName != expected.WorkflowName || lifecycle.WorkflowVersion != expected.WorkflowVersion ||
		lifecycle.Mode != expected.Mode || lifecycle.Status != expected.Status || lifecycle.StageDefined != expected.StageDefined ||
		!lifecycle.TerminatedAt.Equal(expected.TerminatedAt) || !workflowCommitJSONEqual(lifecycle.Config, expected.Config) {
		return fmt.Errorf("historical workflow header %s disagrees with fixed snapshot", expected.Identity.Route.InstancePath)
	}
	if expected.EntityType != "" {
		if !workflowCommitJSONEqual(target.State.Fields, expected.Fields) {
			return fmt.Errorf("historical workflow fields disagree with fixed snapshot")
		}
	}
	if header.EntityID != expected.EntityID || header.FlowInstance != expected.Identity.Route.InstancePath ||
		header.EntityType != expected.EntityType || header.Slug != expected.Slug || header.Name != expected.Name ||
		header.CurrentState != expected.CurrentState || header.Revision != revision {
		return fmt.Errorf("historical workflow state %s disagrees with fixed snapshot", expected.Identity.Route.InstancePath)
	}
	if !header.EnteredStageAt.Equal(expected.EnteredStageAt) || !header.CreatedAt.Equal(expected.CreatedAt) || !header.UpdatedAt.Equal(expected.UpdatedAt) {
		return fmt.Errorf("historical workflow clocks %s disagree with fixed snapshot", expected.Identity.Route.InstancePath)
	}
	if !workflowCommitJSONEqual(header.Gates, expected.Gates) || !workflowCommitJSONEqual(header.Bookkeeping, expected.Bookkeeping) || !workflowCommitJSONEqual(header.Accumulator, expected.Accumulator) {
		return fmt.Errorf("historical workflow projection %s disagrees with fixed snapshot", expected.Identity.Route.InstancePath)
	}
	return nil
}

// Imported historical fields remain non-executable. Reuse compares recorded
// facts through the paired reader and never manufactures a lifecycle header.
func RequireHistoricalImportedWorkflowState(ctx context.Context, tx *sql.Tx, postgres bool, owner flowidentity.RunScopedFlowInstance, expected pipeline.WorkflowEntityStatePersistenceRecord) error {
	target, err := loadWorkflowTargetPersistence(ctx, tx, owner, identity.NormalizeEntityID(expected.EntityID), !postgres)
	if err != nil {
		return err
	}
	if target.Presence != pipeline.WorkflowTargetPersistenceStateOnly {
		return fmt.Errorf("historical imported fields require exact state-only presence")
	}
	if err := target.Validate(owner.Route, identity.NormalizeEntityID(expected.EntityID)); err != nil {
		return err
	}
	state := target.State
	if state.EntityID != expected.EntityID || state.FlowInstance != expected.FlowInstance ||
		state.EntityType != expected.EntityType || state.Slug != expected.Slug || state.Name != expected.Name ||
		state.CurrentState != expected.CurrentState || state.Revision != expected.Revision ||
		!state.EnteredStageAt.Equal(expected.EnteredStageAt) || !state.CreatedAt.Equal(expected.CreatedAt) || !state.UpdatedAt.Equal(expected.UpdatedAt) ||
		!workflowCommitJSONEqual(state.Fields, expected.Fields) || !workflowCommitJSONEqual(state.Gates, expected.Gates) ||
		!workflowCommitJSONEqual(state.Bookkeeping, expected.Bookkeeping) || !workflowCommitJSONEqual(state.Accumulator, expected.Accumulator) {
		return fmt.Errorf("historical imported fields %s disagree with fixed snapshot", expected.FlowInstance)
	}
	return nil
}

func requireFieldlessWorkflowStateAbsent(ctx context.Context, tx *sql.Tx, postgres bool, record pipeline.WorkflowEngineStateRecord) error {
	query := `SELECT EXISTS (SELECT 1 FROM entity_state WHERE run_id = ? AND (entity_id = ? OR flow_instance = ?))`
	if postgres {
		query = `SELECT EXISTS (SELECT 1 FROM entity_state WHERE run_id = $1::uuid AND (entity_id = $2::uuid OR flow_instance = $3))`
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, query, record.Identity.RunID, record.EntityID, record.Identity.Route.InstancePath).Scan(&exists); err != nil {
		return fmt.Errorf("check fieldless workflow state absence: %w", err)
	}
	if exists {
		return fmt.Errorf("fieldless workflow %s has an unexpected entity state row", record.Identity.Route.InstancePath)
	}
	return nil
}
