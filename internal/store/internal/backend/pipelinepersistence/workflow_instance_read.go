package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/lib/pq"
)

func (s *PipelinePostgresOwner) LoadWorkflowInstance(ctx context.Context, route runtimeflowidentity.Route) (runtimepipeline.WorkflowInstance, bool, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.WorkflowInstance{}, false, fmt.Errorf("postgres workflow instance reader is required")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return runtimepipeline.WorkflowInstance{}, false, &runtimepipeline.WorkflowInstanceLookupMiss{RequestedKey: strings.TrimSpace(route.InstancePath)}
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return runtimepipeline.WorkflowInstance{}, false, err
	}
	row := s.backend.QueryRowContext(ctx, postgresWorkflowInstanceSelect+`
		WHERE es.flow_instance = $1 AND es.run_id = $2::uuid
		ORDER BY es.created_at DESC, es.entity_id DESC
		LIMIT 1
	`, route.InstancePath, runID)
	record, err := scanPostgresWorkflowInstance(row)
	if err == sql.ErrNoRows {
		return runtimepipeline.WorkflowInstance{}, false, nil
	}
	if err != nil {
		return runtimepipeline.WorkflowInstance{}, false, err
	}
	item, err := runtimepipeline.DecodeWorkflowInstancePersistenceRecord(record)
	return item, err == nil, err
}

func (s *PipelinePostgresOwner) LoadWorkflowEntityState(ctx context.Context, route runtimeflowidentity.Route, entityID runtimeidentity.EntityID) (runtimepipeline.WorkflowEntityStatePersistenceRecord, bool, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, false, fmt.Errorf("postgres workflow entity state reader is required")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	entityID = runtimeidentity.NormalizeEntityID(entityID.String())
	if !route.Valid() || entityID.IsZero() {
		return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, false, fmt.Errorf("workflow entity state lookup requires exact route and entity identity")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, false, err
	}
	record, err := scanPostgresWorkflowEntityState(s.backend.QueryRowContext(ctx, postgresWorkflowEntityStateSelect+`
		WHERE es.run_id = $1::uuid AND es.entity_id = $2::uuid AND es.flow_instance = $3
	`, runID, entityID.String(), route.InstancePath))
	if err == sql.ErrNoRows {
		return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, false, nil
	}
	if err != nil {
		return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, false, err
	}
	return record, true, nil
}

func (s *PipelinePostgresOwner) LoadWorkflowTargetPersistence(ctx context.Context, route runtimeflowidentity.Route, entityID runtimeidentity.EntityID) (runtimepipeline.WorkflowTargetPersistenceRecord, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.WorkflowTargetPersistenceRecord{}, fmt.Errorf("postgres workflow target persistence reader is required")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	entityID = runtimeidentity.NormalizeEntityID(entityID.String())
	if !route.Valid() || entityID.IsZero() {
		return runtimepipeline.WorkflowTargetPersistenceRecord{}, fmt.Errorf("workflow target persistence lookup requires exact route and entity identity")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return runtimepipeline.WorkflowTargetPersistenceRecord{}, err
	}
	return scanPostgresWorkflowTargetPersistence(s.backend.QueryRowContext(ctx, postgresWorkflowTargetPersistenceSelect, runID, entityID.String(), route.InstancePath), route, entityID)
}

func (s *PipelinePostgresOwner) SelectActiveWorkflowEntityStates(ctx context.Context, owner runtimepipeline.WorkflowEntityStateSelectionOwner, selectors []runtimepipeline.WorkflowInstanceFieldSelector, excludedStates []string) ([]runtimepipeline.WorkflowEntityStatePersistenceRecord, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres workflow entity state reader is required")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, err
	}
	scopeKey := owner.ScopeKey()
	selectors = runtimepipeline.NormalizeWorkflowInstanceFieldSelectors(selectors)
	if scopeKey == "" || len(selectors) == 0 {
		return nil, nil
	}
	activeStates := runtimerunlifecycle.ActiveStates()
	rows, err := s.backend.QueryContext(ctx, postgresWorkflowEntityStateSelect+`
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = $1::uuid
		  AND EXISTS (
			SELECT 1 FROM runs run
			WHERE run.run_id = es.run_id AND run.status IN ($4, $5)
		  )
		  AND (es.flow_instance = $2 OR es.flow_instance LIKE $3 OR ($6::boolean AND es.flow_instance = $1::text))
		  AND (fi.instance_id IS NULL OR (LOWER(BTRIM(fi.status)) = 'active' AND fi.terminated_at IS NULL))
		ORDER BY es.created_at ASC, es.entity_id ASC
	`, runID, scopeKey, scopeKey+"/%", string(activeStates[0]), string(activeStates[1]), owner.Owns(runID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records, err := scanPostgresWorkflowEntityStates(rows)
	if err != nil {
		return nil, err
	}
	return runtimepipeline.FilterWorkflowEntityStatePersistenceRecords(records, owner, selectors, excludedStates)
}

func (s *PipelinePostgresOwner) QueryWorkflowEntityCollection(ctx context.Context, owner runtimepipeline.WorkflowEntityCollectionOwner) ([]runtimepipeline.WorkflowEntityStatePersistenceRecord, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres workflow entity collection reader is required")
	}
	if !owner.Valid() {
		return nil, fmt.Errorf("postgres workflow entity collection requires an admitted owner")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, err
	}
	activeStates := runtimerunlifecycle.ActiveStates()
	rows, err := s.backend.QueryContext(ctx, postgresWorkflowEntityStateSelect+`
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = $1::uuid
		  AND es.entity_type = $2
		  AND EXISTS (
			SELECT 1 FROM runs run
			WHERE run.run_id = es.run_id AND run.status IN ($5, $6)
		  )
		  AND (es.flow_instance = $3 OR es.flow_instance LIKE $4 OR ($7::boolean AND es.flow_instance = $1::text))
		  AND (fi.instance_id IS NULL OR (LOWER(BTRIM(fi.status)) = 'active' AND fi.terminated_at IS NULL))
		ORDER BY es.created_at ASC, es.entity_id ASC
	`, runID, owner.EntityType(), owner.ScopeKey(), owner.ScopeKey()+"/%", string(activeStates[0]), string(activeStates[1]), owner.ScopeKey() == runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records, err := scanPostgresWorkflowEntityStates(rows)
	if err != nil {
		return nil, err
	}
	return runtimepipeline.FilterWorkflowEntityCollectionRecords(records, owner)
}

func (s *PipelinePostgresOwner) ListWorkflowInstances(ctx context.Context) ([]runtimepipeline.WorkflowInstance, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres workflow instance reader is required")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.backend.QueryContext(ctx, postgresWorkflowInstanceSelect+`
		WHERE es.run_id = $1::uuid
		ORDER BY es.created_at ASC
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPostgresWorkflowInstances(rows)
}

func (s *PipelinePostgresOwner) SelectActiveWorkflowInstances(ctx context.Context, scopeKey string, selectors []runtimepipeline.WorkflowInstanceFieldSelector, excludedStates []string) ([]runtimepipeline.WorkflowInstance, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres workflow instance reader is required")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, err
	}
	scopeKey = strings.Trim(strings.TrimSpace(scopeKey), "/")
	selectors = runtimepipeline.NormalizeWorkflowInstanceFieldSelectors(selectors)
	if scopeKey == "" || len(selectors) == 0 {
		return nil, nil
	}
	activeStates := runtimerunlifecycle.ActiveStates()
	args := []any{runID, scopeKey, scopeKey + "/%", string(activeStates[0]), string(activeStates[1])}
	var where strings.Builder
	where.WriteString(`
		WHERE es.run_id = $1::uuid
		  AND EXISTS (
			SELECT 1 FROM runs run
			WHERE run.run_id = es.run_id AND run.status IN ($4, $5)
		  )
		  AND (es.flow_instance = $2 OR es.flow_instance LIKE $3)
		  AND LOWER(BTRIM(fi.status)) = 'active'
		  AND fi.terminated_at IS NULL
	`)
	terminalStates := runtimepipeline.NormalizeWorkflowInstanceExcludedStates(excludedStates)
	if len(terminalStates) > 0 {
		args = append(args, pq.Array(terminalStates))
		where.WriteString(fmt.Sprintf(` AND NOT (LOWER(es.current_state) = ANY($%d::text[]))`, len(args)))
	}
	for _, selector := range selectors {
		segments := runtimepipeline.WorkflowInstanceFieldSelectorPath(selector.Field)
		if len(segments) == 0 {
			return nil, fmt.Errorf("workflow instance selector field is required")
		}
		valueJSON, err := json.Marshal(selector.Value)
		if err != nil {
			return nil, fmt.Errorf("marshal workflow instance selector %s: %w", selector.Field, err)
		}
		args = append(args, pq.Array(segments), string(valueJSON))
		where.WriteString(fmt.Sprintf(` AND es.fields #> $%d::text[] = $%d::jsonb`, len(args)-1, len(args)))
	}
	where.WriteString(` ORDER BY es.created_at ASC`)
	rows, err := s.backend.QueryContext(ctx, postgresWorkflowInstanceSelect+where.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPostgresWorkflowInstances(rows)
}

const postgresWorkflowInstanceSelect = `
	SELECT
		es.entity_id::text,
		fi.flow_template,
		fi.config->>'workflow_version',
		fi.mode,
		fi.status,
		fi.terminated_at,
		es.current_state,
		es.revision,
		es.entered_state_at,
		es.gates,
		es.fields,
		es.bookkeeping,
		es.accumulator,
		fi.config,
		es.flow_instance,
		es.entity_type,
		es.slug,
		es.name,
		es.created_at,
		es.updated_at
	FROM entity_state es
	JOIN flow_instances fi ON fi.instance_id = es.flow_instance
`

const postgresWorkflowEntityStateSelect = `
	SELECT
		es.entity_id::text,
		es.flow_instance,
		es.entity_type,
		es.slug,
		es.name,
		es.current_state,
		es.revision,
		es.entered_state_at,
		es.gates,
		es.fields,
		es.bookkeeping,
		es.accumulator,
		es.created_at,
		es.updated_at
	FROM entity_state es
`

const postgresWorkflowTargetPersistenceSelect = `
	SELECT
		es.entity_id::text,
		es.flow_instance,
		es.entity_type,
		es.slug,
		es.name,
		es.current_state,
		es.revision,
		es.entered_state_at,
		es.gates,
		es.fields,
		es.bookkeeping,
		es.accumulator,
		es.created_at,
		es.updated_at,
		fi.instance_id,
		fi.flow_template,
		fi.config->>'workflow_version',
		fi.mode,
		fi.status,
		fi.config,
		fi.terminated_at,
		fi.created_at
	FROM (VALUES ($1::uuid, $2::uuid, $3::text)) AS target(run_id, entity_id, flow_instance)
	LEFT JOIN entity_state es
		ON es.run_id = target.run_id
		AND es.entity_id = target.entity_id
		AND es.flow_instance = target.flow_instance
	LEFT JOIN flow_instances fi ON fi.instance_id = target.flow_instance
`

type workflowInstanceScanner interface {
	Scan(...any) error
}

func scanPostgresWorkflowInstance(row workflowInstanceScanner) (runtimepipeline.WorkflowInstancePersistenceRecord, error) {
	var record runtimepipeline.WorkflowInstancePersistenceRecord
	var terminatedAt sql.NullTime
	var slug, name sql.NullString
	if err := row.Scan(
		&record.EntityID,
		&record.WorkflowName,
		&record.WorkflowVersion,
		&record.Mode,
		&record.Status,
		&terminatedAt,
		&record.CurrentState,
		&record.Revision,
		&record.EnteredStageAt,
		&record.Gates,
		&record.Fields,
		&record.Bookkeeping,
		&record.Accumulator,
		&record.Config,
		&record.FlowInstance,
		&record.EntityType,
		&slug,
		&name,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return runtimepipeline.WorkflowInstancePersistenceRecord{}, err
	}
	if terminatedAt.Valid {
		record.TerminatedAt = terminatedAt.Time.UTC()
	}
	record.Slug = slug.String
	record.Name = name.String
	return record, nil
}

func scanPostgresWorkflowEntityState(row workflowInstanceScanner) (runtimepipeline.WorkflowEntityStatePersistenceRecord, error) {
	var record runtimepipeline.WorkflowEntityStatePersistenceRecord
	var slug, name sql.NullString
	if err := row.Scan(
		&record.EntityID,
		&record.FlowInstance,
		&record.EntityType,
		&slug,
		&name,
		&record.CurrentState,
		&record.Revision,
		&record.EnteredStageAt,
		&record.Gates,
		&record.Fields,
		&record.Bookkeeping,
		&record.Accumulator,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, err
	}
	record.Slug = slug.String
	record.Name = name.String
	return record, nil
}

func scanPostgresWorkflowTargetPersistence(row workflowInstanceScanner, route runtimeflowidentity.Route, entityID runtimeidentity.EntityID) (runtimepipeline.WorkflowTargetPersistenceRecord, error) {
	var stateEntityID, stateFlowInstance, stateEntityType, stateSlug, stateName, stateCurrent sql.NullString
	var stateRevision sql.NullInt64
	var stateEnteredAt, stateCreatedAt, stateUpdatedAt sql.NullTime
	var stateGates, stateFields, stateBookkeeping, stateAccumulator []byte
	var lifecycleFlowInstance, lifecycleWorkflowName, lifecycleWorkflowVersion, lifecycleMode, lifecycleStatus sql.NullString
	var lifecycleConfig []byte
	var lifecycleTerminatedAt, lifecycleCreatedAt sql.NullTime
	if err := row.Scan(
		&stateEntityID, &stateFlowInstance, &stateEntityType, &stateSlug, &stateName,
		&stateCurrent, &stateRevision, &stateEnteredAt, &stateGates, &stateFields,
		&stateBookkeeping, &stateAccumulator, &stateCreatedAt, &stateUpdatedAt,
		&lifecycleFlowInstance, &lifecycleWorkflowName, &lifecycleWorkflowVersion,
		&lifecycleMode, &lifecycleStatus, &lifecycleConfig, &lifecycleTerminatedAt, &lifecycleCreatedAt,
	); err != nil {
		return runtimepipeline.WorkflowTargetPersistenceRecord{}, err
	}
	state := runtimepipeline.WorkflowEntityStatePersistenceRecord{}
	if stateEntityID.Valid {
		state = runtimepipeline.WorkflowEntityStatePersistenceRecord{
			EntityID: stateEntityID.String, FlowInstance: stateFlowInstance.String, EntityType: stateEntityType.String,
			Slug: stateSlug.String, Name: stateName.String, CurrentState: stateCurrent.String, Revision: stateRevision.Int64,
			Gates: append(json.RawMessage(nil), stateGates...), Fields: append(json.RawMessage(nil), stateFields...),
			Bookkeeping: append(json.RawMessage(nil), stateBookkeeping...), Accumulator: append(json.RawMessage(nil), stateAccumulator...),
		}
		if stateEnteredAt.Valid {
			state.EnteredStageAt = stateEnteredAt.Time.UTC()
		}
		if stateCreatedAt.Valid {
			state.CreatedAt = stateCreatedAt.Time.UTC()
		}
		if stateUpdatedAt.Valid {
			state.UpdatedAt = stateUpdatedAt.Time.UTC()
		}
	}
	lifecycle := runtimepipeline.WorkflowLifecycleCompanionPersistenceRecord{}
	if lifecycleFlowInstance.Valid {
		lifecycle = runtimepipeline.WorkflowLifecycleCompanionPersistenceRecord{
			FlowInstance: lifecycleFlowInstance.String, WorkflowName: lifecycleWorkflowName.String,
			WorkflowVersion: lifecycleWorkflowVersion.String, Mode: lifecycleMode.String, Status: lifecycleStatus.String,
			Config: append(json.RawMessage(nil), lifecycleConfig...),
		}
		if lifecycleTerminatedAt.Valid {
			lifecycle.TerminatedAt = lifecycleTerminatedAt.Time.UTC()
		}
		if lifecycleCreatedAt.Valid {
			lifecycle.CreatedAt = lifecycleCreatedAt.Time.UTC()
		}
	}
	return assembleWorkflowTargetPersistence(route, entityID, state, stateEntityID.Valid, lifecycle, lifecycleFlowInstance.Valid)
}

func scanPostgresWorkflowEntityStates(rows *sql.Rows) ([]runtimepipeline.WorkflowEntityStatePersistenceRecord, error) {
	records := make([]runtimepipeline.WorkflowEntityStatePersistenceRecord, 0, 32)
	for rows.Next() {
		record, err := scanPostgresWorkflowEntityState(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func scanPostgresWorkflowInstances(rows *sql.Rows) ([]runtimepipeline.WorkflowInstance, error) {
	items := make([]runtimepipeline.WorkflowInstance, 0, 32)
	for rows.Next() {
		record, err := scanPostgresWorkflowInstance(rows)
		if err != nil {
			return nil, err
		}
		item, err := runtimepipeline.DecodeWorkflowInstancePersistenceRecord(record)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *PipelineSQLiteOwner) LoadWorkflowInstance(ctx context.Context, route runtimeflowidentity.Route) (runtimepipeline.WorkflowInstance, bool, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.WorkflowInstance{}, false, fmt.Errorf("sqlite workflow instance reader is required")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return runtimepipeline.WorkflowInstance{}, false, &runtimepipeline.WorkflowInstanceLookupMiss{RequestedKey: strings.TrimSpace(route.InstancePath)}
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return runtimepipeline.WorkflowInstance{}, false, err
	}
	rows, err := s.backend.QueryContext(ctx, sqliteWorkflowInstanceSelect+`
		WHERE es.flow_instance = ? AND es.run_id = ?
		ORDER BY es.created_at DESC, es.entity_id DESC
		LIMIT 1
	`, route.InstancePath, runID)
	if err != nil {
		return runtimepipeline.WorkflowInstance{}, false, err
	}
	defer rows.Close()
	items, err := scanSQLiteWorkflowInstances(rows)
	if err != nil || len(items) == 0 {
		return runtimepipeline.WorkflowInstance{}, false, err
	}
	return items[0], true, nil
}

func (s *PipelineSQLiteOwner) LoadWorkflowEntityState(ctx context.Context, route runtimeflowidentity.Route, entityID runtimeidentity.EntityID) (runtimepipeline.WorkflowEntityStatePersistenceRecord, bool, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, false, fmt.Errorf("sqlite workflow entity state reader is required")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	entityID = runtimeidentity.NormalizeEntityID(entityID.String())
	if !route.Valid() || entityID.IsZero() {
		return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, false, fmt.Errorf("workflow entity state lookup requires exact route and entity identity")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, false, err
	}
	record, err := scanSQLiteWorkflowEntityState(s.backend.QueryRowContext(ctx, sqliteWorkflowEntityStateSelect+`
		WHERE es.run_id = ? AND es.entity_id = ? AND es.flow_instance = ?
	`, runID, entityID.String(), route.InstancePath))
	if err == sql.ErrNoRows {
		return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, false, nil
	}
	if err != nil {
		return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, false, err
	}
	return record, true, nil
}

func (s *PipelineSQLiteOwner) LoadWorkflowTargetPersistence(ctx context.Context, route runtimeflowidentity.Route, entityID runtimeidentity.EntityID) (runtimepipeline.WorkflowTargetPersistenceRecord, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.WorkflowTargetPersistenceRecord{}, fmt.Errorf("sqlite workflow target persistence reader is required")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	entityID = runtimeidentity.NormalizeEntityID(entityID.String())
	if !route.Valid() || entityID.IsZero() {
		return runtimepipeline.WorkflowTargetPersistenceRecord{}, fmt.Errorf("workflow target persistence lookup requires exact route and entity identity")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return runtimepipeline.WorkflowTargetPersistenceRecord{}, err
	}
	return scanSQLiteWorkflowTargetPersistence(s.backend.QueryRowContext(ctx, sqliteWorkflowTargetPersistenceSelect, runID, entityID.String(), route.InstancePath), route, entityID)
}

func assembleWorkflowTargetPersistence(
	route runtimeflowidentity.Route,
	entityID runtimeidentity.EntityID,
	state runtimepipeline.WorkflowEntityStatePersistenceRecord,
	stateExists bool,
	companion runtimepipeline.WorkflowLifecycleCompanionPersistenceRecord,
	companionExists bool,
) (runtimepipeline.WorkflowTargetPersistenceRecord, error) {
	record := runtimepipeline.WorkflowTargetPersistenceRecord{State: state, Lifecycle: companion}
	switch {
	case stateExists && companionExists:
		record.Presence = runtimepipeline.WorkflowTargetPersistenceComplete
	case stateExists:
		record.Presence = runtimepipeline.WorkflowTargetPersistenceStateOnly
	case companionExists:
		record.Presence = runtimepipeline.WorkflowTargetPersistenceLifecycleOnly
	default:
		record.Presence = runtimepipeline.WorkflowTargetPersistenceAbsent
	}
	if err := record.Validate(route, entityID); err != nil {
		return runtimepipeline.WorkflowTargetPersistenceRecord{}, err
	}
	return record, nil
}

func (s *PipelineSQLiteOwner) SelectActiveWorkflowEntityStates(ctx context.Context, owner runtimepipeline.WorkflowEntityStateSelectionOwner, selectors []runtimepipeline.WorkflowInstanceFieldSelector, excludedStates []string) ([]runtimepipeline.WorkflowEntityStatePersistenceRecord, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite workflow entity state reader is required")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, err
	}
	scopeKey := owner.ScopeKey()
	selectors = runtimepipeline.NormalizeWorkflowInstanceFieldSelectors(selectors)
	if scopeKey == "" || len(selectors) == 0 {
		return nil, nil
	}
	activeStates := runtimerunlifecycle.ActiveStates()
	rows, err := s.backend.QueryContext(ctx, sqliteWorkflowEntityStateSelect+`
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = ?
		  AND EXISTS (
			SELECT 1 FROM runs run
			WHERE run.run_id = es.run_id AND run.status IN (?, ?)
		  )
		  AND (es.flow_instance = ? OR es.flow_instance LIKE ? OR (? AND es.flow_instance = ?))
		  AND (fi.instance_id IS NULL OR (LOWER(TRIM(fi.status)) = 'active' AND fi.terminated_at IS NULL))
		ORDER BY es.created_at ASC, es.entity_id ASC
	`, runID, string(activeStates[0]), string(activeStates[1]), scopeKey, scopeKey+"/%", owner.Owns(runID), runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records, err := scanSQLiteWorkflowEntityStates(rows)
	if err != nil {
		return nil, err
	}
	return runtimepipeline.FilterWorkflowEntityStatePersistenceRecords(records, owner, selectors, excludedStates)
}

func (s *PipelineSQLiteOwner) QueryWorkflowEntityCollection(ctx context.Context, owner runtimepipeline.WorkflowEntityCollectionOwner) ([]runtimepipeline.WorkflowEntityStatePersistenceRecord, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite workflow entity collection reader is required")
	}
	if !owner.Valid() {
		return nil, fmt.Errorf("sqlite workflow entity collection requires an admitted owner")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, err
	}
	activeStates := runtimerunlifecycle.ActiveStates()
	rows, err := s.backend.QueryContext(ctx, sqliteWorkflowEntityStateSelect+`
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = ?
		  AND es.entity_type = ?
		  AND EXISTS (
			SELECT 1 FROM runs run
			WHERE run.run_id = es.run_id AND run.status IN (?, ?)
		  )
		  AND (es.flow_instance = ? OR es.flow_instance LIKE ? OR (? AND es.flow_instance = ?))
		  AND (fi.instance_id IS NULL OR (LOWER(TRIM(fi.status)) = 'active' AND fi.terminated_at IS NULL))
		ORDER BY es.created_at ASC, es.entity_id ASC
	`, runID, owner.EntityType(), string(activeStates[0]), string(activeStates[1]), owner.ScopeKey(), owner.ScopeKey()+"/%", owner.ScopeKey() == runID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records, err := scanSQLiteWorkflowEntityStates(rows)
	if err != nil {
		return nil, err
	}
	return runtimepipeline.FilterWorkflowEntityCollectionRecords(records, owner)
}

func (s *PipelineSQLiteOwner) ListWorkflowInstances(ctx context.Context) ([]runtimepipeline.WorkflowInstance, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite workflow instance reader is required")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.backend.QueryContext(ctx, sqliteWorkflowInstanceSelect+` WHERE es.run_id = ? ORDER BY es.created_at ASC`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSQLiteWorkflowInstances(rows)
}

func (s *PipelineSQLiteOwner) SelectActiveWorkflowInstances(ctx context.Context, scopeKey string, selectors []runtimepipeline.WorkflowInstanceFieldSelector, excludedStates []string) ([]runtimepipeline.WorkflowInstance, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite workflow instance reader is required")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, err
	}
	activeStates := runtimerunlifecycle.ActiveStates()
	var active bool
	if err := s.backend.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = ? AND status IN (?, ?))`, runID, string(activeStates[0]), string(activeStates[1])).Scan(&active); err != nil {
		return nil, err
	}
	scopeKey = strings.Trim(strings.TrimSpace(scopeKey), "/")
	selectors = runtimepipeline.NormalizeWorkflowInstanceFieldSelectors(selectors)
	if !active || scopeKey == "" || len(selectors) == 0 {
		return nil, nil
	}
	items, err := s.ListWorkflowInstances(ctx)
	if err != nil {
		return nil, err
	}
	terminalStates := make(map[string]struct{})
	for _, state := range runtimepipeline.NormalizeWorkflowInstanceExcludedStates(excludedStates) {
		terminalStates[state] = struct{}{}
	}
	out := make([]runtimepipeline.WorkflowInstance, 0, len(items))
	for _, item := range items {
		storageRef := strings.Trim(strings.TrimSpace(item.StorageRef), "/")
		if storageRef != scopeKey && !strings.HasPrefix(storageRef, scopeKey+"/") {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(item.Status), "active") || !item.TerminatedAt.IsZero() {
			continue
		}
		if _, terminal := terminalStates[strings.ToLower(strings.TrimSpace(item.CurrentState))]; terminal {
			continue
		}
		matches := true
		for _, selector := range selectors {
			value, ok := runtimepipeline.WorkflowMetadataValue(item.Fields, selector.Field)
			if !ok || !runtimepipeline.WorkflowJSONValuesEqual(value, selector.Value) {
				matches = false
				break
			}
		}
		if matches {
			out = append(out, item)
		}
	}
	return out, nil
}

const sqliteWorkflowInstanceSelect = `
	SELECT
		es.entity_id,
		fi.flow_template,
		json_extract(fi.config, '$.workflow_version'),
		fi.mode,
		fi.status,
		fi.terminated_at,
		es.current_state,
		es.revision,
		es.entered_state_at,
		es.gates,
		es.fields,
		es.bookkeeping,
		es.accumulator,
		fi.config,
		es.flow_instance,
		es.entity_type,
		es.slug,
		es.name,
		es.created_at,
		es.updated_at
	FROM entity_state es
	JOIN flow_instances fi ON fi.instance_id = es.flow_instance
`

const sqliteWorkflowEntityStateSelect = `
	SELECT
		es.entity_id,
		es.flow_instance,
		es.entity_type,
		es.slug,
		es.name,
		es.current_state,
		es.revision,
		es.entered_state_at,
		es.gates,
		es.fields,
		es.bookkeeping,
		es.accumulator,
		es.created_at,
		es.updated_at
	FROM entity_state es
`

const sqliteWorkflowTargetPersistenceSelect = `
	SELECT
		es.entity_id,
		es.flow_instance,
		es.entity_type,
		es.slug,
		es.name,
		es.current_state,
		es.revision,
		es.entered_state_at,
		es.gates,
		es.fields,
		es.bookkeeping,
		es.accumulator,
		es.created_at,
		es.updated_at,
		fi.instance_id,
		fi.flow_template,
		json_extract(fi.config, '$.workflow_version'),
		fi.mode,
		fi.status,
		fi.config,
		fi.terminated_at,
		fi.created_at
	FROM (SELECT ? AS run_id, ? AS entity_id, ? AS flow_instance) AS target
	LEFT JOIN entity_state es
		ON es.run_id = target.run_id
		AND es.entity_id = target.entity_id
		AND es.flow_instance = target.flow_instance
	LEFT JOIN flow_instances fi ON fi.instance_id = target.flow_instance
`

func scanSQLiteWorkflowEntityState(row workflowInstanceScanner) (runtimepipeline.WorkflowEntityStatePersistenceRecord, error) {
	var record runtimepipeline.WorkflowEntityStatePersistenceRecord
	var slug, name sql.NullString
	var enteredAt, gates, fields, bookkeeping, accumulator, createdAt, updatedAt any
	if err := row.Scan(
		&record.EntityID,
		&record.FlowInstance,
		&record.EntityType,
		&slug,
		&name,
		&record.CurrentState,
		&record.Revision,
		&enteredAt,
		&gates,
		&fields,
		&bookkeeping,
		&accumulator,
		&createdAt,
		&updatedAt,
	); err != nil {
		return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, err
	}
	record.Slug = slug.String
	record.Name = name.String
	for _, item := range []struct {
		raw    any
		target *json.RawMessage
	}{{gates, &record.Gates}, {fields, &record.Fields}, {bookkeeping, &record.Bookkeeping}, {accumulator, &record.Accumulator}} {
		switch value := item.raw.(type) {
		case string:
			*item.target = json.RawMessage(value)
		case []byte:
			*item.target = append(json.RawMessage(nil), value...)
		default:
			return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, fmt.Errorf("unsupported sqlite workflow entity JSON value %T", item.raw)
		}
	}
	for _, item := range []struct {
		raw    any
		target *time.Time
	}{{enteredAt, &record.EnteredStageAt}, {createdAt, &record.CreatedAt}, {updatedAt, &record.UpdatedAt}} {
		value, ok, err := sqliteTimeValue(item.raw)
		if err != nil || !ok {
			if err == nil {
				err = fmt.Errorf("sqlite workflow entity timestamp is required")
			}
			return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, err
		}
		*item.target = value
	}
	return record, nil
}

func scanSQLiteWorkflowTargetPersistence(row workflowInstanceScanner, route runtimeflowidentity.Route, entityID runtimeidentity.EntityID) (runtimepipeline.WorkflowTargetPersistenceRecord, error) {
	var stateEntityID, stateFlowInstance, stateEntityType, stateSlug, stateName, stateCurrent sql.NullString
	var stateRevision sql.NullInt64
	var stateEnteredAt, stateGates, stateFields, stateBookkeeping, stateAccumulator, stateCreatedAt, stateUpdatedAt any
	var lifecycleFlowInstance, lifecycleWorkflowName, lifecycleWorkflowVersion, lifecycleMode, lifecycleStatus sql.NullString
	var lifecycleConfig, lifecycleTerminatedAt, lifecycleCreatedAt any
	if err := row.Scan(
		&stateEntityID, &stateFlowInstance, &stateEntityType, &stateSlug, &stateName,
		&stateCurrent, &stateRevision, &stateEnteredAt, &stateGates, &stateFields,
		&stateBookkeeping, &stateAccumulator, &stateCreatedAt, &stateUpdatedAt,
		&lifecycleFlowInstance, &lifecycleWorkflowName, &lifecycleWorkflowVersion,
		&lifecycleMode, &lifecycleStatus, &lifecycleConfig, &lifecycleTerminatedAt, &lifecycleCreatedAt,
	); err != nil {
		return runtimepipeline.WorkflowTargetPersistenceRecord{}, err
	}
	state := runtimepipeline.WorkflowEntityStatePersistenceRecord{}
	if stateEntityID.Valid {
		state = runtimepipeline.WorkflowEntityStatePersistenceRecord{
			EntityID: stateEntityID.String, FlowInstance: stateFlowInstance.String, EntityType: stateEntityType.String,
			Slug: stateSlug.String, Name: stateName.String, CurrentState: stateCurrent.String, Revision: stateRevision.Int64,
		}
		for _, item := range []struct {
			raw    any
			target *json.RawMessage
			name   string
		}{{stateGates, &state.Gates, "gates"}, {stateFields, &state.Fields, "fields"}, {stateBookkeeping, &state.Bookkeeping, "bookkeeping"}, {stateAccumulator, &state.Accumulator, "accumulator"}} {
			value, err := sqliteWorkflowJSONValue(item.raw)
			if err != nil {
				return runtimepipeline.WorkflowTargetPersistenceRecord{}, fmt.Errorf("decode sqlite workflow target state %s: %w", item.name, err)
			}
			*item.target = value
		}
		for _, item := range []struct {
			raw    any
			target *time.Time
			name   string
		}{{stateEnteredAt, &state.EnteredStageAt, "entered state"}, {stateCreatedAt, &state.CreatedAt, "created"}, {stateUpdatedAt, &state.UpdatedAt, "updated"}} {
			value, ok, err := sqliteTimeValue(item.raw)
			if err != nil || !ok {
				if err == nil {
					err = fmt.Errorf("sqlite workflow target state %s time is required", item.name)
				}
				return runtimepipeline.WorkflowTargetPersistenceRecord{}, err
			}
			*item.target = value
		}
	}
	lifecycle := runtimepipeline.WorkflowLifecycleCompanionPersistenceRecord{}
	if lifecycleFlowInstance.Valid {
		config, err := sqliteWorkflowJSONValue(lifecycleConfig)
		if err != nil {
			return runtimepipeline.WorkflowTargetPersistenceRecord{}, fmt.Errorf("decode sqlite workflow target lifecycle config: %w", err)
		}
		lifecycle = runtimepipeline.WorkflowLifecycleCompanionPersistenceRecord{
			FlowInstance: lifecycleFlowInstance.String, WorkflowName: lifecycleWorkflowName.String,
			WorkflowVersion: lifecycleWorkflowVersion.String, Mode: lifecycleMode.String, Status: lifecycleStatus.String,
			Config: config,
		}
		createdAt, ok, err := sqliteTimeValue(lifecycleCreatedAt)
		if err != nil || !ok {
			if err == nil {
				err = fmt.Errorf("sqlite workflow target lifecycle creation time is required")
			}
			return runtimepipeline.WorkflowTargetPersistenceRecord{}, err
		}
		lifecycle.CreatedAt = createdAt
		if terminatedAt, ok, err := sqliteTimeValue(lifecycleTerminatedAt); err != nil {
			return runtimepipeline.WorkflowTargetPersistenceRecord{}, err
		} else if ok {
			lifecycle.TerminatedAt = terminatedAt
		}
	}
	return assembleWorkflowTargetPersistence(route, entityID, state, stateEntityID.Valid, lifecycle, lifecycleFlowInstance.Valid)
}

func sqliteWorkflowJSONValue(raw any) (json.RawMessage, error) {
	switch value := raw.(type) {
	case string:
		return json.RawMessage(value), nil
	case []byte:
		return append(json.RawMessage(nil), value...), nil
	default:
		return nil, fmt.Errorf("unsupported sqlite workflow JSON value %T", raw)
	}
}

func scanSQLiteWorkflowEntityStates(rows *sql.Rows) ([]runtimepipeline.WorkflowEntityStatePersistenceRecord, error) {
	records := make([]runtimepipeline.WorkflowEntityStatePersistenceRecord, 0, 32)
	for rows.Next() {
		record, err := scanSQLiteWorkflowEntityState(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func scanSQLiteWorkflowInstances(rows *sql.Rows) ([]runtimepipeline.WorkflowInstance, error) {
	items := make([]runtimepipeline.WorkflowInstance, 0, 32)
	for rows.Next() {
		var record runtimepipeline.WorkflowInstancePersistenceRecord
		var workflowVersion, slug, name sql.NullString
		var terminatedAt, enteredAt, createdAt, updatedAt any
		var gates, fields, bookkeeping, accumulator, config any
		if err := rows.Scan(
			&record.EntityID,
			&record.WorkflowName,
			&workflowVersion,
			&record.Mode,
			&record.Status,
			&terminatedAt,
			&record.CurrentState,
			&record.Revision,
			&enteredAt,
			&gates,
			&fields,
			&bookkeeping,
			&accumulator,
			&config,
			&record.FlowInstance,
			&record.EntityType,
			&slug,
			&name,
			&createdAt,
			&updatedAt,
		); err != nil {
			return nil, err
		}
		record.WorkflowVersion = workflowVersion.String
		record.Slug = slug.String
		record.Name = name.String
		for _, item := range []struct {
			raw    any
			target *json.RawMessage
		}{{gates, &record.Gates}, {fields, &record.Fields}, {bookkeeping, &record.Bookkeeping}, {accumulator, &record.Accumulator}, {config, &record.Config}} {
			raw, target := item.raw, item.target
			switch value := raw.(type) {
			case string:
				*target = json.RawMessage(value)
			case []byte:
				*target = append(json.RawMessage(nil), value...)
			default:
				return nil, fmt.Errorf("unsupported sqlite workflow JSON value %T", raw)
			}
		}
		for _, item := range []struct {
			raw    any
			target *time.Time
		}{{enteredAt, &record.EnteredStageAt}, {createdAt, &record.CreatedAt}, {updatedAt, &record.UpdatedAt}} {
			raw, target := item.raw, item.target
			value, ok, err := sqliteTimeValue(raw)
			if err != nil || !ok {
				if err == nil {
					err = fmt.Errorf("sqlite workflow timestamp is required")
				}
				return nil, err
			}
			*target = value
		}
		if value, ok, err := sqliteTimeValue(terminatedAt); err != nil {
			return nil, err
		} else if ok {
			record.TerminatedAt = value
		}
		item, err := runtimepipeline.DecodeWorkflowInstancePersistenceRecord(record)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

var _ runtimepipeline.WorkflowInstancePersistenceReader = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.WorkflowInstancePersistenceReader = (*PipelineSQLiteOwner)(nil)
var _ runtimepipeline.WorkflowEntityStatePersistenceReader = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.WorkflowEntityStatePersistenceReader = (*PipelineSQLiteOwner)(nil)
var _ runtimepipeline.WorkflowTargetPersistenceReader = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.WorkflowTargetPersistenceReader = (*PipelineSQLiteOwner)(nil)
