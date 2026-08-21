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
		  AND fi.status NOT IN ('terminated', 'inactive')
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
		if item.Status == "terminated" || item.Status == "inactive" || !item.TerminatedAt.IsZero() {
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
