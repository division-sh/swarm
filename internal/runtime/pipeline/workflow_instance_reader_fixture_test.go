package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type pipelineTestWorkflowInstanceReader struct {
	db      *sql.DB
	dialect workflowStoreDialect
}

func (r pipelineTestWorkflowInstanceReader) LoadWorkflowInstance(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) (WorkflowInstance, bool, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return WorkflowInstance{}, false, &WorkflowInstanceLookupMiss{RequestedKey: strings.TrimSpace(identity.Route.InstancePath)}
	}
	route, runID := identity.Route, identity.RunID
	query := pipelineTestWorkflowInstanceSelectSQLite + ` WHERE es.flow_instance = ? AND es.run_id = ? ORDER BY es.created_at DESC, es.entity_id DESC LIMIT 1`
	if r.dialect == workflowStoreDialectPostgres {
		query = pipelineTestWorkflowInstanceSelectPostgres + ` WHERE es.flow_instance = $1 AND es.run_id = $2::uuid ORDER BY es.created_at DESC, es.entity_id DESC LIMIT 1`
	}
	rows, err := r.db.QueryContext(ctx, query, route.InstancePath, runID)
	if err != nil {
		return WorkflowInstance{}, false, err
	}
	defer rows.Close()
	items, err := scanPipelineTestWorkflowInstances(rows, r.dialect)
	if err != nil || len(items) == 0 {
		return WorkflowInstance{}, false, err
	}
	return items[0], true, nil
}

func (r pipelineTestWorkflowInstanceReader) LoadWorkflowEntityState(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance, entityID runtimeidentity.EntityID) (WorkflowEntityStatePersistenceRecord, bool, error) {
	identity = identity.Normalize()
	entityID = runtimeidentity.NormalizeEntityID(entityID.String())
	if err := identity.Validate(); err != nil || entityID.IsZero() {
		return WorkflowEntityStatePersistenceRecord{}, false, fmt.Errorf("workflow entity state lookup requires exact route and entity identity")
	}
	route, runID := identity.Route, identity.RunID
	query := `SELECT entity_id, flow_instance, entity_type, slug, name, current_state, revision, entered_state_at, gates, fields, bookkeeping, accumulator, created_at, updated_at FROM entity_state WHERE run_id = ? AND entity_id = ? AND flow_instance = ?`
	if r.dialect == workflowStoreDialectPostgres {
		query = `SELECT entity_id::text, flow_instance, entity_type, slug, name, current_state, revision, entered_state_at, gates, fields, bookkeeping, accumulator, created_at, updated_at FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid AND flow_instance = $3`
	}
	record, err := scanPipelineTestWorkflowEntityState(r.db.QueryRowContext(ctx, query, runID, entityID.String(), route.InstancePath))
	if err == sql.ErrNoRows {
		return WorkflowEntityStatePersistenceRecord{}, false, nil
	}
	if err != nil {
		return WorkflowEntityStatePersistenceRecord{}, false, err
	}
	return record, true, nil
}

func (r pipelineTestWorkflowInstanceReader) LoadWorkflowTargetPersistence(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance, entityID runtimeidentity.EntityID) (WorkflowTargetPersistenceRecord, error) {
	identity = identity.Normalize()
	entityID = runtimeidentity.NormalizeEntityID(entityID.String())
	if err := identity.Validate(); err != nil || entityID.IsZero() {
		return WorkflowTargetPersistenceRecord{}, fmt.Errorf("workflow target persistence lookup requires exact route and entity identity")
	}
	route, runID := identity.Route, identity.RunID
	txOptions := &sql.TxOptions{ReadOnly: true}
	if r.dialect == workflowStoreDialectPostgres {
		txOptions.Isolation = sql.LevelRepeatableRead
	}
	tx, err := r.db.BeginTx(ctx, txOptions)
	if err != nil {
		return WorkflowTargetPersistenceRecord{}, err
	}
	defer tx.Rollback()
	stateQuery := `SELECT entity_id, flow_instance, entity_type, slug, name, current_state, revision, entered_state_at, gates, fields, bookkeeping, accumulator, created_at, updated_at FROM entity_state WHERE run_id = ? AND entity_id = ? AND flow_instance = ?`
	if r.dialect == workflowStoreDialectPostgres {
		stateQuery = `SELECT entity_id::text, flow_instance, entity_type, slug, name, current_state, revision, entered_state_at, gates, fields, bookkeeping, accumulator, created_at, updated_at FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid AND flow_instance = $3`
	}
	state, stateErr := scanPipelineTestWorkflowEntityState(tx.QueryRowContext(ctx, stateQuery, runID, entityID.String(), route.InstancePath))
	stateExists := stateErr == nil
	if stateErr != nil && stateErr != sql.ErrNoRows {
		return WorkflowTargetPersistenceRecord{}, stateErr
	}
	var companion WorkflowLifecycleCompanionPersistenceRecord
	var workflowVersion sql.NullString
	if r.dialect == workflowStoreDialectPostgres {
		var terminatedAt sql.NullTime
		err = tx.QueryRowContext(ctx, `
				SELECT instance_path, flow_template, COALESCE(config->>'workflow_version', ''), mode, status, config, terminated_at, created_at
				FROM flow_instances WHERE run_id = $1::uuid AND instance_path = $2
		`, runID, route.InstancePath).Scan(
			&companion.FlowInstance, &companion.WorkflowName, &workflowVersion, &companion.Mode,
			&companion.Status, &companion.Config, &terminatedAt, &companion.CreatedAt,
		)
		if terminatedAt.Valid {
			companion.TerminatedAt = terminatedAt.Time.UTC()
		}
	} else {
		var config, terminatedAt, createdAt any
		err = tx.QueryRowContext(ctx, `
			SELECT instance_path, flow_template, json_extract(config, '$.workflow_version'), mode, status, config, terminated_at, created_at
			FROM flow_instances WHERE run_id = ? AND instance_path = ?
		`, runID, route.InstancePath).Scan(
			&companion.FlowInstance, &companion.WorkflowName, &workflowVersion, &companion.Mode,
			&companion.Status, &config, &terminatedAt, &createdAt,
		)
		if err == nil {
			companion.Config = pipelineTestJSONBytes(config)
			created, present, parseErr := sqliteWorkflowTimeValue(createdAt)
			if parseErr != nil || !present {
				if parseErr == nil {
					parseErr = fmt.Errorf("pipeline test workflow lifecycle creation time is required")
				}
				return WorkflowTargetPersistenceRecord{}, parseErr
			}
			companion.CreatedAt = created
			if terminated, present, parseErr := sqliteWorkflowTimeValue(terminatedAt); parseErr != nil {
				return WorkflowTargetPersistenceRecord{}, parseErr
			} else if present {
				companion.TerminatedAt = terminated
			}
		}
	}
	companionExists := err == nil
	if err != nil && err != sql.ErrNoRows {
		return WorkflowTargetPersistenceRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkflowTargetPersistenceRecord{}, err
	}
	companion.WorkflowVersion = workflowVersion.String
	record := WorkflowTargetPersistenceRecord{State: state, Lifecycle: companion}
	switch {
	case stateExists && companionExists:
		record.Presence = WorkflowTargetPersistenceComplete
	case stateExists:
		record.Presence = WorkflowTargetPersistenceStateOnly
	case companionExists:
		record.Presence = WorkflowTargetPersistenceLifecycleOnly
	default:
		record.Presence = WorkflowTargetPersistenceAbsent
	}
	if err := record.Validate(route, entityID); err != nil {
		return WorkflowTargetPersistenceRecord{}, err
	}
	return record, nil
}

func (r pipelineTestWorkflowInstanceReader) SelectActiveWorkflowEntityStates(ctx context.Context, runID string, owner WorkflowEntityStateSelectionOwner, selectors []WorkflowInstanceFieldSelector, excludedStates []string) ([]WorkflowEntityStatePersistenceRecord, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("workflow entity-state selection requires exact run_id")
	}
	scopeKey := owner.ScopeKey()
	selectors = NormalizeWorkflowInstanceFieldSelectors(selectors)
	if scopeKey == "" || len(selectors) == 0 {
		return nil, nil
	}
	activeStates := runtimerunlifecycle.ActiveStates()
	query := `SELECT es.entity_id, es.flow_instance, es.entity_type, es.slug, es.name, es.current_state, es.revision, es.entered_state_at, es.gates, es.fields, es.bookkeeping, es.accumulator, es.created_at, es.updated_at
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance
		WHERE es.run_id = ?
		  AND EXISTS (SELECT 1 FROM runs run WHERE run.run_id = es.run_id AND run.status IN (?, ?))
		  AND (es.flow_instance = ? OR es.flow_instance LIKE ? OR (? AND es.flow_instance = ?))
		  AND (fi.instance_path IS NULL OR (LOWER(TRIM(fi.status)) = 'active' AND fi.terminated_at IS NULL))
		ORDER BY es.created_at ASC, es.entity_id ASC`
	args := []any{runID, string(activeStates[0]), string(activeStates[1]), scopeKey, scopeKey + "/%", owner.Owns(runID), runID}
	if r.dialect == workflowStoreDialectPostgres {
		query = `SELECT es.entity_id::text, es.flow_instance, es.entity_type, es.slug, es.name, es.current_state, es.revision, es.entered_state_at, es.gates, es.fields, es.bookkeeping, es.accumulator, es.created_at, es.updated_at
			FROM entity_state es
			LEFT JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance
			WHERE es.run_id = $1::uuid
			  AND EXISTS (SELECT 1 FROM runs run WHERE run.run_id = es.run_id AND run.status IN ($2, $3))
				  AND (es.flow_instance = $4 OR es.flow_instance LIKE $5 OR ($6::boolean AND es.flow_instance = $1::text))
				  AND (fi.instance_path IS NULL OR (LOWER(BTRIM(fi.status)) = 'active' AND fi.terminated_at IS NULL))
			ORDER BY es.created_at ASC, es.entity_id ASC`
	}
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]WorkflowEntityStatePersistenceRecord, 0, 8)
	for rows.Next() {
		record, err := scanPipelineTestWorkflowEntityState(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return FilterWorkflowEntityStatePersistenceRecords(records, owner, selectors, excludedStates)
}

func scanPipelineTestWorkflowEntityState(row interface{ Scan(...any) error }) (WorkflowEntityStatePersistenceRecord, error) {
	var record WorkflowEntityStatePersistenceRecord
	var slug, name sql.NullString
	var enteredAt, gates, fields, bookkeeping, accumulator, createdAt, updatedAt any
	err := row.Scan(
		&record.EntityID, &record.FlowInstance, &record.EntityType, &slug, &name,
		&record.CurrentState, &record.Revision, &enteredAt, &gates, &fields, &bookkeeping, &accumulator,
		&createdAt, &updatedAt,
	)
	if err != nil {
		return WorkflowEntityStatePersistenceRecord{}, err
	}
	record.Slug = slug.String
	record.Name = name.String
	record.Gates = pipelineTestJSONBytes(gates)
	record.Fields = pipelineTestJSONBytes(fields)
	record.Bookkeeping = pipelineTestJSONBytes(bookkeeping)
	record.Accumulator = pipelineTestJSONBytes(accumulator)
	for _, item := range []struct {
		raw    any
		target *time.Time
	}{{enteredAt, &record.EnteredStageAt}, {createdAt, &record.CreatedAt}, {updatedAt, &record.UpdatedAt}} {
		if value, ok := item.raw.(time.Time); ok {
			*item.target = value.UTC()
			continue
		}
		value, ok, err := sqliteWorkflowTimeValue(item.raw)
		if err != nil || !ok {
			if err == nil {
				err = fmt.Errorf("workflow entity timestamp is required")
			}
			return WorkflowEntityStatePersistenceRecord{}, err
		}
		*item.target = value
	}
	return record, nil
}

func (r pipelineTestWorkflowInstanceReader) ListWorkflowInstances(ctx context.Context, runID string) ([]WorkflowInstance, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("workflow instance list requires exact run_id")
	}
	query := pipelineTestWorkflowInstanceSelectSQLite + ` WHERE es.run_id = ? ORDER BY es.created_at ASC`
	if r.dialect == workflowStoreDialectPostgres {
		query = pipelineTestWorkflowInstanceSelectPostgres + ` WHERE es.run_id = $1::uuid ORDER BY es.created_at ASC`
	}
	rows, err := r.db.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPipelineTestWorkflowInstances(rows, r.dialect)
}

func (r pipelineTestWorkflowInstanceReader) SelectActiveWorkflowInstances(ctx context.Context, runID, scopeKey string, selectors []WorkflowInstanceFieldSelector, excludedStates []string) ([]WorkflowInstance, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("workflow instance selection requires exact run_id")
	}
	activeStates := runtimerunlifecycle.ActiveStates()
	query := `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = ? AND status IN (?, ?))`
	args := []any{runID, string(activeStates[0]), string(activeStates[1])}
	if r.dialect == workflowStoreDialectPostgres {
		query = `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = $1::uuid AND status IN ($2, $3))`
	}
	var active bool
	if err := r.db.QueryRowContext(ctx, query, args...).Scan(&active); err != nil {
		return nil, err
	}
	scopeKey = strings.Trim(strings.TrimSpace(scopeKey), "/")
	selectors = NormalizeWorkflowInstanceFieldSelectors(selectors)
	if !active || scopeKey == "" || len(selectors) == 0 {
		return nil, nil
	}
	items, err := r.ListWorkflowInstances(ctx, runID)
	if err != nil {
		return nil, err
	}
	excluded := make(map[string]struct{})
	for _, state := range NormalizeWorkflowInstanceExcludedStates(excludedStates) {
		excluded[state] = struct{}{}
	}
	out := make([]WorkflowInstance, 0, len(items))
	for _, item := range items {
		path := strings.Trim(strings.TrimSpace(item.StorageRef), "/")
		if path != scopeKey && !strings.HasPrefix(path, scopeKey+"/") {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(item.Status), "active") || !item.TerminatedAt.IsZero() {
			continue
		}
		if _, found := excluded[strings.ToLower(strings.TrimSpace(item.CurrentState))]; found {
			continue
		}
		matched := true
		for _, selector := range selectors {
			value, found := workflowMetadataValue(item.Fields, selector.Field)
			if !found || !workflowJSONValuesEqual(value, selector.Value) {
				matched = false
				break
			}
		}
		if matched {
			out = append(out, item)
		}
	}
	return out, nil
}

const pipelineTestWorkflowInstanceSelectPostgres = `
	SELECT es.entity_id::text, fi.flow_template, fi.config->>'workflow_version', fi.mode, fi.status,
	       fi.terminated_at, es.current_state, es.revision, es.entered_state_at,
	       es.gates, es.fields, es.bookkeeping, es.accumulator, fi.config, es.flow_instance,
	       es.entity_type, es.slug, es.name, es.created_at, es.updated_at
	FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance
`

const pipelineTestWorkflowInstanceSelectSQLite = `
	SELECT es.entity_id, fi.flow_template, json_extract(fi.config, '$.workflow_version'), fi.mode, fi.status,
	       fi.terminated_at, es.current_state, es.revision, es.entered_state_at,
	       es.gates, es.fields, es.bookkeeping, es.accumulator, fi.config, es.flow_instance,
	       es.entity_type, es.slug, es.name, es.created_at, es.updated_at
	FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance
`

func scanPipelineTestWorkflowInstances(rows *sql.Rows, dialect workflowStoreDialect) ([]WorkflowInstance, error) {
	items := make([]WorkflowInstance, 0, 16)
	for rows.Next() {
		var record WorkflowInstancePersistenceRecord
		var workflowVersion, slug, name sql.NullString
		var terminatedAt sql.NullTime
		var terminatedAtRaw, enteredAtRaw, createdAtRaw, updatedAtRaw any
		var gates, fields, bookkeeping, accumulator, config any
		destinations := []any{
			&record.EntityID, &record.WorkflowName, &workflowVersion, &record.Mode, &record.Status,
			&terminatedAt, &record.CurrentState, &record.Revision, &record.EnteredStageAt,
			&record.Gates, &record.Fields, &record.Bookkeeping, &record.Accumulator, &record.Config,
			&record.FlowInstance, &record.EntityType, &slug, &name, &record.CreatedAt, &record.UpdatedAt,
		}
		if dialect != workflowStoreDialectPostgres {
			destinations = []any{
				&record.EntityID, &record.WorkflowName, &workflowVersion, &record.Mode, &record.Status,
				&terminatedAtRaw, &record.CurrentState, &record.Revision, &enteredAtRaw,
				&gates, &fields, &bookkeeping, &accumulator, &config,
				&record.FlowInstance, &record.EntityType, &slug, &name, &createdAtRaw, &updatedAtRaw,
			}
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}
		record.WorkflowVersion, record.Slug, record.Name = workflowVersion.String, slug.String, name.String
		if dialect == workflowStoreDialectPostgres {
			if terminatedAt.Valid {
				record.TerminatedAt = terminatedAt.Time.UTC()
			}
		} else {
			record.Gates, record.Fields = pipelineTestJSONBytes(gates), pipelineTestJSONBytes(fields)
			record.Bookkeeping = pipelineTestJSONBytes(bookkeeping)
			record.Accumulator, record.Config = pipelineTestJSONBytes(accumulator), pipelineTestJSONBytes(config)
			for _, value := range []struct {
				raw    any
				target *time.Time
			}{{enteredAtRaw, &record.EnteredStageAt}, {createdAtRaw, &record.CreatedAt}, {updatedAtRaw, &record.UpdatedAt}} {
				parsed, present, err := sqliteWorkflowTimeValue(value.raw)
				if err != nil || !present {
					if err == nil {
						err = fmt.Errorf("pipeline test workflow timestamp is required")
					}
					return nil, err
				}
				*value.target = parsed
			}
			if parsed, present, err := sqliteWorkflowTimeValue(terminatedAtRaw); err != nil {
				return nil, err
			} else if present {
				record.TerminatedAt = parsed
			}
		}
		item, err := DecodeWorkflowInstancePersistenceRecord(record)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func pipelineTestJSONBytes(value any) json.RawMessage {
	switch value := value.(type) {
	case string:
		return json.RawMessage(value)
	case []byte:
		return append(json.RawMessage(nil), value...)
	default:
		return nil
	}
}

var _ WorkflowInstancePersistenceReader = pipelineTestWorkflowInstanceReader{}
var _ WorkflowEntityStatePersistenceReader = pipelineTestWorkflowInstanceReader{}
var _ WorkflowTargetPersistenceReader = pipelineTestWorkflowInstanceReader{}

func (r *recordingRuntimeMutationRunner) LoadWorkflowInstance(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) (WorkflowInstance, bool, error) {
	return pipelineTestWorkflowInstanceReader{db: r.db, dialect: r.dialect}.LoadWorkflowInstance(ctx, identity)
}

func (r *recordingRuntimeMutationRunner) LoadWorkflowEntityState(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance, entityID runtimeidentity.EntityID) (WorkflowEntityStatePersistenceRecord, bool, error) {
	return pipelineTestWorkflowInstanceReader{db: r.db, dialect: r.dialect}.LoadWorkflowEntityState(ctx, identity, entityID)
}

func (r *recordingRuntimeMutationRunner) LoadWorkflowTargetPersistence(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance, entityID runtimeidentity.EntityID) (WorkflowTargetPersistenceRecord, error) {
	return pipelineTestWorkflowInstanceReader{db: r.db, dialect: r.dialect}.LoadWorkflowTargetPersistence(ctx, identity, entityID)
}

func (r *recordingRuntimeMutationRunner) SelectActiveWorkflowEntityStates(ctx context.Context, runID string, owner WorkflowEntityStateSelectionOwner, selectors []WorkflowInstanceFieldSelector, excludedStates []string) ([]WorkflowEntityStatePersistenceRecord, error) {
	return pipelineTestWorkflowInstanceReader{db: r.db, dialect: r.dialect}.SelectActiveWorkflowEntityStates(ctx, runID, owner, selectors, excludedStates)
}

func (r *recordingRuntimeMutationRunner) ListWorkflowInstances(ctx context.Context, runID string) ([]WorkflowInstance, error) {
	return pipelineTestWorkflowInstanceReader{db: r.db, dialect: r.dialect}.ListWorkflowInstances(ctx, runID)
}

func (r *recordingRuntimeMutationRunner) SelectActiveWorkflowInstances(ctx context.Context, runID, scopeKey string, selectors []WorkflowInstanceFieldSelector, excludedStates []string) ([]WorkflowInstance, error) {
	return pipelineTestWorkflowInstanceReader{db: r.db, dialect: r.dialect}.SelectActiveWorkflowInstances(ctx, runID, scopeKey, selectors, excludedStates)
}

var _ WorkflowInstancePersistenceReader = (*recordingRuntimeMutationRunner)(nil)
var _ WorkflowEntityStatePersistenceReader = (*recordingRuntimeMutationRunner)(nil)
var _ WorkflowTargetPersistenceReader = (*recordingRuntimeMutationRunner)(nil)

func (s *workflowInstanceStore) mutate(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance, fn func(*WorkflowInstance)) error {
	if fn == nil {
		return nil
	}
	return s.mutateE(ctx, identity, func(instance *WorkflowInstance) error {
		fn(instance)
		return nil
	})
}

func (s *workflowInstanceStore) mutateE(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance, fn func(*WorkflowInstance) error) error {
	identity = identity.Normalize()
	requestedKey := strings.TrimSpace(identity.Route.InstancePath)
	if err := identity.Validate(); err != nil {
		return &WorkflowInstanceLookupMiss{RequestedKey: requestedKey}
	}
	if s == nil || !s.enabled() || fn == nil {
		return nil
	}
	if s.engineMutations == nil {
		return fmt.Errorf("workflow instance mutation requires the selected workflow engine mutation owner")
	}
	route, runID := identity.Route, identity.RunID
	instance, ok, err := s.Load(ctx, identity)
	if err != nil {
		return err
	}
	if !ok {
		return &WorkflowInstanceLookupMiss{RequestedKey: requestedKey}
	}
	expectedState := strings.TrimSpace(instance.CurrentState)
	expectedRevision := instance.Revision
	if err := fn(&instance); err != nil {
		return err
	}
	record, err := workflowEngineStateRecord(runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: route}, instance, expectedState, expectedRevision, WorkflowEngineStateTransitionUpdateStateAndCompanion, time.Now().UTC())
	if err != nil {
		return err
	}
	_, err = s.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{State: record})
	return err
}
