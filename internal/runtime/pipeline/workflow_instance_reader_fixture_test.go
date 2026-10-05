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
	query := `SELECT entity_id FROM flow_instances WHERE instance_path = ? AND run_id = ?`
	if r.dialect == workflowStoreDialectPostgres {
		query = `SELECT entity_id::text FROM flow_instances WHERE instance_path = $1 AND run_id = $2::uuid`
	}
	var entityID string
	err := r.db.QueryRowContext(ctx, query, route.InstancePath, runID).Scan(&entityID)
	if err == sql.ErrNoRows {
		return WorkflowInstance{}, false, nil
	}
	if err != nil {
		return WorkflowInstance{}, false, err
	}
	paired, err := r.LoadWorkflowTargetPersistence(ctx, identity, runtimeidentity.NormalizeEntityID(entityID))
	if err != nil {
		return WorkflowInstance{}, false, err
	}
	instance, err := paired.DecodeComplete(route, runtimeidentity.NormalizeEntityID(entityID))
	return instance, err == nil, err
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
	var headerType, headerSlug, headerName sql.NullString
	var config, terminatedAt, createdAt, enteredAt, updatedAt, gates, bookkeeping, accumulator any
	headerQuery := `SELECT instance_path, flow_template, json_extract(config, '$.workflow_version'), mode, status, config, terminated_at, created_at,
		entity_id, entity_type, slug, name, current_state, revision, entered_state_at, updated_at, stage_defined, gates, bookkeeping, accumulator
		FROM flow_instances WHERE run_id = ? AND instance_path = ?`
	if r.dialect == workflowStoreDialectPostgres {
		headerQuery = `SELECT instance_path, flow_template, config->>'workflow_version', mode, status, config, terminated_at, created_at,
			entity_id::text, entity_type, slug, name, current_state, revision, entered_state_at, updated_at, stage_defined, gates, bookkeeping, accumulator
			FROM flow_instances WHERE run_id = $1::uuid AND instance_path = $2`
	}
	err = tx.QueryRowContext(ctx, headerQuery, runID, route.InstancePath).Scan(
		&companion.FlowInstance, &companion.WorkflowName, &workflowVersion, &companion.Mode,
		&companion.Status, &config, &terminatedAt, &createdAt,
		&companion.State.EntityID, &headerType, &headerSlug, &headerName,
		&companion.State.CurrentState, &companion.State.Revision, &enteredAt, &updatedAt, &companion.StageDefined,
		&gates, &bookkeeping, &accumulator,
	)
	companionExists := err == nil
	if err != nil && err != sql.ErrNoRows {
		return WorkflowTargetPersistenceRecord{}, err
	}
	if companionExists {
		companion.Config = pipelineTestJSONBytes(config)
		companion.State.FlowInstance = companion.FlowInstance
		companion.State.EntityType, companion.State.Slug, companion.State.Name = headerType.String, headerSlug.String, headerName.String
		companion.State.Gates = pipelineTestJSONBytes(gates)
		companion.State.Bookkeeping = pipelineTestJSONBytes(bookkeeping)
		companion.State.Accumulator = pipelineTestJSONBytes(accumulator)
		for _, item := range []struct {
			raw    any
			target *time.Time
		}{{createdAt, &companion.CreatedAt}, {enteredAt, &companion.State.EnteredStageAt}, {updatedAt, &companion.State.UpdatedAt}} {
			value, present, parseErr := sqliteWorkflowTimeValue(item.raw)
			if parseErr != nil || !present {
				if parseErr == nil {
					parseErr = fmt.Errorf("pipeline test constructed header timestamp is required")
				}
				return WorkflowTargetPersistenceRecord{}, parseErr
			}
			*item.target = value
		}
		companion.State.CreatedAt = companion.CreatedAt
		if terminated, present, parseErr := sqliteWorkflowTimeValue(terminatedAt); parseErr != nil {
			return WorkflowTargetPersistenceRecord{}, parseErr
		} else if present {
			companion.TerminatedAt = terminated
		}
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
	case companionExists && companion.State.EntityType == "":
		record.Presence = WorkflowTargetPersistenceCompleteFieldless
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

func (r pipelineTestWorkflowInstanceReader) QueryWorkflowEntityCollection(ctx context.Context, owner WorkflowEntityCollectionOwner) ([]WorkflowEntityStatePersistenceRecord, error) {
	runID := owner.RunID()
	activeStates := runtimerunlifecycle.ActiveStates()
	query := `SELECT es.entity_id, es.flow_instance, es.entity_type, es.slug, es.name, es.current_state, es.revision, es.entered_state_at, es.gates, es.fields, es.bookkeeping, es.accumulator, es.created_at, es.updated_at
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance
		WHERE es.run_id = ? AND es.entity_type = ?
		  AND EXISTS (SELECT 1 FROM runs run WHERE run.run_id = es.run_id AND run.status IN (?, ?))
		  AND (es.flow_instance = ? OR es.flow_instance LIKE ? OR (? AND es.flow_instance = ?))
		  AND (fi.instance_path IS NULL OR (LOWER(TRIM(fi.status)) = 'active' AND fi.terminated_at IS NULL))
		ORDER BY es.created_at ASC, es.entity_id ASC`
	args := []any{runID, owner.EntityType(), string(activeStates[0]), string(activeStates[1]), owner.ScopeKey(), owner.ScopeKey() + "/%", owner.ScopeKey() == runID, runID}
	if r.dialect == workflowStoreDialectPostgres {
		query = `SELECT es.entity_id::text, es.flow_instance, es.entity_type, es.slug, es.name, es.current_state, es.revision, es.entered_state_at, es.gates, es.fields, es.bookkeeping, es.accumulator, es.created_at, es.updated_at
			FROM entity_state es
			LEFT JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance
			WHERE es.run_id = $1::uuid AND es.entity_type = $2
			  AND EXISTS (SELECT 1 FROM runs run WHERE run.run_id = es.run_id AND run.status IN ($3, $4))
			  AND (es.flow_instance = $5 OR es.flow_instance LIKE $6 OR ($7::boolean AND es.flow_instance = $1::text))
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
	return FilterWorkflowEntityCollectionRecords(records, owner)
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
	query := pipelineTestWorkflowInstanceSelectSQLite + ` WHERE fi.run_id = ? ORDER BY fi.created_at ASC`
	if r.dialect == workflowStoreDialectPostgres {
		query = pipelineTestWorkflowInstanceSelectPostgres + ` WHERE fi.run_id = $1::uuid ORDER BY fi.created_at ASC`
	}
	rows, err := r.db.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPipelineTestWorkflowInstances(rows, r.dialect)
}

const pipelineTestWorkflowInstanceSelectPostgres = `
	SELECT fi.entity_id::text, fi.flow_template, fi.config->>'workflow_version', fi.mode, fi.status,
	       fi.terminated_at, fi.current_state, fi.revision, fi.entered_state_at,
	       fi.gates, es.fields, fi.bookkeeping, fi.accumulator, fi.config, fi.instance_path,
	       fi.entity_type, fi.slug, fi.name, fi.created_at, fi.updated_at, fi.stage_defined
	FROM flow_instances fi LEFT JOIN entity_state es
	  ON es.run_id = fi.run_id AND es.flow_instance = fi.instance_path AND es.entity_id = fi.entity_id
`

const pipelineTestWorkflowInstanceSelectSQLite = `
	SELECT fi.entity_id, fi.flow_template, json_extract(fi.config, '$.workflow_version'), fi.mode, fi.status,
	       fi.terminated_at, fi.current_state, fi.revision, fi.entered_state_at,
	       fi.gates, es.fields, fi.bookkeeping, fi.accumulator, fi.config, fi.instance_path,
	       fi.entity_type, fi.slug, fi.name, fi.created_at, fi.updated_at, fi.stage_defined
	FROM flow_instances fi LEFT JOIN entity_state es
	  ON es.run_id = fi.run_id AND es.flow_instance = fi.instance_path AND es.entity_id = fi.entity_id
`

func scanPipelineTestWorkflowInstances(rows *sql.Rows, dialect workflowStoreDialect) ([]WorkflowInstance, error) {
	items := make([]WorkflowInstance, 0, 16)
	for rows.Next() {
		var record WorkflowInstancePersistenceRecord
		var workflowVersion, entityType, slug, name sql.NullString
		var terminatedAt sql.NullTime
		var terminatedAtRaw, enteredAtRaw, createdAtRaw, updatedAtRaw any
		var gates, fields, bookkeeping, accumulator, config any
		var postgresFields []byte
		destinations := []any{
			&record.EntityID, &record.WorkflowName, &workflowVersion, &record.Mode, &record.Status,
			&terminatedAt, &record.CurrentState, &record.Revision, &record.EnteredStageAt,
			&record.Gates, &postgresFields, &record.Bookkeeping, &record.Accumulator, &record.Config,
			&record.FlowInstance, &entityType, &slug, &name, &record.CreatedAt, &record.UpdatedAt, &record.StageDefined,
		}
		if dialect != workflowStoreDialectPostgres {
			destinations = []any{
				&record.EntityID, &record.WorkflowName, &workflowVersion, &record.Mode, &record.Status,
				&terminatedAtRaw, &record.CurrentState, &record.Revision, &enteredAtRaw,
				&gates, &fields, &bookkeeping, &accumulator, &config,
				&record.FlowInstance, &entityType, &slug, &name, &createdAtRaw, &updatedAtRaw, &record.StageDefined,
			}
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}
		record.WorkflowVersion, record.Slug, record.Name = workflowVersion.String, slug.String, name.String
		record.EntityType = entityType.String
		if dialect == workflowStoreDialectPostgres {
			record.Fields = postgresFields
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
var _ WorkflowEntityCollectionPersistenceReader = pipelineTestWorkflowInstanceReader{}
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

func (r *recordingRuntimeMutationRunner) QueryWorkflowEntityCollection(ctx context.Context, owner WorkflowEntityCollectionOwner) ([]WorkflowEntityStatePersistenceRecord, error) {
	return pipelineTestWorkflowInstanceReader{db: r.db, dialect: r.dialect}.QueryWorkflowEntityCollection(ctx, owner)
}
func (r *recordingRuntimeMutationRunner) ListWorkflowInstances(ctx context.Context, runID string) ([]WorkflowInstance, error) {
	return pipelineTestWorkflowInstanceReader{db: r.db, dialect: r.dialect}.ListWorkflowInstances(ctx, runID)
}

var _ WorkflowInstancePersistenceReader = (*recordingRuntimeMutationRunner)(nil)
var _ WorkflowEntityStatePersistenceReader = (*recordingRuntimeMutationRunner)(nil)
var _ WorkflowEntityCollectionPersistenceReader = (*recordingRuntimeMutationRunner)(nil)
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
