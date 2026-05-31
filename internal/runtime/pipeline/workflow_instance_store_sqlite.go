package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimecurrentstate "swarm/internal/runtime/currentstate"
	"swarm/internal/runtime/entityruntime"
	runtimemutationlog "swarm/internal/runtime/mutationlog"
	"swarm/internal/runtime/semanticview"
)

func (s *WorkflowInstanceStore) loadSQLite(ctx context.Context, instanceID string) (WorkflowInstance, bool, error) {
	keys := workflowInstanceLookupKeys(instanceID)
	if len(keys) == 0 {
		return WorkflowInstance{}, false, nil
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return WorkflowInstance{}, false, err
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(keys)), ",")
	args := make([]any, 0, len(keys)+1)
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, runID)
	rows, err := dbQueryContext(ctx, s.db, `
		SELECT
			es.entity_id,
			COALESCE(fi.flow_template, ''),
			COALESCE(json_extract(fi.config, '$.workflow_version'), ''),
			COALESCE(fi.status, ''),
			fi.terminated_at,
			es.current_state,
			es.entered_state_at,
			COALESCE(es.gates, '{}'),
			COALESCE(es.fields, '{}'),
			COALESCE(es.accumulator, '{}'),
			COALESCE(fi.config, '{}'),
			COALESCE(es.flow_instance, ''),
			COALESCE(es.entity_type, ''),
			es.slug,
			es.name,
			es.created_at,
			es.updated_at
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.entity_id IN (`+placeholders+`)
		  AND es.run_id = ?
		ORDER BY es.created_at DESC, es.entity_id DESC
		LIMIT 1
	`, args...)
	if err != nil {
		return WorkflowInstance{}, false, err
	}
	defer rows.Close()
	items, err := s.scanSQLiteWorkflowInstances(ctx, rows, runID)
	if err != nil {
		return WorkflowInstance{}, false, err
	}
	if len(items) == 0 {
		return WorkflowInstance{}, false, nil
	}
	return items[0], true, nil
}

func (s *WorkflowInstanceStore) listSQLite(ctx context.Context) ([]WorkflowInstance, error) {
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := dbQueryContext(ctx, s.db, `
		SELECT
			es.entity_id,
			COALESCE(fi.flow_template, ''),
			COALESCE(json_extract(fi.config, '$.workflow_version'), ''),
			COALESCE(fi.status, ''),
			fi.terminated_at,
			es.current_state,
			es.entered_state_at,
			COALESCE(es.gates, '{}'),
			COALESCE(es.fields, '{}'),
			COALESCE(es.accumulator, '{}'),
			COALESCE(fi.config, '{}'),
			COALESCE(es.flow_instance, ''),
			COALESCE(es.entity_type, ''),
			es.slug,
			es.name,
			es.created_at,
			es.updated_at
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = ?
		ORDER BY es.created_at ASC
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return s.scanSQLiteWorkflowInstances(ctx, rows, runID)
}

func (s *WorkflowInstanceStore) selectActiveByFieldsSQLite(ctx context.Context, scopeKey string, selectors []WorkflowInstanceFieldSelector, excludedStates []string) ([]WorkflowInstance, error) {
	scopeKey = strings.Trim(strings.TrimSpace(scopeKey), "/")
	if scopeKey == "" {
		return nil, nil
	}
	selectors = normalizeWorkflowInstanceFieldSelectors(selectors)
	if len(selectors) == 0 {
		return nil, nil
	}
	items, err := s.listSQLite(ctx)
	if err != nil {
		return nil, err
	}
	terminalStates := map[string]struct{}{}
	for _, state := range normalizeWorkflowInstanceExcludedStates(excludedStates) {
		terminalStates[state] = struct{}{}
	}
	out := make([]WorkflowInstance, 0, len(items))
	for _, item := range items {
		storageRef := strings.Trim(strings.TrimSpace(item.StorageRef), "/")
		if storageRef != scopeKey && !strings.HasPrefix(storageRef, scopeKey+"/") {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(item.Status), "terminated") || !item.TerminatedAt.IsZero() {
			continue
		}
		if _, terminal := terminalStates[strings.ToLower(strings.TrimSpace(item.CurrentState))]; terminal {
			continue
		}
		matches := true
		for _, selector := range selectors {
			value, ok := workflowMetadataValue(item.Metadata, selector.Field)
			if !ok || !workflowJSONValuesEqual(value, selector.Value) {
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

func (s *WorkflowInstanceStore) upsertSQLite(ctx context.Context, instance WorkflowInstance) error {
	instance, identity, ok, err := normalizeWorkflowInstanceForPersistence(instance)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return s.writeSQLite(ctx, identity.RowID(), identity.StorageRef, instance, false)
}

func (s *WorkflowInstanceStore) createSQLite(ctx context.Context, instance WorkflowInstance) error {
	instance, identity, ok, err := normalizeWorkflowInstanceForPersistence(instance)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return s.writeSQLite(ctx, identity.RowID(), identity.StorageRef, instance, true)
}

func (s *WorkflowInstanceStore) mutateSQLite(ctx context.Context, instanceID string, fn func(*WorkflowInstance)) error {
	return s.RunInPipelineTransaction(ctx, func(txctx context.Context, _ *sql.Tx) error {
		instance, ok, err := s.loadSQLite(txctx, instanceID)
		if err != nil {
			return err
		}
		if !ok {
			instance = WorkflowInstance{InstanceID: strings.TrimSpace(instanceID)}
		}
		fn(&instance)
		return s.Upsert(txctx, instance)
	})
}

func (s *WorkflowInstanceStore) markTerminatedSQLite(ctx context.Context, storageRef string, terminatedAt time.Time) error {
	storageRef = strings.TrimSpace(storageRef)
	if storageRef == "" {
		return fmt.Errorf("workflow instance storage_ref is required")
	}
	if terminatedAt.IsZero() {
		terminatedAt = time.Now().UTC()
	}
	res, err := dbExecContext(ctx, s.db, `
		UPDATE flow_instances
		SET status = 'terminated',
		    terminated_at = COALESCE(terminated_at, ?)
		WHERE instance_id = ?
	`, terminatedAt.UTC(), storageRef)
	if err != nil {
		return err
	}
	if rows, err := res.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("flow instance not found: %s", storageRef)
	}
	return nil
}

func (s *WorkflowInstanceStore) writeSQLite(ctx context.Context, rowID, storageRef string, instance WorkflowInstance, createOnly bool) error {
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return err
	}
	return s.RunInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if createOnly {
			exists, err := workflowInstanceSQLiteCreateTargetExists(txctx, tx, runID, rowID, storageRef)
			if err != nil {
				return err
			}
			if exists {
				return fmt.Errorf("flow instance already exists: %s", storageRef)
			}
		}
		previous, err := s.loadTrackedEntityStateProjectionSQLite(txctx, tx, runID, rowID)
		if err != nil {
			return err
		}
		projection, err := workflowInstancePersistedProjectionFromInstance(instance, storageRef)
		if err != nil {
			return err
		}
		fieldsJSON, err := json.Marshal(projection.Fields)
		if err != nil {
			return err
		}
		gatesJSON, err := json.Marshal(projection.GatesAny())
		if err != nil {
			return err
		}
		config := projection.ConfigPayload(instance.WorkflowVersion)
		configJSON, err := json.Marshal(config)
		if err != nil {
			return err
		}
		accumulatorState, err := json.Marshal(projection.Accumulator)
		if err != nil {
			return err
		}
		mode := workflowInstanceMode(storageRef)
		now := time.Now().UTC()
		if _, err := tx.ExecContext(txctx, `
			INSERT OR IGNORE INTO runs (run_id, status, started_at)
			VALUES (?, 'running', ?)
		`, runID, now); err != nil {
			return err
		}
		if createOnly {
			if _, err := tx.ExecContext(txctx, `
				INSERT INTO flow_instances (
					instance_id, flow_template, mode, config, status, created_at
				)
				VALUES (?, ?, ?, ?, 'active', ?)
			`, storageRef, instance.WorkflowName, mode, jsonOrDefault(configJSON, "{}"), now); err != nil {
				return err
			}
			if _, err := tx.ExecContext(txctx, `
				INSERT INTO entity_state (
					run_id, entity_id, flow_instance, entity_type, slug, name,
					current_state, gates, fields, accumulator, revision,
					entered_state_at, created_at, updated_at
				)
				VALUES (?, ?, ?, ?, NULLIF(?,''), NULLIF(?,''), ?, ?, ?, ?, 1, ?, ?, ?)
			`, runID, rowID, storageRef, projection.Control.EntityType, projection.Control.Slug, projection.Control.Name,
				instance.CurrentState, jsonOrDefault(gatesJSON, "{}"), jsonOrDefault(fieldsJSON, "{}"), jsonOrDefault(accumulatorState, "{}"),
				instance.EnteredStageAt.UTC(), now, now); err != nil {
				return err
			}
		} else {
			if _, err := tx.ExecContext(txctx, `
				INSERT INTO flow_instances (
					instance_id, flow_template, mode, config, status, created_at
				)
				VALUES (?, ?, ?, ?, 'active', ?)
				ON CONFLICT(instance_id) DO UPDATE SET
					flow_template = excluded.flow_template,
					mode = excluded.mode,
					config = excluded.config,
					status = CASE WHEN flow_instances.status = 'terminated' THEN flow_instances.status ELSE 'active' END
			`, storageRef, instance.WorkflowName, mode, jsonOrDefault(configJSON, "{}"), now); err != nil {
				return err
			}
			if _, err := tx.ExecContext(txctx, `
				INSERT INTO entity_state (
					run_id, entity_id, flow_instance, entity_type, slug, name,
					current_state, gates, fields, accumulator, revision,
					entered_state_at, created_at, updated_at
				)
				VALUES (?, ?, ?, ?, NULLIF(?,''), NULLIF(?,''), ?, ?, ?, ?, 1, ?, ?, ?)
				ON CONFLICT(run_id, entity_id) DO UPDATE SET
					flow_instance = excluded.flow_instance,
					entity_type = excluded.entity_type,
					slug = excluded.slug,
					name = excluded.name,
					current_state = excluded.current_state,
					gates = excluded.gates,
					fields = excluded.fields,
					accumulator = excluded.accumulator,
					revision = entity_state.revision + 1,
					entered_state_at = excluded.entered_state_at,
					updated_at = excluded.updated_at
			`, runID, rowID, storageRef, projection.Control.EntityType, projection.Control.Slug, projection.Control.Name,
				instance.CurrentState, jsonOrDefault(gatesJSON, "{}"), jsonOrDefault(fieldsJSON, "{}"), jsonOrDefault(accumulatorState, "{}"),
				instance.EnteredStageAt.UTC(), now, now); err != nil {
				return err
			}
		}
		afterProjection := runtimemutationlog.EntityStateProjection{
			CurrentState: strings.TrimSpace(instance.CurrentState),
			Fields:       projection.Fields,
			Gates:        projection.GatesAny(),
			Accumulator:  projection.Accumulator,
		}
		if err := insertSQLiteEntityStateDiff(txctx, tx, rowID, previous, afterProjection, runtimemutationlog.Writer{
			Type:        "platform",
			ID:          "workflow_instance_store",
			HandlerStep: map[bool]string{true: "create", false: "upsert"}[createOnly],
		}); err != nil {
			return err
		}
		if err := s.replaceWorkflowTimersSQLite(txctx, tx, runID, rowID, storageRef, instance.TimerState); err != nil {
			return err
		}
		return nil
	})
}

func (s *WorkflowInstanceStore) scanSQLiteWorkflowInstances(ctx context.Context, rows *sql.Rows, runID string) ([]WorkflowInstance, error) {
	out := make([]WorkflowInstance, 0, 32)
	for rows.Next() {
		var (
			item            WorkflowInstance
			gatesRaw        any
			fieldsRaw       any
			configRaw       any
			accRaw          any
			flowInstance    string
			entityType      string
			slug            sql.NullString
			name            sql.NullString
			status          sql.NullString
			terminatedAtRaw any
			enteredAtRaw    any
			createdAtRaw    any
			updatedAtRaw    any
		)
		if err := rows.Scan(
			&item.InstanceID,
			&item.WorkflowName,
			&item.WorkflowVersion,
			&status,
			&terminatedAtRaw,
			&item.CurrentState,
			&enteredAtRaw,
			&gatesRaw,
			&fieldsRaw,
			&accRaw,
			&configRaw,
			&flowInstance,
			&entityType,
			&slug,
			&name,
			&createdAtRaw,
			&updatedAtRaw,
		); err != nil {
			return nil, err
		}
		var err error
		item.EnteredStageAt, _, err = sqliteWorkflowTimeValue(enteredAtRaw)
		if err != nil {
			return nil, err
		}
		item.CreatedAt, _, err = sqliteWorkflowTimeValue(createdAtRaw)
		if err != nil {
			return nil, err
		}
		item.UpdatedAt, _, err = sqliteWorkflowTimeValue(updatedAtRaw)
		if err != nil {
			return nil, err
		}
		if terminatedAt, ok, err := sqliteWorkflowTimeValue(terminatedAtRaw); err != nil {
			return nil, err
		} else if ok {
			item.TerminatedAt = terminatedAt
		}
		item.Status = strings.TrimSpace(status.String)
		projection, err := decodeWorkflowInstancePersistedProjection(sqliteWorkflowJSONBytes(fieldsRaw), sqliteWorkflowJSONBytes(gatesRaw), sqliteWorkflowJSONBytes(accRaw), sqliteWorkflowJSONBytes(configRaw), workflowInstancePersistedControl{
			StorageRef: strings.TrimSpace(flowInstance),
			Slug:       slug.String,
			Name:       name.String,
			EntityType: entityType,
		})
		if err != nil {
			return nil, err
		}
		item.StateBuckets = projection.Accumulator
		item.Config = projection.Config
		item.Metadata = projection.Metadata()
		persistedIdentity, err := workflowInstancePersistedIdentity(nil, WorkflowInstance{
			InstanceID:   item.InstanceID,
			StorageRef:   projection.Control.StorageRef,
			WorkflowName: item.WorkflowName,
			Metadata:     item.Metadata,
		})
		if err != nil {
			return nil, err
		}
		item.StorageRef = persistedIdentity.StorageRef
		item.InstanceID = persistedIdentity.InstanceID
		item.TransitionHistory = append([]WorkflowTransitionRecord{}, projection.Control.TransitionHistory...)
		timers, err := s.loadWorkflowTimersSQLite(ctx, runID, persistedIdentity.RowID())
		if err != nil {
			return nil, err
		}
		item.TimerState = timers
		if item.StateBuckets == nil {
			item.StateBuckets = map[string]any{}
		}
		if item.Metadata == nil {
			item.Metadata = map[string]any{}
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func workflowInstanceSQLiteCreateTargetExists(ctx context.Context, tx *sql.Tx, runID, rowID, storageRef string) (bool, error) {
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM flow_instances WHERE instance_id = ?)`, storageRef).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM entity_state WHERE run_id = ? AND entity_id = ?
		)
	`, runID, rowID).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func (s *WorkflowInstanceStore) loadTrackedEntityStateProjectionSQLite(ctx context.Context, tx *sql.Tx, runID, entityID string) (runtimemutationlog.EntityStateProjection, error) {
	if tx == nil || strings.TrimSpace(entityID) == "" {
		return runtimemutationlog.EntityStateProjection{}, nil
	}
	var currentState sql.NullString
	var fieldsRaw, gatesRaw, accRaw any
	err := tx.QueryRowContext(ctx, `
		SELECT current_state, COALESCE(fields, '{}'), COALESCE(gates, '{}'), COALESCE(accumulator, '{}')
		FROM entity_state
		WHERE run_id = ? AND entity_id = ?
	`, runID, entityID).Scan(&currentState, &fieldsRaw, &gatesRaw, &accRaw)
	if err == sql.ErrNoRows {
		return runtimemutationlog.EntityStateProjection{}, nil
	}
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	fields, err := decodeWorkflowInstanceJSONMap("entity_state.fields", sqliteWorkflowJSONBytes(fieldsRaw))
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	gates, err := decodeWorkflowInstanceJSONBoolMap("entity_state.gates", sqliteWorkflowJSONBytes(gatesRaw))
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	accumulator, err := decodeWorkflowInstanceJSONMap("entity_state.accumulator", sqliteWorkflowJSONBytes(accRaw))
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	return runtimemutationlog.EntityStateProjection{
		CurrentState: strings.TrimSpace(currentState.String),
		Fields:       fields,
		Gates:        workflowBoolGatesAsMap(gates),
		Accumulator:  accumulator,
	}, nil
}

func insertSQLiteEntityStateDiff(ctx context.Context, tx *sql.Tx, entityID string, before, after runtimemutationlog.EntityStateProjection, writer runtimemutationlog.Writer) error {
	records, err := runtimemutationlog.BuildEntityStateDiffRecords(entityID, before, after, writer)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, runID, time.Now().UTC()); err != nil {
		return err
	}
	causedByEvent := ""
	if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
		if parsed := validSQLiteWorkflowUUID(inbound.ID); parsed != "" {
			causedByEvent = parsed
		}
	}
	for _, rec := range records {
		oldValue, err := json.Marshal(rec.OldValue)
		if err != nil {
			return err
		}
		newValue, err := json.Marshal(rec.NewValue)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO entity_mutations (
				mutation_id, run_id, entity_id, field, old_value, new_value,
				caused_by_event, writer_type, writer_id, handler_step, created_at
			)
			VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?)
		`, uuid.NewString(), runID, entityID, rec.Field, string(oldValue), string(newValue), causedByEvent, rec.WriterType, rec.WriterID, strings.TrimSpace(rec.HandlerStep), time.Now().UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (s *WorkflowInstanceStore) replaceWorkflowTimersSQLite(ctx context.Context, tx *sql.Tx, runID, entityID, storageRef string, timers []WorkflowTimerState) error {
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM timers
		WHERE run_id = ? AND entity_id = ? AND flow_instance = ? AND owner_node = ? AND owner_agent IS NULL
	`, runID, entityID, storageRef, workflowInstanceTimerOwnerNode); err != nil {
		return err
	}
	for _, timer := range timers {
		payloadJSON, err := json.Marshal(map[string]any{
			"started_by": strings.TrimSpace(timer.StartedBy),
			"timer_id":   strings.TrimSpace(timer.TimerID),
		})
		if err != nil {
			return err
		}
		status := "active"
		if timer.Cancelled {
			status = "cancelled"
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
				fire_at, recurring, owner_node, task_type, status, created_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, workflowInstanceTimerRowID(runID, strings.TrimSpace(timer.TimerID), entityID), runID, strings.TrimSpace(timer.TimerID), entityID, storageRef,
			strings.TrimSpace(timer.EventType), jsonOrDefault(payloadJSON, "{}"), timer.FiresAt.UTC(), timer.Recurring,
			workflowInstanceTimerOwnerNode, workflowInstanceTimerTaskType(timer), status, workflowTimeOrNow(timer.CreatedAt)); err != nil {
			return err
		}
	}
	return nil
}

func (s *WorkflowInstanceStore) loadWorkflowTimersSQLite(ctx context.Context, runID, entityID string) ([]WorkflowTimerState, error) {
	rows, err := dbQueryContext(ctx, s.db, `
		SELECT timer_name, fire_event, created_at, fire_at, COALESCE(json_extract(fire_payload, '$.started_by'), ''), recurring, status = 'cancelled'
		FROM timers
		WHERE run_id = ? AND entity_id = ? AND owner_node = ? AND owner_agent IS NULL
		ORDER BY created_at ASC, timer_name ASC
	`, runID, entityID, workflowInstanceTimerOwnerNode)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]WorkflowTimerState, 0, 4)
	for rows.Next() {
		var timer WorkflowTimerState
		var createdRaw, firesRaw any
		if err := rows.Scan(&timer.TimerID, &timer.EventType, &createdRaw, &firesRaw, &timer.StartedBy, &timer.Recurring, &timer.Cancelled); err != nil {
			return nil, err
		}
		if at, ok, err := sqliteWorkflowTimeValue(createdRaw); err != nil {
			return nil, err
		} else if ok {
			timer.CreatedAt = at
		}
		if at, ok, err := sqliteWorkflowTimeValue(firesRaw); err != nil {
			return nil, err
		} else if ok {
			timer.FiresAt = at
		}
		out = append(out, timer)
	}
	return out, rows.Err()
}

func (s *WorkflowInstanceStore) queryEntityStateCountSQLite(ctx context.Context, runID string, source semanticview.Source, contract entityruntime.Contract, predicate workflowEntityQueryPredicate) (int, error) {
	runID, err := runtimecurrentstate.ValidateRunID(runID)
	if err != nil {
		return 0, err
	}
	items, err := s.listSQLite(runtimecorrelation.WithRunID(ctx, runID))
	if err != nil {
		return 0, err
	}
	flowRoot := runtimeflowidentity.ScopeKey(source, contract.FlowID)
	count := 0
	for _, item := range items {
		storageRef := strings.Trim(strings.TrimSpace(item.StorageRef), "/")
		if flowRoot != "" && storageRef != flowRoot && !strings.HasPrefix(storageRef, flowRoot+"/") {
			continue
		}
		materialized, err := entityruntime.Materialize(contract, entityruntime.DeclaredValues(contract, item.Metadata))
		if err != nil {
			return 0, err
		}
		if workflowQueryPredicateMatches(map[string]any{
			"fields":         materialized,
			"current_state":  strings.TrimSpace(item.CurrentState),
			"entity_type":    contract.EntityType,
			"flow_instance":  flowRoot,
			"workflow_name":  contract.FlowID,
			"workflow_state": strings.TrimSpace(item.CurrentState),
		}, predicate) {
			count++
		}
	}
	return count, nil
}

func sqliteWorkflowJSONBytes(raw any) []byte {
	switch typed := raw.(type) {
	case nil:
		return nil
	case []byte:
		return typed
	case string:
		return []byte(typed)
	default:
		encoded, _ := json.Marshal(typed)
		return encoded
	}
}

func sqliteWorkflowTimeValue(raw any) (time.Time, bool, error) {
	switch typed := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		if typed.IsZero() {
			return time.Time{}, false, nil
		}
		return typed.UTC(), true, nil
	case string:
		return parseSQLiteWorkflowTime(typed)
	case []byte:
		return parseSQLiteWorkflowTime(string(typed))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported sqlite time value %T", raw)
	}
}

func parseSQLiteWorkflowTime(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05.999999 -0700 MST", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("parse sqlite workflow time %q", raw)
}

func validSQLiteWorkflowUUID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}
