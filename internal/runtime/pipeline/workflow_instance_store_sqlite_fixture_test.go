package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func (s *workflowInstanceStore) upsertSQLite(ctx context.Context, instance WorkflowInstance) error {
	instance, identity, ok, err := normalizeWorkflowInstanceForPersistence(instance)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return s.writeSQLite(ctx, identity.RowID(), identity.StorageRef, instance, false)
}

func (s *workflowInstanceStore) createSQLite(ctx context.Context, instance WorkflowInstance) error {
	instance, identity, ok, err := normalizeWorkflowInstanceForPersistence(instance)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return s.writeSQLite(ctx, identity.RowID(), identity.StorageRef, instance, true)
}

func (s *workflowInstanceStore) writeSQLite(ctx context.Context, rowID, storageRef string, instance WorkflowInstance, createOnly bool) error {
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return err
	}
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if _, err := s.requireActiveWorkflowRun(txctx, tx); err != nil {
			return err
		}
		if createOnly {
			exists, err := workflowInstanceSQLiteCreateTargetExists(txctx, tx, runID, rowID, storageRef)
			if err != nil {
				return err
			}
			if exists {
				return runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "flow_instance_already_exists", "workflow-instance-store", "create", map[string]any{"flow_instance": storageRef})
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
		bookkeepingJSON, err := json.Marshal(projection.Bookkeeping)
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
		mode := workflowInstanceMode(instance)
		now := time.Now().UTC()
		if createOnly {
			now = instance.CreatedAt.UTC()
			if now.IsZero() {
				return fmt.Errorf("workflow initial materialization requires exact creation time")
			}
			flowInstanceInsert := `
				INSERT INTO flow_instances (
					instance_id, flow_template, mode, config, status, created_at
				)
				VALUES (?, ?, ?, ?, 'active', ?)
			`
			if _, err := tx.ExecContext(txctx, flowInstanceInsert, storageRef, instance.WorkflowName, mode, jsonOrDefault(configJSON, "{}"), now); err != nil {
				return err
			}
			if _, err := tx.ExecContext(txctx, `
				INSERT INTO entity_state (
					run_id, entity_id, flow_instance, entity_type, slug, name,
					current_state, gates, fields, bookkeeping, accumulator, revision,
					entered_state_at, created_at, updated_at
				)
				VALUES (?, ?, ?, ?, NULLIF(?,''), NULLIF(?,''), ?, ?, ?, ?, ?, 1, ?, ?, ?)
			`, runID, rowID, storageRef, projection.Control.EntityType, projection.Control.Slug, projection.Control.Name,
				instance.CurrentState, jsonOrDefault(gatesJSON, "{}"), jsonOrDefault(fieldsJSON, "{}"), jsonOrDefault(bookkeepingJSON, "{}"), jsonOrDefault(accumulatorState, "{}"),
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
					config = excluded.config,
					status = CASE WHEN flow_instances.status = 'terminated' THEN flow_instances.status ELSE 'active' END
			`, storageRef, instance.WorkflowName, mode, jsonOrDefault(configJSON, "{}"), instance.CreatedAt.UTC()); err != nil {
				return err
			}
			if _, err := tx.ExecContext(txctx, `
				INSERT INTO entity_state (
					run_id, entity_id, flow_instance, entity_type, slug, name,
					current_state, gates, fields, bookkeeping, accumulator, revision,
					entered_state_at, created_at, updated_at
				)
				VALUES (?, ?, ?, ?, NULLIF(?,''), NULLIF(?,''), ?, ?, ?, ?, ?, 1, ?, ?, ?)
				ON CONFLICT(run_id, entity_id) DO UPDATE SET
					flow_instance = excluded.flow_instance,
					entity_type = excluded.entity_type,
					slug = excluded.slug,
					name = excluded.name,
					current_state = excluded.current_state,
					gates = excluded.gates,
					fields = excluded.fields,
					bookkeeping = excluded.bookkeeping,
					accumulator = excluded.accumulator,
					revision = entity_state.revision + 1,
					entered_state_at = excluded.entered_state_at,
					updated_at = excluded.updated_at
			`, runID, rowID, storageRef, projection.Control.EntityType, projection.Control.Slug, projection.Control.Name,
				instance.CurrentState, jsonOrDefault(gatesJSON, "{}"), jsonOrDefault(fieldsJSON, "{}"), jsonOrDefault(bookkeepingJSON, "{}"), jsonOrDefault(accumulatorState, "{}"),
				instance.EnteredStageAt.UTC(), instance.CreatedAt.UTC(), now); err != nil {
				return err
			}
		}
		afterProjection := runtimemutationlog.EntityStateProjection{
			CurrentState: strings.TrimSpace(instance.CurrentState),
			Fields:       projection.Fields,
			Bookkeeping:  projection.Bookkeeping,
			Gates:        projection.GatesAny(),
			Accumulator:  projection.Accumulator,
		}
		previousForDiff := previous
		if len(instance.InitialFieldValues) > 0 {
			nextPrevious, err := insertSQLiteWorkflowCreateEntityInitialValueMutations(txctx, tx, s.runLifecycle, rowID, previous, afterProjection, instance.InitialFieldValues)
			if err != nil {
				return err
			}
			previousForDiff = nextPrevious
		}
		if err := insertSQLiteEntityStateDiff(txctx, tx, s.runLifecycle, rowID, previousForDiff, afterProjection, runtimemutationlog.Writer{
			Type:        "platform",
			ID:          "workflow_instance_store",
			HandlerStep: map[bool]string{true: "create", false: "upsert"}[createOnly],
		}); err != nil {
			return err
		}
		if !createOnly {
			return s.requestRunCompletionCandidate(txctx, runID)
		}
		return nil
	})
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

func (s *workflowInstanceStore) loadTrackedEntityStateProjectionSQLite(ctx context.Context, tx *sql.Tx, runID, entityID string) (runtimemutationlog.EntityStateProjection, error) {
	if tx == nil || strings.TrimSpace(entityID) == "" {
		return runtimemutationlog.EntityStateProjection{}, nil
	}
	var currentState sql.NullString
	var fieldsRaw, bookkeepingRaw, gatesRaw, accRaw any
	err := tx.QueryRowContext(ctx, `
		SELECT current_state, COALESCE(fields, '{}'), COALESCE(bookkeeping, '{}'), COALESCE(gates, '{}'), COALESCE(accumulator, '{}')
		FROM entity_state
		WHERE run_id = ? AND entity_id = ?
	`, runID, entityID).Scan(&currentState, &fieldsRaw, &bookkeepingRaw, &gatesRaw, &accRaw)
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
	bookkeeping, err := decodeWorkflowInstanceJSONMap("entity_state.bookkeeping", sqliteWorkflowJSONBytes(bookkeepingRaw))
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
		Bookkeeping:  bookkeeping,
		Gates:        workflowBoolGatesAsMap(gates),
		Accumulator:  accumulator,
	}, nil
}

func insertSQLiteEntityStateDiff(ctx context.Context, tx *sql.Tx, runLifecycle runtimerunlifecycle.OperationOwner, entityID string, before, after runtimemutationlog.EntityStateProjection, writer runtimemutationlog.Writer) error {
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
	if runLifecycle == nil {
		return errors.New("SQLite entity mutation run lifecycle owner is required")
	}
	if err := runLifecycle.RequireActiveRun(ctx, runID); err != nil {
		return err
	}
	for _, rec := range records {
		if err := insertSQLiteEntityMutationRecord(ctx, tx, runID, rec); err != nil {
			return err
		}
	}
	return nil
}

func insertSQLiteWorkflowCreateEntityInitialValueMutations(
	ctx context.Context,
	tx *sql.Tx,
	runLifecycle runtimerunlifecycle.OperationOwner,
	entityID string,
	before, after runtimemutationlog.EntityStateProjection,
	initialValues map[string]any,
) (runtimemutationlog.EntityStateProjection, error) {
	if len(initialValues) == 0 {
		return before, nil
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	if runLifecycle == nil {
		return runtimemutationlog.EntityStateProjection{}, errors.New("SQLite initial-value run lifecycle owner is required")
	}
	if err := runLifecycle.RequireActiveRun(ctx, runID); err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	adjusted := runtimemutationlog.EntityStateProjection{
		CurrentState: before.CurrentState,
		Fields:       cloneStringAnyMap(before.Fields),
		Bookkeeping:  cloneStringAnyMap(before.Bookkeeping),
		Gates:        cloneStringAnyMap(before.Gates),
		Accumulator:  cloneStringAnyMap(before.Accumulator),
	}
	if adjusted.Fields == nil {
		adjusted.Fields = map[string]any{}
	}
	for field, declared := range initialValues {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		finalValue, ok := after.Fields[field]
		oldValue, hadOld := adjusted.Fields[field]
		if hadOld {
			continue
		}
		if err := insertSQLiteEntityMutationRecord(ctx, tx, runID, runtimemutationlog.Record{
			EntityID:    entityID,
			Domain:      runtimemutationlog.DomainAuthoredField,
			Path:        field,
			OldValue:    oldValueOrNil(oldValue, hadOld),
			NewValue:    declared,
			WriterType:  "platform",
			WriterID:    "entity_initial_value",
			HandlerStep: "create_entity",
		}); err != nil {
			return runtimemutationlog.EntityStateProjection{}, err
		}
		if ok && workflowJSONValuesEqual(finalValue, declared) {
			adjusted.Fields[field] = declared
			continue
		}
		adjusted.Fields[field] = declared
	}
	return adjusted, nil
}

func insertSQLiteEntityMutationRecord(ctx context.Context, tx *sql.Tx, runID string, rec runtimemutationlog.Record) error {
	if tx == nil {
		return runtimemutationlog.ErrInvalidMutationLogWriter("mutation log DB is required")
	}
	entityID := strings.TrimSpace(rec.EntityID)
	domain := rec.Domain
	path := strings.TrimSpace(rec.Path)
	writerType := strings.TrimSpace(rec.WriterType)
	writerID := strings.TrimSpace(rec.WriterID)
	if entityID == "" || writerType == "" || writerID == "" {
		return runtimemutationlog.ErrInvalidMutationLogWriter("entity_id, writer_type, and writer_id are required")
	}
	if err := runtimemutationlog.ValidateDomainPath(domain, path); err != nil {
		return err
	}
	if domain == runtimemutationlog.DomainLifecycleState {
		if err := authoractivityfixture.Require(ctx); err != nil {
			return runtimemutationlog.ErrInvalidMutationLogWriter(err.Error())
		}
	}
	oldValue, err := json.Marshal(rec.OldValue)
	if err != nil {
		return err
	}
	newValue, err := json.Marshal(rec.NewValue)
	if err != nil {
		return err
	}
	causedByEvent := ""
	if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
		if parsed := validSQLiteWorkflowUUID(inbound.ID()); parsed != "" {
			causedByEvent = parsed
		}
	}
	mutationID := uuid.NewString()
	occurredAt := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
			INSERT INTO entity_mutations (
				mutation_id, run_id, entity_id, domain, path, old_value, new_value,
				caused_by_event, writer_type, writer_id, handler_step, created_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?)
	`, mutationID, runID, entityID, domain, path, string(oldValue), string(newValue), causedByEvent, writerType, writerID, strings.TrimSpace(rec.HandlerStep), occurredAt); err != nil {
		return err
	}
	if domain != runtimemutationlog.DomainLifecycleState {
		return nil
	}
	draft, admitted, err := runtimemutationlog.AuthorActivityDraft(ctx, runID, mutationID, rec, occurredAt)
	if err != nil {
		return err
	}
	if !admitted {
		return nil
	}
	return authoractivityfixture.Record(ctx, draft)
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
