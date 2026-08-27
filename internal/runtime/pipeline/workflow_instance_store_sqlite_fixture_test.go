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
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

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
