package mutationlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

func ErrInvalidMutationLogWriter(message string) error {
	return fmt.Errorf("mutation log completeness violation: %s", strings.TrimSpace(message))
}

type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Record struct {
	EntityID    string
	Field       string
	OldValue    any
	NewValue    any
	WriterType  string
	WriterID    string
	HandlerStep string
}

type Writer struct {
	Type        string
	ID          string
	HandlerStep string
}

type EntityStateProjection struct {
	CurrentState string
	Fields       map[string]any
	Gates        map[string]any
	Accumulator  map[string]any
}

type ProjectionMutation struct {
	Field    string
	NewValue any
}

func Insert(ctx context.Context, db DBTX, rec Record) error {
	if db == nil {
		return ErrInvalidMutationLogWriter("mutation log DB is required")
	}
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return ErrInvalidMutationLogWriter(err.Error())
	}
	tx, ok := db.(*sql.Tx)
	if !ok {
		return ErrInvalidMutationLogWriter("PostgreSQL mutation log writes require the existing persistence transaction")
	}
	entityID := strings.TrimSpace(rec.EntityID)
	field := strings.TrimSpace(rec.Field)
	writerType := strings.TrimSpace(rec.WriterType)
	writerID := strings.TrimSpace(rec.WriterID)
	if entityID == "" || field == "" || writerType == "" || writerID == "" {
		return ErrInvalidMutationLogWriter("entity_id, field, writer_type, and writer_id are required")
	}

	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return ErrInvalidMutationLogWriter(err.Error())
	}
	if err := storerunlifecycle.RequireActive(ctx, tx, runID, storerunlifecycle.DialectPostgres); err != nil {
		return err
	}
	if err := requireBundleSourceAvailable(ctx, tx); err != nil {
		return err
	}

	oldValue, err := jsonbArg(rec.OldValue)
	if err != nil {
		return err
	}
	newValue, err := jsonbArg(rec.NewValue)
	if err != nil {
		return err
	}

	causedByEvent := ""
	if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
		if parsed := validUUIDString(inbound.ID()); parsed != "" {
			causedByEvent = parsed
		}
	}

	mutationID := uuid.NewString()
	occurredAt := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			mutation_id, run_id, entity_id, field, old_value, new_value,
			caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4, $5::jsonb, $6::jsonb,
			NULLIF($7, '')::uuid, $8, $9, NULLIF($10, ''), $11
		)
	`, mutationID, runID, entityID, field, oldValue, newValue, causedByEvent, writerType, writerID, strings.TrimSpace(rec.HandlerStep), occurredAt)
	if err != nil {
		return err
	}
	if field != "current_state" {
		return nil
	}
	draft, admitted, err := AuthorActivityDraft(runID, mutationID, rec, occurredAt)
	if err != nil {
		return err
	}
	if !admitted {
		return nil
	}
	return runtimeauthoractivity.Record(ctx, draft)
}

func requireBundleSourceAvailable(ctx context.Context, db DBTX) error {
	fact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
	if !ok {
		return nil
	}
	bundleSource, err := storerunlifecycle.CanonicalBundleSource(fact.BundleSource)
	if err != nil {
		return err
	}
	bundleHash := strings.TrimSpace(fact.BundleHash)
	if bundleSource == storerunlifecycle.BundleSourceLegacy && bundleHash != "" {
		return fmt.Errorf("mutation log bundle source: legacy bundle_source cannot carry canonical bundle_hash")
	}
	if bundleSource != storerunlifecycle.BundleSourceLegacy && bundleHash == "" {
		return fmt.Errorf("mutation log bundle source: bundle_hash is required for bundle_source=%s", bundleSource)
	}
	if bundleSource != storerunlifecycle.BundleSourcePersisted {
		return nil
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = $1)`, bundleHash).Scan(&exists); err != nil {
		return fmt.Errorf("validate mutation log persisted bundle source: %w", err)
	}
	if !exists {
		return &storerunlifecycle.PersistedBundleUnavailableError{
			BundleHash:   bundleHash,
			BundleSource: bundleSource,
			Cause:        "persisted_missing_bundle_row",
		}
	}
	return nil
}

func AuthorActivityDraft(runID, mutationID string, rec Record, occurredAt time.Time) (runtimeauthoractivity.Draft, bool, error) {
	if strings.TrimSpace(rec.Field) != "current_state" {
		return runtimeauthoractivity.Draft{}, false, nil
	}
	runID = strings.TrimSpace(runID)
	mutationID = strings.TrimSpace(mutationID)
	entityID := strings.TrimSpace(rec.EntityID)
	writerType := strings.TrimSpace(rec.WriterType)
	writerID := strings.TrimSpace(rec.WriterID)
	if runID == "" || mutationID == "" || entityID == "" || writerType == "" || writerID == "" || occurredAt.IsZero() {
		return runtimeauthoractivity.Draft{}, false, ErrInvalidMutationLogWriter("author activity requires run_id, mutation_id, entity_id, writer, and occurred_at")
	}
	oldState := authorActivityStateString(rec.OldValue)
	newState := authorActivityStateString(rec.NewValue)
	transition := "stage_changed"
	if oldState == "" {
		transition = "created"
	}
	return runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindEntityLifecycle, Transition: transition,
		SourceOwner: "entity_mutations", SourceIdentity: mutationID, DedupKey: "entity-mutation:" + mutationID,
		OccurredAt: occurredAt.UTC(), RunID: runID, EntityID: entityID,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "entity", SubjectID: entityID, OldState: oldState, NewState: newState,
			WriterType: writerType, WriterID: writerID,
		},
	}, true, nil
}

func authorActivityStateString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case *string:
		if typed != nil {
			return strings.TrimSpace(*typed)
		}
	}
	return ""
}

func BuildEntityStateDiffRecords(entityID string, before, after EntityStateProjection, writer Writer) ([]Record, error) {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return nil, ErrInvalidMutationLogWriter("entity_id is required")
	}
	writerType := strings.TrimSpace(writer.Type)
	writerID := strings.TrimSpace(writer.ID)
	if writerType == "" || writerID == "" {
		return nil, ErrInvalidMutationLogWriter("writer_type and writer_id are required")
	}
	handlerStep := strings.TrimSpace(writer.HandlerStep)
	records := make([]Record, 0, 8)
	if strings.TrimSpace(before.CurrentState) != strings.TrimSpace(after.CurrentState) {
		records = append(records, Record{
			EntityID:    entityID,
			Field:       "current_state",
			OldValue:    stringOrNil(before.CurrentState),
			NewValue:    stringOrNil(after.CurrentState),
			WriterType:  writerType,
			WriterID:    writerID,
			HandlerStep: handlerStep,
		})
	}
	fieldRecords, err := diffMapRecords(entityID, "", before.Fields, after.Fields, writerType, writerID, handlerStep)
	if err != nil {
		return nil, err
	}
	records = append(records, fieldRecords...)
	gateRecords, err := diffMapRecords(entityID, "gates.", before.Gates, after.Gates, writerType, writerID, handlerStep)
	if err != nil {
		return nil, err
	}
	records = append(records, gateRecords...)
	accRecords, err := diffMapRecords(entityID, "accumulator.", before.Accumulator, after.Accumulator, writerType, writerID, handlerStep)
	if err != nil {
		return nil, err
	}
	records = append(records, accRecords...)
	return records, nil
}

func InsertEntityStateDiff(ctx context.Context, db DBTX, entityID string, before, after EntityStateProjection, writer Writer) error {
	records, err := BuildEntityStateDiffRecords(entityID, before, after, writer)
	if err != nil {
		return err
	}
	for _, rec := range records {
		if err := Insert(ctx, db, rec); err != nil {
			return err
		}
	}
	return nil
}

func ReconstructEntityStateProjection(records []ProjectionMutation) (EntityStateProjection, error) {
	state := EntityStateProjection{
		Fields:      map[string]any{},
		Gates:       map[string]any{},
		Accumulator: map[string]any{},
	}
	for _, rec := range records {
		if err := ApplyEntityStateProjectionMutation(&state, rec.Field, rec.NewValue); err != nil {
			return EntityStateProjection{}, err
		}
	}
	return state, nil
}

func ApplyEntityStateProjectionMutation(state *EntityStateProjection, field string, value any) error {
	if state == nil {
		return ErrInvalidMutationLogWriter("projection state is required")
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return ErrInvalidMutationLogWriter("mutation field is required")
	}
	switch {
	case field == "current_state":
		next, ok := value.(string)
		if !ok {
			return ErrInvalidMutationLogWriter("current_state mutation value must be a string")
		}
		state.CurrentState = strings.TrimSpace(next)
		return nil
	case strings.HasPrefix(field, "gates."):
		return applyProjectionMapValue(state.ensureGates(), strings.TrimPrefix(field, "gates."), value, "gates")
	case strings.HasPrefix(field, "accumulator."):
		return applyProjectionMapValue(state.ensureAccumulator(), strings.TrimPrefix(field, "accumulator."), value, "accumulator")
	default:
		return applyProjectionMapValue(state.ensureFields(), field, value, "fields")
	}
}

func diffMapRecords(entityID, prefix string, before, after map[string]any, writerType, writerID, handlerStep string) ([]Record, error) {
	keys := make([]string, 0, len(before)+len(after))
	seen := map[string]struct{}{}
	for key := range before {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range after {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	records := make([]Record, 0, len(keys))
	for _, key := range keys {
		oldValue, oldOK := before[key]
		newValue, newOK := after[key]
		if !oldOK {
			oldValue = nil
		}
		if !newOK {
			newValue = nil
		}
		same, err := jsonValuesEqual(oldValue, newValue)
		if err != nil {
			return nil, err
		}
		if same {
			continue
		}
		records = append(records, Record{
			EntityID:    entityID,
			Field:       strings.TrimSpace(prefix + key),
			OldValue:    oldValue,
			NewValue:    newValue,
			WriterType:  writerType,
			WriterID:    writerID,
			HandlerStep: handlerStep,
		})
	}
	return records, nil
}

func jsonValuesEqual(left, right any) (bool, error) {
	leftJSON, err := json.Marshal(left)
	if err != nil {
		return false, err
	}
	rightJSON, err := json.Marshal(right)
	if err != nil {
		return false, err
	}
	return string(leftJSON) == string(rightJSON), nil
}

func stringOrNil(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func (p *EntityStateProjection) ensureFields() map[string]any {
	if p.Fields == nil {
		p.Fields = map[string]any{}
	}
	return p.Fields
}

func (p *EntityStateProjection) ensureGates() map[string]any {
	if p.Gates == nil {
		p.Gates = map[string]any{}
	}
	return p.Gates
}

func (p *EntityStateProjection) ensureAccumulator() map[string]any {
	if p.Accumulator == nil {
		p.Accumulator = map[string]any{}
	}
	return p.Accumulator
}

func applyProjectionMapValue(target map[string]any, key string, value any, bucket string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		if bucket == "" {
			return ErrInvalidMutationLogWriter("mutation field is required")
		}
		return ErrInvalidMutationLogWriter(fmt.Sprintf("%s mutation key is required", strings.TrimSpace(bucket)))
	}
	if value == nil {
		deleteProjectionMapValue(target, strings.Split(key, "."))
		return nil
	}
	applyProjectionNestedMapValue(target, strings.Split(key, "."), value)
	return nil
}

func applyProjectionNestedMapValue(target map[string]any, path []string, value any) {
	if len(path) == 0 {
		return
	}
	segment := strings.TrimSpace(path[0])
	if segment == "" {
		return
	}
	if len(path) == 1 {
		target[segment] = value
		return
	}
	next, _ := target[segment].(map[string]any)
	if next == nil {
		next = map[string]any{}
		target[segment] = next
	}
	applyProjectionNestedMapValue(next, path[1:], value)
}

func deleteProjectionMapValue(target map[string]any, path []string) bool {
	if len(path) == 0 {
		return len(target) == 0
	}
	segment := strings.TrimSpace(path[0])
	if segment == "" {
		return len(target) == 0
	}
	if len(path) == 1 {
		delete(target, segment)
		return len(target) == 0
	}
	next, ok := target[segment].(map[string]any)
	if !ok || next == nil {
		return len(target) == 0
	}
	if empty := deleteProjectionMapValue(next, path[1:]); empty {
		delete(target, segment)
	}
	return len(target) == 0
}

func validUUIDString(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}

func jsonbArg(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if len(typed) == 0 {
			return nil, nil
		}
		return string(typed), nil
	case []byte:
		if len(typed) == 0 {
			return nil, nil
		}
		return string(typed), nil
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return string(raw), nil
	}
}
