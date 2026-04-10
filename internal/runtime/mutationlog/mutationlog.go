package mutationlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	runtimecorrelation "swarm/internal/runtime/correlation"
	storerunlifecycle "swarm/internal/store/runlifecycle"
)

var syntheticRunNamespace = uuid.MustParse("7e7e89e6-0d4f-4eeb-a8a0-99a3e8ec2ef1")

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
	entityID := strings.TrimSpace(rec.EntityID)
	field := strings.TrimSpace(rec.Field)
	writerType := strings.TrimSpace(rec.WriterType)
	writerID := strings.TrimSpace(rec.WriterID)
	if entityID == "" || field == "" || writerType == "" || writerID == "" {
		return ErrInvalidMutationLogWriter("entity_id, field, writer_type, and writer_id are required")
	}

	runID := normalizeRunID(runtimecorrelation.RunIDFromContext(ctx))
	if err := storerunlifecycle.EnsureActive(ctx, db, runID, "", "", storerunlifecycle.EnsureActiveOptions{}); err != nil {
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
		if parsed := validUUIDString(inbound.ID); parsed != "" {
			causedByEvent = parsed
		}
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value,
			caused_by_event, writer_type, writer_id, handler_step
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4::jsonb, $5::jsonb,
			NULLIF($6, '')::uuid, $7, $8, NULLIF($9, '')
		)
	`, runID, entityID, field, oldValue, newValue, causedByEvent, writerType, writerID, strings.TrimSpace(rec.HandlerStep))
	return err
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
		delete(target, key)
		return nil
	}
	target[key] = value
	return nil
}

func normalizeRunID(runID string) string {
	if parsed := validUUIDString(runID); parsed != "" {
		return parsed
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return uuid.NewString()
	}
	return uuid.NewSHA1(syntheticRunNamespace, []byte(runID)).String()
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
