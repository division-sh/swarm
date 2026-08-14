package mutationlog

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
)

func ErrInvalidMutationLogWriter(message string) error {
	return fmt.Errorf("mutation log completeness violation: %s", strings.TrimSpace(message))
}

type Record struct {
	EntityID    string
	Domain      Domain
	Path        string
	OldValue    any
	NewValue    any
	WriterType  string
	WriterID    string
	HandlerStep string
}

type Domain string

const (
	DomainLifecycleState Domain = "lifecycle_state"
	DomainAuthoredField  Domain = "authored_field"
	DomainBookkeeping    Domain = "bookkeeping"
	DomainGate           Domain = "gate"
	DomainAccumulator    Domain = "accumulator"
)

func (d Domain) Valid() bool {
	switch d {
	case DomainLifecycleState, DomainAuthoredField, DomainBookkeeping, DomainGate, DomainAccumulator:
		return true
	default:
		return false
	}
}

func ValidateDomainPath(domain Domain, path string) error {
	if !domain.Valid() {
		return ErrInvalidMutationLogWriter(fmt.Sprintf("mutation domain %q is invalid", domain))
	}
	path = strings.TrimSpace(path)
	if domain == DomainLifecycleState {
		if path != "" {
			return ErrInvalidMutationLogWriter("lifecycle_state mutation path must be empty")
		}
		return nil
	}
	if path == "" {
		return ErrInvalidMutationLogWriter(fmt.Sprintf("%s mutation path is required", domain))
	}
	return nil
}

type Writer struct {
	Type        string
	ID          string
	HandlerStep string
}

type EntityStateProjection struct {
	CurrentState string
	Fields       map[string]any
	Bookkeeping  map[string]any
	Gates        map[string]any
	Accumulator  map[string]any
}

type ProjectionMutation struct {
	Domain   Domain
	Path     string
	NewValue any
}

func AuthorActivityDraft(ctx context.Context, runID, mutationID string, rec Record, occurredAt time.Time) (runtimeauthoractivity.Draft, bool, error) {
	if rec.Domain != DomainLifecycleState {
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
	draft := runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindEntityLifecycle, Transition: transition,
		SourceOwner: "entity_mutations", SourceIdentity: mutationID, DedupKey: "entity-mutation:" + mutationID,
		OccurredAt: occurredAt.UTC(), RunID: runID, EntityID: entityID,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "entity", SubjectID: entityID, OldState: oldState, NewState: newState,
			WriterType: writerType, WriterID: writerID,
		},
	}
	if subject, ok := runtimeauthoractivity.InboundProjectionFromContext(ctx); ok {
		draft.Projection.AuthorSubjectType = subject.SubjectType
		draft.Projection.AuthorSubjectID = subject.SubjectID
	}
	return draft, true, nil
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
			Domain:      DomainLifecycleState,
			OldValue:    stringOrNil(before.CurrentState),
			NewValue:    stringOrNil(after.CurrentState),
			WriterType:  writerType,
			WriterID:    writerID,
			HandlerStep: handlerStep,
		})
	}
	fieldRecords, err := diffMapRecords(entityID, DomainAuthoredField, before.Fields, after.Fields, writerType, writerID, handlerStep)
	if err != nil {
		return nil, err
	}
	records = append(records, fieldRecords...)
	bookkeepingRecords, err := diffMapRecords(entityID, DomainBookkeeping, before.Bookkeeping, after.Bookkeeping, writerType, writerID, handlerStep)
	if err != nil {
		return nil, err
	}
	records = append(records, bookkeepingRecords...)
	gateRecords, err := diffMapRecords(entityID, DomainGate, before.Gates, after.Gates, writerType, writerID, handlerStep)
	if err != nil {
		return nil, err
	}
	records = append(records, gateRecords...)
	accRecords, err := diffMapRecords(entityID, DomainAccumulator, before.Accumulator, after.Accumulator, writerType, writerID, handlerStep)
	if err != nil {
		return nil, err
	}
	records = append(records, accRecords...)
	return records, nil
}

func ReconstructEntityStateProjection(records []ProjectionMutation) (EntityStateProjection, error) {
	state := EntityStateProjection{
		Fields:      map[string]any{},
		Bookkeeping: map[string]any{},
		Gates:       map[string]any{},
		Accumulator: map[string]any{},
	}
	for _, rec := range records {
		if err := ApplyEntityStateProjectionMutation(&state, rec.Domain, rec.Path, rec.NewValue); err != nil {
			return EntityStateProjection{}, err
		}
	}
	return state, nil
}

func ApplyEntityStateProjectionMutation(state *EntityStateProjection, domain Domain, path string, value any) error {
	if state == nil {
		return ErrInvalidMutationLogWriter("projection state is required")
	}
	path = strings.TrimSpace(path)
	if err := ValidateDomainPath(domain, path); err != nil {
		return err
	}
	switch domain {
	case DomainLifecycleState:
		next, ok := value.(string)
		if !ok {
			return ErrInvalidMutationLogWriter("lifecycle_state mutation value must be a string")
		}
		state.CurrentState = strings.TrimSpace(next)
		return nil
	case DomainAuthoredField:
		return applyProjectionMapValue(state.ensureFields(), path, value, "authored_field")
	case DomainBookkeeping:
		return applyProjectionMapValue(state.ensureBookkeeping(), path, value, "bookkeeping")
	case DomainGate:
		return applyProjectionMapValue(state.ensureGates(), path, value, "gate")
	case DomainAccumulator:
		return applyProjectionMapValue(state.ensureAccumulator(), path, value, "accumulator")
	}
	return ErrInvalidMutationLogWriter(fmt.Sprintf("mutation domain %q is invalid", domain))
}

func diffMapRecords(entityID string, domain Domain, before, after map[string]any, writerType, writerID, handlerStep string) ([]Record, error) {
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
			Domain:      domain,
			Path:        key,
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

func (p *EntityStateProjection) ensureBookkeeping() map[string]any {
	if p.Bookkeeping == nil {
		p.Bookkeeping = map[string]any{}
	}
	return p.Bookkeeping
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
