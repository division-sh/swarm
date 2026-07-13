package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"
	"time"
)

var ErrEventIdentityConflict = errors.New("event identity conflict")

type eventIdentityConflictError struct {
	EventID string
}

func (e *eventIdentityConflictError) Error() string {
	if e == nil {
		return ErrEventIdentityConflict.Error()
	}
	return fmt.Sprintf("%s: event_id=%s", ErrEventIdentityConflict, strings.TrimSpace(e.EventID))
}

func (e *eventIdentityConflictError) Unwrap() error {
	return ErrEventIdentityConflict
}

type persistedEventIdentity struct {
	RunID          string
	EventName      string
	EntityID       string
	FlowInstance   string
	Scope          string
	Payload        []byte
	ChainDepth     int
	ProducedBy     string
	ProducedByType string
	SourceEventID  string
	CreatedAt      time.Time
	SourceRoute    []byte
	TargetRoute    []byte
	TargetSet      []byte
}

func newPersistedEventIdentity(
	runID string,
	eventName string,
	entityID string,
	flowInstance string,
	scope string,
	payload []byte,
	chainDepth int,
	producedBy string,
	producedByType string,
	sourceEventID string,
	createdAt time.Time,
	sourceRoute []byte,
	targetRoute []byte,
	targetSet []byte,
) persistedEventIdentity {
	return persistedEventIdentity{
		RunID:          strings.TrimSpace(runID),
		EventName:      strings.TrimSpace(eventName),
		EntityID:       strings.TrimSpace(entityID),
		FlowInstance:   strings.TrimSpace(flowInstance),
		Scope:          strings.TrimSpace(scope),
		Payload:        bytes.Clone(payload),
		ChainDepth:     chainDepth,
		ProducedBy:     strings.TrimSpace(producedBy),
		ProducedByType: strings.TrimSpace(producedByType),
		SourceEventID:  strings.TrimSpace(sourceEventID),
		CreatedAt:      createdAt.UTC(),
		SourceRoute:    bytes.Clone(sourceRoute),
		TargetRoute:    bytes.Clone(targetRoute),
		TargetSet:      bytes.Clone(targetSet),
	}
}

func (i persistedEventIdentity) equal(other persistedEventIdentity) bool {
	return i.RunID == other.RunID &&
		i.EventName == other.EventName &&
		i.EntityID == other.EntityID &&
		i.FlowInstance == other.FlowInstance &&
		i.Scope == other.Scope &&
		jsonSemanticallyEqual(i.Payload, other.Payload) &&
		i.ChainDepth == other.ChainDepth &&
		i.ProducedBy == other.ProducedBy &&
		i.ProducedByType == other.ProducedByType &&
		i.SourceEventID == other.SourceEventID &&
		i.CreatedAt.Truncate(time.Microsecond).Equal(other.CreatedAt.Truncate(time.Microsecond)) &&
		jsonSemanticallyEqual(i.SourceRoute, other.SourceRoute) &&
		jsonSemanticallyEqual(i.TargetRoute, other.TargetRoute) &&
		jsonSemanticallyEqual(i.TargetSet, other.TargetSet)
}

func jsonSemanticallyEqual(left, right []byte) bool {
	leftValue, err := decodeJSONPreservingNumbers(left)
	if err != nil {
		return bytes.Equal(bytes.TrimSpace(left), bytes.TrimSpace(right))
	}
	rightValue, err := decodeJSONPreservingNumbers(right)
	if err != nil {
		return false
	}
	return jsonValuesSemanticallyEqual(leftValue, rightValue)
}

func decodeJSONPreservingNumbers(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

func jsonValuesSemanticallyEqual(left, right any) bool {
	switch leftValue := left.(type) {
	case json.Number:
		rightValue, ok := right.(json.Number)
		if !ok {
			return false
		}
		leftNumber, leftOK := new(big.Rat).SetString(leftValue.String())
		rightNumber, rightOK := new(big.Rat).SetString(rightValue.String())
		return leftOK && rightOK && leftNumber.Cmp(rightNumber) == 0
	case []any:
		rightValue, ok := right.([]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for index := range leftValue {
			if !jsonValuesSemanticallyEqual(leftValue[index], rightValue[index]) {
				return false
			}
		}
		return true
	case map[string]any:
		rightValue, ok := right.(map[string]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for key, leftItem := range leftValue {
			rightItem, found := rightValue[key]
			if !found || !jsonValuesSemanticallyEqual(leftItem, rightItem) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(left, right)
	}
}

func resolveExistingEventIdentity(eventID string, want persistedEventIdentity, got persistedEventIdentity, found bool) (bool, error) {
	if !found {
		return false, nil
	}
	if want.equal(got) {
		return true, nil
	}
	return false, &eventIdentityConflictError{EventID: eventID}
}

func loadPostgresEventIdentity(ctx context.Context, q rowQueryer, caps StoreSchemaCapabilities, eventID string) (persistedEventIdentity, bool, error) {
	runIDExpr := `''`
	if caps.Events.LogRunID {
		runIDExpr = `COALESCE(run_id::text, '')`
	}
	sourceRouteExpr := `'{}'::jsonb`
	targetRouteExpr := `'{}'::jsonb`
	targetSetExpr := `'[]'::jsonb`
	if caps.Events.LogRouteIdentity {
		sourceRouteExpr = `COALESCE(source_route, '{}'::jsonb)`
		targetRouteExpr = `COALESCE(target_route, '{}'::jsonb)`
		targetSetExpr = `COALESCE(target_set, '[]'::jsonb)`
	}
	query := `
		SELECT
			` + runIDExpr + `,
			COALESCE(event_name, ''),
			COALESCE(entity_id::text, ''),
			COALESCE(flow_instance, ''),
			COALESCE(scope, ''),
			payload,
			COALESCE(chain_depth, 0),
			COALESCE(produced_by, ''),
			COALESCE(produced_by_type, ''),
			COALESCE(source_event_id::text, ''),
			created_at,
			` + sourceRouteExpr + `,
			` + targetRouteExpr + `,
			` + targetSetExpr + `
		FROM events
		WHERE event_id = $1::uuid`
	var row persistedEventIdentity
	err := q.QueryRowContext(ctx, query, eventID).Scan(
		&row.RunID,
		&row.EventName,
		&row.EntityID,
		&row.FlowInstance,
		&row.Scope,
		&row.Payload,
		&row.ChainDepth,
		&row.ProducedBy,
		&row.ProducedByType,
		&row.SourceEventID,
		&row.CreatedAt,
		&row.SourceRoute,
		&row.TargetRoute,
		&row.TargetSet,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return persistedEventIdentity{}, false, nil
	}
	if err != nil {
		return persistedEventIdentity{}, false, fmt.Errorf("load existing event identity: %w", err)
	}
	return newPersistedEventIdentity(
		row.RunID, row.EventName, row.EntityID, row.FlowInstance, row.Scope, row.Payload,
		row.ChainDepth, row.ProducedBy, row.ProducedByType, row.SourceEventID, row.CreatedAt,
		row.SourceRoute, row.TargetRoute, row.TargetSet,
	), true, nil
}

func loadSQLiteEventIdentity(ctx context.Context, q rowQueryer, caps StoreSchemaCapabilities, eventID string) (persistedEventIdentity, bool, error) {
	runIDExpr := `''`
	if caps.Events.LogRunID {
		runIDExpr = `COALESCE(run_id, '')`
	}
	sourceRouteExpr := `'{}'`
	targetRouteExpr := `'{}'`
	targetSetExpr := `'[]'`
	if caps.Events.LogRouteIdentity {
		sourceRouteExpr = `COALESCE(source_route, '{}')`
		targetRouteExpr = `COALESCE(target_route, '{}')`
		targetSetExpr = `COALESCE(target_set, '[]')`
	}
	query := `
		SELECT
			` + runIDExpr + `,
			COALESCE(event_name, ''),
			COALESCE(entity_id, ''),
			COALESCE(flow_instance, ''),
			COALESCE(scope, ''),
			payload,
			COALESCE(chain_depth, 0),
			COALESCE(produced_by, ''),
			COALESCE(produced_by_type, ''),
			COALESCE(source_event_id, ''),
			created_at,
			` + sourceRouteExpr + `,
			` + targetRouteExpr + `,
			` + targetSetExpr + `
		FROM events
		WHERE event_id = ?`
	var row persistedEventIdentity
	var createdAt any
	err := q.QueryRowContext(ctx, query, eventID).Scan(
		&row.RunID,
		&row.EventName,
		&row.EntityID,
		&row.FlowInstance,
		&row.Scope,
		&row.Payload,
		&row.ChainDepth,
		&row.ProducedBy,
		&row.ProducedByType,
		&row.SourceEventID,
		&createdAt,
		&row.SourceRoute,
		&row.TargetRoute,
		&row.TargetSet,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return persistedEventIdentity{}, false, nil
	}
	if err != nil {
		return persistedEventIdentity{}, false, fmt.Errorf("load existing sqlite event identity: %w", err)
	}
	parsedCreatedAt, valid, err := sqliteTimeValue(createdAt)
	if err != nil {
		return persistedEventIdentity{}, false, fmt.Errorf("load existing sqlite event identity created_at: %w", err)
	}
	if !valid {
		return persistedEventIdentity{}, false, fmt.Errorf("load existing sqlite event identity: created_at is required")
	}
	return newPersistedEventIdentity(
		row.RunID, row.EventName, row.EntityID, row.FlowInstance, row.Scope, row.Payload,
		row.ChainDepth, row.ProducedBy, row.ProducedByType, row.SourceEventID, parsedCreatedAt,
		row.SourceRoute, row.TargetRoute, row.TargetSet,
	), true, nil
}
