package eventpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
)

var ErrEventIdentityConflict = events.ErrEventIdentityConflict

type eventIdentityConflictError struct{ EventID string }

func (e *eventIdentityConflictError) Error() string {
	if e == nil {
		return ErrEventIdentityConflict.Error()
	}
	return fmt.Sprintf("%s: event_id=%s", ErrEventIdentityConflict, strings.TrimSpace(e.EventID))
}

func (e *eventIdentityConflictError) Unwrap() error { return ErrEventIdentityConflict }

type persistedEventIdentity = eventrecord.Record

func decodeEventRecord(row persistedEventIdentity) (events.AdmittedEvent, error) {
	return row.Decode()
}

func DecodeEventRecord(row eventrecord.Record) (events.AdmittedEvent, error) {
	return decodeEventRecord(row)
}

func resolveExistingEventIdentity(eventID string, want, got persistedEventIdentity, found bool) (bool, error) {
	if !found {
		return false, nil
	}
	if want.Equal(got) {
		return true, nil
	}
	return false, &eventIdentityConflictError{EventID: eventID}
}

func ResolveExistingEventIdentity(eventID string, want, got eventrecord.Record, found bool) (bool, error) {
	return resolveExistingEventIdentity(eventID, want, got, found)
}

func loadPostgresEventIdentity(ctx context.Context, q rowQueryer, eventID string) (persistedEventIdentity, bool, error) {
	return eventrecordpostgres.Load(ctx, q, eventID)
}

func loadSQLiteEventIdentity(ctx context.Context, q rowQueryer, eventID string) (persistedEventIdentity, bool, error) {
	return eventrecordsqlite.Load(ctx, q, eventID)
}

func LoadPostgresEventIdentity(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, eventID string) (eventrecord.Record, bool, error) {
	return loadPostgresEventIdentity(ctx, q, eventID)
}

func LoadSQLiteEventIdentity(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, eventID string) (eventrecord.Record, bool, error) {
	return loadSQLiteEventIdentity(ctx, q, eventID)
}

func (s *EventPostgresOwner) LoadDirectiveEventTx(ctx context.Context, tx *sql.Tx, eventID string) (events.AdmittedEvent, bool, error) {
	row, found, err := loadPostgresEventIdentity(ctx, tx, eventID)
	if err != nil || !found {
		return events.AdmittedEvent{}, found, err
	}
	admitted, err := decodeEventRecord(row)
	return admitted, err == nil, err
}

func (s *EventSQLiteOwner) LoadDirectiveEventTx(ctx context.Context, tx *sql.Tx, eventID string) (events.AdmittedEvent, bool, error) {
	row, found, err := loadSQLiteEventIdentity(ctx, tx, eventID)
	if err != nil || !found {
		return events.AdmittedEvent{}, found, err
	}
	admitted, err := decodeEventRecord(row)
	return admitted, err == nil, err
}

func (s *EventPostgresOwner) LoadPreparedPublishEvent(ctx context.Context, eventID string) (runtimebus.PreparedPublishEvent, bool, error) {
	if s == nil || s.backend == nil {
		return runtimebus.PreparedPublishEvent{}, false, fmt.Errorf("postgres store is required")
	}
	row, found, err := loadPostgresEventIdentity(ctx, s.backend, eventID)
	if err != nil || !found {
		return runtimebus.PreparedPublishEvent{}, found, err
	}
	admitted, err := decodeEventRecord(row)
	if err != nil {
		return runtimebus.PreparedPublishEvent{}, false, err
	}
	settlement, err := row.DecodeSettlement()
	if err != nil {
		return runtimebus.PreparedPublishEvent{}, false, err
	}
	snapshots, err := s.DeliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return runtimebus.PreparedPublishEvent{}, false, err
	}
	return preparedPublishEvent(admitted, settlement, deliveryRoutesFromSnapshots(snapshots))
}

func (s *EventSQLiteOwner) LoadPreparedPublishEvent(ctx context.Context, eventID string) (runtimebus.PreparedPublishEvent, bool, error) {
	if s == nil || s.backend == nil {
		return runtimebus.PreparedPublishEvent{}, false, fmt.Errorf("sqlite runtime store is required")
	}
	row, found, err := loadSQLiteEventIdentity(ctx, s.backend, eventID)
	if err != nil || !found {
		return runtimebus.PreparedPublishEvent{}, found, err
	}
	admitted, err := decodeEventRecord(row)
	if err != nil {
		return runtimebus.PreparedPublishEvent{}, false, err
	}
	settlement, err := row.DecodeSettlement()
	if err != nil {
		return runtimebus.PreparedPublishEvent{}, false, err
	}
	snapshots, err := s.DeliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return runtimebus.PreparedPublishEvent{}, false, err
	}
	return preparedPublishEvent(admitted, settlement, deliveryRoutesFromSnapshots(snapshots))
}

func deliveryRoutesFromSnapshots(snapshots []runtimedelivery.Snapshot) []events.DeliveryRoute {
	routes := make([]events.DeliveryRoute, 0, len(snapshots))
	for _, snapshot := range snapshots {
		routes = append(routes, snapshot.Route)
	}
	return events.NormalizeDeliveryRoutes(routes)
}

func preparedPublishEvent(admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute) (runtimebus.PreparedPublishEvent, bool, error) {
	prepared := runtimebus.PreparedPublishEvent{
		Event: admitted, Settlement: settlement, DeliveryRoutes: routes,
	}
	if err := prepared.Validate(); err != nil {
		return runtimebus.PreparedPublishEvent{}, false, fmt.Errorf("validate persisted prepared publication: %w", err)
	}
	return prepared, true, nil
}

func jsonSemanticallyEqual(left, right []byte) bool {
	var leftValue, rightValue any
	leftDecoder := json.NewDecoder(bytes.NewReader(left))
	leftDecoder.UseNumber()
	rightDecoder := json.NewDecoder(bytes.NewReader(right))
	rightDecoder.UseNumber()
	if leftDecoder.Decode(&leftValue) != nil || rightDecoder.Decode(&rightValue) != nil {
		return string(left) == string(right)
	}
	return jsonValuesEqual(leftValue, rightValue)
}

func JSONSemanticallyEqual(left, right []byte) bool {
	return jsonSemanticallyEqual(left, right)
}

func jsonValuesEqual(left, right any) bool {
	switch left := left.(type) {
	case nil:
		return right == nil
	case bool:
		right, ok := right.(bool)
		return ok && left == right
	case string:
		right, ok := right.(string)
		return ok && left == right
	case json.Number:
		right, ok := right.(json.Number)
		if !ok {
			return false
		}
		leftNumber, leftOK := new(big.Rat).SetString(string(left))
		rightNumber, rightOK := new(big.Rat).SetString(string(right))
		return leftOK && rightOK && leftNumber.Cmp(rightNumber) == 0
	case []any:
		right, ok := right.([]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for index := range left {
			if !jsonValuesEqual(left[index], right[index]) {
				return false
			}
		}
		return true
	case map[string]any:
		right, ok := right.(map[string]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for key, value := range left {
			rightValue, exists := right[key]
			if !exists || !jsonValuesEqual(value, rightValue) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
