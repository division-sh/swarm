package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/eventrecord/sqlite"
)

var ErrEventIdentityConflict = errors.New("event identity conflict")

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

func resolveExistingEventIdentity(eventID string, want, got persistedEventIdentity, found bool) (bool, error) {
	if !found {
		return false, nil
	}
	if want.Equal(got) {
		return true, nil
	}
	return false, &eventIdentityConflictError{EventID: eventID}
}

func loadPostgresEventIdentity(ctx context.Context, q rowQueryer, eventID string) (persistedEventIdentity, bool, error) {
	return eventrecordpostgres.Load(ctx, q, eventID)
}

func loadSQLiteEventIdentity(ctx context.Context, q rowQueryer, eventID string) (persistedEventIdentity, bool, error) {
	return eventrecordsqlite.Load(ctx, q, eventID)
}

func loadPostgresEventIdentities(ctx context.Context, q eventReadQueryer, eventIDs []string) ([]persistedEventIdentity, error) {
	return eventrecordpostgres.LoadMany(ctx, q, eventIDs)
}

func loadSQLiteEventIdentities(ctx context.Context, q eventReadQueryer, eventIDs []string) ([]persistedEventIdentity, error) {
	return eventrecordsqlite.LoadMany(ctx, q, eventIDs)
}

func hydratePostgresPersistedReplayEvents(ctx context.Context, q eventReadQueryer, eventIDs []string) ([]events.PersistedReplayEvent, error) {
	records, err := loadPostgresEventIdentities(ctx, q, eventIDs)
	if err != nil {
		return nil, err
	}
	return persistedReplayEventsFromRecords(records)
}

func hydrateSQLitePersistedReplayEvents(ctx context.Context, q eventReadQueryer, eventIDs []string) ([]events.PersistedReplayEvent, error) {
	records, err := loadSQLiteEventIdentities(ctx, q, eventIDs)
	if err != nil {
		return nil, err
	}
	return persistedReplayEventsFromRecords(records)
}

func persistedReplayEventsFromRecords(records []persistedEventIdentity) ([]events.PersistedReplayEvent, error) {
	out := make([]events.PersistedReplayEvent, 0, len(records))
	for _, durable := range records {
		admitted, err := decodeEventRecord(durable)
		if err != nil {
			return nil, err
		}
		event := admitted.Event()
		record := events.PersistedReplayEvent{Event: event}
		if event.RunID() == "" {
			record.ReplayFailure = replayAdmissionFailure("missing_canonical_run_id")
		}
		out = append(out, record)
	}
	return out, nil
}

func scanOrderedEventIDs(rows *sql.Rows, label string) ([]string, error) {
	if rows == nil {
		return nil, fmt.Errorf("%s rows are required", label)
	}
	defer rows.Close()
	var eventIDs []string
	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			return nil, fmt.Errorf("scan %s event id: %w", label, err)
		}
		eventIDs = append(eventIDs, eventID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read %s event ids: %w", label, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close %s event ids: %w", label, err)
	}
	return eventIDs, nil
}

// pendingDeliveryEventRecordIDs collapses the intentional one-event-to-many-
// deliveries fan-out before canonical hydration. Blank IDs are durable
// corruption and never disappear as a normalization side effect.
func pendingDeliveryEventRecordIDs(ids []string) ([]string, error) {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for index, raw := range ids {
		id := strings.TrimSpace(raw)
		if id == "" {
			return nil, fmt.Errorf("pending delivery event id at index %d is required", index)
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
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
