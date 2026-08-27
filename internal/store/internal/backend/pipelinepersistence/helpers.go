package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	deliverystore "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	runstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	storefailurecodec "github.com/division-sh/swarm/internal/store/internal/failurecodec"
)

const runLifecycleActiveStateSQLValues = runstate.ActiveStateSQLValues

var (
	postgresDeliveryAdapter = mustDeliveryAdapter(deliverystore.DialectPostgres)
	sqliteDeliveryAdapter   = mustDeliveryAdapter(deliverystore.DialectSQLite)
)

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type eventReadQueryer interface {
	rowQueryer
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type pipelineReceiptSideEffects struct {
	ManagerStatus string `json:"manager_status"`
	ReasonCode    string `json:"reason_code,omitempty"`
}

func decodeStoredFailure(raw any) (*runtimefailures.Envelope, error) {
	return storefailurecodec.Decode(raw)
}

func encodeStoredFailure(failure *runtimefailures.Envelope) (any, error) {
	return storefailurecodec.Encode(failure)
}

func requireActiveRunForEvent(ctx context.Context, tx *sql.Tx, eventID string, postgres bool) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event_id is required")
	}
	if tx == nil {
		return errors.New("require active event run: transaction is required")
	}
	query := `SELECT COALESCE(CAST(run_id AS TEXT), '') FROM events WHERE event_id = ?`
	if postgres {
		query = `SELECT COALESCE(run_id::text, '') FROM events WHERE event_id = $1::uuid`
	}
	var runID string
	if err := tx.QueryRowContext(ctx, query, eventID).Scan(&runID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("require active event run: event %s not found", eventID)
		}
		return fmt.Errorf("require active event run: %w", err)
	}
	if strings.TrimSpace(runID) == "" {
		return nil
	}
	if postgres {
		return runstate.RequirePostgresActiveTx(ctx, tx, runID)
	}
	return runstate.RequireSQLiteActiveTx(ctx, tx, runID)
}

func hydratePostgresPersistedReplayEvents(ctx context.Context, q eventReadQueryer, eventIDs []string) ([]events.PersistedReplayEvent, error) {
	records, err := eventrecordpostgres.LoadMany(ctx, q, eventIDs)
	if err != nil {
		return nil, err
	}
	return persistedReplayEvents(records)
}

func hydrateSQLitePersistedReplayEvents(ctx context.Context, q eventReadQueryer, eventIDs []string) ([]events.PersistedReplayEvent, error) {
	records, err := eventrecordsqlite.LoadMany(ctx, q, eventIDs)
	if err != nil {
		return nil, err
	}
	return persistedReplayEvents(records)
}

type eventDecoder interface {
	Decode() (events.AdmittedEvent, error)
}

func persistedReplayEvents[T eventDecoder](records []T) ([]events.PersistedReplayEvent, error) {
	out := make([]events.PersistedReplayEvent, 0, len(records))
	for _, durable := range records {
		admitted, err := durable.Decode()
		if err != nil {
			return nil, err
		}
		event := admitted.Event()
		record := events.PersistedReplayEvent{Event: event}
		if event.RunID() == "" {
			failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassSchemaInvalid, "persisted_replay_run_identity_invalid", "event-store", "load_replay", map[string]any{"reason_code": "missing_canonical_run_id"}), "event-store", "load_replay")
			record.ReplayFailure = &failure
		}
		out = append(out, record)
	}
	return out, nil
}

func loadFanOutSourceEvent(ctx context.Context, q eventReadQueryer, eventID string, postgres bool) (events.Event, events.RouteSettlement, error) {
	var (
		record     eventDecoder
		settlement events.RouteSettlement
	)
	if postgres {
		loaded, found, err := eventrecordpostgres.Load(ctx, q, eventID)
		if err != nil {
			return events.Event{}, settlement, err
		}
		if !found {
			return events.Event{}, settlement, fmt.Errorf("event %s not found", strings.TrimSpace(eventID))
		}
		record = loaded
		settlement, err = loaded.DecodeSettlement()
		if err != nil {
			return events.Event{}, events.RouteSettlement{}, err
		}
	} else {
		loaded, found, err := eventrecordsqlite.Load(ctx, q, eventID)
		if err != nil {
			return events.Event{}, settlement, err
		}
		if !found {
			return events.Event{}, settlement, fmt.Errorf("event %s not found", strings.TrimSpace(eventID))
		}
		record = loaded
		settlement, err = loaded.DecodeSettlement()
		if err != nil {
			return events.Event{}, events.RouteSettlement{}, err
		}
	}
	admitted, err := record.Decode()
	if err != nil {
		return events.Event{}, events.RouteSettlement{}, err
	}
	return admitted.Event(), settlement, nil
}

func marshalPipelineReceiptSideEffects(value pipelineReceiptSideEffects) ([]byte, error) {
	return json.Marshal(value)
}

func mustDeliveryAdapter(dialect deliverystore.Dialect) *deliverystore.Adapter {
	adapter, err := deliverystore.NewAdapter(dialect)
	if err != nil {
		panic(err)
	}
	return adapter
}

func diagnosticDirectReplayEventNames() []string {
	types := events.DiagnosticDirectEventTypes()
	names := make([]string, 0, len(types))
	for _, eventType := range types {
		names = append(names, string(eventType))
	}
	return names
}

func diagnosticDirectReplayEventArgs() []any {
	names := diagnosticDirectReplayEventNames()
	args := make([]any, len(names))
	for index := range names {
		args[index] = names[index]
	}
	return args
}

func sqliteDiagnosticDirectReplayExclusionSQL(alias string) string {
	return diagnosticDirectReplayColumn(alias) + " NOT IN (" + strings.TrimSuffix(strings.Repeat("?,", len(diagnosticDirectReplayEventNames())), ",") + ")"
}

func postgresDiagnosticDirectReplayExclusionSQL(alias string, start int) string {
	placeholders := make([]string, len(diagnosticDirectReplayEventNames()))
	for index := range placeholders {
		placeholders[index] = fmt.Sprintf("$%d", start+index)
	}
	return diagnosticDirectReplayColumn(alias) + " NOT IN (" + strings.Join(placeholders, ", ") + ")"
}

func diagnosticDirectReplayColumn(alias string) string {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return "event_name"
	}
	return alias + ".event_name"
}

func sqliteTimeValue(raw any) (time.Time, bool, error) {
	switch value := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		return value.UTC(), !value.IsZero(), nil
	case string:
		return parseSQLiteTime(value)
	case []byte:
		return parseSQLiteTime(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported SQLite time value %T", raw)
	}
}

func parseSQLiteTime(raw string) (time.Time, bool, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST"} {
		if parsed, err := time.Parse(layout, strings.TrimSpace(raw)); err == nil {
			return parsed.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("invalid SQLite time %q", raw)
}

func eventRunIDForCompletionCandidateTx(ctx context.Context, tx *sql.Tx, eventID string, postgres bool) (string, error) {
	query := `SELECT COALESCE(CAST(run_id AS TEXT), '') FROM events WHERE event_id = ?`
	if postgres {
		query = `SELECT COALESCE(run_id::text, '') FROM events WHERE event_id = $1::uuid`
	}
	var runID string
	if err := tx.QueryRowContext(ctx, query, strings.TrimSpace(eventID)).Scan(&runID); err != nil {
		return "", fmt.Errorf("load pipeline event run: %w", err)
	}
	return strings.TrimSpace(runID), nil
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
	return eventIDs, nil
}
