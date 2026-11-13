package eventpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
)

func (s *EventPostgresOwner) ReadCanonicalEventRecordForTest(ctx context.Context, eventID string) (events.Event, bool, error) {
	var empty events.Event
	var record eventrecord.Record
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		record, found, err = eventrecordpostgres.Load(ctx, tx, eventID)
		return err
	})
	if err != nil || !found {
		return empty, false, err
	}
	admitted, err := record.Decode()
	if err != nil {
		return empty, false, fmt.Errorf("decode canonical event record %s: %w", eventID, err)
	}
	return admitted.Event(), true, nil
}

func (s *EventSQLiteOwner) ReadCanonicalEventRecordForTest(ctx context.Context, eventID string) (events.Event, bool, error) {
	var empty events.Event
	var record eventrecord.Record
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		record, found, err = eventrecordsqlite.Load(ctx, tx, eventID)
		return err
	})
	if err != nil || !found {
		return empty, false, err
	}
	admitted, err := record.Decode()
	if err != nil {
		return empty, false, fmt.Errorf("decode canonical event record %s: %w", eventID, err)
	}
	return admitted.Event(), true, nil
}

func (s *EventPostgresOwner) ReadLatestRuntimeLogRecordForTest(ctx context.Context, startupDecision bool) (events.Event, error) {
	var empty events.Event
	query := `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name='platform.runtime_log' ORDER BY created_at DESC LIMIT 1`
	if startupDecision {
		query = `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name='platform.runtime_log' AND payload->'details'->>'component'='runtime' AND payload->'details'->>'action'='startup_recovery_decision' ORDER BY created_at DESC LIMIT 1`
	}
	var record eventrecord.Record
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var id string
		if err := tx.QueryRowContext(ctx, query).Scan(&id); err != nil {
			return err
		}
		var found bool
		var err error
		record, found, err = eventrecordpostgres.Load(ctx, tx, id)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("selected runtime log %s is absent from its original read snapshot", id)
		}
		return nil
	})
	if err != nil {
		return empty, err
	}
	admitted, err := record.Decode()
	if err != nil {
		return empty, err
	}
	return admitted.Event(), nil
}

func (s *EventSQLiteOwner) ReadLatestRuntimeLogRecordForTest(ctx context.Context, startupDecision bool) (events.Event, error) {
	var empty events.Event
	query := `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name='platform.runtime_log' ORDER BY created_at DESC LIMIT 1`
	if startupDecision {
		query = `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name='platform.runtime_log' AND json_extract(payload,'$.details.component')='runtime' AND json_extract(payload,'$.details.action')='startup_recovery_decision' ORDER BY created_at DESC LIMIT 1`
	}
	var record eventrecord.Record
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var id string
		if err := tx.QueryRowContext(ctx, query).Scan(&id); err != nil {
			return err
		}
		var found bool
		var err error
		record, found, err = eventrecordsqlite.Load(ctx, tx, id)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("selected runtime log %s is absent from its original read snapshot", id)
		}
		return nil
	})
	if err != nil {
		return empty, err
	}
	admitted, err := record.Decode()
	if err != nil {
		return empty, err
	}
	return admitted.Event(), nil
}

func (s *EventPostgresOwner) ReadActivityForkRequestPayloadStorageForTest(ctx context.Context, runID, eventID string) ([]byte, error) {
	var out []byte
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadActivityForkRequestPayloadStorageTx(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *EventSQLiteOwner) ReadActivityForkRequestPayloadStorageForTest(ctx context.Context, runID, eventID string) ([]byte, error) {
	var out []byte
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadActivityForkRequestPayloadStorageTx(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *EventPostgresOwner) ReadActivityForkResultStorageForTest(ctx context.Context, runID, eventID string) (eventrecord.ActivityForkResultStorage, error) {
	var out eventrecord.ActivityForkResultStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadActivityForkResultStorageTx(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return eventrecord.ActivityForkResultStorage{}, err
	}
	return out, nil
}

func (s *EventSQLiteOwner) ReadActivityForkResultStorageForTest(ctx context.Context, runID, eventID string) (eventrecord.ActivityForkResultStorage, error) {
	var out eventrecord.ActivityForkResultStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadActivityForkResultStorageTx(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return eventrecord.ActivityForkResultStorage{}, err
	}
	return out, nil
}

func (s *EventPostgresOwner) CountRunNamedEventStorageForTest(ctx context.Context, runID, eventName string) (int, error) {
	var out int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.CountRunNamedEventStorageTx(ctx, tx, runID, eventName)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (s *EventSQLiteOwner) CountRunNamedEventStorageForTest(ctx context.Context, runID, eventName string) (int, error) {
	var out int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.CountRunNamedEventStorageTx(ctx, tx, runID, eventName)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}
