package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/google/uuid"
)

const runtimeLogEventName = "platform.runtime_log"

func runtimeLogEvent(record runtimepkg.RuntimeLogPersistenceRecord) events.Event {
	return events.NewRuntimeDiagnosticEvent(
		uuid.NewString(),
		events.EventType(runtimeLogEventName),
		"runtime",
		"",
		json.RawMessage(record.Payload),
		0,
		strings.TrimSpace(record.RunID),
		strings.TrimSpace(record.ParentEventID),
		events.EventEnvelope{},
		time.Now().UTC(),
	)
}

func (s *PostgresStore) RuntimeLogLineageParentEventID(ctx context.Context, runID, explicitParentEventID, subjectEventID string) (string, error) {
	explicitParentEventID = strings.TrimSpace(explicitParentEventID)
	if explicitParentEventID != "" {
		return explicitParentEventID, nil
	}
	runID = strings.TrimSpace(runID)
	subjectEventID = strings.TrimSpace(subjectEventID)
	if s == nil || s.DB == nil || runID == "" || subjectEventID == "" {
		return "", nil
	}
	if _, err := uuid.Parse(runID); err != nil {
		return "", err
	}
	if _, err := uuid.Parse(subjectEventID); err != nil {
		return "", nil
	}
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM events
			WHERE run_id = $1::uuid
			  AND event_id = $2::uuid
		)
	`, runID, subjectEventID).Scan(&exists); err != nil {
		return "", err
	}
	if !exists {
		return "", nil
	}
	return subjectEventID, nil
}

func (s *PostgresStore) PersistRuntimeLog(ctx context.Context, record runtimepkg.RuntimeLogPersistenceRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	enabled, _, err := s.CanonicalRuntimeLogCapability(ctx)
	if err != nil {
		return err
	}
	if !enabled {
		return unsupportedSchemaCapability("events", SchemaFlavorUnavailable)
	}
	if err := s.validateEventPayload(runtimeLogEventName, record.Payload); err != nil {
		return err
	}
	if strings.TrimSpace(record.RunID) != "" {
		return s.AppendEvent(ctx, runtimeLogEvent(record))
	}
	_, err = s.DB.ExecContext(ctx, `
		INSERT INTO events (
			event_id, event_name, entity_id, flow_instance, scope, payload,
			chain_depth, produced_by, produced_by_type, source_event_id, created_at
		)
		VALUES (
			gen_random_uuid(), 'platform.runtime_log', NULL, NULL, 'global', $1::jsonb,
			0, 'runtime', 'platform', NULLIF($2,'')::uuid, now()
		)
	`, string(record.Payload), strings.TrimSpace(record.ParentEventID))
	if err != nil {
		return fmt.Errorf("persist postgres runtime log: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) RuntimeLogLineageParentEventID(ctx context.Context, runID, explicitParentEventID, subjectEventID string) (string, error) {
	explicitParentEventID = strings.TrimSpace(explicitParentEventID)
	if explicitParentEventID != "" {
		return explicitParentEventID, nil
	}
	runID = strings.TrimSpace(runID)
	subjectEventID = strings.TrimSpace(subjectEventID)
	if s == nil || s.DB == nil || runID == "" || subjectEventID == "" {
		return "", nil
	}
	if _, err := uuid.Parse(runID); err != nil {
		return "", err
	}
	if _, err := uuid.Parse(subjectEventID); err != nil {
		return "", nil
	}
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM events
			WHERE run_id = ?
			  AND event_id = ?
		)
	`, runID, subjectEventID).Scan(&exists); err != nil {
		return "", err
	}
	if !exists {
		return "", nil
	}
	return subjectEventID, nil
}

func (s *SQLiteRuntimeStore) PersistRuntimeLog(ctx context.Context, record runtimepkg.RuntimeLogPersistenceRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	enabled, _, err := s.CanonicalRuntimeLogCapability(ctx)
	if err != nil {
		return err
	}
	if !enabled {
		return unsupportedSchemaCapability("events", SchemaFlavorUnavailable)
	}
	if err := s.validateEventPayload(runtimeLogEventName, record.Payload); err != nil {
		return err
	}
	if strings.TrimSpace(record.RunID) != "" {
		return s.AppendEvent(ctx, runtimeLogEvent(record))
	}
	_, err = s.DB.ExecContext(ctx, `
		INSERT OR IGNORE INTO events (
			event_id, run_id, event_name, entity_id, flow_instance, source_route, target_route, target_set,
			scope, payload, chain_depth, produced_by, produced_by_type, source_event_id, created_at
		)
		VALUES (?, NULL, 'platform.runtime_log', NULL, NULL, '{}', '{}', '[]',
			'global', ?, 0, 'runtime', 'platform', ?, ?)
	`, uuid.NewString(), string(record.Payload), sqliteNullUUID(record.ParentEventID), time.Now().UTC())
	if err != nil {
		return fmt.Errorf("persist sqlite runtime log: %w", err)
	}
	return nil
}
