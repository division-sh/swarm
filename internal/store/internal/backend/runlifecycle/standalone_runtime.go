package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/google/uuid"
)

type standaloneRuntimePlatformRunRecord struct {
	RunID          string
	RunStatus      string
	Origin         runtimerunlifecycle.RunOrigin
	EventID        string
	EventClass     string
	EventType      string
	ProducedBy     string
	ProducedByType string
	SourceEventID  string
}

type StandaloneRuntimePlatformRunRecord = standaloneRuntimePlatformRunRecord

func loadPostgresStandaloneRuntimePlatformRunRecord(ctx context.Context, q rowQueryer, eventID string) (standaloneRuntimePlatformRunRecord, bool, error) {
	eventID = sanitizeOptionalUUID(eventID)
	if q == nil || eventID == "" {
		return standaloneRuntimePlatformRunRecord{}, false, nil
	}
	durable, found, err := eventrecordpostgres.Load(ctx, q, eventID)
	if err != nil || !found {
		return standaloneRuntimePlatformRunRecord{}, found, err
	}
	record, err := standaloneRuntimeRecordFromEvent(durable)
	if err != nil {
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("decode standalone runtime platform event: %w", err)
	}
	snapshot, err := loadPostgresRunLifecycleSnapshot(ctx, q, record.RunID, false)
	switch {
	case errors.Is(err, runtimerunlifecycle.ErrRunNotFound):
		return standaloneRuntimePlatformRunRecord{}, false, nil
	case err != nil:
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("load standalone runtime platform run candidate: %w", err)
	default:
		record.RunStatus = string(snapshot.State)
		record.Origin = snapshot.Origin
		return record, true, nil
	}
}

func loadSQLiteStandaloneRuntimePlatformRunRecord(ctx context.Context, q rowQueryer, eventID string) (standaloneRuntimePlatformRunRecord, bool, error) {
	eventID = sanitizeOptionalUUID(eventID)
	if q == nil || eventID == "" {
		return standaloneRuntimePlatformRunRecord{}, false, nil
	}
	durable, found, err := eventrecordsqlite.Load(ctx, q, eventID)
	if err != nil || !found {
		return standaloneRuntimePlatformRunRecord{}, found, err
	}
	record, err := standaloneRuntimeRecordFromEvent(durable)
	if err != nil {
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("decode sqlite standalone runtime platform event: %w", err)
	}
	snapshot, err := loadSQLiteRunLifecycleSnapshot(ctx, q, record.RunID)
	switch {
	case errors.Is(err, runtimerunlifecycle.ErrRunNotFound):
		return standaloneRuntimePlatformRunRecord{}, false, nil
	case err != nil:
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("load sqlite standalone runtime platform run candidate: %w", err)
	default:
		record.RunStatus = string(snapshot.State)
		record.Origin = snapshot.Origin
		return record, true, nil
	}
}

func standaloneRuntimeRecordFromEvent(durable eventrecord.Record) (standaloneRuntimePlatformRunRecord, error) {
	admitted, err := durable.Decode()
	if err != nil {
		return standaloneRuntimePlatformRunRecord{}, err
	}
	event := admitted.Event()
	return standaloneRuntimePlatformRunRecord{
		RunID: event.RunID(), EventID: event.ID(), EventClass: string(event.AdmissionClass()),
		EventType: string(event.Type()), ProducedBy: event.SourceAgent(), ProducedByType: string(event.ProducerType()),
		SourceEventID: event.ParentEventID(),
	}, nil
}

func isStandaloneRuntimePlatformRunRecord(record standaloneRuntimePlatformRunRecord) bool {
	if strings.TrimSpace(record.RunID) == "" {
		return false
	}
	if record.EventClass != string(events.EventAdmissionRuntimeControl) && record.EventClass != string(events.EventAdmissionRuntimeDiagnostic) {
		return false
	}
	if strings.TrimSpace(record.ProducedByType) != string(events.EventProducerPlatform) || strings.TrimSpace(record.ProducedBy) != "runtime" {
		return false
	}
	if strings.TrimSpace(record.SourceEventID) != "" || record.Origin.Kind() != runtimerunlifecycle.OriginEvent {
		return false
	}
	return record.Origin.EventID() == strings.TrimSpace(record.EventID) && record.Origin.EventType() == strings.TrimSpace(record.EventType)
}

func IsStandaloneRuntimePlatformRunRecord(record StandaloneRuntimePlatformRunRecord) bool {
	return isStandaloneRuntimePlatformRunRecord(record)
}

func sanitizeOptionalUUID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}

func (s *RunLifecyclePostgresOwner) IsStandaloneRuntimePlatformEventTx(ctx context.Context, tx *sql.Tx, eventID string) (bool, error) {
	record, found, err := loadPostgresStandaloneRuntimePlatformRunRecord(ctx, tx, eventID)
	return found && isStandaloneRuntimePlatformRunRecord(record), err
}

func (s *RunLifecycleSQLiteOwner) IsStandaloneRuntimePlatformEventTx(ctx context.Context, tx *sql.Tx, eventID string) (bool, error) {
	record, found, err := loadSQLiteStandaloneRuntimePlatformRunRecord(ctx, tx, eventID)
	return found && isStandaloneRuntimePlatformRunRecord(record), err
}
