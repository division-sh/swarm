package store

import (
	"context"
	"errors"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func (s *SQLiteRuntimeStore) LoadRunLifecycleSnapshot(ctx context.Context, runID string) (runtimebus.RunLifecycleSnapshot, error) {
	if s == nil || s.DB == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("sqlite runtime store is required")
	}
	snap, err := loadSQLiteRunLifecycleSnapshot(ctx, s.DB, runID)
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	return projectBusRunLifecycleSnapshot(snap), nil
}

func sqliteLoadStandaloneRuntimePlatformRunRecord(ctx context.Context, q rowQueryer, eventID string) (standaloneRuntimePlatformRunRecord, bool, error) {
	eventID = sanitizeOptionalUUID(eventID)
	if q == nil || eventID == "" {
		return standaloneRuntimePlatformRunRecord{}, false, nil
	}
	durable, found, err := loadSQLiteEventIdentity(ctx, q, eventID)
	if err != nil || !found {
		return standaloneRuntimePlatformRunRecord{}, found, err
	}
	admitted, err := decodeEventRecord(durable)
	if err != nil {
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("decode sqlite standalone runtime platform event: %w", err)
	}
	event := admitted.Event()
	rec := standaloneRuntimePlatformRunRecord{
		RunID: event.RunID(), EventID: event.ID(), EventClass: string(event.AdmissionClass()),
		EventType: string(event.Type()), ProducedBy: event.SourceAgent(), ProducedByType: string(event.ProducerType()),
		SourceEventID: event.ParentEventID(),
	}
	snapshot, err := loadSQLiteRunLifecycleSnapshot(ctx, q, rec.RunID)
	switch {
	case errors.Is(err, runtimerunlifecycle.ErrRunNotFound):
		return standaloneRuntimePlatformRunRecord{}, false, nil
	case err != nil:
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("load sqlite standalone runtime platform run candidate: %w", err)
	default:
		rec.RunStatus = string(snapshot.State)
		rec.Origin = snapshot.Origin
		return rec, true, nil
	}
}
