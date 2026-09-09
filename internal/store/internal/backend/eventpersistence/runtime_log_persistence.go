package eventpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

const runtimeLogEventName = "platform.runtime_log"

const RuntimeLogEventName = runtimeLogEventName

func (s *EventPostgresOwner) ValidateRuntimeLogRecordTx(ctx context.Context, tx *sql.Tx, record runtimepkg.RuntimeLogPersistenceRecord) error {
	admitted, err := s.admitRuntimeLogRecord(ctx, record)
	if err != nil {
		return err
	}
	got, found, err := loadPostgresEventIdentity(ctx, tx, record.EventID)
	if err != nil {
		return err
	}
	return validateRuntimeLogRecordTx(ctx, tx, postgresDeliveryAdapter, admitted, got, found)
}

func (s *EventSQLiteOwner) ValidateRuntimeLogRecordTx(ctx context.Context, tx *sql.Tx, record runtimepkg.RuntimeLogPersistenceRecord) error {
	admitted, err := s.admitRuntimeLogRecord(ctx, record)
	if err != nil {
		return err
	}
	got, found, err := loadSQLiteEventIdentity(ctx, tx, record.EventID)
	if err != nil {
		return err
	}
	return validateRuntimeLogRecordTx(ctx, tx, mustDeliveryAdapter(storedelivery.DialectSQLite), admitted, got, found)
}

func validateRuntimeLogRecordTx(ctx context.Context, tx *sql.Tx, delivery *storedelivery.Adapter, admitted events.AdmittedEvent, got eventrecord.Record, found bool) error {
	if !found {
		return eventrecord.Missing(admitted.ID())
	}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteRuntimeLogDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	if err != nil {
		return err
	}
	want, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		return err
	}
	if !want.Equal(got) {
		return &eventIdentityConflictError{EventID: admitted.ID()}
	}
	deliveries, err := delivery.SnapshotsForEvent(ctx, tx, admitted.ID())
	if err != nil {
		return err
	}
	if len(deliveries) != 0 {
		return fmt.Errorf("lifecycle diagnostic %s unexpectedly has deliveries", admitted.ID())
	}
	return nil
}

func admitRuntimeLogRecord(record runtimepkg.RuntimeLogPersistenceRecord) (events.AdmittedEvent, error) {
	if !record.PayloadAdmission.Valid() {
		return events.AdmittedEvent{}, fmt.Errorf("runtime log payload admission evidence is required")
	}
	if !bytes.Equal(record.Payload, record.PayloadAdmission.Payload()) {
		return events.AdmittedEvent{}, fmt.Errorf("runtime log payload differs from its admission evidence")
	}
	constructed, err := runtimeLogEvent(record)
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	constructed, err = events.ApplyPayloadAdmission(constructed, record.PayloadAdmission)
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	return events.AdmitForPersistence(constructed, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
}

func (s *EventPostgresOwner) admitRuntimeLogRecord(ctx context.Context, record runtimepkg.RuntimeLogPersistenceRecord) (events.AdmittedEvent, error) {
	constructed, err := runtimeLogEvent(record)
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	admitted, err := events.AdmitForPersistence(constructed, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	return s.ensureEventPayloadAdmission(ctx, admitted)
}

func (s *EventSQLiteOwner) admitRuntimeLogRecord(ctx context.Context, record runtimepkg.RuntimeLogPersistenceRecord) (events.AdmittedEvent, error) {
	constructed, err := runtimeLogEvent(record)
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	admitted, err := events.AdmitForPersistence(constructed, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	return s.ensureEventPayloadAdmission(ctx, admitted)
}

func runtimeLogEvent(record runtimepkg.RuntimeLogPersistenceRecord) (events.Event, error) {
	var event events.Event
	facts := events.EventFacts{
		ID:       record.EventID,
		Type:     events.EventType(runtimeLogEventName),
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload:  json.RawMessage(record.Payload), CreatedAt: record.CreatedAt, ExecutionMode: record.ExecutionMode,
	}
	runID := strings.TrimSpace(record.RunID)
	parentEventID := strings.TrimSpace(record.ParentEventID)
	var err error
	if parentEventID != "" {
		event, err = events.NewCausalDiagnosticDirectEvent(events.CausalRuntimeEventInput{Facts: facts, Lineage: events.EventLineage{
			RunID: runID, ParentEventID: parentEventID, ExecutionMode: record.ExecutionMode,
		}})
	} else if runID != "" {
		event, err = events.NewRunScopedDiagnosticDirectEvent(events.RunScopedRuntimeEventInput{Facts: facts, RunID: runID})
	} else {
		event, err = events.NewStandaloneDiagnosticDirectEvent(events.StandaloneRuntimeEventInput{Facts: facts})
	}
	if err != nil {
		return event, err
	}
	return event, nil
}

func (s *EventPostgresOwner) RuntimeLogLineageParentEventID(ctx context.Context, runID, explicitParentEventID, subjectEventID string) (string, error) {
	explicitParentEventID = strings.TrimSpace(explicitParentEventID)
	if explicitParentEventID != "" {
		return explicitParentEventID, nil
	}
	runID = strings.TrimSpace(runID)
	subjectEventID = strings.TrimSpace(subjectEventID)
	if s == nil || s.backend == nil || runID == "" || subjectEventID == "" {
		return "", nil
	}
	if _, err := uuid.Parse(runID); err != nil {
		return "", err
	}
	if _, err := uuid.Parse(subjectEventID); err != nil {
		return "", nil
	}
	var exists bool
	if err := s.backend.QueryRowContext(ctx, `
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

func (s *EventPostgresOwner) PersistRuntimeLog(ctx context.Context, record runtimepkg.RuntimeLogPersistenceRecord) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	evt, err := admitRuntimeLogRecord(record)
	if err != nil {
		return err
	}
	_, err = s.CommitRuntimeLogEvent(ctx, evt)
	return err
}

func (s *EventSQLiteOwner) RuntimeLogLineageParentEventID(ctx context.Context, runID, explicitParentEventID, subjectEventID string) (string, error) {
	explicitParentEventID = strings.TrimSpace(explicitParentEventID)
	if explicitParentEventID != "" {
		return explicitParentEventID, nil
	}
	runID = strings.TrimSpace(runID)
	subjectEventID = strings.TrimSpace(subjectEventID)
	if s == nil || s.backend == nil || runID == "" || subjectEventID == "" {
		return "", nil
	}
	if _, err := uuid.Parse(runID); err != nil {
		return "", err
	}
	if _, err := uuid.Parse(subjectEventID); err != nil {
		return "", nil
	}
	var exists bool
	if err := s.backend.QueryRowContext(ctx, `
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

func (s *EventSQLiteOwner) PersistRuntimeLog(ctx context.Context, record runtimepkg.RuntimeLogPersistenceRecord) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	evt, err := admitRuntimeLogRecord(record)
	if err != nil {
		return err
	}
	_, err = s.CommitRuntimeLogEvent(ctx, evt)
	return err
}
