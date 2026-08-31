package eventpersistence

import (
	"bytes"
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

const RuntimeLogEventName = runtimeLogEventName

func runtimeLogEvent(record runtimepkg.RuntimeLogPersistenceRecord) (events.Event, error) {
	var event events.Event
	if !record.PayloadAdmission.Valid() {
		return event, fmt.Errorf("runtime log payload admission evidence is required")
	}
	if !bytes.Equal(record.Payload, record.PayloadAdmission.Payload()) {
		return event, fmt.Errorf("runtime log payload differs from its admission evidence")
	}
	facts := events.EventFacts{
		Type:     events.EventType(runtimeLogEventName),
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload:  json.RawMessage(record.Payload), CreatedAt: time.Time{}, ExecutionMode: record.ExecutionMode,
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
	return events.ApplyPayloadAdmission(event, record.PayloadAdmission)
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
	constructed, err := runtimeLogEvent(record)
	if err != nil {
		return err
	}
	evt, err := events.AdmitForPersistence(constructed, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
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
	constructed, err := runtimeLogEvent(record)
	if err != nil {
		return err
	}
	evt, err := events.AdmitForPersistence(constructed, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	_, err = s.CommitRuntimeLogEvent(ctx, evt)
	return err
}
