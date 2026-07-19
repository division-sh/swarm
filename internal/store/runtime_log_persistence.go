package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

const runtimeLogEventName = "platform.runtime_log"

func runtimeLogEvent(record runtimepkg.RuntimeLogPersistenceRecord) (events.Event, error) {
	return events.NewDiagnosticDirectEvent(events.DiagnosticDirectEventInput{
		Facts: events.EventFacts{
			Type:     events.EventType(runtimeLogEventName),
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
			Payload:  json.RawMessage(record.Payload), CreatedAt: time.Time{}, ExecutionMode: record.ExecutionMode,
		},
		RunID: strings.TrimSpace(record.RunID), ParentEventID: strings.TrimSpace(record.ParentEventID),
	})
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
	queryer := rowQueryer(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		queryer = tx
	}
	var exists bool
	if err := queryer.QueryRowContext(ctx, `
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
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if err := s.validateEventPayload(ctx, runtimeLogEventName, record.Payload); err != nil {
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
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		_, err := s.appendAdmittedEventTxOutcome(ctx, tx, evt)
		return err
	}
	_, err = s.commitRuntimeLogEvent(ctx, evt)
	return err
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
	queryer := rowQueryer(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		queryer = tx
	}
	var exists bool
	if err := queryer.QueryRowContext(ctx, `
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
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if err := s.validateEventPayload(ctx, runtimeLogEventName, record.Payload); err != nil {
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
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		_, err := s.appendAdmittedEventTxOutcome(ctx, tx, evt)
		return err
	}
	_, err = s.commitRuntimeLogEvent(ctx, evt)
	return err
}
