package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

const runtimeLogEventName = "platform.runtime_log"

func runtimeLogEvent(ctx context.Context, record runtimepkg.RuntimeLogPersistenceRecord) events.Event {
	evt := events.NewDiagnosticDirectEvent(
		"",
		events.EventType(runtimeLogEventName),
		events.PlatformProducer("runtime"),
		"",
		json.RawMessage(record.Payload),
		0,
		strings.TrimSpace(record.RunID),
		strings.TrimSpace(record.ParentEventID),
		events.EventEnvelope{},
		time.Time{},
	)
	if mode, ok := runtimeeffects.ExecutionModeFromContext(ctx); ok {
		evt = evt.WithExecutionMode(mode)
	}
	return evt
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
	enabled, _, err := s.CanonicalRuntimeLogCapability(ctx)
	if err != nil {
		return err
	}
	if !enabled {
		return unsupportedSchemaCapability("events", SchemaFlavorUnavailable)
	}
	if err := s.validateEventPayload(ctx, runtimeLogEventName, record.Payload); err != nil {
		return err
	}
	evt, err := events.AdmitForPersistence(runtimeLogEvent(ctx, record), events.AdmissionOptions{})
	if err != nil {
		return err
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return s.AppendEventTx(withDiagnosticDirectOwner(ctx, diagnosticDirectRuntimeLog), tx, evt)
	}
	return s.AppendEvent(withDiagnosticDirectOwner(ctx, diagnosticDirectRuntimeLog), evt)
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
	enabled, _, err := s.CanonicalRuntimeLogCapability(ctx)
	if err != nil {
		return err
	}
	if !enabled {
		return unsupportedSchemaCapability("events", SchemaFlavorUnavailable)
	}
	if err := s.validateEventPayload(ctx, runtimeLogEventName, record.Payload); err != nil {
		return err
	}
	evt, err := events.AdmitForPersistence(runtimeLogEvent(ctx, record), events.AdmissionOptions{})
	if err != nil {
		return err
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return s.AppendEventTx(withDiagnosticDirectOwner(ctx, diagnosticDirectRuntimeLog), tx, evt)
	}
	return s.AppendEvent(withDiagnosticDirectOwner(ctx, diagnosticDirectRuntimeLog), evt)
}
