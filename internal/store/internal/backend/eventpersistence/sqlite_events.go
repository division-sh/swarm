package eventpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storeactivityjournal "github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	storescenarioexecution "github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
	"github.com/google/uuid"
)

func (s *EventSQLiteOwner) SetEventPayloadAdmitter(admitter runtimebus.PayloadAdmitter) {
	if s == nil {
		return
	}
	s.payloadAdmitterMu.Lock()
	s.payloadAdmitter = admitter
	s.payloadAdmitterMu.Unlock()
}

func (s *EventSQLiteOwner) ensureEventPayloadAdmission(ctx context.Context, admitted events.AdmittedEvent) (events.AdmittedEvent, error) {
	if s == nil {
		return events.AdmittedEvent{}, fmt.Errorf("sqlite event owner is required")
	}
	event := admitted.Event()
	if _, ok := event.PayloadAdmission(); ok {
		return admitted, nil
	}
	s.payloadAdmitterMu.RLock()
	admitter := s.payloadAdmitter
	s.payloadAdmitterMu.RUnlock()
	if admitter == nil {
		return events.AdmittedEvent{}, fmt.Errorf("event payload admission evidence is required")
	}
	flowID := strings.TrimSpace(event.RoutingSource().Route().FlowID)
	payload, err := admitter(ctx, event, flowID)
	if err != nil {
		return events.AdmittedEvent{}, fmt.Errorf("admit event payload: %w", err)
	}
	restored, err := events.ApplyAdmittedPayload(admitted, payload)
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	return restored, nil
}

func (s *EventSQLiteOwner) appendAdmittedEventTxOutcome(ctx context.Context, attempt *mutationprotocol.Attempt, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if attempt == nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("event append mutation attempt is required")
	}
	var outcome runtimebus.EventAppendOutcome
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var writeErr error
		outcome, writeErr = s.appendEventSpec(ctx, tx, attempt, admitted, settlement)
		return writeErr
	})
	return outcome, err
}

func (s *EventSQLiteOwner) appendEventSpec(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	admitted, err := s.ensureEventPayloadAdmission(ctx, admitted)
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	evt := admitted.Event()
	wantIdentity, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	existingIdentity, found, err := loadSQLiteEventIdentity(ctx, tx, wantIdentity.EventID)
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	duplicate, err := resolveExistingEventIdentity(wantIdentity.EventID, wantIdentity, existingIdentity, found)
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if duplicate {
		return runtimebus.EventAppendExactDuplicate, s.validateDuplicatePublicationTx(ctx, tx, existingIdentity)
	}
	var ensureErr error
	switch admitted.RunDisposition() {
	case events.AdmittedRunCreateAuthorized:
		ensureErr = s.ensureActiveRunRow(ctx, tx, attempt, wantIdentity.RunID, wantIdentity.EventID, wantIdentity.EventName, wantIdentity.CreatedAt)
	case events.AdmittedRunRequireActive:
		ensureErr = storerunstate.RequireSQLiteActiveTx(ctx, tx, wantIdentity.RunID)
		if ensureErr == nil {
			ensureErr = storescenarioexecution.RequireSQLiteFromContext(ctx, tx, wantIdentity.RunID)
		}
	case events.AdmittedRunRequirePresent:
		if evt.AdmissionClass() != events.EventAdmissionDiagnosticDirect || evt.Type() != events.EventTypePlatformRuntimeLog || strings.TrimSpace(wantIdentity.RunID) == "" {
			ensureErr = fmt.Errorf("event %s has invalid require-present run disposition", wantIdentity.EventID)
		} else {
			ensureErr = s.requireRunRowPresent(ctx, tx, wantIdentity.RunID)
		}
	case events.AdmittedRunless:
		if strings.TrimSpace(wantIdentity.RunID) != "" {
			ensureErr = fmt.Errorf("event %s has runless disposition with run_id", wantIdentity.EventID)
		}
	default:
		ensureErr = fmt.Errorf("event %s has invalid admitted run disposition %q", wantIdentity.EventID, admitted.RunDisposition())
	}
	if ensureErr != nil {
		return runtimebus.EventAppendOutcomeUnknown, ensureErr
	}
	if err := requireEventOwnedReferences(ctx, tx, false, wantIdentity); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	inserted, err := eventrecordsqlite.Insert(ctx, attempt, wantIdentity)
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if !inserted {
		existingIdentity, found, err := loadSQLiteEventIdentity(ctx, tx, wantIdentity.EventID)
		if err != nil {
			return runtimebus.EventAppendOutcomeUnknown, err
		}
		duplicate, err := resolveExistingEventIdentity(wantIdentity.EventID, wantIdentity, existingIdentity, found)
		if err != nil {
			return runtimebus.EventAppendOutcomeUnknown, err
		}
		if !duplicate {
			return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("append sqlite event: event_id=%s was not inserted", wantIdentity.EventID)
		}
		return runtimebus.EventAppendExactDuplicate, s.validateDuplicatePublicationTx(ctx, tx, existingIdentity)
	}
	if admitted.RunDisposition() != events.AdmittedRunless {
		if err := s.RunLifecycleSQLiteOwner.SyncCountersTx(ctx, attempt, wantIdentity.RunID); err != nil {
			return runtimebus.EventAppendOutcomeUnknown, err
		}
	}
	if err := storeactivityjournal.RecordPersistedEvent(ctx, attempt, s, admitted, wantIdentity.ProducedBy, string(wantIdentity.ProducedByType)); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if err := storeactivityjournal.RecordNoDeliveryWarning(ctx, attempt, admitted, settlement); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return runtimebus.EventAppendInserted, nil
}

func (s *EventSQLiteOwner) AppendAdmittedEventTxOutcome(ctx context.Context, attempt *mutationprotocol.Attempt, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	if admitted.Event().AdmissionClass() == events.EventAdmissionInheritedFanOut {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("inherited fan-out origin requires named chunk publication")
	}
	return s.appendAdmittedEventTxOutcome(ctx, attempt, admitted, settlement)
}

func (s *EventSQLiteOwner) ensureActiveRunRow(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, runID, triggerEventID, triggerEventType string, now time.Time) error {
	fact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		return fmt.Errorf("ensure active sqlite run row: executable bundle source fact is required")
	}
	origin, err := runtimerunlifecycle.EventRunOrigin(triggerEventID, triggerEventType)
	if err != nil {
		return fmt.Errorf("ensure active sqlite run row origin: %w", err)
	}
	_, err = s.RunLifecycleSQLiteOwner.CreateRunTx(ctx, attempt, runtimerunlifecycle.CreateRequest{
		RunID: runID, Origin: origin, Source: fact, StartedAt: runtimerunlifecycle.CanonicalTimestamp(now),
	})
	if err != nil {
		return err
	}
	return storescenarioexecution.EnsureSQLiteFromContext(ctx, tx, runID, runtimerunlifecycle.CanonicalTimestamp(now))
}

func (s *EventSQLiteOwner) requireRunRowPresent(ctx context.Context, tx *sql.Tx, runID string) error {
	runID = validUUIDString(runID)
	if runID == "" {
		return nil
	}
	return s.RunLifecycleSQLiteOwner.RequirePresentTx(ctx, tx, runID)
}

func validUUIDString(raw string) string {
	raw = strings.TrimSpace(raw)
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}
