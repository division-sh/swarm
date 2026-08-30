package eventpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storeactivityjournal "github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	storescenarioexecution "github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
	"github.com/google/uuid"
)

func (s *EventSQLiteOwner) SetEventPayloadValidator(validator func(context.Context, string, []byte) error) {
	if s == nil {
		return
	}
	s.validatorMu.Lock()
	s.validator = validator
	s.validatorMu.Unlock()
}

func (s *EventSQLiteOwner) validateEventPayload(ctx context.Context, eventType string, payload []byte) error {
	if s == nil {
		return nil
	}
	s.validatorMu.RLock()
	validator := s.validator
	s.validatorMu.RUnlock()
	if validator == nil {
		return nil
	}
	if err := validator(ctx, strings.TrimSpace(eventType), payload); err != nil {
		return fmt.Errorf("validate event payload: %w", err)
	}
	return nil
}

func (s *EventSQLiteOwner) appendAdmittedEventTxOutcome(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *revisionEffects, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if tx == nil {
		outcome := runtimebus.EventAppendOutcomeUnknown
		err := s.runPrivateAuthorActivityMutation(ctx, "sqlite append admitted event", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *revisionEffects) error {
			var err error
			outcome, err = s.appendAdmittedEventTxOutcome(txctx, tx, runtimeAuthorActivityMutation(story), effects, admitted, settlement)
			return err
		})
		return outcome, err
	}
	if story == nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("persisted event author activity mutation is required")
	}
	evt := admitted.Event()
	wantIdentity, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if err := s.validateEventPayload(ctx, wantIdentity.EventName, wantIdentity.Payload); err != nil {
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
		return runtimebus.EventAppendExactDuplicate, nil
	}
	var ensureErr error
	switch admitted.RunDisposition() {
	case events.AdmittedRunCreateAuthorized:
		ensureErr = s.ensureActiveRunRow(ctx, tx, story, wantIdentity.RunID, wantIdentity.EventID, wantIdentity.EventName, wantIdentity.CreatedAt)
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
	inserted, err := eventrecordsqlite.Insert(ctx, tx, wantIdentity)
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
		return runtimebus.EventAppendExactDuplicate, nil
	}
	if admitted.RunDisposition() != events.AdmittedRunless {
		if err := s.RunLifecycleSQLiteOwner.SyncCountersTx(ctx, tx, story, wantIdentity.RunID); err != nil {
			return runtimebus.EventAppendOutcomeUnknown, err
		}
	}
	if err := storeactivityjournal.RecordPersistedEvent(ctx, story, s, admitted, wantIdentity.ProducedBy, string(wantIdentity.ProducedByType)); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if err := storeactivityjournal.RecordNoDeliveryWarning(ctx, story, admitted, settlement); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if runID := strings.TrimSpace(admitted.Event().RunID()); runID != "" {
		if err := effects.Add(runID, privaterunforkrevision.FamilyEvents); err != nil {
			return runtimebus.EventAppendOutcomeUnknown, err
		}
	}
	return runtimebus.EventAppendInserted, nil
}

func (s *EventSQLiteOwner) AppendAdmittedEventTxOutcome(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *revisionEffects, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	return s.appendAdmittedEventTxOutcome(ctx, tx, story, effects, admitted, settlement)
}

func (s *EventSQLiteOwner) ensureActiveRunRow(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, runID, triggerEventID, triggerEventType string, now time.Time) error {
	fact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		return fmt.Errorf("ensure active sqlite run row: executable bundle source fact is required")
	}
	origin, err := runtimerunlifecycle.EventRunOrigin(triggerEventID, triggerEventType)
	if err != nil {
		return fmt.Errorf("ensure active sqlite run row origin: %w", err)
	}
	_, err = s.RunLifecycleSQLiteOwner.CreateRunTx(ctx, tx, story, runtimerunlifecycle.CreateRequest{
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
