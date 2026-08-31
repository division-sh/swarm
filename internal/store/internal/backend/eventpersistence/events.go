package eventpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storeactivityjournal "github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storescenarioexecution "github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
)

const (
	replayScopeReasonDirect     = "replay_scope_direct"
	replayScopeReasonSubscribed = "replay_scope_subscribed"
)

const (
	ReplayScopeReasonDirect     = replayScopeReasonDirect
	ReplayScopeReasonSubscribed = replayScopeReasonSubscribed
)

type rowQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type eventReadQueryer interface {
	rowQueryer
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func eventReadQueryerFromDB(db eventReadQueryer) eventReadQueryer {
	return db
}

func (s *EventPostgresOwner) SetEventPayloadValidator(validator func(context.Context, string, []byte) error) {
	if s == nil {
		return
	}
	s.validatorMu.Lock()
	s.validator = validator
	s.validatorMu.Unlock()
}

// validateEventPayload is the store-side canonical admission guard for append
// paths that may not pass through an emit-surface owner immediately before
// persistence.
func (s *EventPostgresOwner) validateEventPayload(ctx context.Context, eventType string, payload []byte) error {
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

func (s *EventPostgresOwner) appendAdmittedEventTxOutcome(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *revisionEffects, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if tx == nil {
		outcome := runtimebus.EventAppendOutcomeUnknown
		err := s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *revisionEffects) error {
			var err error
			outcome, err = s.appendAdmittedEventTxOutcome(txctx, tx, runtimeAuthorActivityMutation(story), effects, admitted, settlement)
			return err
		})
		return outcome, err
	}
	outcome := runtimebus.EventAppendOutcomeUnknown
	err := withEventStoreRetry(ctx, tx, func() error {
		var err error
		outcome, err = s.appendEventSpec(ctx, tx, story, admitted, settlement)
		return err
	})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if runID := strings.TrimSpace(admitted.Event().RunID()); runID != "" {
		if err := effects.Add(runID, privaterunforkrevision.FamilyEvents); err != nil {
			return runtimebus.EventAppendOutcomeUnknown, err
		}
	}
	return outcome, nil
}

func (s *EventPostgresOwner) AppendAdmittedEventTxOutcome(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *revisionEffects, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	return s.appendAdmittedEventTxOutcome(ctx, tx, story, effects, admitted, settlement)
}

func (s *EventPostgresOwner) EventExists(ctx context.Context, eventID string) (bool, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false, nil
	}
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1::uuid)`
	if err := eventReadQueryerFromDB(s.backend).QueryRowContext(ctx, query, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("event exists lookup: %w", err)
	}
	return exists, nil
}

func (s *EventSQLiteOwner) EventExists(ctx context.Context, eventID string) (bool, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false, nil
	}
	var exists bool
	if err := eventReadQueryerFromDB(s.backend).QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE event_id = ?)`, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("event exists lookup: %w", err)
	}
	return exists, nil
}

func (s *EventPostgresOwner) ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	snapshots, err := s.DeliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("list event delivery recipients: %w", err)
	}
	recipients := make([]string, 0, 8)
	for _, snapshot := range snapshots {
		if snapshot.SubscriberClass != runtimedelivery.SubscriberAgent {
			continue
		}
		agentID := strings.TrimSpace(snapshot.SubscriberID)
		if agentID != "" {
			recipients = append(recipients, agentID)
		}
	}
	sort.Strings(recipients)
	return recipients, nil
}

func (s *EventPostgresOwner) ListEventDeliveryRoutes(ctx context.Context, eventID string) ([]events.DeliveryRoute, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	snapshots, err := s.DeliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("list event delivery routes: %w", err)
	}
	out := make([]events.DeliveryRoute, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, snapshot.Route)
	}
	return events.NormalizeDeliveryRoutes(out), nil
}

func (s *EventPostgresOwner) appendEventSpec(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
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
	queryer := chooseRowQueryer(s.backend, tx)
	existingIdentity, found, err := loadPostgresEventIdentity(ctx, queryer, wantIdentity.EventID)
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
	var recordExec eventrecordpostgres.Execer = s.backend
	if tx != nil {
		recordExec = tx
	}
	var ensureErr error
	switch admitted.RunDisposition() {
	case events.AdmittedRunCreateAuthorized:
		ensureErr = s.ensureRunRow(ctx, tx, story, wantIdentity.RunID, wantIdentity.EventID, wantIdentity.EventName)
	case events.AdmittedRunRequireActive:
		ensureErr = requirePostgresRunActive(ctx, tx, wantIdentity.RunID)
		if ensureErr == nil {
			ensureErr = storescenarioexecution.RequirePostgresFromContext(ctx, tx, wantIdentity.RunID)
		}
	case events.AdmittedRunRequirePresent:
		if evt.AdmissionClass() != events.EventAdmissionDiagnosticDirect || evt.Type() != events.EventTypePlatformRuntimeLog || strings.TrimSpace(wantIdentity.RunID) == "" {
			ensureErr = fmt.Errorf("event %s has invalid require-present run disposition", wantIdentity.EventID)
		} else {
			ensureErr = s.ensureRuntimeLogRunRow(ctx, tx, wantIdentity.RunID, wantIdentity.EventID, wantIdentity.EventName)
		}
	case events.AdmittedRunless:
		if strings.TrimSpace(wantIdentity.RunID) != "" {
			ensureErr = fmt.Errorf("event %s has runless disposition with run_id", wantIdentity.EventID)
		}
	default:
		ensureErr = fmt.Errorf("event %s has invalid admitted run disposition %q", wantIdentity.EventID, admitted.RunDisposition())
	}
	if ensureErr != nil {
		if errors.Is(ensureErr, runtimerunlifecycle.ErrRunNotActive) {
			existingIdentity, found, loadErr := loadPostgresEventIdentity(ctx, queryer, wantIdentity.EventID)
			if loadErr != nil {
				return runtimebus.EventAppendOutcomeUnknown, loadErr
			}
			duplicate, duplicateErr := resolveExistingEventIdentity(wantIdentity.EventID, wantIdentity, existingIdentity, found)
			if duplicateErr != nil {
				return runtimebus.EventAppendOutcomeUnknown, duplicateErr
			}
			if duplicate {
				return runtimebus.EventAppendExactDuplicate, nil
			}
		}
		return runtimebus.EventAppendOutcomeUnknown, ensureErr
	}
	if err := requireEventOwnedReferences(ctx, tx, true, wantIdentity); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	inserted, err := eventrecordpostgres.Insert(ctx, recordExec, wantIdentity)
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if !inserted {
		existingIdentity, found, err := loadPostgresEventIdentity(ctx, queryer, wantIdentity.EventID)
		if err != nil {
			return runtimebus.EventAppendOutcomeUnknown, err
		}
		duplicate, err := resolveExistingEventIdentity(wantIdentity.EventID, wantIdentity, existingIdentity, found)
		if err != nil {
			return runtimebus.EventAppendOutcomeUnknown, err
		}
		if !duplicate {
			return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("append event: event_id=%s was not inserted", wantIdentity.EventID)
		}
		return runtimebus.EventAppendExactDuplicate, nil
	}
	if admitted.RunDisposition() != events.AdmittedRunless {
		if err := s.RunLifecyclePostgresOwner.SyncCountersTx(ctx, tx, story, wantIdentity.RunID); err != nil {
			return runtimebus.EventAppendOutcomeUnknown, err
		}
	}
	if err := storeactivityjournal.RecordPersistedEvent(ctx, story, s, admitted, wantIdentity.ProducedBy, string(wantIdentity.ProducedByType)); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if err := storeactivityjournal.RecordNoDeliveryWarning(ctx, story, admitted, settlement); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return runtimebus.EventAppendInserted, nil
}

func withEventStoreRetry(ctx context.Context, tx *sql.Tx, fn func() error) error {
	if fn == nil {
		return nil
	}
	attempts := 1
	if tx == nil {
		attempts = 3
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		lastErr = fn()
		if !isTransientEventStoreConnectionError(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

func isTransientEventStoreConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrConnDone) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "bad connection")
}

func committedReplayScopeFromReasonCode(reasonCode string) (runtimepipelineobligation.CommittedScope, bool) {
	switch strings.TrimSpace(reasonCode) {
	case replayScopeReasonDirect:
		return runtimepipelineobligation.ScopeDirect, true
	case replayScopeReasonSubscribed:
		return runtimepipelineobligation.ScopeSubscribed, true
	default:
		return "", false
	}
}

func CommittedReplayScopeFromReasonCode(reasonCode string) (runtimepipelineobligation.CommittedScope, bool) {
	return committedReplayScopeFromReasonCode(reasonCode)
}

func chooseRowQueryer(db rowQueryer, tx *sql.Tx) rowQueryer {
	if tx != nil {
		return tx
	}
	return db
}

func (s *EventPostgresOwner) ensureRunRow(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, runID, triggerEventID, triggerEventType string) error {
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil
	}
	fact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		return fmt.Errorf("ensure run row: executable bundle source fact is required")
	}
	origin, err := runtimerunlifecycle.EventRunOrigin(triggerEventID, triggerEventType)
	if err != nil {
		return fmt.Errorf("ensure run row origin: %w", err)
	}
	_, err = s.RunLifecyclePostgresOwner.CreateRunTx(ctx, tx, story, runtimerunlifecycle.CreateRequest{
		RunID: runID, Origin: origin, Source: fact, StartedAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	return storescenarioexecution.EnsurePostgresFromContext(ctx, tx, runID, time.Now().UTC())
}

func (s *EventPostgresOwner) ensureRuntimeLogRunRow(ctx context.Context, tx *sql.Tx, runID, triggerEventID, triggerEventType string) error {
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil
	}
	return s.RunLifecyclePostgresOwner.RequirePresentTx(ctx, tx, runID)
}
