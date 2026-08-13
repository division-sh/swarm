package eventpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storeactivityjournal "github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	storescenarioexecution "github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
	"github.com/google/uuid"
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

type execQueryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func requireActiveRunForEvent(ctx context.Context, tx *sql.Tx, eventID string, postgres bool) error {
	return requireActiveRunForEventMode(ctx, tx, eventID, postgres, false)
}

func requireActiveRunForEventMode(ctx context.Context, tx *sql.Tx, eventID string, postgres, allowMissing bool) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event_id is required")
	}
	if tx == nil {
		return errors.New("require active event run: transaction is required")
	}
	query := `SELECT COALESCE(CAST(run_id AS TEXT), '') FROM events WHERE event_id = ?`
	if postgres {
		query = `SELECT COALESCE(run_id::text, '') FROM events WHERE event_id = $1::uuid`
	}
	var runID string
	if err := tx.QueryRowContext(ctx, query, eventID).Scan(&runID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if allowMissing {
				return nil
			}
			return fmt.Errorf("require active event run: event %s not found", eventID)
		}
		return fmt.Errorf("require active event run: %w", err)
	}
	if strings.TrimSpace(runID) == "" {
		return nil
	}
	if postgres {
		return requirePostgresRunActive(ctx, tx, runID)
	}
	return requireSQLiteRunActive(ctx, tx, runID)
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

func (s *EventPostgresOwner) appendAdmittedEventTxOutcome(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if tx == nil {
		outcome := runtimebus.EventAppendOutcomeUnknown
		err := s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			var err error
			outcome, err = s.appendAdmittedEventTxOutcome(txctx, tx, runtimeAuthorActivityMutation(story), admitted, settlement)
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
	return outcome, err
}

func (s *EventPostgresOwner) AppendAdmittedEventTxOutcome(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	return s.appendAdmittedEventTxOutcome(ctx, tx, story, admitted, settlement)
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

func validateOptionalEntityUUID(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if _, err := uuid.Parse(raw); err != nil {
		return "", fmt.Errorf("invalid entity_id %q: must be a UUID", raw)
	}
	return raw, nil
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

func (s *EventPostgresOwner) ListEventDeliveryTargets(ctx context.Context, eventID string) (map[string]events.RouteIdentity, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	snapshots, err := s.DeliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("list event delivery targets: %w", err)
	}
	out := map[string]events.RouteIdentity{}
	for _, snapshot := range snapshots {
		if snapshot.SubscriberClass != runtimedelivery.SubscriberAgent {
			continue
		}
		owner := snapshot.Route.Target
		if owner.Empty() {
			continue
		}
		out[strings.TrimSpace(snapshot.SubscriberID)] = owner.Route()
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
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

func deliveryContextJSON(deliveryContext events.DeliveryContext) json.RawMessage {
	deliveryContext = deliveryContext.Normalized()
	if deliveryContext.Empty() {
		return json.RawMessage(`{}`)
	}
	raw, err := json.Marshal(deliveryContext)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

func decodeDeliveryContextJSON(raw []byte) events.DeliveryContext {
	if len(raw) == 0 {
		return events.DeliveryContext{}
	}
	var deliveryContext events.DeliveryContext
	if err := json.Unmarshal(raw, &deliveryContext); err != nil {
		return events.DeliveryContext{}
	}
	return deliveryContext.Normalized()
}

func deliveryPayloadProjectionJSON(projection events.DeliveryPayloadProjection) (json.RawMessage, error) {
	canonical, err := projection.Canonical()
	if err != nil {
		return nil, err
	}
	raw, err := canonicaljson.Bytes(canonical)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func decodeDeliveryPayloadProjectionJSON(raw []byte) (events.DeliveryPayloadProjection, error) {
	if len(raw) == 0 {
		return events.DeliveryPayloadProjection{}, nil
	}
	var projection events.DeliveryPayloadProjection
	if err := canonicaljson.DecodeInto(raw, &projection); err != nil {
		return events.DeliveryPayloadProjection{}, err
	}
	return projection.Canonical()
}

func deliveryRouteReasonCode(route events.DeliveryRoute) string {
	switch {
	case route.Recipient.IsAgent():
		return "matched_agent_subscription"
	case route.Recipient.IsNode():
		return "matched_node_subscription"
	default:
		return "matched_subscription"
	}
}

func committedReplayScopeReasonCode(scope runtimepipelineobligation.CommittedScope) (string, error) {
	switch scope {
	case runtimepipelineobligation.ScopeDirect:
		return replayScopeReasonDirect, nil
	case runtimepipelineobligation.ScopeSubscribed:
		return replayScopeReasonSubscribed, nil
	default:
		return "", fmt.Errorf("committed replay scope: unsupported scope %q", strings.TrimSpace(string(scope)))
	}
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

func chooseExecQueryer(db execQueryer, tx *sql.Tx) execQueryer {
	if tx != nil {
		return tx
	}
	return db
}

func lookupEventRunID(ctx context.Context, q rowQueryer, eventID string) string {
	eventID = strings.TrimSpace(eventID)
	if q == nil || eventID == "" {
		return ""
	}
	var runID string
	if err := q.QueryRowContext(ctx, `
		SELECT COALESCE(run_id::text, '')
		FROM events
		WHERE event_id = $1::uuid
		LIMIT 1
	`, eventID).Scan(&runID); err != nil {
		return ""
	}
	return strings.TrimSpace(runID)
}

func (s *EventPostgresOwner) ensureRunRow(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, runID, triggerEventID, triggerEventType string) error {
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil
	}
	fact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
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

func eventRouteStorageEnvelope(evt events.Event) (sourceRoute, targetRoute, targetSet []byte) {
	envelope := evt.NormalizedEnvelope()
	sourceRoute = routeIdentityJSON(envelope.Source)
	targetRoute = routeIdentityJSON(envelope.Target)
	targetSet = routeIdentitySetJSON(envelope.TargetSet)
	return sourceRoute, targetRoute, targetSet
}

func eventHasRouteIdentity(evt events.Event) bool {
	envelope := evt.NormalizedEnvelope()
	return !envelope.Source.Empty() || !envelope.Target.Empty() || len(envelope.TargetSet) > 0
}

func routeIdentityJSON(route events.RouteIdentity) []byte {
	route = route.Normalized()
	if route.Empty() {
		return []byte("{}")
	}
	payload := map[string]string{}
	if route.FlowInstance != "" {
		payload["flow_instance"] = route.FlowInstance
	}
	if route.EntityID != "" {
		payload["entity_id"] = route.EntityID
	}
	if route.FlowID != "" {
		payload["flow_id"] = route.FlowID
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return []byte("{}")
	}
	return encoded
}

func routeIdentitySetJSON(routes []events.RouteIdentity) []byte {
	if len(routes) == 0 {
		return []byte("[]")
	}
	payload := make([]map[string]string, 0, len(routes))
	for _, route := range routes {
		route = route.Normalized()
		if route.Empty() {
			continue
		}
		item := map[string]string{}
		if route.FlowInstance != "" {
			item["flow_instance"] = route.FlowInstance
		}
		if route.EntityID != "" {
			item["entity_id"] = route.EntityID
		}
		if route.FlowID != "" {
			item["flow_id"] = route.FlowID
		}
		payload = append(payload, item)
	}
	if len(payload) == 0 {
		return []byte("[]")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return []byte("[]")
	}
	return encoded
}

func decodeRouteIdentityJSON(raw []byte) events.RouteIdentity {
	if len(raw) == 0 {
		return events.RouteIdentity{}
	}
	var route events.RouteIdentity
	if err := json.Unmarshal(raw, &route); err != nil {
		return events.RouteIdentity{}
	}
	return route.Normalized()
}

func mapPipelineStatusToOutcome(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "error", "dead_letter":
		return "dead_letter"
	default:
		return "success"
	}
}

func pipelineReceiptReasonCode(status string, failure *runtimefailures.Envelope) string {
	status = strings.TrimSpace(strings.ToLower(status))
	if failure != nil {
		return strings.TrimSpace(failure.Detail.Code)
	}
	switch status {
	case "dead_letter":
		return "pipeline_dead_letter"
	case "error":
		return "pipeline_error"
	default:
		return "pipeline_persisted"
	}
}
