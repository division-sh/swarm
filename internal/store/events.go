package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

const (
	replayScopeMarkerSubscriberType = "node"
	replayScopeMarkerSubscriberID   = "__runtime_replay_scope__"
	replayScopeReasonDirect         = "replay_scope_direct"
	replayScopeReasonSubscribed     = "replay_scope_subscribed"
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

func requireActiveRunForEvent(ctx context.Context, db storerunlifecycle.DBTX, eventID string, dialect storerunlifecycle.Dialect) error {
	return requireActiveRunForEventMode(ctx, db, eventID, dialect, false)
}

func requireActiveRunForEventMode(ctx context.Context, db storerunlifecycle.DBTX, eventID string, dialect storerunlifecycle.Dialect, allowMissing bool) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event_id is required")
	}
	var query string
	switch dialect {
	case storerunlifecycle.DialectPostgres:
		query = `SELECT COALESCE(run_id::text, '') FROM events WHERE event_id = $1::uuid`
	case storerunlifecycle.DialectSQLite:
		query = `SELECT COALESCE(CAST(run_id AS TEXT), '') FROM events WHERE event_id = ?`
	default:
		return fmt.Errorf("require active event run: unsupported dialect %q", dialect)
	}
	var runID string
	if err := db.QueryRowContext(ctx, query, eventID).Scan(&runID); err != nil {
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
	return storerunlifecycle.RequireActive(ctx, db, runID, dialect)
}

func requireEventRunNotForked(ctx context.Context, db storerunlifecycle.DBTX, eventID string, dialect storerunlifecycle.Dialect, allowMissing bool) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event_id is required")
	}
	var eventQuery, runQuery string
	switch dialect {
	case storerunlifecycle.DialectPostgres:
		eventQuery = `SELECT COALESCE(run_id::text, '') FROM events WHERE event_id = $1::uuid`
		runQuery = `SELECT COALESCE(status, '') FROM runs WHERE run_id = $1::uuid FOR UPDATE`
	case storerunlifecycle.DialectSQLite:
		eventQuery = `SELECT COALESCE(CAST(run_id AS TEXT), '') FROM events WHERE event_id = ?`
		runQuery = `SELECT COALESCE(status, '') FROM runs WHERE run_id = ?`
	default:
		return fmt.Errorf("require non-forked event run: unsupported dialect %q", dialect)
	}
	var runID string
	if err := db.QueryRowContext(ctx, eventQuery, eventID).Scan(&runID); err != nil {
		if errors.Is(err, sql.ErrNoRows) && allowMissing {
			return nil
		}
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("require non-forked event run: event %s not found", eventID)
		}
		return fmt.Errorf("require non-forked event run: %w", err)
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil
	}
	var status string
	if err := db.QueryRowContext(ctx, runQuery, runID).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &storerunlifecycle.RunNotFoundError{RunID: runID}
		}
		return fmt.Errorf("require non-forked event run: %w", err)
	}
	status = strings.ToLower(strings.TrimSpace(status))
	if status == RunForkSourceFrozenStatus {
		return &storerunlifecycle.RunNotActiveError{RunID: runID, Status: status}
	}
	return nil
}

func requireActiveRunForPipelineReceipt(ctx context.Context, db storerunlifecycle.DBTX, eventID string, dialect storerunlifecycle.Dialect) error {
	var query string
	switch dialect {
	case storerunlifecycle.DialectPostgres:
		query = `SELECT EXISTS (SELECT 1 FROM event_deliveries WHERE event_id = $1::uuid AND status = 'dead_letter' AND reason_code IN ($2, $3))`
	case storerunlifecycle.DialectSQLite:
		query = `SELECT EXISTS (SELECT 1 FROM event_deliveries WHERE event_id = ? AND status = 'dead_letter' AND reason_code IN (?, ?))`
	default:
		return fmt.Errorf("require active pipeline receipt run: unsupported dialect %q", dialect)
	}
	var quiesced bool
	if err := db.QueryRowContext(ctx, query, strings.TrimSpace(eventID), destructivereset.QuiescenceReasonCode, runtimerunquiescence.ServeAbandonReasonCode).Scan(&quiesced); err != nil {
		return fmt.Errorf("inspect pipeline receipt quiescence: %w", err)
	}
	if quiesced {
		return nil
	}
	return requireActiveRunForEvent(ctx, db, eventID, dialect)
}

func eventReadQueryerFromContext(ctx context.Context, db *sql.DB) eventReadQueryer {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return tx
	}
	if conn, ok := runtimepipeline.PipelineSQLConnFromContext(ctx); ok {
		return conn
	}
	return db
}

func (s *PostgresStore) SetEventPayloadValidator(validator func(context.Context, string, []byte) error) {
	if s == nil {
		return
	}
	s.eventPayloadValidator = EventPayloadValidator(validator)
}

// validateEventPayload is the store-side canonical admission guard for append
// paths that may not pass through an emit-surface owner immediately before
// persistence.
func (s *PostgresStore) validateEventPayload(ctx context.Context, eventType string, payload []byte) error {
	if s == nil || s.eventPayloadValidator == nil {
		return nil
	}
	if err := s.eventPayloadValidator(ctx, strings.TrimSpace(eventType), payload); err != nil {
		return fmt.Errorf("validate event payload: %w", err)
	}
	return nil
}

func (s *PostgresStore) AppendEvent(ctx context.Context, evt events.Event) error {
	_, err := s.AppendEventOutcome(ctx, evt)
	return err
}

func (s *PostgresStore) AppendEventOutcome(ctx context.Context, evt events.Event) (runtimebus.EventAppendOutcome, error) {
	return s.AppendEventTxOutcome(ctx, nil, evt)
}

func (s *PostgresStore) BeginEventTx(ctx context.Context) (*sql.Tx, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return s.DB.BeginTx(ctx, nil)
}

func (s *PostgresStore) AppendEventTx(ctx context.Context, tx *sql.Tx, evt events.Event) error {
	_, err := s.AppendEventTxOutcome(ctx, tx, evt)
	return err
}

func (s *PostgresStore) AppendEventTxOutcome(ctx context.Context, tx *sql.Tx, evt events.Event) (runtimebus.EventAppendOutcome, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if tx == nil {
		outcome := runtimebus.EventAppendOutcomeUnknown
		err := s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			outcome, err = s.AppendEventTxOutcome(txctx, tx, evt)
			return err
		})
		return outcome, err
	}
	if err := validateDiagnosticDirectOwner(ctx, evt); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	outcome := runtimebus.EventAppendOutcomeUnknown
	err := withEventStoreRetry(ctx, tx, func() error {
		outcome = runtimebus.EventAppendOutcomeUnknown
		var err error
		evt, err = events.AdmitForPersistence(evt, s.eventPersistenceAdmissionOptions(ctx, chooseRowQueryer(s.DB, tx), evt))
		if err != nil {
			return err
		}
		outcome, err = s.appendEventSpec(ctx, tx, evt)
		return err
	})
	return outcome, err
}

func (s *PostgresStore) PersistEventWithDeliveries(ctx context.Context, evt events.Event, agentIDs []string) error {
	_, err := s.PersistEventWithDeliveriesOutcome(ctx, evt, agentIDs)
	return err
}

func (s *PostgresStore) PersistEventWithDeliveriesOutcome(ctx context.Context, evt events.Event, agentIDs []string) (runtimebus.EventAppendOutcome, error) {
	if err := rejectDiagnosticDirectDeliveryPersistence(evt); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	var err error
	evt, err = events.AdmitForPersistence(evt, s.eventPersistenceAdmissionOptions(ctx, chooseRowQueryer(s.DB, nil), evt))
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return s.persistEventWithDeliveriesSpec(ctx, evt, agentIDs)
}

func (s *PostgresStore) PersistEventWithDeliveriesAndScope(
	ctx context.Context,
	evt events.Event,
	agentIDs []string,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	_, err := s.PersistEventWithDeliveriesAndScopeOutcome(ctx, evt, agentIDs, scope)
	return err
}

func (s *PostgresStore) PersistEventWithDeliveriesAndScopeOutcome(
	ctx context.Context,
	evt events.Event,
	agentIDs []string,
	scope runtimereplayclaim.CommittedReplayScope,
) (runtimebus.EventAppendOutcome, error) {
	if err := rejectDiagnosticDirectDeliveryPersistence(evt); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	var err error
	evt, err = events.AdmitForPersistence(evt, s.eventPersistenceAdmissionOptions(ctx, chooseRowQueryer(s.DB, nil), evt))
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return s.persistEventWithDeliveriesAndScopeSpec(ctx, evt, agentIDs, scope)
}

func (s *PostgresStore) PersistEventWithDeliveryRoutesAndScope(
	ctx context.Context,
	evt events.Event,
	agentIDs []string,
	deliveryTargets map[string]events.RouteIdentity,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	_, err := s.PersistEventWithDeliveryRoutesAndScopeOutcome(ctx, evt, agentIDs, deliveryTargets, scope)
	return err
}

func (s *PostgresStore) PersistEventWithDeliveryRoutesAndScopeOutcome(
	ctx context.Context,
	evt events.Event,
	agentIDs []string,
	deliveryTargets map[string]events.RouteIdentity,
	scope runtimereplayclaim.CommittedReplayScope,
) (runtimebus.EventAppendOutcome, error) {
	if err := rejectDiagnosticDirectDeliveryPersistence(evt); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	var err error
	evt, err = events.AdmitForPersistence(evt, s.eventPersistenceAdmissionOptions(ctx, chooseRowQueryer(s.DB, nil), evt))
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return s.persistEventWithDeliveryRoutesAndScopeSpec(ctx, evt, agentIDs, deliveryTargets, scope)
}

func (s *PostgresStore) PersistEventWithDeliveryRouteSetAndScope(
	ctx context.Context,
	evt events.Event,
	deliveryRoutes []events.DeliveryRoute,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	_, err := s.PersistEventWithDeliveryRouteSetAndScopeOutcome(ctx, evt, deliveryRoutes, scope)
	return err
}

func (s *PostgresStore) PersistEventWithDeliveryRouteSetAndScopeOutcome(
	ctx context.Context,
	evt events.Event,
	deliveryRoutes []events.DeliveryRoute,
	scope runtimereplayclaim.CommittedReplayScope,
) (runtimebus.EventAppendOutcome, error) {
	if err := rejectDiagnosticDirectDeliveryPersistence(evt); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	var err error
	evt, err = events.AdmitForPersistence(evt, s.eventPersistenceAdmissionOptions(ctx, chooseRowQueryer(s.DB, nil), evt))
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return s.persistEventWithDeliveryRouteSetAndScopeSpec(ctx, evt, deliveryRoutes, scope)
}

func (s *PostgresStore) eventPersistenceAdmissionOptions(ctx context.Context, q rowQueryer, evt events.Event) events.AdmissionOptions {
	runID := strings.TrimSpace(evt.RunID())
	if runID == "" {
		if lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx); ok {
			runID = strings.TrimSpace(lineage.RunID)
		}
	}
	parentID := strings.TrimSpace(evt.ParentEventID())
	if parentID == "" {
		if lineageParentID := runtimecorrelation.RuntimeLineageParentForEvent(ctx, evt.ID()); lineageParentID != "" {
			parentID = lineageParentID
		}
	}
	if parentID == "" {
		if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
			if inboundID := strings.TrimSpace(inbound.ID()); inboundID != "" && inboundID != strings.TrimSpace(evt.ID()) {
				parentID = inboundID
			}
		}
	}
	if runID == "" && parentID != "" {
		if foundRunID := lookupEventRunID(ctx, q, parentID); foundRunID != "" {
			runID = foundRunID
		}
	}
	return events.AdmissionOptions{
		RunIDCandidate:                runID,
		ParentEventIDCandidate:        parentID,
		SelectedForkLineageOwner:      selectedForkLineageOwnerFromContext(ctx),
		RequirePersistentUUIDIdentity: true,
	}
}

func selectedForkLineageOwnerFromContext(ctx context.Context) string {
	lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx)
	if !ok || !lineage.SelectedForkContext {
		return ""
	}
	if lineage.Classification != runtimecorrelation.RuntimeLineageClassificationForkLocal {
		return ""
	}
	if lineage.RowCategory != runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer {
		return ""
	}
	return strings.TrimSpace(lineage.SelectedForkOwner)
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

func (s *PostgresStore) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	return s.InsertEventDeliveriesTx(ctx, nil, eventID, agentIDs)
}

func (s *PostgresStore) InsertEventDeliveriesTx(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if len(agentIDs) == 0 {
		return nil
	}
	if tx == nil {
		return s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			return s.InsertEventDeliveriesTx(txctx, tx, eventID, agentIDs)
		})
	}
	if err := requireActiveRunForEvent(ctx, tx, eventID, storerunlifecycle.DialectPostgres); err != nil {
		return err
	}
	return s.insertEventDeliveriesSpec(ctx, tx, eventID, agentIDs)
}

func (s *PostgresStore) UpsertCommittedReplayScope(
	ctx context.Context,
	eventID string,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	return s.UpsertCommittedReplayScopeTx(ctx, nil, eventID, scope)
}

func (s *PostgresStore) UpsertCommittedReplayScopeTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if tx == nil {
		return s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			return s.UpsertCommittedReplayScopeTx(txctx, tx, eventID, scope)
		})
	}
	if err := requireActiveRunForEvent(ctx, tx, eventID, storerunlifecycle.DialectPostgres); err != nil {
		return err
	}
	return s.upsertCommittedReplayScopeSpec(ctx, tx, eventID, scope)
}

func (s *PostgresStore) UpsertPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, failure)
}

func (s *PostgresStore) HasProcessedPipelineReceipt(ctx context.Context, eventID string) (bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false, nil
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	var processed bool
	err := eventReadQueryerFromContext(ctx, s.DB).QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM event_receipts
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'platform'
			  AND subscriber_id = 'pipeline'
			  AND outcome = 'success'
			  AND reason_code = 'pipeline_persisted'
		)
	`, eventID).Scan(&processed)
	if err != nil {
		return false, fmt.Errorf("load processed pipeline receipt: %w", err)
	}
	return processed, nil
}

func (s *PostgresStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status string, failure *runtimefailures.Envelope) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if tx == nil {
		return s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			return s.UpsertPipelineReceiptTx(txctx, tx, eventID, status, failure)
		})
	}
	if err := requireActiveRunForPipelineReceipt(ctx, tx, eventID, storerunlifecycle.DialectPostgres); err != nil {
		return err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "" {
		status = "processed"
	}
	if failure != nil && status == "processed" {
		status = "error"
	}
	return s.upsertPipelineReceiptSpec(ctx, tx, eventID, status, failure)
}

func (s *PostgresStore) ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	return s.listEventsMissingPipelineReceiptSpec(ctx, since, limit)
}

func (s *PostgresStore) ListEventsMissingPipelineReceiptForRun(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	return s.listEventsMissingPipelineReceiptForRunSpec(ctx, runID, since, limit)
}

func (s *PostgresStore) ListEventsWithPendingDeliveriesForRun(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	return s.listEventsWithPendingDeliveriesForRunSpec(ctx, runID, since, limit)
}

func (s *PostgresStore) ClaimPipelineReplay(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, false, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, false, fmt.Errorf("event_id is required")
	}
	lease, claimed, err := acquireAdvisoryLockLease(ctx, s.DB, replayClaimLockKey(eventID))
	if err != nil || !claimed {
		return nil, claimed, err
	}
	pendingQuery := `
		SELECT EXISTS (
			SELECT 1
			FROM events e
			LEFT JOIN runs run ON run.run_id = e.run_id
			LEFT JOIN event_receipts r
				ON r.event_id = e.event_id
				AND r.subscriber_type = 'platform'
				AND r.subscriber_id = 'pipeline'
			WHERE e.event_id = $1::uuid
			  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
			  AND r.event_id IS NULL
		)
	`
	var pending bool
	err = lease.conn.QueryRowContext(ctx, pendingQuery, eventID).Scan(&pending)
	if err != nil {
		_ = lease.Release(ctx)
		return nil, false, fmt.Errorf("claim pipeline replay: %w", err)
	}
	if !pending {
		_ = lease.Release(ctx)
		return nil, false, nil
	}
	return lease, true, nil
}

func (s *PostgresStore) ClaimPipelinePublication(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, false, fmt.Errorf("event_id is required")
	}
	// Publication ownership serializes same-ID attempts. Canonical append must
	// classify exact/conflicting duplicates before applying run-status admission.
	return acquireAdvisoryLockLease(ctx, s.DB, replayClaimLockKey(eventID))
}

func (s *PostgresStore) ClaimPipelineSettlement(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, false, fmt.Errorf("event_id is required")
	}
	lease, claimed, err := acquireAdvisoryLockLease(ctx, s.DB, replayClaimLockKey(eventID))
	if err != nil || !claimed {
		return nil, claimed, err
	}
	admissionErr := requireEventRunNotForked(ctx, lease.conn, eventID, storerunlifecycle.DialectPostgres, true)
	if admissionErr != nil {
		_ = lease.Release(ctx)
		if errors.Is(admissionErr, storerunlifecycle.ErrRunNotActive) {
			return nil, false, nil
		}
		return nil, false, admissionErr
	}
	return lease, true, nil
}

func (s *PostgresStore) EventExists(ctx context.Context, eventID string) (bool, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false, nil
	}
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1::uuid)`
	if err := eventReadQueryerFromContext(ctx, s.DB).QueryRowContext(ctx, query, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("event exists lookup: %w", err)
	}
	return exists, nil
}

func (s *PostgresStore) ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	query := `
		SELECT subscriber_id
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		ORDER BY subscriber_id ASC
	`
	rows, err := eventReadQueryerFromContext(ctx, s.DB).QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, fmt.Errorf("list event delivery recipients: %w", err)
	}
	defer rows.Close()

	recipients := make([]string, 0, 8)
	for rows.Next() {
		var agentID string
		if err := rows.Scan(&agentID); err != nil {
			return nil, fmt.Errorf("scan event delivery recipient: %w", err)
		}
		agentID = strings.TrimSpace(agentID)
		if agentID != "" {
			recipients = append(recipients, agentID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event delivery recipients: %w", err)
	}
	return recipients, nil
}

func (s *PostgresStore) ListEventDeliveryTargets(ctx context.Context, eventID string) (map[string]events.RouteIdentity, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	rows, err := eventReadQueryerFromContext(ctx, s.DB).QueryContext(ctx, `
		SELECT subscriber_id, COALESCE(delivery_target_route, '{}'::jsonb)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		ORDER BY created_at ASC, subscriber_id ASC
	`, eventID)
	if err != nil {
		return nil, fmt.Errorf("list event delivery targets: %w", err)
	}
	defer rows.Close()
	out := map[string]events.RouteIdentity{}
	for rows.Next() {
		var subscriberID string
		var raw json.RawMessage
		if err := rows.Scan(&subscriberID, &raw); err != nil {
			return nil, fmt.Errorf("scan event delivery target: %w", err)
		}
		route := decodeRouteIdentityJSON(raw)
		if route.Empty() {
			continue
		}
		out[strings.TrimSpace(subscriberID)] = route
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event delivery targets: %w", err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func (s *PostgresStore) ListEventDeliveryRoutes(ctx context.Context, eventID string) ([]events.DeliveryRoute, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	rows, err := eventReadQueryerFromContext(ctx, s.DB).QueryContext(ctx, `
		SELECT subscriber_type, subscriber_id,
		       COALESCE(delivery_target_route, '{}'::jsonb),
		       COALESCE(delivery_context, '{}'::jsonb),
		       COALESCE(delivery_payload_projection, '{}'::jsonb)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND NOT (subscriber_type = $2 AND subscriber_id = $3)
		ORDER BY created_at ASC, delivery_id ASC
	`, eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID)
	if err != nil {
		return nil, fmt.Errorf("list event delivery routes: %w", err)
	}
	defer rows.Close()
	out := make([]events.DeliveryRoute, 0, 8)
	for rows.Next() {
		var subscriberType, subscriberID string
		var raw, contextRaw, projectionRaw json.RawMessage
		if err := rows.Scan(&subscriberType, &subscriberID, &raw, &contextRaw, &projectionRaw); err != nil {
			return nil, fmt.Errorf("scan event delivery route: %w", err)
		}
		projection, err := decodeDeliveryPayloadProjectionJSON(projectionRaw)
		if err != nil {
			return nil, fmt.Errorf("decode event delivery route projection (%s=%s): %w", subscriberType, subscriberID, err)
		}
		out = append(out, events.DeliveryRoute{
			SubscriberType:    subscriberType,
			SubscriberID:      subscriberID,
			Target:            decodeRouteIdentityJSON(raw),
			Context:           decodeDeliveryContextJSON(contextRaw),
			PayloadProjection: projection,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event delivery routes: %w", err)
	}
	return events.NormalizeDeliveryRoutes(out), nil
}

func (s *PostgresStore) LoadCommittedReplayScope(
	ctx context.Context,
	eventID string,
) (runtimereplayclaim.CommittedReplayScope, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	}
	var reasonCode string
	err := eventReadQueryerFromContext(ctx, s.DB).QueryRowContext(ctx, `
		SELECT COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = $2
		  AND subscriber_id = $3
		ORDER BY created_at DESC, delivery_id DESC
		LIMIT 1
	`, eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID).Scan(&reasonCode)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	case err != nil:
		return "", fmt.Errorf("load committed replay scope: %w", err)
	}
	scope, ok := committedReplayScopeFromReasonCode(reasonCode)
	if !ok {
		return "", fmt.Errorf("load committed replay scope: unrecognized reason_code %q", strings.TrimSpace(reasonCode))
	}
	return scope, nil
}

func (s *PostgresStore) appendEventSpec(ctx context.Context, tx *sql.Tx, evt events.Event) (runtimebus.EventAppendOutcome, error) {
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	wantIdentity := eventStorageEnvelope(evt)
	if err := s.validateEventPayload(ctx, wantIdentity.EventName, wantIdentity.Payload); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	queryer := chooseRowQueryer(s.DB, tx)
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
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	} else if conn, ok := runtimepipeline.PipelineSQLConnFromContext(ctx); ok {
		execFn = conn.ExecContext
	}
	var ensureErr error
	if evt.AdmissionClass() == events.EventAdmissionDiagnosticDirect && evt.Type() == events.EventTypePlatformRuntimeLog {
		ensureErr = s.ensureRuntimeLogRunRow(ctx, tx, wantIdentity.RunID, wantIdentity.EventID, wantIdentity.EventName)
	} else {
		ensureErr = s.ensureRunRow(ctx, tx, wantIdentity.RunID, wantIdentity.EventID, wantIdentity.EventName)
	}
	if ensureErr != nil {
		if errors.Is(ensureErr, storerunlifecycle.ErrRunNotActive) {
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
	q := `
		INSERT INTO events (
			event_id, run_id, event_name, task_id, entity_id, flow_instance, scope, payload,
			execution_mode, chain_depth, produced_by, produced_by_type, source_event_id, created_at,
			source_route, target_route, target_set
		)
		VALUES (
			$1::uuid, NULLIF($2,'')::uuid, $3, NULLIF($4,''), NULLIF($5,'')::uuid, NULLIF($6,''), $7, $8::jsonb,
			$9, $10, NULLIF($11,''), $12, NULLIF($13,'')::uuid, $14,
			$15::jsonb, $16::jsonb, $17::jsonb
		)
		ON CONFLICT (event_id) DO NOTHING
	`
	args := []any{wantIdentity.EventID, wantIdentity.RunID, wantIdentity.EventName, wantIdentity.TaskID, wantIdentity.EntityID, wantIdentity.FlowInstance, wantIdentity.Scope, string(wantIdentity.Payload), wantIdentity.ExecutionMode, wantIdentity.ChainDepth, wantIdentity.ProducedBy, wantIdentity.ProducedByType, wantIdentity.SourceEventID, wantIdentity.CreatedAt, string(wantIdentity.SourceRoute), string(wantIdentity.TargetRoute), string(wantIdentity.TargetSet)}
	res, err := execFn(ctx, q, args...)
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("append event: %w", err)
	}
	rows, rowsErr := res.RowsAffected()
	if rowsErr != nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("append event: read affected rows: %w", rowsErr)
	}
	if rows == 0 {
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
	if err := storerunlifecycle.SyncCounts(ctx, chooseExecQueryer(s.DB, tx), wantIdentity.RunID); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if err := recordPersistedEventAuthorActivity(ctx, s, evt, wantIdentity.ProducedBy, wantIdentity.ProducedByType); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return runtimebus.EventAppendInserted, nil
}

func (s *PostgresStore) persistEventWithDeliveriesSpec(ctx context.Context, evt events.Event, agentIDs []string) (runtimebus.EventAppendOutcome, error) {
	return s.persistEventAtomicSpec(ctx, evt, func(txctx context.Context, tx *sql.Tx) error {
		return s.insertEventDeliveriesSpec(txctx, tx, evt.ID(), agentIDs)
	})
}

func (s *PostgresStore) persistEventWithDeliveriesAndScopeSpec(
	ctx context.Context,
	evt events.Event,
	agentIDs []string,
	scope runtimereplayclaim.CommittedReplayScope,
) (runtimebus.EventAppendOutcome, error) {
	return s.persistEventAtomicSpec(ctx, evt, func(txctx context.Context, tx *sql.Tx) error {
		if err := s.insertEventDeliveriesSpec(txctx, tx, evt.ID(), agentIDs); err != nil {
			return err
		}
		return s.upsertCommittedReplayScopeSpec(txctx, tx, evt.ID(), scope)
	})
}

func (s *PostgresStore) persistEventWithDeliveryRoutesAndScopeSpec(
	ctx context.Context,
	evt events.Event,
	agentIDs []string,
	deliveryTargets map[string]events.RouteIdentity,
	scope runtimereplayclaim.CommittedReplayScope,
) (runtimebus.EventAppendOutcome, error) {
	return s.persistEventAtomicSpec(ctx, evt, func(txctx context.Context, tx *sql.Tx) error {
		if err := s.insertEventDeliveriesWithTargetsSpec(txctx, tx, evt.ID(), agentIDs, deliveryTargets); err != nil {
			return err
		}
		return s.upsertCommittedReplayScopeSpec(txctx, tx, evt.ID(), scope)
	})
}

func (s *PostgresStore) persistEventWithDeliveryRouteSetAndScopeSpec(
	ctx context.Context,
	evt events.Event,
	deliveryRoutes []events.DeliveryRoute,
	scope runtimereplayclaim.CommittedReplayScope,
) (runtimebus.EventAppendOutcome, error) {
	return s.persistEventAtomicSpec(ctx, evt, func(txctx context.Context, tx *sql.Tx) error {
		if err := s.insertEventDeliveryRoutesSpec(txctx, tx, evt.ID(), deliveryRoutes); err != nil {
			return err
		}
		return s.upsertCommittedReplayScopeSpec(txctx, tx, evt.ID(), scope)
	})
}

func (s *PostgresStore) persistEventAtomicSpec(
	ctx context.Context,
	evt events.Event,
	persistSideEffects func(context.Context, *sql.Tx) error,
) (runtimebus.EventAppendOutcome, error) {
	outcome := runtimebus.EventAppendOutcomeUnknown
	err := withEventStoreRetry(ctx, nil, func() error {
		outcome = runtimebus.EventAppendOutcomeUnknown
		return s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			appendOutcome, err := s.appendEventSpec(txctx, tx, evt)
			if err != nil {
				return err
			}
			if appendOutcome == runtimebus.EventAppendExactDuplicate {
				outcome = appendOutcome
				return nil
			}
			if persistSideEffects != nil {
				if err := persistSideEffects(txctx, tx); err != nil {
					return err
				}
			}
			outcome = runtimebus.EventAppendInserted
			return nil
		})
	})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if outcome == runtimebus.EventAppendOutcomeUnknown {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("event transaction completed without append outcome")
	}
	return outcome, nil
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

func (s *PostgresStore) insertEventDeliveriesSpec(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string) error {
	return s.insertEventDeliveriesWithTargetsSpec(ctx, tx, eventID, agentIDs, nil)
}

func (s *PostgresStore) InsertEventDeliveriesWithTargets(ctx context.Context, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	return s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return s.InsertEventDeliveriesWithTargetsTx(txctx, tx, eventID, agentIDs, deliveryTargets)
	})
}

func (s *PostgresStore) InsertEventDeliveriesWithTargetsTx(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if tx == nil {
		return s.InsertEventDeliveriesWithTargets(ctx, eventID, agentIDs, deliveryTargets)
	}
	return s.insertEventDeliveriesWithTargetsSpec(ctx, tx, eventID, agentIDs, deliveryTargets)
}

func (s *PostgresStore) InsertEventDeliveryRoutes(ctx context.Context, eventID string, deliveryRoutes []events.DeliveryRoute) error {
	return s.InsertEventDeliveryRoutesTx(ctx, nil, eventID, deliveryRoutes)
}

func (s *PostgresStore) InsertEventDeliveryRoutesTx(ctx context.Context, tx *sql.Tx, eventID string, deliveryRoutes []events.DeliveryRoute) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if tx == nil {
		return s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			return s.InsertEventDeliveryRoutesTx(txctx, tx, eventID, deliveryRoutes)
		})
	}
	if err := requireActiveRunForEvent(ctx, tx, eventID, storerunlifecycle.DialectPostgres); err != nil {
		return err
	}
	return s.insertEventDeliveryRoutesSpec(ctx, tx, eventID, deliveryRoutes)
}

func (s *PostgresStore) insertEventDeliveryRoutesSpec(ctx context.Context, tx *sql.Tx, eventID string, deliveryRoutes []events.DeliveryRoute) error {
	if err := events.ValidateDeliveryRouteProjections(deliveryRoutes); err != nil {
		return err
	}
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) == 0 {
		return nil
	}
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	q := `
			INSERT INTO event_deliveries (
				run_id, event_id, subscriber_type, subscriber_id, reason_code,
				delivery_target_route, delivery_context, delivery_payload_projection, created_at
			)
			SELECT e.run_id, e.event_id, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, now()
			FROM events e
			WHERE e.event_id = $1::uuid
			ON CONFLICT DO NOTHING
		`
	for _, route := range deliveryRoutes {
		route = route.Normalized()
		if route.SubscriberType == "" || route.SubscriberID == "" {
			continue
		}
		projectionRaw, err := deliveryPayloadProjectionJSON(route.PayloadProjection)
		if err != nil {
			return fmt.Errorf("encode event delivery projection (%s=%s): %w", route.SubscriberType, route.SubscriberID, err)
		}
		args := []any{
			eventID, route.SubscriberType, route.SubscriberID, deliveryRouteReasonCode(route),
			string(routeIdentityJSON(route.Target)), string(deliveryContextJSON(route.Context)), string(projectionRaw),
		}
		if _, err := execFn(ctx, q, args...); err != nil {
			return fmt.Errorf("insert event delivery (%s=%s): %w", route.SubscriberType, route.SubscriberID, err)
		}
	}
	return nil
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
	switch strings.TrimSpace(route.SubscriberType) {
	case "agent":
		return "matched_agent_subscription"
	case "node":
		return "matched_node_subscription"
	default:
		return "matched_subscription"
	}
}

func (s *PostgresStore) insertEventDeliveriesWithTargetsSpec(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	q := `
				INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, reason_code, delivery_target_route, created_at)
				SELECT e.run_id, e.event_id, 'agent', $2, 'matched_agent_subscription', $3::jsonb, now()
				FROM events e
				WHERE e.event_id = $1::uuid
				ON CONFLICT DO NOTHING
			`
	seen := make(map[string]struct{}, len(agentIDs))
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if _, ok := seen[agentID]; ok {
			continue
		}
		seen[agentID] = struct{}{}
		args := []any{eventID, agentID}
		args = append(args, string(routeIdentityJSON(deliveryTargets[agentID])))
		if _, err := execFn(ctx, q, args...); err != nil {
			return fmt.Errorf("insert event delivery (agent=%s): %w", agentID, err)
		}
	}
	return nil
}

func (s *PostgresStore) upsertCommittedReplayScopeSpec(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	reasonCode, err := committedReplayScopeReasonCode(scope)
	if err != nil {
		return err
	}
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	res, err := execFn(ctx, `
		UPDATE event_deliveries
		SET reason_code = $4,
		    status = 'delivered',
		    delivered_at = now()
		WHERE event_id = $1::uuid
		  AND subscriber_type = $2
		  AND subscriber_id = $3
	`, eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID, reasonCode)
	if err != nil {
		return fmt.Errorf("update committed replay scope: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update committed replay scope rows: %w", err)
	}
	if rows > 0 {
		return nil
	}
	q := `
			INSERT INTO event_deliveries (
				run_id, event_id, subscriber_type, subscriber_id, status, reason_code, delivered_at, created_at
			)
			SELECT
				e.run_id, e.event_id, $2, $3, 'delivered', $4, now(), now()
			FROM events e
			WHERE e.event_id = $1::uuid
		`
	if _, err := execFn(ctx, q, eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID, reasonCode); err != nil {
		return fmt.Errorf("insert committed replay scope: %w", err)
	}
	return nil
}

func committedReplayScopeReasonCode(scope runtimereplayclaim.CommittedReplayScope) (string, error) {
	switch scope {
	case runtimereplayclaim.CommittedReplayScopeDirect:
		return replayScopeReasonDirect, nil
	case runtimereplayclaim.CommittedReplayScopeSubscribed:
		return replayScopeReasonSubscribed, nil
	default:
		return "", fmt.Errorf("committed replay scope: unsupported scope %q", strings.TrimSpace(string(scope)))
	}
}

func committedReplayScopeFromReasonCode(reasonCode string) (runtimereplayclaim.CommittedReplayScope, bool) {
	switch strings.TrimSpace(reasonCode) {
	case replayScopeReasonDirect:
		return runtimereplayclaim.CommittedReplayScopeDirect, true
	case replayScopeReasonSubscribed:
		return runtimereplayclaim.CommittedReplayScopeSubscribed, true
	default:
		return "", false
	}
}

func (s *PostgresStore) upsertPipelineReceiptSpec(ctx context.Context, tx *sql.Tx, eventID, status string, failure *runtimefailures.Envelope) error {
	reasonCode := pipelineReceiptReasonCode(status, failure)
	failureJSON, err := encodeStoredFailure(failure)
	if err != nil {
		return err
	}
	sideEffects, err := marshalPipelineReceiptSideEffects(newPipelineReceiptSideEffects(status, reasonCode))
	if err != nil {
		return fmt.Errorf("marshal pipeline receipt side effects: %w", err)
	}
	outcome := mapPipelineStatusToOutcome(status)
	const q = `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, failure, side_effects, processed_at
		)
		SELECT
			e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance,
			$2, NULLIF($3,''), $4::jsonb, $5::jsonb, now()
		FROM events e
		WHERE e.event_id = $1::uuid
		  AND NOT EXISTS (
			SELECT 1
			FROM event_deliveries d
			WHERE d.event_id = e.event_id
			  AND d.status = 'dead_letter'
			  AND d.reason_code IN ($6, $7)
		  )
		ON CONFLICT (event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			failure = EXCLUDED.failure,
			side_effects = EXCLUDED.side_effects,
			processed_at = now()
		WHERE COALESCE(event_receipts.reason_code, '') NOT IN ($6, $7)
	`
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	} else if conn, ok := runtimepipeline.PipelineSQLConnFromContext(ctx); ok {
		execFn = conn.ExecContext
	}
	if _, err := execFn(ctx, q, eventID, outcome, reasonCode, failureJSON, string(sideEffects), destructivereset.QuiescenceReasonCode, runtimerunquiescence.ServeAbandonReasonCode); err != nil {
		return fmt.Errorf("upsert pipeline receipt: %w", err)
	}
	return nil
}

func (s *PostgresStore) listEventsMissingPipelineReceiptSpec(ctx context.Context, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	runIDExpr := `COALESCE(e.run_id::text, '')`
	runJoin := `LEFT JOIN runs run ON run.run_id = e.run_id`
	runAdmission := `AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))`
	routeSelect := `COALESCE(e.source_route, '{}'::jsonb), COALESCE(e.target_route, '{}'::jsonb), COALESCE(e.target_set, '[]'::jsonb)`
	exclusionArgs := diagnosticDirectReplayEventArgs()
	limitPlaceholder := 2 + len(exclusionArgs)
	args := append([]any{since}, exclusionArgs...)
	args = append(args, limit)
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			e.event_id::text, %s, e.event_name, COALESCE(e.task_id, ''),
			COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(e.scope, 'global'),
			e.payload, COALESCE(e.chain_depth, 0), COALESCE(e.produced_by, ''), COALESCE(e.produced_by_type, ''),
			COALESCE(e.source_event_id::text, ''), e.created_at, e.execution_mode,
			%s
		FROM events e
		%s
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'platform'
			AND r.subscriber_id = 'pipeline'
		WHERE r.event_id IS NULL
		  %s
		  AND e.created_at >= $1
		  AND NOT EXISTS (
			SELECT 1 FROM decision_card_route_obligations o
			WHERE o.event_id = e.event_id AND o.status <> 'completed'
		  )
		  AND %s
		ORDER BY e.created_at ASC
		LIMIT $%d
	`, runIDExpr, routeSelect, runJoin, runAdmission, postgresDiagnosticDirectReplayExclusionSQL("e", 2), limitPlaceholder), args...)
	if err != nil {
		return nil, fmt.Errorf("list events missing pipeline receipt: %w", err)
	}
	defer rows.Close()

	out := make([]events.PersistedReplayEvent, 0, limit)
	for rows.Next() {
		var row persistedEventIdentity
		if err := rows.Scan(
			&row.EventID,
			&row.RunID,
			&row.EventName,
			&row.TaskID,
			&row.EntityID,
			&row.FlowInstance,
			&row.Scope,
			&row.Payload,
			&row.ChainDepth,
			&row.ProducedBy,
			&row.ProducedByType,
			&row.SourceEventID,
			&row.CreatedAt,
			&row.ExecutionMode,
			&row.SourceRoute,
			&row.TargetRoute,
			&row.TargetSet,
		); err != nil {
			return nil, fmt.Errorf("scan missing pipeline receipt event: %w", err)
		}
		evt, err := eventFromPersistedIdentity(row)
		if err != nil {
			return nil, err
		}
		record := events.PersistedReplayEvent{Event: evt}
		if strings.TrimSpace(evt.RunID()) == "" {
			record.ReplayFailure = replayAdmissionFailure("missing_canonical_run_id")
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read missing pipeline receipt events: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) listEventsMissingPipelineReceiptForRunSpec(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	routeSelect := `COALESCE(e.source_route, '{}'::jsonb), COALESCE(e.target_route, '{}'::jsonb), COALESCE(e.target_set, '[]'::jsonb)`
	exclusionArgs := diagnosticDirectReplayEventArgs()
	limitPlaceholder := 3 + len(exclusionArgs)
	args := append([]any{runID, since}, exclusionArgs...)
	args = append(args, limit)
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			e.event_id::text, COALESCE(e.run_id::text, ''), e.event_name, COALESCE(e.task_id, ''),
			COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(e.scope, 'global'),
			e.payload, COALESCE(e.chain_depth, 0), COALESCE(e.produced_by, ''), COALESCE(e.produced_by_type, ''),
			COALESCE(e.source_event_id::text, ''), e.created_at, e.execution_mode,
			%s
		FROM events e
		JOIN runs run ON run.run_id = e.run_id
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'platform'
			AND r.subscriber_id = 'pipeline'
		WHERE r.event_id IS NULL
		  AND e.run_id = $1::uuid
		  AND run.status IN ('running', 'paused')
		  AND e.created_at >= $2
		  AND NOT EXISTS (
			SELECT 1 FROM decision_card_route_obligations o
			WHERE o.event_id = e.event_id AND o.status <> 'completed'
		  )
		  AND %s
		ORDER BY e.created_at ASC
		LIMIT $%d
	`, routeSelect, postgresDiagnosticDirectReplayExclusionSQL("e", 3), limitPlaceholder), args...)
	if err != nil {
		return nil, fmt.Errorf("list run events missing pipeline receipt: %w", err)
	}
	defer rows.Close()

	out := make([]events.PersistedReplayEvent, 0, limit)
	for rows.Next() {
		var row persistedEventIdentity
		if err := rows.Scan(
			&row.EventID,
			&row.RunID,
			&row.EventName,
			&row.TaskID,
			&row.EntityID,
			&row.FlowInstance,
			&row.Scope,
			&row.Payload,
			&row.ChainDepth,
			&row.ProducedBy,
			&row.ProducedByType,
			&row.SourceEventID,
			&row.CreatedAt,
			&row.ExecutionMode,
			&row.SourceRoute,
			&row.TargetRoute,
			&row.TargetSet,
		); err != nil {
			return nil, fmt.Errorf("scan run missing pipeline receipt event: %w", err)
		}
		evt, err := eventFromPersistedIdentity(row)
		if err != nil {
			return nil, err
		}
		record := events.PersistedReplayEvent{Event: evt}
		if strings.TrimSpace(evt.RunID()) == "" {
			record.ReplayFailure = replayAdmissionFailure("missing_canonical_run_id")
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read run missing pipeline receipt events: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) listEventsWithPendingDeliveriesForRunSpec(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	routeSelect := `COALESCE(e.source_route, '{}'::jsonb), COALESCE(e.target_route, '{}'::jsonb), COALESCE(e.target_set, '[]'::jsonb)`
	exclusionArgs := diagnosticDirectReplayEventArgs()
	limitPlaceholder := 3 + len(exclusionArgs)
	args := append([]any{runID, since}, exclusionArgs...)
	args = append(args, limit)
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			e.event_id::text, COALESCE(e.run_id::text, ''), e.event_name, COALESCE(e.task_id, ''),
			COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(e.scope, 'global'),
			e.payload, COALESCE(e.chain_depth, 0), COALESCE(e.produced_by, ''), COALESCE(e.produced_by_type, ''),
			COALESCE(e.source_event_id::text, ''), e.created_at, e.execution_mode,
			%s
		FROM events e
		JOIN runs run ON run.run_id = e.run_id
		WHERE e.run_id = $1::uuid
		  AND run.status IN ('running', 'paused')
		  AND e.created_at >= $2
		  AND EXISTS (
			SELECT 1
			FROM event_deliveries d
			WHERE d.event_id = e.event_id
			  AND d.run_id = e.run_id
			  AND d.status = 'pending'
		  )
		  AND %s
		ORDER BY e.created_at ASC
		LIMIT $%d
	`, routeSelect, postgresDiagnosticDirectReplayExclusionSQL("e", 3), limitPlaceholder), args...)
	if err != nil {
		return nil, fmt.Errorf("list run events with pending deliveries: %w", err)
	}
	defer rows.Close()

	out := make([]events.PersistedReplayEvent, 0, limit)
	for rows.Next() {
		var row persistedEventIdentity
		if err := rows.Scan(
			&row.EventID,
			&row.RunID,
			&row.EventName,
			&row.TaskID,
			&row.EntityID,
			&row.FlowInstance,
			&row.Scope,
			&row.Payload,
			&row.ChainDepth,
			&row.ProducedBy,
			&row.ProducedByType,
			&row.SourceEventID,
			&row.CreatedAt,
			&row.ExecutionMode,
			&row.SourceRoute,
			&row.TargetRoute,
			&row.TargetSet,
		); err != nil {
			return nil, fmt.Errorf("scan run event with pending deliveries: %w", err)
		}
		evt, err := eventFromPersistedIdentity(row)
		if err != nil {
			return nil, err
		}
		record := events.PersistedReplayEvent{Event: evt}
		if strings.TrimSpace(evt.RunID()) == "" {
			record.ReplayFailure = replayAdmissionFailure("missing_canonical_run_id")
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read run events with pending deliveries: %w", err)
	}
	return out, nil
}

func chooseRowQueryer(db *sql.DB, tx *sql.Tx) rowQueryer {
	if tx != nil {
		return tx
	}
	return db
}

func chooseExecQueryer(db *sql.DB, tx *sql.Tx) execQueryer {
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

func (s *PostgresStore) ensureRunRow(ctx context.Context, tx *sql.Tx, runID, triggerEventID, triggerEventType string) error {
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil
	}
	opts := runLifecycleOptions()
	if fact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx); ok {
		opts.BundleHash = fact.BundleHash
		opts.BundleSource = fact.BundleSource
		opts.BundleFingerprint = fact.BundleFingerprint
	} else {
		opts.BundleSource = storerunlifecycle.BundleSourceLegacy
		opts.BundleFingerprint = runtimecorrelation.BundleFingerprintFromContext(ctx)
	}
	return storerunlifecycle.EnsureActive(ctx, chooseExecQueryer(s.DB, tx), runID, triggerEventID, triggerEventType, opts)
}

func (s *PostgresStore) ensureRuntimeLogRunRow(ctx context.Context, tx *sql.Tx, runID, triggerEventID, triggerEventType string) error {
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil
	}
	return storerunlifecycle.RequirePresent(ctx, chooseExecQueryer(s.DB, tx), runID)
}

func canonicalRunTerminalStatus(raw string) (string, error) {
	return storerunlifecycle.CanonicalTerminalStatus(raw)
}

func (s *PostgresStore) LoadRunLifecycleSnapshot(ctx context.Context, runID string) (runtimebus.RunLifecycleSnapshot, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	snap, err := storerunlifecycle.LoadSnapshot(ctx, s.DB, nullUUIDString(runID), runLifecycleOptions())
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	return runtimebus.RunLifecycleSnapshot{
		RunID:       snap.RunID,
		Status:      snap.Status,
		EventCount:  snap.EventCount,
		EntityCount: snap.EntityCount,
		Failure:     runtimefailures.CloneEnvelope(snap.Failure),
		StartedAt:   snap.StartedAt,
		EndedAt:     snap.EndedAt,
	}, nil
}

func (s *PostgresStore) MarkRunTerminal(ctx context.Context, runID, status string, failure *runtimefailures.Envelope, endedAt time.Time) (runtimebus.RunLifecycleSnapshot, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	runID = nullUUIDString(runID)
	if runID == "" {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("run_id is required")
	}
	var err error
	status, err = canonicalRunTerminalStatus(status)
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	if status == "completed" {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("completed run terminalization is owned by normal run completion convergence")
	}
	if endedAt.IsZero() {
		endedAt = time.Now().UTC()
	}
	var snap storerunlifecycle.Snapshot
	err = s.runAuthorActivityMutation(ctx, "postgres mark run terminal", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		snap, err = storerunlifecycle.MarkTerminal(txctx, tx, runID, status, failure, endedAt, runLifecycleOptions())
		if err != nil {
			return err
		}
		return supersedeDecisionCardsForRun(txctx, tx, runID, "run_"+status, endedAt, false, true)
	})
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	return runtimebus.RunLifecycleSnapshot{
		RunID:       snap.RunID,
		Status:      snap.Status,
		EventCount:  snap.EventCount,
		EntityCount: snap.EntityCount,
		Failure:     runtimefailures.CloneEnvelope(snap.Failure),
		StartedAt:   snap.StartedAt,
		EndedAt:     snap.EndedAt,
	}, nil
}

func (s *PostgresStore) ConvergeStandaloneRuntimePlatformRun(ctx context.Context, evt events.Event) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.runAuthorActivityMutation(ctx, "postgres standalone platform run convergence", func(txctx context.Context, tx *sql.Tx) error {
		return s.convergeStandaloneRuntimePlatformRunByEventID(txctx, tx, strings.TrimSpace(evt.ID()))
	})
}

func runLifecycleOptions() storerunlifecycle.EnsureActiveOptions {
	return storerunlifecycle.EnsureActiveOptions{
		HasStartedAtCol:         true,
		HasTriggerCols:          true,
		HasCounterCols:          true,
		HasEntityStateCountSrc:  true,
		RequireEntityStateCount: true,
		HasTerminalCols:         true,
		HasBundleHashCol:        true,
		HasBundleSourceCol:      true,
		HasBundleFingerprintCol: true,
	}
}

type standaloneRuntimePlatformRunRecord struct {
	RunID            string
	RunStatus        string
	EventID          string
	EventType        string
	ProducedBy       string
	ProducedByType   string
	SourceEventID    string
	TriggerEventID   string
	TriggerEventType string
}

func isStandaloneRuntimePlatformEventType(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "platform.boot",
		"platform.recovery_failed",
		"platform.event_quarantined",
		"platform.agent_panic",
		"platform.agent_failed",
		"platform.auth_required",
		"platform.paused",
		"platform.resumed",
		"platform.dead_letter_escalation",
		"platform.run_stalled",
		"platform.budget_threshold_crossed":
		return true
	default:
		return false
	}
}

func loadStandaloneRuntimePlatformRunRecord(ctx context.Context, db storerunlifecycle.DBTX, eventID string) (standaloneRuntimePlatformRunRecord, bool, error) {
	eventID = sanitizeOptionalUUID(eventID)
	if db == nil || eventID == "" {
		return standaloneRuntimePlatformRunRecord{}, false, nil
	}
	var rec standaloneRuntimePlatformRunRecord
	err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(r.run_id::text, ''),
			COALESCE(r.status, ''),
			COALESCE(e.event_id::text, ''),
			COALESCE(e.event_name, ''),
			COALESCE(e.produced_by, ''),
			COALESCE(e.produced_by_type, ''),
			COALESCE(e.source_event_id::text, ''),
			COALESCE(r.trigger_event_id::text, ''),
			COALESCE(r.trigger_event_type, '')
		FROM events e
		INNER JOIN runs r ON r.run_id = e.run_id
		WHERE e.event_id = $1::uuid
		LIMIT 1
	`, eventID).Scan(
		&rec.RunID,
		&rec.RunStatus,
		&rec.EventID,
		&rec.EventType,
		&rec.ProducedBy,
		&rec.ProducedByType,
		&rec.SourceEventID,
		&rec.TriggerEventID,
		&rec.TriggerEventType,
	)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return standaloneRuntimePlatformRunRecord{}, false, nil
	case err != nil:
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("load standalone runtime platform run candidate: %w", err)
	default:
		return rec, true, nil
	}
}

func isStandaloneRuntimePlatformRunRecord(rec standaloneRuntimePlatformRunRecord) bool {
	if strings.TrimSpace(rec.RunID) == "" {
		return false
	}
	if !isStandaloneRuntimePlatformEventType(rec.EventType) {
		return false
	}
	if strings.TrimSpace(rec.ProducedByType) != "platform" {
		return false
	}
	producedBy := strings.TrimSpace(rec.ProducedBy)
	if producedBy != "" && producedBy != "runtime" {
		return false
	}
	if strings.TrimSpace(rec.SourceEventID) != "" {
		return false
	}
	if strings.TrimSpace(rec.TriggerEventID) != strings.TrimSpace(rec.EventID) {
		return false
	}
	if strings.TrimSpace(rec.TriggerEventType) != strings.TrimSpace(rec.EventType) {
		return false
	}
	return true
}

func (s *PostgresStore) convergeStandaloneRuntimePlatformRunByEventID(
	ctx context.Context,
	db storerunlifecycle.DBTX,
	eventID string,
) error {
	eventID = sanitizeOptionalUUID(eventID)
	if db == nil || eventID == "" {
		return nil
	}
	rec, found, err := loadStandaloneRuntimePlatformRunRecord(ctx, db, eventID)
	if err != nil || !found || !isStandaloneRuntimePlatformRunRecord(rec) {
		return err
	}
	switch strings.TrimSpace(rec.RunStatus) {
	case "completed":
		return nil
	case "failed", "cancelled", "forked":
		return fmt.Errorf("standalone runtime platform run %s already terminal with status %s", rec.RunID, strings.TrimSpace(rec.RunStatus))
	}
	active, err := storerunlifecycle.HasActiveDeliveries(ctx, db, rec.RunID)
	if err != nil {
		return err
	}
	if active {
		return nil
	}
	_, err = storerunlifecycle.MarkTerminal(ctx, db, rec.RunID, "completed", nil, time.Now().UTC(), runLifecycleOptions())
	if err != nil {
		return fmt.Errorf("converge standalone runtime platform run: %w", err)
	}
	return nil
}

func eventStorageEnvelope(evt events.Event) persistedEventIdentity {
	envelope := evt.NormalizedEnvelope()
	sourceRoute, targetRoute, targetSet := eventRouteStorageEnvelope(evt)
	producer := evt.Producer()
	return newPersistedEventIdentity(
		evt.ID(), evt.RunID(), string(evt.Type()), evt.TaskID(), envelope.EntityID, envelope.FlowInstance,
		string(envelope.Scope), evt.Payload(), evt.ExecutionMode(), evt.ChainDepth(), producer.ID(),
		string(producer.Type()), evt.ParentEventID(), evt.CreatedAt(), sourceRoute, targetRoute, targetSet,
	)
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
