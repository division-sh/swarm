package store

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
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/eventrecord/postgres"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

const (
	replayScopeReasonDirect     = "replay_scope_direct"
	replayScopeReasonSubscribed = "replay_scope_subscribed"
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

func (s *PostgresStore) requireActiveRunForPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID string) error {
	return requireActiveRunForEvent(ctx, tx, eventID, storerunlifecycle.DialectPostgres)
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

func (s *PostgresStore) appendAdmittedEventTxOutcome(ctx context.Context, tx *sql.Tx, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if tx == nil {
		outcome := runtimebus.EventAppendOutcomeUnknown
		err := s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			outcome, err = s.appendAdmittedEventTxOutcome(txctx, tx, admitted)
			return err
		})
		return outcome, err
	}
	outcome := runtimebus.EventAppendOutcomeUnknown
	err := withEventStoreRetry(ctx, tx, func() error {
		var err error
		outcome, err = s.appendEventSpec(ctx, tx, admitted)
		return err
	})
	return outcome, err
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
		return s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
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
		return s.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			return s.UpsertPipelineReceiptTx(txctx, tx, eventID, status, failure)
		})
	}
	if err := s.requireActiveRunForPipelineReceiptTx(ctx, tx, eventID); err != nil {
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
	// A publication claim survives its creating transaction and may dispatch
	// asynchronously, so it must never share a transaction/parent claim session.
	claimCtx := runtimepipeline.WithoutPipelineSQLTxContext(ctx)
	claimCtx = runtimepipeline.WithoutPipelineSQLConnContext(claimCtx)
	return acquireAdvisoryLockLease(claimCtx, s.DB, replayClaimLockKey(eventID))
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
	snapshots, err := s.deliverySnapshotsForEvent(ctx, eventID)
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

func (s *PostgresStore) ListEventDeliveryTargets(ctx context.Context, eventID string) (map[string]events.RouteIdentity, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	snapshots, err := s.deliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("list event delivery targets: %w", err)
	}
	out := map[string]events.RouteIdentity{}
	for _, snapshot := range snapshots {
		if snapshot.SubscriberClass != runtimedelivery.SubscriberAgent {
			continue
		}
		route := snapshot.Route.Target
		if route.Empty() {
			continue
		}
		out[strings.TrimSpace(snapshot.SubscriberID)] = route
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
	snapshots, err := s.deliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("list event delivery routes: %w", err)
	}
	out := make([]events.DeliveryRoute, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, snapshot.Route)
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
	var rawScope string
	err := eventReadQueryerFromContext(ctx, s.DB).QueryRowContext(ctx, `
		SELECT scope
		FROM committed_replay_scopes
		WHERE event_id = $1::uuid
	`, eventID).Scan(&rawScope)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	case err != nil:
		return "", fmt.Errorf("load committed replay scope: %w", err)
	}
	scope := runtimereplayclaim.CommittedReplayScope(strings.TrimSpace(rawScope))
	if _, err := committedReplayScopeReasonCode(scope); err != nil {
		return "", fmt.Errorf("load committed replay scope: %w", err)
	}
	return scope, nil
}

func (s *PostgresStore) appendEventSpec(ctx context.Context, tx *sql.Tx, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	evt := admitted.Event()
	wantIdentity, err := eventrecord.FromAdmitted(admitted)
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
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
	var recordExec eventrecordpostgres.Execer = s.DB
	if tx != nil {
		recordExec = tx
	} else if conn, ok := runtimepipeline.PipelineSQLConnFromContext(ctx); ok {
		recordExec = conn
	}
	var ensureErr error
	switch admitted.RunDisposition() {
	case events.AdmittedRunCreateAuthorized:
		ensureErr = s.ensureRunRow(ctx, tx, wantIdentity.RunID, wantIdentity.EventID, wantIdentity.EventName)
	case events.AdmittedRunRequireActive:
		ensureErr = storerunlifecycle.RequireActive(ctx, chooseExecQueryer(s.DB, tx), wantIdentity.RunID, storerunlifecycle.DialectPostgres)
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
	if err := requireEventOwnedReferences(ctx, chooseExecQueryer(s.DB, tx), storerunlifecycle.DialectPostgres, wantIdentity); err != nil {
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
	if err := storerunlifecycle.SyncCounts(ctx, chooseExecQueryer(s.DB, tx), wantIdentity.RunID); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if err := recordPersistedEventAuthorActivity(ctx, s, evt, wantIdentity.ProducedBy, string(wantIdentity.ProducedByType)); err != nil {
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
	switch strings.TrimSpace(route.SubscriberType) {
	case "agent":
		return "matched_agent_subscription"
	case "node":
		return "matched_node_subscription"
	default:
		return "matched_subscription"
	}
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
	if _, err := committedReplayScopeReasonCode(scope); err != nil {
		return err
	}
	now := time.Now().UTC()
	execFn := s.DB.ExecContext
	queryFn := s.DB.QueryRowContext
	if tx != nil {
		execFn = tx.ExecContext
		queryFn = tx.QueryRowContext
	}
	res, err := execFn(ctx, `
		INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
		SELECT e.event_id, e.run_id, $2, $3, $3 FROM events e WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id) DO NOTHING
	`, eventID, string(scope), now)
	if err != nil {
		return fmt.Errorf("insert committed replay scope: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		var persisted string
		if err := queryFn(ctx, `SELECT scope FROM committed_replay_scopes WHERE event_id = $1::uuid`, eventID).Scan(&persisted); err != nil {
			return fmt.Errorf("read committed replay scope duplicate: %w", err)
		}
		if strings.TrimSpace(persisted) != string(scope) {
			return fmt.Errorf("committed replay scope conflicts with persisted scope")
		}
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
		ON CONFLICT (event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			failure = EXCLUDED.failure,
			side_effects = EXCLUDED.side_effects,
			processed_at = now()
	`
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	} else if conn, ok := runtimepipeline.PipelineSQLConnFromContext(ctx); ok {
		execFn = conn.ExecContext
	}
	if _, err := execFn(ctx, q, eventID, outcome, reasonCode, failureJSON, string(sideEffects)); err != nil {
		return fmt.Errorf("upsert pipeline receipt: %w", err)
	}
	return nil
}

func (s *PostgresStore) listEventsMissingPipelineReceiptSpec(ctx context.Context, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	runJoin := `LEFT JOIN runs run ON run.run_id = e.run_id`
	runAdmission := `AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))`
	exclusionArgs := diagnosticDirectReplayEventArgs()
	limitPlaceholder := 2 + len(exclusionArgs)
	args := append([]any{since}, exclusionArgs...)
	args = append(args, limit)
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT e.event_id::text
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
	`, runJoin, runAdmission, postgresDiagnosticDirectReplayExclusionSQL("e", 2), limitPlaceholder), args...)
	if err != nil {
		return nil, fmt.Errorf("list events missing pipeline receipt: %w", err)
	}
	defer rows.Close()

	eventIDs := make([]string, 0, limit)
	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			return nil, fmt.Errorf("scan missing pipeline receipt event: %w", err)
		}
		eventIDs = append(eventIDs, eventID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read missing pipeline receipt events: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close missing pipeline receipt events: %w", err)
	}
	return hydratePostgresPersistedReplayEvents(ctx, s.DB, eventIDs)
}

func (s *PostgresStore) listEventsMissingPipelineReceiptForRunSpec(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	exclusionArgs := diagnosticDirectReplayEventArgs()
	limitPlaceholder := 3 + len(exclusionArgs)
	args := append([]any{runID, since}, exclusionArgs...)
	args = append(args, limit)
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT e.event_id::text
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
	`, postgresDiagnosticDirectReplayExclusionSQL("e", 3), limitPlaceholder), args...)
	if err != nil {
		return nil, fmt.Errorf("list run events missing pipeline receipt: %w", err)
	}
	defer rows.Close()

	eventIDs := make([]string, 0, limit)
	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			return nil, fmt.Errorf("scan run missing pipeline receipt event: %w", err)
		}
		eventIDs = append(eventIDs, eventID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read run missing pipeline receipt events: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close run missing pipeline receipt events: %w", err)
	}
	return hydratePostgresPersistedReplayEvents(ctx, s.DB, eventIDs)
}

func (s *PostgresStore) listEventsWithPendingDeliveriesForRunSpec(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	var active bool
	if err := s.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = $1::uuid AND status IN ('running', 'paused'))`, runID).Scan(&active); err != nil {
		return nil, fmt.Errorf("inspect pending-delivery run: %w", err)
	}
	if !active {
		return []events.PersistedReplayEvent{}, nil
	}
	snapshots, err := postgresDeliveryAdapter.SnapshotsForRun(ctx, s.DB, runID)
	if err != nil {
		return nil, fmt.Errorf("list run pending delivery snapshots: %w", err)
	}
	records, err := hydratePostgresPersistedReplayEvents(ctx, s.DB, pendingDeliveryEventIDs(snapshots, since))
	if err != nil {
		return nil, err
	}
	return filterExecutableReplayEvents(records, limit), nil
}

func pendingDeliveryEventIDs(snapshots []runtimedelivery.Snapshot, since time.Time) []string {
	filtered := make([]runtimedelivery.Snapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.Status == runtimedelivery.StatusPending && !snapshot.CreatedAt.Before(since) {
			filtered = append(filtered, snapshot)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if !filtered[i].CreatedAt.Equal(filtered[j].CreatedAt) {
			return filtered[i].CreatedAt.Before(filtered[j].CreatedAt)
		}
		return filtered[i].EventID < filtered[j].EventID
	})
	seen := map[string]struct{}{}
	ids := make([]string, 0, len(filtered))
	for _, snapshot := range filtered {
		if _, ok := seen[snapshot.EventID]; ok {
			continue
		}
		seen[snapshot.EventID] = struct{}{}
		ids = append(ids, snapshot.EventID)
	}
	return ids
}

func filterExecutableReplayEvents(records []events.PersistedReplayEvent, limit int) []events.PersistedReplayEvent {
	excluded := map[events.EventType]struct{}{}
	for _, eventType := range events.DiagnosticDirectEventTypes() {
		excluded[eventType] = struct{}{}
	}
	out := make([]events.PersistedReplayEvent, 0, min(limit, len(records)))
	for _, record := range records {
		if _, skip := excluded[record.Event.Type()]; skip {
			continue
		}
		out = append(out, record)
		if len(out) == limit {
			break
		}
	}
	return out
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
		snap, err = s.markRunTerminalTx(txctx, tx, runID, status, failure, endedAt)
		return err
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

func (s *PostgresStore) markRunTerminalTx(ctx context.Context, tx *sql.Tx, runID, status string, failure *runtimefailures.Envelope, endedAt time.Time) (storerunlifecycle.Snapshot, error) {
	snapshot, err := storerunlifecycle.MarkTerminal(ctx, tx, runID, status, failure, endedAt, runLifecycleOptions())
	if err != nil {
		return storerunlifecycle.Snapshot{}, err
	}
	if _, err := postgresDeliveryAdapter.TerminalizeRun(ctx, tx, runID, "run_"+status); err != nil {
		return storerunlifecycle.Snapshot{}, err
	}
	if err := supersedeDecisionCardsForRun(ctx, tx, runID, "run_"+status, endedAt, false, true); err != nil {
		return storerunlifecycle.Snapshot{}, err
	}
	return snapshot, nil
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
	EventClass       string
	EventType        string
	ProducedBy       string
	ProducedByType   string
	SourceEventID    string
	TriggerEventID   string
	TriggerEventType string
}

func loadStandaloneRuntimePlatformRunRecord(ctx context.Context, db storerunlifecycle.DBTX, eventID string) (standaloneRuntimePlatformRunRecord, bool, error) {
	eventID = sanitizeOptionalUUID(eventID)
	if db == nil || eventID == "" {
		return standaloneRuntimePlatformRunRecord{}, false, nil
	}
	durable, found, err := loadPostgresEventIdentity(ctx, db, eventID)
	if err != nil || !found {
		return standaloneRuntimePlatformRunRecord{}, found, err
	}
	admitted, err := decodeEventRecord(durable)
	if err != nil {
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("decode standalone runtime platform event: %w", err)
	}
	event := admitted.Event()
	rec := standaloneRuntimePlatformRunRecord{
		RunID: event.RunID(), EventID: event.ID(), EventClass: string(event.AdmissionClass()),
		EventType: string(event.Type()), ProducedBy: event.SourceAgent(), ProducedByType: string(event.ProducerType()),
		SourceEventID: event.ParentEventID(),
	}
	err = db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(trigger_event_id::text, ''), COALESCE(trigger_event_type, '')
		FROM runs WHERE run_id = $1::uuid
	`, rec.RunID).Scan(&rec.RunStatus, &rec.TriggerEventID, &rec.TriggerEventType)
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
	if rec.EventClass != string(events.EventAdmissionRuntimeControl) && rec.EventClass != string(events.EventAdmissionRuntimeDiagnostic) {
		return false
	}
	if strings.TrimSpace(rec.ProducedByType) != string(events.EventProducerPlatform) || strings.TrimSpace(rec.ProducedBy) != "runtime" {
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
	db *sql.Tx,
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
	summary, err := postgresDeliveryAdapter.SummarizeRun(ctx, db, rec.RunID)
	if err != nil {
		return err
	}
	if !summary.Settled() {
		return nil
	}
	_, err = storerunlifecycle.MarkTerminal(ctx, db, rec.RunID, "completed", nil, time.Now().UTC(), runLifecycleOptions())
	if err != nil {
		return fmt.Errorf("converge standalone runtime platform run: %w", err)
	}
	return nil
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
