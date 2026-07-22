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
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/eventrecord/sqlite"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

type sqliteReplayLease struct {
	store   *SQLiteRuntimeStore
	eventID string
}

func (s *SQLiteRuntimeStore) requireActiveRunForPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID string) error {
	return requireActiveRunForEvent(ctx, tx, eventID, storerunlifecycle.DialectSQLite)
}

func (s *SQLiteRuntimeStore) UpsertPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, failure)
}

func (s *SQLiteRuntimeStore) HasProcessedPipelineReceipt(ctx context.Context, eventID string) (bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false, nil
	}
	var processed bool
	err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM event_receipts
			WHERE event_id = ?
			  AND subscriber_type = 'platform'
			  AND subscriber_id = 'pipeline'
			  AND outcome = 'success'
			  AND reason_code = 'pipeline_persisted'
		)
	`, eventID).Scan(&processed)
	if err != nil {
		return false, fmt.Errorf("load sqlite processed pipeline receipt: %w", err)
	}
	return processed, nil
}

func (s *SQLiteRuntimeStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status string, failure *runtimefailures.Envelope) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	if tx == nil {
		return s.runRuntimeMutation(ctx, "sqlite pipeline receipt", func(txctx context.Context, tx *sql.Tx) error {
			return s.UpsertPipelineReceiptTx(txctx, tx, eventID, status, failure)
		})
	}
	if err := s.requireActiveRunForPipelineReceiptTx(ctx, tx, eventID); err != nil {
		return err
	}
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "" {
		status = "processed"
	}
	if failure != nil && status == "processed" {
		status = "error"
	}
	reasonCode := pipelineReceiptReasonCode(status, failure)
	failureJSON, err := encodeStoredFailure(failure)
	if err != nil {
		return err
	}
	sideEffects, err := marshalPipelineReceiptSideEffects(newPipelineReceiptSideEffects(status, reasonCode))
	if err != nil {
		return fmt.Errorf("marshal sqlite pipeline receipt side effects: %w", err)
	}
	outcome := mapPipelineStatusToOutcome(status)
	args := []any{uuid.NewString(), outcome, sqliteNullString(reasonCode), failureJSON, string(sideEffects), s.now(), eventID}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, failure, side_effects, processed_at
		)
		SELECT
			?, e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance,
			?, ?, ?, ?, ?
		FROM events e
		WHERE e.event_id = ?
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = excluded.outcome,
			reason_code = excluded.reason_code,
			failure = excluded.failure,
			side_effects = excluded.side_effects,
			processed_at = excluded.processed_at
	`, args...)
	if err != nil {
		return fmt.Errorf("upsert sqlite pipeline receipt: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) UpsertCommittedReplayScope(ctx context.Context, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	return s.UpsertCommittedReplayScopeTx(ctx, nil, eventID, scope)
}

func (s *SQLiteRuntimeStore) UpsertCommittedReplayScopeTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	if tx == nil {
		return s.runRuntimeMutation(ctx, "sqlite committed replay scope", func(txctx context.Context, tx *sql.Tx) error {
			return s.UpsertCommittedReplayScopeTx(txctx, tx, eventID, scope)
		})
	}
	if err := requireActiveRunForEvent(ctx, tx, eventID, storerunlifecycle.DialectSQLite); err != nil {
		return err
	}
	if _, err := committedReplayScopeReasonCode(scope); err != nil {
		return err
	}
	now := s.now()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
		SELECT e.event_id, e.run_id, ?, ?, ? FROM events e WHERE e.event_id = ?
		ON CONFLICT(event_id) DO NOTHING
	`, string(scope), now, now, eventID)
	if err != nil {
		return fmt.Errorf("insert sqlite committed replay scope: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		var persisted string
		if err := tx.QueryRowContext(ctx, `SELECT scope FROM committed_replay_scopes WHERE event_id = ?`, eventID).Scan(&persisted); err != nil {
			return fmt.Errorf("read sqlite committed replay scope duplicate: %w", err)
		}
		if strings.TrimSpace(persisted) != string(scope) {
			return fmt.Errorf("committed replay scope conflicts with persisted scope")
		}
	}
	return nil
}

func (s *SQLiteRuntimeStore) LoadCommittedReplayScope(ctx context.Context, eventID string) (runtimereplayclaim.CommittedReplayScope, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	}
	var rawScope string
	err := s.DB.QueryRowContext(ctx, `
		SELECT scope
		FROM committed_replay_scopes
		WHERE event_id = ?
	`, eventID).Scan(&rawScope)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	case err != nil:
		return "", fmt.Errorf("load sqlite committed replay scope: %w", err)
	}
	scope := runtimereplayclaim.CommittedReplayScope(strings.TrimSpace(rawScope))
	if _, err := committedReplayScopeReasonCode(scope); err != nil {
		return "", fmt.Errorf("load sqlite committed replay scope: %w", err)
	}
	return scope, nil
}

func (s *SQLiteRuntimeStore) ListEventDeliveryTargets(ctx context.Context, eventID string) (map[string]events.RouteIdentity, error) {
	routes, err := s.ListEventDeliveryRoutes(ctx, eventID)
	if err != nil {
		return nil, err
	}
	out := map[string]events.RouteIdentity{}
	for _, route := range routes {
		if route.SubscriberType == "agent" && !route.Target.Empty() {
			out[route.SubscriberID] = route.Target
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) ListEventDeliveryRoutes(ctx context.Context, eventID string) ([]events.DeliveryRoute, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	snapshots, err := s.deliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("list sqlite event delivery routes: %w", err)
	}
	out := make([]events.DeliveryRoute, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, snapshot.Route)
	}
	return events.NormalizeDeliveryRoutes(out), nil
}

func (s *SQLiteRuntimeStore) ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	return s.listSQLiteEventsMissingPipelineReceipt(ctx, "e.created_at >= ?", []any{since.UTC()}, limit)
}

func (s *SQLiteRuntimeStore) ListEventsMissingPipelineReceiptForRun(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
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
	return s.listSQLiteEventsMissingPipelineReceipt(ctx, "e.run_id = ? AND e.created_at >= ?", []any{runID, since.UTC()}, limit)
}

func (s *SQLiteRuntimeStore) ListEventsWithPendingDeliveriesForRun(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
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
	return s.listSQLiteEventsWithPendingDeliveriesForRun(ctx, runID, since.UTC(), limit)
}

func (s *SQLiteRuntimeStore) listSQLiteEventsMissingPipelineReceipt(ctx context.Context, where string, args []any, limit int) ([]events.PersistedReplayEvent, error) {
	queryArgs := append([]any{}, args...)
	queryArgs = append(queryArgs, diagnosticDirectReplayEventArgs()...)
	queryArgs = append(queryArgs, limit)
	rows, err := s.DB.QueryContext(ctx, `
		SELECT e.event_id
		FROM events e
		LEFT JOIN runs run ON run.run_id = e.run_id
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'platform'
			AND r.subscriber_id = 'pipeline'
		WHERE r.event_id IS NULL
		  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
		  AND `+where+`
		  AND NOT EXISTS (
			SELECT 1 FROM decision_card_route_obligations o
			WHERE o.event_id = e.event_id AND o.status <> 'completed'
		  )
		  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
		ORDER BY e.created_at ASC, e.event_id ASC
		LIMIT ?
	`, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("list sqlite events missing pipeline receipt: %w", err)
	}
	eventIDs, err := scanOrderedEventIDs(rows, "sqlite missing pipeline receipt")
	if err != nil {
		return nil, err
	}
	return hydrateSQLitePersistedReplayEvents(ctx, s.DB, eventIDs)
}

func (s *SQLiteRuntimeStore) listSQLiteEventsWithPendingDeliveriesForRun(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	var active bool
	if err := s.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = ? AND status IN ('running', 'paused'))`, runID).Scan(&active); err != nil {
		return nil, fmt.Errorf("inspect sqlite pending-delivery run: %w", err)
	}
	if !active {
		return []events.PersistedReplayEvent{}, nil
	}
	snapshots, err := sqliteDeliveryAdapter.SnapshotsForRun(ctx, s.DB, runID)
	if err != nil {
		return nil, err
	}
	records, err := hydrateSQLitePersistedReplayEvents(ctx, s.DB, pendingDeliveryEventIDs(snapshots, since))
	if err != nil {
		return nil, err
	}
	return filterExecutableReplayEvents(records, limit), nil
}

func (s *SQLiteRuntimeStore) ClaimPipelineReplay(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, false, fmt.Errorf("event_id is required")
	}
	var pending bool
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM events e
			LEFT JOIN runs run ON run.run_id = e.run_id
			LEFT JOIN event_receipts r
				ON r.event_id = e.event_id
				AND r.subscriber_type = 'platform'
				AND r.subscriber_id = 'pipeline'
			WHERE e.event_id = ?
			  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
			  AND r.event_id IS NULL
		)
	`, eventID).Scan(&pending); err != nil {
		return nil, false, fmt.Errorf("claim sqlite pipeline replay: %w", err)
	}
	if !pending {
		return nil, false, nil
	}
	return s.claimPipelineEvent(eventID)
}

func (s *SQLiteRuntimeStore) ClaimPipelinePublication(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, false, fmt.Errorf("event_id is required")
	}
	// Publication ownership serializes same-ID attempts. Canonical append must
	// classify exact/conflicting duplicates before applying run-status admission.
	return s.claimPipelineEvent(eventID)
}

func (s *SQLiteRuntimeStore) ClaimPipelineSettlement(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, false, fmt.Errorf("event_id is required")
	}
	if err := requireEventRunNotForked(ctx, s.DB, eventID, storerunlifecycle.DialectSQLite, true); err != nil {
		if errors.Is(err, storerunlifecycle.ErrRunNotActive) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return s.claimPipelineEvent(eventID)
}

func (s *SQLiteRuntimeStore) claimPipelineEvent(eventID string) (runtimeownership.Lease, bool, error) {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()
	if s.replayClaims == nil {
		s.replayClaims = map[string]struct{}{}
	}
	if _, exists := s.replayClaims[eventID]; exists {
		return nil, false, nil
	}
	s.replayClaims[eventID] = struct{}{}
	return &sqliteReplayLease{store: s, eventID: eventID}, true, nil
}

func (l *sqliteReplayLease) Release(context.Context) error {
	if l == nil || l.store == nil {
		return nil
	}
	l.store.replayMu.Lock()
	delete(l.store.replayClaims, strings.TrimSpace(l.eventID))
	l.store.replayMu.Unlock()
	return nil
}

func (s *SQLiteRuntimeStore) ListPendingAgentDeliveryFacts(ctx context.Context, agentIDs []string, since time.Time) (map[string]PendingAgentDeliveryFacts, error) {
	normalized := normalizePendingAgentIDs(agentIDs)
	out := make(map[string]PendingAgentDeliveryFacts, len(normalized))
	for _, agentID := range normalized {
		out[agentID] = PendingAgentDeliveryFacts{}
	}
	if len(normalized) == 0 {
		return out, nil
	}
	records, err := s.listSQLitePendingAgentDeliveryRecords(ctx, normalized, since)
	if err != nil {
		return nil, err
	}
	grouped := make(map[string][]pendingAgentDeliveryRecord, len(normalized))
	for _, record := range records {
		grouped[record.AgentID] = append(grouped[record.AgentID], record)
	}
	now := s.now()
	for _, agentID := range normalized {
		out[agentID] = pendingAgentDeliveryFactsFromRecords(grouped[agentID], now)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) ListPendingAgentDeliveryDetails(ctx context.Context, opts PendingAgentDeliveryListOptions) (PendingAgentDeliveryPage, error) {
	opts.AgentID = strings.TrimSpace(opts.AgentID)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.AgentID == "" {
		return PendingAgentDeliveryPage{PendingDeliveries: []PendingAgentDeliveryDetail{}}, nil
	}
	if opts.Limit == 0 {
		opts.Limit = DefaultPendingAgentDeliveryDetailLimit
	}
	if opts.Limit < 0 || opts.Limit > MaxPendingAgentDeliveryDetailLimit {
		return PendingAgentDeliveryPage{}, fmt.Errorf("pending agent delivery detail limit must be from 1 to %d", MaxPendingAgentDeliveryDetailLimit)
	}
	var cursor *pendingAgentDeliveryCursorPosition
	if opts.Cursor != "" {
		decoded, err := decodePendingAgentDeliveryCursor(opts.Cursor)
		if err != nil {
			return PendingAgentDeliveryPage{}, err
		}
		cursor = &decoded
	}
	records, err := s.listSQLitePendingAgentDeliveryRecords(ctx, []string{opts.AgentID}, opts.Since)
	if err != nil {
		return PendingAgentDeliveryPage{}, err
	}
	return pendingAgentDeliveryPageFromRecords(records, s.now(), opts.Limit, cursor)
}

func (s *SQLiteRuntimeStore) listSQLitePendingAgentDeliveryRecords(ctx context.Context, agentIDs []string, since time.Time) ([]pendingAgentDeliveryRecord, error) {
	out := []pendingAgentDeliveryRecord{}
	for _, agentID := range agentIDs {
		snapshots, err := sqliteDeliveryAdapter.EligibleAgentSnapshots(ctx, s.DB, agentID, since)
		if err != nil {
			return nil, err
		}
		for _, snapshot := range snapshots {
			durable, found, err := eventrecordsqlite.Load(ctx, s.DB, snapshot.EventID)
			if err != nil || !found {
				if err == nil {
					err = eventrecord.Missing(snapshot.EventID)
				}
				return nil, err
			}
			admitted, err := durable.Decode()
			if err != nil {
				return nil, err
			}
			delivery, err := events.NewDeliveryEvent(admitted.Event(), snapshot.Route)
			if err != nil {
				return nil, err
			}
			out = append(out, pendingAgentDeliveryRecord{
				AgentID:            snapshot.SubscriberID,
				Event:              delivery.Event(),
				DeliveryFound:      true,
				DeliveryStatus:     string(snapshot.Status),
				DeliveryRetryCount: snapshot.RetryCount,
				DeliveryCreatedAt:  snapshot.CreatedAt,
			})
		}
	}
	return out, nil
}

func sqliteJSONRawMessage(raw any) json.RawMessage {
	switch v := raw.(type) {
	case nil:
		return nil
	case json.RawMessage:
		return append(json.RawMessage(nil), v...)
	case []byte:
		return json.RawMessage(append([]byte(nil), v...))
	case string:
		return json.RawMessage(v)
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		return encoded
	}
}
