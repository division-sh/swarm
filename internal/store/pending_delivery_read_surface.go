package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/lib/pq"
)

const canonicalPendingDeliveryBackoff = time.Minute
const pendingAgentDeliveryCursorKind = "agent.diagnose.queue"

const DefaultPendingAgentDeliveryDetailLimit = 50
const MaxPendingAgentDeliveryDetailLimit = 500
const MaxAgentDiagnosisQueueLimit = 200

type PendingAgentDeliveryFacts struct {
	PendingCount        int
	OldestPendingAgeSec int
}

type PendingAgentDeliveryListOptions struct {
	AgentID string
	Since   time.Time
	Limit   int
	Cursor  string
}

type PendingAgentDeliveryPage struct {
	PendingCount        int
	OldestPendingAgeSec int
	PendingDeliveries   []PendingAgentDeliveryDetail
	NextCursor          string
}

type PendingAgentDeliveryDetail struct {
	EventID    string
	EventName  string
	EnqueuedAt time.Time
	Attempts   int
	Event      events.Event `json:"-"`
}

type pendingAgentDeliveryRecord struct {
	AgentID             string
	Event               events.Event
	DeliveryFound       bool
	DeliveryStatus      string
	DeliveryRetryCount  int
	DeliveryCreatedAt   time.Time
	DeliveryDeliveredAt sql.NullTime
	ReceiptFound        bool
}

type pendingAgentDeliveryCursor struct {
	Kind       string `json:"kind"`
	EnqueuedAt string `json:"enqueued_at"`
	EventID    string `json:"event_id"`
}

type pendingAgentDeliveryCursorPosition struct {
	EnqueuedAt time.Time
	EventID    string
}

func (r pendingAgentDeliveryRecord) isPending(now time.Time) bool {
	if r.DeliveryFound {
		switch strings.TrimSpace(strings.ToLower(r.DeliveryStatus)) {
		case "pending", "in_progress":
			return true
		case "failed":
			if r.DeliveryRetryCount >= 2 {
				return false
			}
			attemptAt := r.DeliveryCreatedAt
			if r.DeliveryDeliveredAt.Valid {
				attemptAt = r.DeliveryDeliveredAt.Time
			}
			return !attemptAt.After(now.Add(-canonicalPendingDeliveryBackoff))
		default:
			return false
		}
	}
	return !r.ReceiptFound
}

// Pending truth is delivery-backed. Receipts can only confirm that a delivery-backed
// attempt already completed; they must not become a substitute ownership source.
func canonicalPendingDeliveryPredicateSQL(deliveryAlias, receiptAlias string) string {
	return fmt.Sprintf(`(
		(
			%s.delivery_id IS NOT NULL
			AND (
				%s.status IN ('pending', 'in_progress')
				OR (
					%s.status = 'failed'
					AND COALESCE(%s.retry_count, 0) < 2
					AND COALESCE(%s.delivered_at, %s.created_at) <= now() - interval '1 minute'
				)
			)
		)
		OR (
			%s.delivery_id IS NULL
			AND %s.event_id IS NULL
		)
	)`,
		deliveryAlias,
		deliveryAlias,
		deliveryAlias,
		deliveryAlias,
		deliveryAlias,
		deliveryAlias,
		deliveryAlias,
		receiptAlias,
	)
}

func (r pendingAgentDeliveryRecord) pendingAgeSec(now time.Time) int {
	if r.Event.CreatedAt().IsZero() {
		return 0
	}
	age := int(now.Sub(r.Event.CreatedAt()).Seconds())
	if age < 0 {
		return 0
	}
	return age
}

func RequireCanonicalPendingAgentDeliveryCapabilities(caps StoreSchemaCapabilities) error {
	switch {
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	case caps.Events.Receipts != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	default:
		return nil
	}
}

func pendingAgentDeliveryFactsFromRecords(records []pendingAgentDeliveryRecord, now time.Time) PendingAgentDeliveryFacts {
	var facts PendingAgentDeliveryFacts
	for _, record := range records {
		if !record.isPending(now) {
			continue
		}
		facts.PendingCount++
		age := record.pendingAgeSec(now)
		if age > facts.OldestPendingAgeSec {
			facts.OldestPendingAgeSec = age
		}
	}
	return facts
}

func pendingEventsFromRecords(records []pendingAgentDeliveryRecord, now time.Time, limit int) []events.Event {
	if limit <= 0 {
		limit = len(records)
	}
	out := make([]events.Event, 0, min(limit, len(records)))
	for _, record := range records {
		if !record.isPending(now) {
			continue
		}
		out = append(out, record.Event)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func (s *PostgresStore) ListPendingAgentDeliveryFacts(ctx context.Context, agentIDs []string, since time.Time) (map[string]PendingAgentDeliveryFacts, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if err := RequireCanonicalPendingAgentDeliveryCapabilities(caps); err != nil {
		return nil, err
	}
	normalized := normalizePendingAgentIDs(agentIDs)
	if len(normalized) == 0 {
		return map[string]PendingAgentDeliveryFacts{}, nil
	}
	records, err := s.listPendingAgentDeliveryRecordsSpec(ctx, normalized, since)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	out := make(map[string]PendingAgentDeliveryFacts, len(normalized))
	for _, agentID := range normalized {
		out[agentID] = PendingAgentDeliveryFacts{}
	}
	grouped := make(map[string][]pendingAgentDeliveryRecord, len(normalized))
	for _, record := range records {
		grouped[record.AgentID] = append(grouped[record.AgentID], record)
	}
	for _, agentID := range normalized {
		out[agentID] = pendingAgentDeliveryFactsFromRecords(grouped[agentID], now)
	}
	return out, nil
}

func (s *PostgresStore) ListPendingAgentDeliveryDetails(ctx context.Context, opts PendingAgentDeliveryListOptions) (PendingAgentDeliveryPage, error) {
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
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return PendingAgentDeliveryPage{}, err
	}
	if err := RequireCanonicalPendingAgentDeliveryCapabilities(caps); err != nil {
		return PendingAgentDeliveryPage{}, err
	}
	records, err := s.listPendingAgentDeliveryRecordsSpec(ctx, []string{opts.AgentID}, opts.Since)
	if err != nil {
		return PendingAgentDeliveryPage{}, err
	}
	return pendingAgentDeliveryPageFromRecords(records, time.Now(), opts.Limit, cursor)
}

func normalizePendingAgentIDs(agentIDs []string) []string {
	seen := make(map[string]struct{}, len(agentIDs))
	out := make([]string, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if _, ok := seen[agentID]; ok {
			continue
		}
		seen[agentID] = struct{}{}
		out = append(out, agentID)
	}
	return out
}

func (s *PostgresStore) listPendingAgentDeliveryRecordsSpec(ctx context.Context, agentIDs []string, since time.Time) ([]pendingAgentDeliveryRecord, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			d.subscriber_id,
			e.event_id::text,
			COALESCE(e.run_id::text, ''),
			e.event_name,
			COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id::text, ''),
			COALESCE(e.flow_instance, ''),
			COALESCE(e.scope, 'global'),
			e.payload,
			e.created_at,
			COALESCE(e.source_event_id::text, ''),
			TRUE,
			COALESCE(d.status, ''),
			COALESCE(d.retry_count, 0),
			d.created_at,
			d.delivered_at,
			CASE WHEN r.event_id IS NULL THEN FALSE ELSE TRUE END
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		LEFT JOIN event_receipts r
			ON r.event_id = d.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = d.subscriber_id
		WHERE d.subscriber_type = 'agent'
		  AND d.subscriber_id = ANY($1)
		  AND ($2::timestamptz IS NULL OR e.created_at >= $2::timestamptz)
		  AND `+canonicalPendingDeliveryPredicateSQL("d", "r")+`
		ORDER BY d.subscriber_id ASC, e.created_at ASC, e.event_id ASC
	`, pq.Array(agentIDs), pendingDeliverySinceArg(since))
	if err != nil {
		return nil, fmt.Errorf("query pending agent delivery records: %w", err)
	}
	defer rows.Close()
	return scanPendingAgentDeliveryRecords(rows)
}

func pendingDeliverySinceArg(since time.Time) any {
	if since.IsZero() {
		return nil
	}
	return since
}

func scanPendingAgentDeliveryRecords(rows *sql.Rows) ([]pendingAgentDeliveryRecord, error) {
	out := make([]pendingAgentDeliveryRecord, 0)
	for rows.Next() {
		var (
			record                 pendingAgentDeliveryRecord
			eventID, runID         string
			eventName, producedBy  string
			payload                json.RawMessage
			createdAt              time.Time
			sourceEventID          string
			entityID, flowInstance string
			scope                  string
		)
		if err := rows.Scan(
			&record.AgentID,
			&eventID,
			&runID,
			&eventName,
			&producedBy,
			&entityID,
			&flowInstance,
			&scope,
			&payload,
			&createdAt,
			&sourceEventID,
			&record.DeliveryFound,
			&record.DeliveryStatus,
			&record.DeliveryRetryCount,
			&record.DeliveryCreatedAt,
			&record.DeliveryDeliveredAt,
			&record.ReceiptFound,
		); err != nil {
			return nil, fmt.Errorf("scan pending agent delivery record: %w", err)
		}
		record.AgentID = strings.TrimSpace(record.AgentID)
		record.Event = events.NewProjectionEvent(
			eventID,
			events.EventType(eventName),
			producedBy,
			"",
			payload,
			0,
			runID,
			sourceEventID,
			events.EventEnvelope{
				EntityID:     entityID,
				FlowInstance: flowInstance,
				Scope:        events.EventScope(scope),
			},
			createdAt,
		)
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending agent delivery records: %w", err)
	}
	return out, nil
}

func pendingAgentDeliveryPageFromRecords(records []pendingAgentDeliveryRecord, now time.Time, limit int, cursor *pendingAgentDeliveryCursorPosition) (PendingAgentDeliveryPage, error) {
	if limit <= 0 {
		limit = DefaultPendingAgentDeliveryDetailLimit
	}
	facts := pendingAgentDeliveryFactsFromRecords(records, now)
	out := PendingAgentDeliveryPage{
		PendingCount:        facts.PendingCount,
		OldestPendingAgeSec: facts.OldestPendingAgeSec,
		PendingDeliveries:   []PendingAgentDeliveryDetail{},
	}
	for _, record := range records {
		if !record.isPending(now) {
			continue
		}
		if cursor != nil && !pendingAgentDeliveryRecordAfterCursor(record, *cursor) {
			continue
		}
		detail, err := pendingAgentDeliveryDetailFromRecord(record)
		if err != nil {
			return PendingAgentDeliveryPage{}, err
		}
		out.PendingDeliveries = append(out.PendingDeliveries, detail)
		if len(out.PendingDeliveries) > limit {
			break
		}
	}
	if len(out.PendingDeliveries) > limit {
		lastVisible := out.PendingDeliveries[limit-1]
		out.NextCursor = encodePendingAgentDeliveryCursor(lastVisible)
		out.PendingDeliveries = out.PendingDeliveries[:limit]
	}
	return out, nil
}

func pendingAgentDeliveryDetailFromRecord(record pendingAgentDeliveryRecord) (PendingAgentDeliveryDetail, error) {
	detail := PendingAgentDeliveryDetail{
		EventID:    strings.TrimSpace(record.Event.ID()),
		EventName:  strings.TrimSpace(string(record.Event.Type())),
		EnqueuedAt: record.Event.CreatedAt().UTC(),
		Attempts:   record.DeliveryRetryCount,
		Event:      record.Event,
	}
	if detail.EventID == "" {
		return PendingAgentDeliveryDetail{}, fmt.Errorf("pending agent delivery detail event_id is required")
	}
	if detail.EventName == "" {
		return PendingAgentDeliveryDetail{}, fmt.Errorf("pending agent delivery detail event_name is required")
	}
	if detail.EnqueuedAt.IsZero() {
		return PendingAgentDeliveryDetail{}, fmt.Errorf("pending agent delivery detail enqueued_at is required")
	}
	if detail.Attempts < 0 {
		return PendingAgentDeliveryDetail{}, fmt.Errorf("pending agent delivery detail attempts must be non-negative")
	}
	return detail, nil
}

func pendingAgentDeliveryRecordAfterCursor(record pendingAgentDeliveryRecord, cursor pendingAgentDeliveryCursorPosition) bool {
	enqueuedAt := record.Event.CreatedAt().UTC()
	if enqueuedAt.After(cursor.EnqueuedAt) {
		return true
	}
	if enqueuedAt.Before(cursor.EnqueuedAt) {
		return false
	}
	return strings.TrimSpace(record.Event.ID()) > cursor.EventID
}

func encodePendingAgentDeliveryCursor(detail PendingAgentDeliveryDetail) string {
	raw, _ := json.Marshal(pendingAgentDeliveryCursor{
		Kind:       pendingAgentDeliveryCursorKind,
		EnqueuedAt: detail.EnqueuedAt.UTC().Format(time.RFC3339Nano),
		EventID:    strings.TrimSpace(detail.EventID),
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodePendingAgentDeliveryCursor(raw string) (pendingAgentDeliveryCursorPosition, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return pendingAgentDeliveryCursorPosition{}, ErrInvalidPendingAgentDeliveryCursor
	}
	var cursor pendingAgentDeliveryCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return pendingAgentDeliveryCursorPosition{}, ErrInvalidPendingAgentDeliveryCursor
	}
	if strings.TrimSpace(cursor.Kind) != pendingAgentDeliveryCursorKind || strings.TrimSpace(cursor.EventID) == "" || strings.TrimSpace(cursor.EnqueuedAt) == "" {
		return pendingAgentDeliveryCursorPosition{}, ErrInvalidPendingAgentDeliveryCursor
	}
	enqueuedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(cursor.EnqueuedAt))
	if err != nil {
		return pendingAgentDeliveryCursorPosition{}, ErrInvalidPendingAgentDeliveryCursor
	}
	return pendingAgentDeliveryCursorPosition{
		EnqueuedAt: enqueuedAt.UTC(),
		EventID:    strings.TrimSpace(cursor.EventID),
	}, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
