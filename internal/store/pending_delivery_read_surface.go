package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"swarm/internal/events"
)

const canonicalPendingDeliveryBackoff = time.Minute

type PendingAgentDeliveryFacts struct {
	PendingCount        int
	OldestPendingAgeSec int
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
	if r.Event.CreatedAt.IsZero() {
		return 0
	}
	age := int(now.Sub(r.Event.CreatedAt).Seconds())
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
	records, err := s.listPendingAgentDeliveryFactRecordsSpec(ctx, normalized, since)
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

func (s *PostgresStore) listPendingAgentDeliveryFactRecordsSpec(ctx context.Context, agentIDs []string, since time.Time) ([]pendingAgentDeliveryRecord, error) {
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
		return nil, fmt.Errorf("query pending agent delivery facts: %w", err)
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
			entityID, flowInstance string
			scope                  string
		)
		if err := rows.Scan(
			&record.AgentID,
			&record.Event.ID,
			&record.Event.RunID,
			&record.Event.Type,
			&record.Event.SourceAgent,
			&entityID,
			&flowInstance,
			&scope,
			&record.Event.Payload,
			&record.Event.CreatedAt,
			&record.Event.ParentEventID,
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
		record.Event = record.Event.WithEnvelope(events.EventEnvelope{
			EntityID:     entityID,
			FlowInstance: flowInstance,
			Scope:        events.EventScope(scope),
		})
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending agent delivery records: %w", err)
	}
	return out, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
