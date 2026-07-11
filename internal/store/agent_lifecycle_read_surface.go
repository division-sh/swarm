package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"

	"github.com/lib/pq"
)

type AgentDeliveryLifecycleFacts struct {
	CurrentState  string
	BlockingLayer string
}

type agentLifecycleDeliveryRecord struct {
	AgentID         string
	Status          string
	ActiveSessionID string
	CreatedAt       time.Time
	DeliveredAt     sql.NullTime
}

func RequireCanonicalAgentLifecycleCapabilities(caps StoreSchemaCapabilities) error {
	if caps.Events.Deliveries == SchemaFlavorCanonical {
		return nil
	}
	return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
}

func (s *PostgresStore) ListAgentDeliveryLifecycleFacts(ctx context.Context, agentIDs []string) (map[string]AgentDeliveryLifecycleFacts, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if err := RequireCanonicalAgentLifecycleCapabilities(caps); err != nil {
		return nil, err
	}
	normalized := normalizePendingAgentIDs(agentIDs)
	if len(normalized) == 0 {
		return map[string]AgentDeliveryLifecycleFacts{}, nil
	}
	records, err := s.listAgentLifecycleRecordsSpec(ctx, normalized)
	if err != nil {
		return nil, err
	}
	out := make(map[string]AgentDeliveryLifecycleFacts, len(normalized))
	for _, agentID := range normalized {
		out[agentID] = AgentDeliveryLifecycleFacts{}
	}
	grouped := make(map[string][]agentLifecycleDeliveryRecord, len(normalized))
	for _, record := range records {
		grouped[record.AgentID] = append(grouped[record.AgentID], record)
	}
	for _, agentID := range normalized {
		out[agentID] = canonicalAgentDeliveryLifecycleFactsFromRecords(grouped[agentID])
	}
	return out, nil
}

func (s *PostgresStore) listAgentLifecycleRecordsSpec(ctx context.Context, agentIDs []string) ([]agentLifecycleDeliveryRecord, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			d.subscriber_id,
			COALESCE(d.status, ''),
			COALESCE(d.active_session_id::text, ''),
			d.created_at,
			d.delivered_at
		FROM event_deliveries d
		WHERE d.subscriber_type = 'agent'
		  AND d.subscriber_id = ANY($1)
		  AND COALESCE(d.status, '') IN ('pending', 'in_progress', 'failed', 'dead_letter')
	`, pq.Array(agentIDs))
	if err != nil {
		return nil, fmt.Errorf("query agent lifecycle records: %w", err)
	}
	defer rows.Close()

	out := make([]agentLifecycleDeliveryRecord, 0)
	for rows.Next() {
		var record agentLifecycleDeliveryRecord
		if err := rows.Scan(
			&record.AgentID,
			&record.Status,
			&record.ActiveSessionID,
			&record.CreatedAt,
			&record.DeliveredAt,
		); err != nil {
			return nil, fmt.Errorf("scan agent lifecycle record: %w", err)
		}
		record.AgentID = strings.TrimSpace(record.AgentID)
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agent lifecycle rows: %w", err)
	}
	return out, nil
}

type agentLifecycleCandidate struct {
	facts      AgentDeliveryLifecycleFacts
	observedAt time.Time
	priority   int
}

func canonicalAgentDeliveryLifecycleFactsFromRecords(records []agentLifecycleDeliveryRecord) AgentDeliveryLifecycleFacts {
	var live *agentLifecycleCandidate
	var exhausted *agentLifecycleCandidate
	for _, record := range records {
		state, ok := runtimedelivery.StateFromDelivery(record.Status, record.ActiveSessionID)
		if !ok {
			continue
		}
		candidate := agentLifecycleCandidate{
			facts: AgentDeliveryLifecycleFacts{
				CurrentState:  string(state),
				BlockingLayer: agentLifecycleBlockingLayer(state),
			},
			observedAt: agentLifecycleObservedAt(record),
			priority:   agentLifecyclePriority(state),
		}
		switch state {
		case runtimedelivery.StateQueued, runtimedelivery.StateLaunching, runtimedelivery.StateActive, runtimedelivery.StateRetrying:
			if live == nil || candidate.priority > live.priority || (candidate.priority == live.priority && candidate.observedAt.After(live.observedAt)) {
				live = &candidate
			}
		case runtimedelivery.StateExhausted:
			if exhausted == nil || candidate.observedAt.After(exhausted.observedAt) {
				exhausted = &candidate
			}
		}
	}
	if live != nil {
		return live.facts
	}
	if exhausted != nil {
		return exhausted.facts
	}
	return AgentDeliveryLifecycleFacts{}
}

func agentLifecycleObservedAt(record agentLifecycleDeliveryRecord) time.Time {
	if record.DeliveredAt.Valid {
		return record.DeliveredAt.Time
	}
	return record.CreatedAt
}

func agentLifecyclePriority(state runtimedelivery.State) int {
	switch state {
	case runtimedelivery.StateRetrying:
		return 4
	case runtimedelivery.StateLaunching:
		return 3
	case runtimedelivery.StateActive:
		return 2
	case runtimedelivery.StateQueued:
		return 1
	default:
		return 0
	}
}

func agentLifecycleBlockingLayer(state runtimedelivery.State) string {
	switch state {
	case runtimedelivery.StateQueued:
		return "delivery_queue"
	case runtimedelivery.StateLaunching:
		return "session_launch"
	case runtimedelivery.StateActive:
		return "session_execution"
	case runtimedelivery.StateRetrying:
		return "delivery_retry"
	case runtimedelivery.StateExhausted:
		return "delivery_terminal"
	default:
		return ""
	}
}
