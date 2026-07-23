package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/eventrecord/postgres"
	"github.com/google/uuid"
)

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
	DeliveryID string
	EventID    string
	EventName  string
	EnqueuedAt time.Time
	Attempts   int
	Event      events.Event `json:"-"`
}

type pendingAgentDeliveryCursor struct {
	Kind       string `json:"kind"`
	EnqueuedAt string `json:"enqueued_at"`
	EventID    string `json:"event_id"`
	DeliveryID string `json:"delivery_id"`
}

func (s *PostgresStore) ListPendingAgentDeliveryFacts(ctx context.Context, agentIDs []string, since time.Time) (map[string]PendingAgentDeliveryFacts, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	normalized := normalizePendingAgentIDs(agentIDs)
	aggregates, err := postgresDeliveryAdapter.AgentPendingAggregates(ctx, s.DB, normalized, since)
	if err != nil {
		return nil, err
	}
	return pendingAgentDeliveryFactsFromAggregates(normalized, aggregates, time.Now()), nil
}

func (s *PostgresStore) ListPendingAgentDeliveryDetails(ctx context.Context, opts PendingAgentDeliveryListOptions) (PendingAgentDeliveryPage, error) {
	opts, cursor, empty, err := normalizePendingAgentDeliveryOptions(opts)
	if err != nil || empty {
		return PendingAgentDeliveryPage{PendingDeliveries: []PendingAgentDeliveryDetail{}}, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return PendingAgentDeliveryPage{}, err
	}
	aggregates, err := postgresDeliveryAdapter.AgentPendingAggregates(ctx, s.DB, []string{opts.AgentID}, opts.Since)
	if err != nil {
		return PendingAgentDeliveryPage{}, err
	}
	page, err := postgresDeliveryAdapter.AgentPendingReferencePage(ctx, s.DB, runtimedelivery.AgentPendingPageQuery{
		AgentID: opts.AgentID,
		Since:   opts.Since,
		Limit:   opts.Limit,
		After:   cursor,
	})
	if err != nil {
		return PendingAgentDeliveryPage{}, err
	}
	return pendingAgentDeliveryPageFromProjection(ctx, opts.AgentID, aggregates, page, time.Now(), func(ctx context.Context, eventID string) (eventrecord.Record, bool, error) {
		return eventrecordpostgres.Load(ctx, s.DB, eventID)
	})
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

func normalizePendingAgentDeliveryOptions(opts PendingAgentDeliveryListOptions) (PendingAgentDeliveryListOptions, *runtimedelivery.AgentPendingPosition, bool, error) {
	opts.AgentID = strings.TrimSpace(opts.AgentID)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.AgentID == "" {
		return opts, nil, true, nil
	}
	if opts.Limit == 0 {
		opts.Limit = DefaultPendingAgentDeliveryDetailLimit
	}
	if opts.Limit < 0 || opts.Limit > MaxPendingAgentDeliveryDetailLimit {
		return opts, nil, false, fmt.Errorf("pending agent delivery detail limit must be from 1 to %d", MaxPendingAgentDeliveryDetailLimit)
	}
	if opts.Cursor == "" {
		return opts, nil, false, nil
	}
	cursor, err := decodePendingAgentDeliveryCursor(opts.Cursor)
	if err != nil {
		return opts, nil, false, err
	}
	return opts, &cursor, false, nil
}

func pendingAgentDeliveryFactsFromAggregates(agentIDs []string, aggregates []runtimedelivery.AgentPendingAggregate, now time.Time) map[string]PendingAgentDeliveryFacts {
	out := make(map[string]PendingAgentDeliveryFacts, len(agentIDs))
	for _, agentID := range agentIDs {
		out[agentID] = PendingAgentDeliveryFacts{}
	}
	for _, aggregate := range aggregates {
		age := int(now.Sub(aggregate.OldestEventAt).Seconds())
		if age < 0 {
			age = 0
		}
		out[strings.TrimSpace(aggregate.AgentID)] = PendingAgentDeliveryFacts{
			PendingCount:        aggregate.Count,
			OldestPendingAgeSec: age,
		}
	}
	return out
}

func pendingAgentDeliveryPageFromProjection(
	ctx context.Context,
	agentID string,
	aggregates []runtimedelivery.AgentPendingAggregate,
	page runtimedelivery.AgentPendingReferencePage,
	now time.Time,
	load func(context.Context, string) (eventrecord.Record, bool, error),
) (PendingAgentDeliveryPage, error) {
	facts := pendingAgentDeliveryFactsFromAggregates([]string{agentID}, aggregates, now)[agentID]
	out := PendingAgentDeliveryPage{
		PendingCount:        facts.PendingCount,
		OldestPendingAgeSec: facts.OldestPendingAgeSec,
		PendingDeliveries:   make([]PendingAgentDeliveryDetail, 0, len(page.References)),
	}
	for _, reference := range page.References {
		durable, found, err := load(ctx, reference.Snapshot.EventID)
		if err != nil || !found {
			if err == nil {
				err = eventrecord.Missing(reference.Snapshot.EventID)
			}
			return PendingAgentDeliveryPage{}, err
		}
		admitted, err := durable.Decode()
		if err != nil {
			return PendingAgentDeliveryPage{}, err
		}
		event := admitted.Event()
		if event.ID() != reference.Snapshot.EventID || !event.CreatedAt().UTC().Equal(reference.EventCreatedAt.UTC()) {
			return PendingAgentDeliveryPage{}, fmt.Errorf("pending agent delivery event %s changed during hydration", reference.Snapshot.EventID)
		}
		delivery, err := events.NewDeliveryEvent(event, reference.Snapshot.Route)
		if err != nil {
			return PendingAgentDeliveryPage{}, err
		}
		detail, err := pendingAgentDeliveryDetailFromReference(reference, delivery.Event())
		if err != nil {
			return PendingAgentDeliveryPage{}, err
		}
		out.PendingDeliveries = append(out.PendingDeliveries, detail)
	}
	if page.HasMore && len(out.PendingDeliveries) > 0 {
		out.NextCursor = encodePendingAgentDeliveryCursor(out.PendingDeliveries[len(out.PendingDeliveries)-1])
	}
	return out, nil
}

func pendingAgentDeliveryDetailFromReference(reference runtimedelivery.AgentPendingReference, event events.Event) (PendingAgentDeliveryDetail, error) {
	detail := PendingAgentDeliveryDetail{
		DeliveryID: strings.TrimSpace(reference.Snapshot.DeliveryID),
		EventID:    strings.TrimSpace(event.ID()),
		EventName:  strings.TrimSpace(string(event.Type())),
		EnqueuedAt: reference.EventCreatedAt.UTC(),
		Attempts:   reference.Snapshot.RetryCount,
		Event:      event,
	}
	if detail.DeliveryID == "" {
		return PendingAgentDeliveryDetail{}, fmt.Errorf("pending agent delivery detail delivery_id is required")
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

func encodePendingAgentDeliveryCursor(detail PendingAgentDeliveryDetail) string {
	raw, _ := json.Marshal(pendingAgentDeliveryCursor{
		Kind:       pendingAgentDeliveryCursorKind,
		EnqueuedAt: detail.EnqueuedAt.UTC().Format(time.RFC3339Nano),
		EventID:    strings.TrimSpace(detail.EventID),
		DeliveryID: strings.TrimSpace(detail.DeliveryID),
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodePendingAgentDeliveryCursor(raw string) (runtimedelivery.AgentPendingPosition, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return runtimedelivery.AgentPendingPosition{}, ErrInvalidPendingAgentDeliveryCursor
	}
	var cursor pendingAgentDeliveryCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return runtimedelivery.AgentPendingPosition{}, ErrInvalidPendingAgentDeliveryCursor
	}
	if strings.TrimSpace(cursor.Kind) != pendingAgentDeliveryCursorKind ||
		strings.TrimSpace(cursor.EventID) == "" ||
		strings.TrimSpace(cursor.DeliveryID) == "" ||
		strings.TrimSpace(cursor.EnqueuedAt) == "" {
		return runtimedelivery.AgentPendingPosition{}, ErrInvalidPendingAgentDeliveryCursor
	}
	enqueuedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(cursor.EnqueuedAt))
	if err != nil {
		return runtimedelivery.AgentPendingPosition{}, ErrInvalidPendingAgentDeliveryCursor
	}
	if _, err := uuid.Parse(strings.TrimSpace(cursor.EventID)); err != nil {
		return runtimedelivery.AgentPendingPosition{}, ErrInvalidPendingAgentDeliveryCursor
	}
	if _, err := uuid.Parse(strings.TrimSpace(cursor.DeliveryID)); err != nil {
		return runtimedelivery.AgentPendingPosition{}, ErrInvalidPendingAgentDeliveryCursor
	}
	return runtimedelivery.AgentPendingPosition{
		EventCreatedAt: enqueuedAt.UTC(),
		EventID:        strings.TrimSpace(cursor.EventID),
		DeliveryID:     strings.TrimSpace(cursor.DeliveryID),
	}, nil
}
