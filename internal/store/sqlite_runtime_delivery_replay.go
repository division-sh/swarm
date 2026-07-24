package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/eventrecord/sqlite"
)

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

func (s *SQLiteRuntimeStore) ListPendingAgentDeliveryFacts(ctx context.Context, agentIDs []string, since time.Time) (map[string]PendingAgentDeliveryFacts, error) {
	normalized := normalizePendingAgentIDs(agentIDs)
	aggregates, err := sqliteDeliveryAdapter.AgentPendingAggregates(ctx, s.DB, normalized, since)
	if err != nil {
		return nil, err
	}
	return pendingAgentDeliveryFactsFromAggregates(normalized, aggregates, s.now()), nil
}

func (s *SQLiteRuntimeStore) ListPendingAgentDeliveryDetails(ctx context.Context, opts PendingAgentDeliveryListOptions) (PendingAgentDeliveryPage, error) {
	opts, cursor, empty, err := normalizePendingAgentDeliveryOptions(opts)
	if err != nil || empty {
		return PendingAgentDeliveryPage{PendingDeliveries: []PendingAgentDeliveryDetail{}}, err
	}
	aggregates, err := sqliteDeliveryAdapter.AgentPendingAggregates(ctx, s.DB, []string{opts.AgentID}, opts.Since)
	if err != nil {
		return PendingAgentDeliveryPage{}, err
	}
	page, err := sqliteDeliveryAdapter.AgentPendingReferencePage(ctx, s.DB, runtimedelivery.AgentPendingPageQuery{
		AgentID: opts.AgentID,
		Since:   opts.Since,
		Limit:   opts.Limit,
		After:   cursor,
	})
	if err != nil {
		return PendingAgentDeliveryPage{}, err
	}
	return pendingAgentDeliveryPageFromProjection(ctx, opts.AgentID, aggregates, page, s.now(), func(ctx context.Context, eventID string) (eventrecord.Record, bool, error) {
		return eventrecordsqlite.Load(ctx, s.DB, eventID)
	})
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
