package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func (s *SQLiteRuntimeStore) ListEventDeliveryTargets(ctx context.Context, eventID string) (map[string]events.RouteIdentity, error) {
	routes, err := s.ListEventDeliveryRoutes(ctx, eventID)
	if err != nil {
		return nil, err
	}
	out := map[string]events.RouteIdentity{}
	for _, route := range routes {
		if route.Recipient.IsAgent() && !route.Target.Empty() {
			out[route.Recipient.ID()] = route.Target.Route()
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
	snapshots, err := s.DeliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("list sqlite event delivery routes: %w", err)
	}
	out := make([]events.DeliveryRoute, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, snapshot.Route)
	}
	return events.NormalizeDeliveryRoutes(out), nil
}

func (s *SQLiteRuntimeStore) ListPendingAgentDeliveryFacts(ctx context.Context, identities []agentidentity.Identity, since time.Time) (map[agentidentity.Identity]operatorread.PendingAgentDeliveryFacts, error) {
	return s.operatorAgentSQLite.ListPendingAgentDeliveryFacts(ctx, identities, since)
}

func (s *SQLiteRuntimeStore) ListPendingAgentDeliveryDetails(ctx context.Context, opts operatorread.PendingAgentDeliveryListOptions) (operatorread.PendingAgentDeliveryPage, error) {
	return s.operatorAgentSQLite.ListPendingAgentDeliveryDetails(ctx, opts)
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
