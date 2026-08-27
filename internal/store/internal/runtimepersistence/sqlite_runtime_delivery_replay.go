package runtimepersistence

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

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
