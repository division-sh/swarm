package store

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"github.com/division-sh/swarm/internal/events"
)

var errLocalDeliveryHandoffNotCommitted = errors.New("exact local delivery handoff is not committed")

func proveLocalDeliveryHandoff(ctx context.Context, eventID string, route events.DeliveryRoute, read func(context.Context, string) ([]events.DeliveryRoute, error)) error {
	eventID = strings.TrimSpace(eventID)
	want := events.NormalizeDeliveryRoutes([]events.DeliveryRoute{route})
	if eventID == "" || len(want) != 1 || strings.TrimSpace(want[0].SubscriberType) == "" || strings.TrimSpace(want[0].SubscriberID) == "" {
		return errLocalDeliveryHandoffNotCommitted
	}
	routes, err := read(ctx, eventID)
	if err != nil {
		return err
	}
	for _, candidate := range events.NormalizeDeliveryRoutes(routes) {
		if reflect.DeepEqual(candidate, want[0]) {
			return nil
		}
	}
	return errLocalDeliveryHandoffNotCommitted
}

func (s *PostgresStore) ProveLocalDeliveryHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) error {
	return proveLocalDeliveryHandoff(ctx, eventID, route, s.ListEventDeliveryRoutes)
}

func (s *SQLiteRuntimeStore) ProveLocalDeliveryHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) error {
	return proveLocalDeliveryHandoff(ctx, eventID, route, s.ListEventDeliveryRoutes)
}
