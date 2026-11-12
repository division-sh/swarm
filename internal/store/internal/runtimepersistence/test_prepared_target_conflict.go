package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/google/uuid"
)

type PreparedTargetConflictStorage = delivery.PreparedTargetConflictStorage

func validatePreparedTargetConflictOwner(selected any, eventID, routeIdentity string) (bool, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return false, err
	}
	id, err := uuid.Parse(eventID)
	if err != nil || id == uuid.Nil || id.String() != eventID {
		return false, fmt.Errorf("prepared target conflict requires its exact canonical event")
	}
	if routeIdentity == "" || strings.TrimSpace(routeIdentity) != routeIdentity {
		return false, fmt.Errorf("prepared target conflict requires its exact existing route identity")
	}
	_, postgres := selected.(*PostgresStore)
	return postgres, nil
}

func SetPreparedTargetConflictForTest(ctx context.Context, selected any, eventID, originalIdentity string, conflicting events.DeliveryRoute) (int64, error) {
	_, err := validatePreparedTargetConflictOwner(selected, eventID, originalIdentity)
	if err != nil {
		return 0, err
	}
	identity, err := conflicting.Identity()
	if err != nil {
		return 0, err
	}
	target, err := json.Marshal(conflicting.Target)
	if err != nil {
		return 0, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.SetPreparedTargetConflictForTest(ctx, eventID, originalIdentity, conflicting.Recipient.ID(), events.EncodeDeliveryRouteIdentity(identity), string(target))
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.SetPreparedTargetConflictForTest(ctx, eventID, originalIdentity, conflicting.Recipient.ID(), events.EncodeDeliveryRouteIdentity(identity), string(target))
	default:
		return 0, fmt.Errorf("prepared target conflict requires its original native owner")
	}
}

func ReadPreparedTargetConflictForTest(ctx context.Context, selected any, eventID, routeIdentity string) (PreparedTargetConflictStorage, error) {
	_, err := validatePreparedTargetConflictOwner(selected, eventID, routeIdentity)
	if err != nil {
		return PreparedTargetConflictStorage{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadPreparedTargetConflictForTest(ctx, eventID, routeIdentity)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadPreparedTargetConflictForTest(ctx, eventID, routeIdentity)
	default:
		return PreparedTargetConflictStorage{}, fmt.Errorf("prepared target conflict requires its original native owner")
	}
}
