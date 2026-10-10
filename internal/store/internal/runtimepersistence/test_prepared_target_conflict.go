package runtimepersistence

import (
	"context"
	"database/sql"
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
	postgres, err := validatePreparedTargetConflictOwner(selected, eventID, originalIdentity)
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
	var changed int64
	err = runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = delivery.SetPreparedTargetConflictTx(ctx, tx, postgres, eventID, originalIdentity, conflicting.Recipient.ID(), events.EncodeDeliveryRouteIdentity(identity), string(target))
		return err
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}

func ReadPreparedTargetConflictForTest(ctx context.Context, selected any, eventID, routeIdentity string) (PreparedTargetConflictStorage, error) {
	postgres, err := validatePreparedTargetConflictOwner(selected, eventID, routeIdentity)
	if err != nil {
		return PreparedTargetConflictStorage{}, err
	}
	var out PreparedTargetConflictStorage
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadPreparedTargetConflictTx(ctx, tx, postgres, eventID, routeIdentity)
		return err
	})
	if err != nil {
		return PreparedTargetConflictStorage{}, err
	}
	return out, nil
}
