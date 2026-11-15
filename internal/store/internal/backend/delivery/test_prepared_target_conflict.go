package delivery

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *DeliveryPostgresOwner) SetPreparedTargetConflictForTest(ctx context.Context, eventID, originalIdentity, subscriberID, corruptIdentity, corruptTarget string) (int64, error) {
	var changed int64
	err := s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if tx == nil {
			return fmt.Errorf("prepared target conflict requires its selected write transaction")
		}
		result, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET subscriber_id=$1, route_identity=$2, delivery_target_route=$3::jsonb WHERE event_id=$4::uuid AND route_identity=$5`, subscriberID, corruptIdentity, corruptTarget, eventID, originalIdentity)
		if err != nil {
			return err
		}
		changed, err = result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return fmt.Errorf("prepared target conflict requires its exact existing delivery: rows=%d", changed)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}

func (s *DeliverySQLiteOwner) SetPreparedTargetConflictForTest(ctx context.Context, eventID, originalIdentity, subscriberID, corruptIdentity, corruptTarget string) (int64, error) {
	var changed int64
	err := s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		if tx == nil {
			return fmt.Errorf("prepared target conflict requires its selected write transaction")
		}
		result, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET subscriber_id=?, route_identity=?, delivery_target_route=? WHERE event_id=? AND route_identity=?`, subscriberID, corruptIdentity, corruptTarget, eventID, originalIdentity)
		if err != nil {
			return err
		}
		changed, err = result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return fmt.Errorf("prepared target conflict requires its exact existing delivery: rows=%d", changed)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}

func (s *DeliveryPostgresOwner) ReadPreparedTargetConflictForTest(ctx context.Context, eventID, routeIdentity string) (PreparedTargetConflictStorage, error) {
	var out PreparedTargetConflictStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readPreparedTargetConflictTx(ctx, tx, true, eventID, routeIdentity)
		return err
	})
	if err != nil {
		return PreparedTargetConflictStorage{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadPreparedTargetConflictForTest(ctx context.Context, eventID, routeIdentity string) (PreparedTargetConflictStorage, error) {
	var out PreparedTargetConflictStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readPreparedTargetConflictTx(ctx, tx, false, eventID, routeIdentity)
		return err
	})
	if err != nil {
		return PreparedTargetConflictStorage{}, err
	}
	return out, nil
}
