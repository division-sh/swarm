package delivery

import (
	"context"
	"database/sql"
)

func (s *DeliveryPostgresOwner) SetPreparedTargetConflictForTest(ctx context.Context, eventID, originalIdentity, subscriberID, corruptIdentity, corruptTarget string) (int64, error) {
	var changed int64
	err := s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = SetPreparedTargetConflictTx(ctx, tx, true, eventID, originalIdentity, subscriberID, corruptIdentity, corruptTarget)
		return err
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}

func (s *DeliverySQLiteOwner) SetPreparedTargetConflictForTest(ctx context.Context, eventID, originalIdentity, subscriberID, corruptIdentity, corruptTarget string) (int64, error) {
	var changed int64
	err := s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = SetPreparedTargetConflictTx(ctx, tx, false, eventID, originalIdentity, subscriberID, corruptIdentity, corruptTarget)
		return err
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
		out, err = ReadPreparedTargetConflictTx(ctx, tx, true, eventID, routeIdentity)
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
		out, err = ReadPreparedTargetConflictTx(ctx, tx, false, eventID, routeIdentity)
		return err
	})
	if err != nil {
		return PreparedTargetConflictStorage{}, err
	}
	return out, nil
}
