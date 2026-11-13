package delivery

import (
	"context"
	"database/sql"
	"time"
)

func (s *DeliveryPostgresOwner) CountDeadLettersForOriginalEventForTest(ctx context.Context, originalEventID string) (int, error) {
	var out int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = CountDeadLettersForOriginalEvent(ctx, tx, originalEventID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) CountDeadLettersForOriginalEventForTest(ctx context.Context, originalEventID string) (int, error) {
	var out int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = CountDeadLettersForOriginalEvent(ctx, tx, originalEventID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ReadTargetFailureDeadLetterStorageForTest(ctx context.Context, eventID string) (TargetFailureDeadLetterStorage, error) {
	var out TargetFailureDeadLetterStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadTargetFailureDeadLetterStorage(ctx, tx, true, eventID)
		return err
	})
	if err != nil {
		return TargetFailureDeadLetterStorage{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadTargetFailureDeadLetterStorageForTest(ctx context.Context, eventID string) (TargetFailureDeadLetterStorage, error) {
	var out TargetFailureDeadLetterStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadTargetFailureDeadLetterStorage(ctx, tx, false, eventID)
		return err
	})
	if err != nil {
		return TargetFailureDeadLetterStorage{}, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ReadChainDepthDeadLetterStorageForTest(ctx context.Context, runID, entityID string) (ChainDepthDeadLetterStorage, error) {
	var out ChainDepthDeadLetterStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadChainDepthDeadLetterStorage(ctx, tx, true, runID, entityID)
		return err
	})
	if err != nil {
		return ChainDepthDeadLetterStorage{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadChainDepthDeadLetterStorageForTest(ctx context.Context, runID, entityID string) (ChainDepthDeadLetterStorage, error) {
	var out ChainDepthDeadLetterStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadChainDepthDeadLetterStorage(ctx, tx, false, runID, entityID)
		return err
	})
	if err != nil {
		return ChainDepthDeadLetterStorage{}, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) CountDeadLetterEntityRelationsSinceForTest(ctx context.Context, since time.Time, entityID string) (int, error) {
	var out int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = CountDeadLetterEntityRelationsSince(ctx, tx, true, since, entityID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) CountDeadLetterEntityRelationsSinceForTest(ctx context.Context, since time.Time, entityID string) (int, error) {
	var out int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = CountDeadLetterEntityRelationsSince(ctx, tx, false, since, entityID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ReadDeadLetterObservationRowsForTest(ctx context.Context) ([]DeadLetterObservationRow, error) {
	var out []DeadLetterObservationRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadDeadLetterObservationRows(ctx, tx, true)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadDeadLetterObservationRowsForTest(ctx context.Context) ([]DeadLetterObservationRow, error) {
	var out []DeadLetterObservationRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadDeadLetterObservationRows(ctx, tx, false)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
