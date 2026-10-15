package delivery

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func (s *DeliveryPostgresOwner) ReadActiveDeliverySnapshots(ctx context.Context) ([]deliverylifecycle.Snapshot, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var snapshots []deliverylifecycle.Snapshot
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		snapshots, err = postgresDeliveryAdapter.ActiveSnapshots(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return snapshots, nil
}

func (s *DeliverySQLiteOwner) ReadActiveDeliverySnapshots(ctx context.Context) ([]deliverylifecycle.Snapshot, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var snapshots []deliverylifecycle.Snapshot
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		snapshots, err = sqliteDeliveryAdapter.ActiveSnapshots(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return snapshots, nil
}
