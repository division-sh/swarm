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

func (s *DeliveryPostgresOwner) ReadFirstAgentClaimProfile(ctx context.Context, expected deliverylifecycle.Snapshot) (deliverylifecycle.AgentClaimProfile, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return deliverylifecycle.AgentClaimProfile{}, err
	}
	var profile deliverylifecycle.AgentClaimProfile
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		profile, err = ReadFirstAgentClaimProfileTx(ctx, tx, DialectPostgres, expected)
		return err
	})
	if err != nil {
		return deliverylifecycle.AgentClaimProfile{}, err
	}
	return profile, nil
}

func (s *DeliverySQLiteOwner) ReadFirstAgentClaimProfile(ctx context.Context, expected deliverylifecycle.Snapshot) (deliverylifecycle.AgentClaimProfile, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return deliverylifecycle.AgentClaimProfile{}, err
	}
	var profile deliverylifecycle.AgentClaimProfile
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		profile, err = ReadFirstAgentClaimProfileTx(ctx, tx, DialectSQLite, expected)
		return err
	})
	if err != nil {
		return deliverylifecycle.AgentClaimProfile{}, err
	}
	return profile, nil
}
