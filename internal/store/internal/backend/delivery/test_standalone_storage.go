package delivery

import (
	"context"
	"database/sql"
)

func (s *DeliveryPostgresOwner) ReadExactAgentDeliveryStatusForTest(ctx context.Context, eventID, agentID string) (string, error) {
	var status string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		status, err = ReadExactAgentDeliveryStatusTx(ctx, tx, true, eventID, agentID)
		return err
	})
	if err != nil {
		return "", err
	}
	return status, nil
}

func (s *DeliverySQLiteOwner) ReadExactAgentDeliveryStatusForTest(ctx context.Context, eventID, agentID string) (string, error) {
	var status string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		status, err = ReadExactAgentDeliveryStatusTx(ctx, tx, false, eventID, agentID)
		return err
	})
	if err != nil {
		return "", err
	}
	return status, nil
}

func (s *DeliveryPostgresOwner) ReadStandaloneAgentDeliveryStorageForTest(ctx context.Context, eventID, agentID string) (StandaloneAgentDeliveryStorage, error) {
	var out StandaloneAgentDeliveryStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadStandaloneAgentDeliveryStorageTx(ctx, tx, eventID, agentID)
		return err
	})
	if err != nil {
		return StandaloneAgentDeliveryStorage{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadStandaloneAgentDeliveryStorageForTest(ctx context.Context, eventID, agentID string) (StandaloneAgentDeliveryStorage, error) {
	var out StandaloneAgentDeliveryStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadStandaloneAgentDeliveryStorageTx(ctx, tx, eventID, agentID)
		return err
	})
	if err != nil {
		return StandaloneAgentDeliveryStorage{}, err
	}
	return out, nil
}
