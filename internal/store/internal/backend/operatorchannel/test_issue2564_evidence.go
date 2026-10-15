package operatorchannel

import (
	"context"
	"database/sql"
)

func ObserveFixturePrincipalTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var id string
	err := tx.QueryRowContext(ctx, `SELECT principal_id FROM operator_principals`).Scan(&id)
	return id, err
}

func (s *PostgresOwner) ObserveFixturePrincipalForTest(ctx context.Context) (string, error) {
	if err := s.requireCurrent(); err != nil {
		return "", err
	}
	var id string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		id, err = ObserveFixturePrincipalTx(ctx, tx)
		return err
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func (s *SQLiteOwner) ObserveFixturePrincipalForTest(ctx context.Context) (string, error) {
	if err := s.requireCurrent(); err != nil {
		return "", err
	}
	var id string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		id, err = ObserveFixturePrincipalTx(ctx, tx)
		return err
	})
	if err != nil {
		return "", err
	}
	return id, nil
}
