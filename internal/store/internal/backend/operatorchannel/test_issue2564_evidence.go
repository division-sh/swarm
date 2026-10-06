package operatorchannel

import (
	"context"
	"database/sql"
)

func (s *PostgresOwner) ObserveFixturePrincipalForTest(ctx context.Context) (string, error) {
	if err := s.requireCurrent(); err != nil {
		return "", err
	}
	var id string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT principal_id FROM operator_principals`).Scan(&id)
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
		return tx.QueryRowContext(ctx, `SELECT principal_id FROM operator_principals`).Scan(&id)
	})
	if err != nil {
		return "", err
	}
	return id, nil
}
