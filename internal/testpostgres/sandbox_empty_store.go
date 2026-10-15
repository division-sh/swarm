package testpostgres

import (
	"context"
	"fmt"
)

func (s *Sandbox) validateEmptyStoreAdministration() error {
	if s == nil || s.manager == nil || s.DB == nil || s.leaseConn == nil || s.Name == "" || s.Name != s.Connection.Parameters().Database {
		return fmt.Errorf("empty-store administration requires the original leased sandbox")
	}
	return nil
}

func (s *Sandbox) PublicTableCount(ctx context.Context) (int, error) {
	if err := s.validateEmptyStoreAdministration(); err != nil {
		return 0, err
	}
	var count int
	err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'`).Scan(&count)
	return count, err
}

func (s *Sandbox) ResetPublicSchema(ctx context.Context) error {
	if err := s.validateEmptyStoreAdministration(); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DROP SCHEMA public CASCADE`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `CREATE SCHEMA public`); err != nil {
		return err
	}
	return tx.Commit()
}
