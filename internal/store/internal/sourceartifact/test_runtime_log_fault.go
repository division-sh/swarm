package sourceartifactstore

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *Postgres) RemoveRuntimeLogFixtureSourceArtifactForTest(ctx context.Context, hash string) (int64, error) {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return 0, fmt.Errorf("runtime log source fault requires the original postgres owner")
	}
	var removed int64
	err := s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		removed, err = removeRuntimeLogFixtureSourceArtifactTx(ctx, tx, hash)
		return err
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}

func (s *SQLite) RemoveRuntimeLogFixtureSourceArtifactForTest(ctx context.Context, hash string) (int64, error) {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return 0, fmt.Errorf("runtime log source fault requires the original sqlite owner")
	}
	var removed int64
	err := s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		var err error
		removed, err = removeRuntimeLogFixtureSourceArtifactTx(ctx, tx, hash)
		return err
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}

func removeRuntimeLogFixtureSourceArtifactTx(ctx context.Context, tx *sql.Tx, hash string) (int64, error) {
	result, err := tx.ExecContext(ctx, `DELETE FROM source_artifacts WHERE bundle_hash=$1`, hash)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
