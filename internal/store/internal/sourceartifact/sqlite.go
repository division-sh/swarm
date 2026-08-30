package sourceartifactstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func (s *SQLite) EnsureSourceArtifact(ctx context.Context, artifact *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error) {
	if err := s.schemaGuard(); err != nil {
		return sourceartifact.EnsureResult{}, err
	}
	var result sourceartifact.EnsureResult
	err := s.backend.RunTransaction(ctx, "sqlite source artifact ensure", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = EnsureSQLiteSourceArtifactTx(txctx, tx, artifact, s.now())
		return err
	})
	return result, err
}

func EnsureSQLiteSourceArtifactTx(ctx context.Context, tx *sql.Tx, artifact *sourceartifact.AdmittedSourceArtifact, now time.Time) (sourceartifact.EnsureResult, error) {
	if tx == nil {
		return sourceartifact.EnsureResult{}, fmt.Errorf("source artifact transaction is required")
	}
	requested, err := sourceartifact.PersistedFromArtifact(artifact, now)
	if err != nil {
		return sourceartifact.EnsureResult{}, err
	}
	execResult, err := tx.ExecContext(ctx, `
			INSERT INTO source_artifacts (bundle_hash, source_blob, member_count, total_bytes, created_at)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT (bundle_hash) DO NOTHING
		`, requested.BundleHash, requested.SourceBlob, requested.MemberCount, requested.TotalBytes, requested.CreatedAt)
	if err != nil {
		return sourceartifact.EnsureResult{}, fmt.Errorf("ensure sqlite source artifact: %w", err)
	}
	created := false
	if rows, rowsErr := execResult.RowsAffected(); rowsErr == nil {
		created = rows > 0
	}
	stored, err := loadSQLiteSourceArtifact(ctx, tx, requested.BundleHash)
	if err != nil {
		return sourceartifact.EnsureResult{}, err
	}
	if !bytes.Equal(stored.SourceBlob, requested.SourceBlob) || stored.MemberCount != requested.MemberCount || stored.TotalBytes != requested.TotalBytes {
		return sourceartifact.EnsureResult{}, &sourceartifact.ConflictError{BundleHash: requested.BundleHash}
	}
	return sourceartifact.EnsureResult{Artifact: stored, Created: created}, nil
}

func (s *SQLite) GetSourceArtifact(ctx context.Context, bundleHash string) (sourceartifact.Persisted, error) {
	if err := s.schemaGuard(); err != nil {
		return sourceartifact.Persisted{}, err
	}
	return loadSQLiteSourceArtifact(ctx, s.backend, bundleHash)
}

type sqliteSourceArtifactQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadSQLiteSourceArtifact(ctx context.Context, queryer sqliteSourceArtifactQueryer, bundleHash string) (sourceartifact.Persisted, error) {
	var out sourceartifact.Persisted
	var created any
	err := queryer.QueryRowContext(ctx, `
		SELECT bundle_hash, source_blob, member_count, total_bytes, created_at
		FROM source_artifacts WHERE bundle_hash = ?
	`, bundleHash).Scan(&out.BundleHash, &out.SourceBlob, &out.MemberCount, &out.TotalBytes, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return sourceartifact.Persisted{}, sourceartifact.ErrNotFound
	}
	if err != nil {
		return sourceartifact.Persisted{}, fmt.Errorf("get sqlite source artifact: %w", err)
	}
	out.CreatedAt, err = sqliteTime(created)
	if err != nil {
		return sourceartifact.Persisted{}, err
	}
	if err := out.Validate(); err != nil {
		return sourceartifact.Persisted{}, err
	}
	return out, nil
}

func sqliteTime(raw any) (time.Time, error) {
	switch value := raw.(type) {
	case time.Time:
		return value.UTC(), nil
	case string:
		return parseSQLiteTime(value)
	case []byte:
		return parseSQLiteTime(string(value))
	default:
		return time.Time{}, fmt.Errorf("source artifact created_at has unsupported type %T", raw)
	}
}

func parseSQLiteTime(raw string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("source artifact created_at %q is invalid", raw)
}
