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

func (s *Postgres) EnsureSourceArtifact(ctx context.Context, artifact *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error) {
	if err := s.schemaGuard(); err != nil {
		return sourceartifact.EnsureResult{}, err
	}
	var result sourceartifact.EnsureResult
	err := s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = EnsurePostgresSourceArtifactTx(txctx, tx, artifact, time.Now())
		return err
	})
	return result, err
}

func EnsurePostgresSourceArtifactTx(ctx context.Context, tx *sql.Tx, artifact *sourceartifact.AdmittedSourceArtifact, now time.Time) (sourceartifact.EnsureResult, error) {
	if tx == nil {
		return sourceartifact.EnsureResult{}, fmt.Errorf("source artifact transaction is required")
	}
	requested, err := sourceartifact.PersistedFromArtifact(artifact, now)
	if err != nil {
		return sourceartifact.EnsureResult{}, err
	}
	execResult, err := tx.ExecContext(ctx, `
			INSERT INTO source_artifacts (bundle_hash, source_blob, member_count, total_bytes)
			VALUES ($1, $2::bytea, $3, $4)
			ON CONFLICT (bundle_hash) DO NOTHING
		`, requested.BundleHash, requested.SourceBlob, requested.MemberCount, requested.TotalBytes)
	if err != nil {
		return sourceartifact.EnsureResult{}, fmt.Errorf("ensure source artifact: %w", err)
	}
	created := false
	if rows, rowsErr := execResult.RowsAffected(); rowsErr == nil {
		created = rows > 0
	}
	stored, err := loadPostgresSourceArtifact(ctx, tx, requested.BundleHash)
	if err != nil {
		return sourceartifact.EnsureResult{}, err
	}
	if !bytes.Equal(stored.SourceBlob, requested.SourceBlob) || stored.MemberCount != requested.MemberCount || stored.TotalBytes != requested.TotalBytes {
		return sourceartifact.EnsureResult{}, &sourceartifact.ConflictError{BundleHash: requested.BundleHash}
	}
	return sourceartifact.EnsureResult{Artifact: stored, Created: created}, nil
}

func (s *Postgres) GetSourceArtifact(ctx context.Context, bundleHash string) (sourceartifact.Persisted, error) {
	if err := s.schemaGuard(); err != nil {
		return sourceartifact.Persisted{}, err
	}
	return loadPostgresSourceArtifact(ctx, s.backend, bundleHash)
}

type postgresSourceArtifactQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadPostgresSourceArtifact(ctx context.Context, queryer postgresSourceArtifactQueryer, bundleHash string) (sourceartifact.Persisted, error) {
	var out sourceartifact.Persisted
	err := queryer.QueryRowContext(ctx, `
		SELECT bundle_hash, source_blob, member_count, total_bytes, created_at
		FROM source_artifacts WHERE bundle_hash = $1
	`, bundleHash).Scan(&out.BundleHash, &out.SourceBlob, &out.MemberCount, &out.TotalBytes, &out.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return sourceartifact.Persisted{}, sourceartifact.ErrNotFound
	}
	if err != nil {
		return sourceartifact.Persisted{}, fmt.Errorf("get source artifact: %w", err)
	}
	if err := out.Validate(); err != nil {
		return sourceartifact.Persisted{}, err
	}
	return out, nil
}
