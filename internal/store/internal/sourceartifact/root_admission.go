package sourceartifactstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func ensureRootAdmissionTx(ctx context.Context, tx *sql.Tx, admission sourceartifact.RootAdmission, postgres bool) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("root admission transaction is required")
	}
	if err := admission.Validate(); err != nil {
		return false, err
	}
	query := `INSERT INTO source_root_admissions (bundle_hash, root_directory) VALUES (?, ?) ON CONFLICT (bundle_hash, root_directory) DO NOTHING`
	if postgres {
		query = `INSERT INTO source_root_admissions (bundle_hash, root_directory) VALUES ($1, $2) ON CONFLICT (bundle_hash, root_directory) DO NOTHING`
	}
	result, err := tx.ExecContext(ctx, query, admission.BundleHash(), admission.RootDirectory())
	if err != nil {
		return false, fmt.Errorf("ensure exact source root admission: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read source root admission insertion evidence: %w", err)
	}
	return count == 1, nil
}

type rootAdmissionQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Only unbound selection may discover a sole retained pair. Bound operations
// use loadRootedSource with their frozen reference, never this fallback.
func resolveUnboundRootedSource(ctx context.Context, queryer rootAdmissionQueryer, hash string, postgres bool) (sourceartifact.RootedSource, error) {
	if err := sourceartifact.ValidateHash(hash); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if err := ctx.Err(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	query := `SELECT COUNT(*), COALESCE(MIN(root_directory), '') FROM source_root_admissions WHERE bundle_hash = ?`
	if postgres {
		query = `SELECT COUNT(*), COALESCE(MIN(root_directory), '') FROM source_root_admissions WHERE bundle_hash = $1`
	}
	var count int64
	var directory string
	if err := queryer.QueryRowContext(ctx, query, hash).Scan(&count, &directory); err != nil {
		return sourceartifact.RootedSource{}, fmt.Errorf("resolve unbound source root admission: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if count == 0 {
		return sourceartifact.RootedSource{}, sourceartifact.ErrRootAdmissionMissing
	}
	if count != 1 {
		return sourceartifact.RootedSource{}, fmt.Errorf("%w for %s: %d retained roots; use existing bound source evidence", sourceartifact.ErrRootAdmissionAmbiguous, hash, count)
	}
	return loadRootedSource(ctx, queryer, sourceartifact.RootAdmissionReference{BundleHash: hash, RootDirectory: directory}, postgres)
}

func loadRootedSource(ctx context.Context, queryer rootAdmissionQueryer, reference sourceartifact.RootAdmissionReference, postgres bool) (sourceartifact.RootedSource, error) {
	if err := reference.Validate(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if err := ctx.Err(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	query := `SELECT a.bundle_hash, a.source_blob, a.member_count, a.total_bytes, a.created_at, r.root_directory
		FROM source_artifacts a JOIN source_root_admissions r ON r.bundle_hash = a.bundle_hash
		WHERE a.bundle_hash = ? AND r.root_directory = ?`
	if postgres {
		query = `SELECT a.bundle_hash, a.source_blob, a.member_count, a.total_bytes, a.created_at, r.root_directory
			FROM source_artifacts a JOIN source_root_admissions r ON r.bundle_hash = a.bundle_hash
			WHERE a.bundle_hash = $1 AND r.root_directory = $2`
	}
	var stored sourceartifact.Persisted
	var directory string
	var created any
	err := queryer.QueryRowContext(ctx, query, reference.BundleHash, reference.RootDirectory).Scan(
		&stored.BundleHash, &stored.SourceBlob, &stored.MemberCount, &stored.TotalBytes, &created, &directory,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sourceartifact.RootedSource{}, sourceartifact.ErrRootAdmissionMissing
	}
	if err != nil {
		return sourceartifact.RootedSource{}, fmt.Errorf("load exact source root admission: %w", err)
	}
	stored.CreatedAt, err = sqliteTime(created)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if stored.BundleHash != reference.BundleHash || directory != reference.RootDirectory {
		return sourceartifact.RootedSource{}, fmt.Errorf("retained source root contradicts its exact requested reference")
	}
	rooted, err := sourceartifact.RestoreRootedSource(stored, reference)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if err := ctx.Err(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	return rooted, nil
}
