package sourceartifactstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

// RequireRootAdmissionTx validates an already-admitted pair in the caller's
// native write transaction. It never discovers a root or creates evidence.
func RequireRootAdmissionTx(ctx context.Context, tx *sql.Tx, admission sourceartifact.RootAdmission, postgres bool) error {
	if tx == nil {
		return fmt.Errorf("root admission transaction is required")
	}
	if err := admission.Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	query := `SELECT EXISTS (SELECT 1 FROM source_root_admissions WHERE bundle_hash = ? AND root_directory = ?)`
	if postgres {
		query = `SELECT EXISTS (SELECT 1 FROM source_root_admissions WHERE bundle_hash = $1 AND root_directory = $2)`
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, query, admission.BundleHash(), admission.RootDirectory()).Scan(&exists); err != nil {
		return fmt.Errorf("validate exact source root admission: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !exists {
		return sourceartifact.ErrRootAdmissionMissing
	}
	return nil
}

func LoadRootedSourceTx(ctx context.Context, tx *sql.Tx, reference sourceartifact.RootAdmissionReference, postgres bool) (sourceartifact.RootedSource, error) {
	if tx == nil {
		return sourceartifact.RootedSource{}, fmt.Errorf("root admission transaction is required")
	}
	return loadRootedSource(ctx, tx, reference, postgres)
}

func EnsureSQLiteRootedSourceTx(ctx context.Context, tx *sql.Tx, source sourceartifact.RootedSource, now time.Time) (sourceartifact.RootedEnsureResult, error) {
	if err := source.Validate(); err != nil {
		return sourceartifact.RootedEnsureResult{}, err
	}
	content, err := EnsureSQLiteSourceArtifactTx(ctx, tx, source.Artifact(), now)
	if err != nil {
		return sourceartifact.RootedEnsureResult{}, err
	}
	created, err := ensureRootAdmissionTx(ctx, tx, source.Admission(), false)
	if err != nil {
		return sourceartifact.RootedEnsureResult{}, err
	}
	return sourceartifact.RootedEnsureResult{Source: source, Artifact: content, Created: created}, nil
}

func EnsurePostgresRootedSourceTx(ctx context.Context, tx *sql.Tx, source sourceartifact.RootedSource, now time.Time) (sourceartifact.RootedEnsureResult, error) {
	if err := source.Validate(); err != nil {
		return sourceartifact.RootedEnsureResult{}, err
	}
	content, err := EnsurePostgresSourceArtifactTx(ctx, tx, source.Artifact(), now)
	if err != nil {
		return sourceartifact.RootedEnsureResult{}, err
	}
	created, err := ensureRootAdmissionTx(ctx, tx, source.Admission(), true)
	if err != nil {
		return sourceartifact.RootedEnsureResult{}, err
	}
	return sourceartifact.RootedEnsureResult{Source: source, Artifact: content, Created: created}, nil
}
