package sourceartifactstore

import (
	"context"
	"database/sql"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

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
