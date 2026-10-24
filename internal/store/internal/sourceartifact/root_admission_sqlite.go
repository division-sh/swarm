package sourceartifactstore

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func (s *SQLite) EnsureRootedSource(ctx context.Context, source sourceartifact.RootedSource) (sourceartifact.RootedEnsureResult, error) {
	if err := source.Validate(); err != nil {
		return sourceartifact.RootedEnsureResult{}, err
	}
	if err := s.schemaGuard(); err != nil {
		return sourceartifact.RootedEnsureResult{}, err
	}
	var result sourceartifact.RootedEnsureResult
	err := s.backend.RunTransaction(ctx, "sqlite rooted source ensure", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = EnsureSQLiteRootedSourceTx(txctx, tx, source, s.now())
		return err
	})
	return result, err
}

func (s *SQLite) GetRootedSource(ctx context.Context, reference sourceartifact.RootAdmissionReference) (sourceartifact.RootedSource, error) {
	if err := s.schemaGuard(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	return loadRootedSource(ctx, s.backend, reference, false)
}

func (s *SQLite) ResolveUnboundRootedSource(ctx context.Context, hash string) (sourceartifact.RootedSource, error) {
	if err := s.schemaGuard(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	return resolveUnboundRootedSource(ctx, s.backend, hash, false)
}
