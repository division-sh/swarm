package sourceartifactstore

import (
	"context"
	"database/sql"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func (s *Postgres) EnsureRootedSource(ctx context.Context, source sourceartifact.RootedSource) (sourceartifact.RootedEnsureResult, error) {
	if err := source.Validate(); err != nil {
		return sourceartifact.RootedEnsureResult{}, err
	}
	if err := s.schemaGuard(); err != nil {
		return sourceartifact.RootedEnsureResult{}, err
	}
	var result sourceartifact.RootedEnsureResult
	err := s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = EnsurePostgresRootedSourceTx(txctx, tx, source, time.Now().UTC())
		return err
	})
	return result, err
}

func (s *Postgres) GetRootedSource(ctx context.Context, reference sourceartifact.RootAdmissionReference) (sourceartifact.RootedSource, error) {
	if err := s.schemaGuard(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	return loadRootedSource(ctx, s.backend, reference, true)
}
