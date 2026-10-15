package startupownership

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
)

func (s *StartupPostgresOwner) InspectSourceSetHead(ctx context.Context) (agenttopology.SourceSetPlan, bool, error) {
	if err := s.schemaGuard(); err != nil {
		return agenttopology.SourceSetPlan{}, false, err
	}
	var plan agenttopology.SourceSetPlan
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		plan, found, err = loadSourceSetModeTx(ctx, tx, false, false)
		return err
	})
	if err != nil {
		return agenttopology.SourceSetPlan{}, false, err
	}
	return plan, found, nil
}

func (s *StartupSQLiteOwner) InspectSourceSetHead(ctx context.Context) (agenttopology.SourceSetPlan, bool, error) {
	if err := s.schemaGuard(); err != nil {
		return agenttopology.SourceSetPlan{}, false, err
	}
	var plan agenttopology.SourceSetPlan
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		plan, found, err = loadSourceSetModeTx(ctx, tx, true, false)
		return err
	})
	if err != nil {
		return agenttopology.SourceSetPlan{}, false, err
	}
	return plan, found, nil
}
