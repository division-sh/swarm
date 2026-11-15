package runforkpersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func (s *RunForkPostgresOwner) CaptureSelectedRunForkRevisionForTest(ctx context.Context, runID string, families ...runforkrevision.Family) (int64, error) {
	var revision int64
	err := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.candidates, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			effects := runforkrevision.NewEffects()
			if err := effects.Add(runID, families...); err != nil {
				return err
			}
			results, err := runforkrevision.FinalizePostgres(ctx, tx, effects)
			if err != nil {
				return err
			}
			revision = results[runID].Revision
			return nil
		})
		return struct{}{}, err
	}).Err()
	if err != nil {
		return 0, err
	}
	return revision, nil
}

func (s *RunForkSQLiteOwner) CaptureSelectedRunForkRevisionForTest(ctx context.Context, runID string, families ...runforkrevision.Family) (int64, error) {
	var revision int64
	err := mutationprotocol.RunSQLite(ctx, s.backend, "canonical event fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.candidates, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			effects := runforkrevision.NewEffects()
			if err := effects.Add(runID, families...); err != nil {
				return err
			}
			results, err := runforkrevision.FinalizeSQLite(ctx, tx, effects)
			if err != nil {
				return err
			}
			revision = results[runID].Revision
			return nil
		})
		return struct{}{}, err
	}).Err()
	if err != nil {
		return 0, err
	}
	return revision, nil
}
