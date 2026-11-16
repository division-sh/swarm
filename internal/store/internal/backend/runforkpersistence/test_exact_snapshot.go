package runforkpersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func (s *RunForkPostgresOwner) CaptureSelectedRunForkRevisionForTest(ctx context.Context, runID string, families ...runforkrevision.Family) (int64, error) {
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.candidates, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		effects := runforkrevision.NewEffects()
		if err := effects.Add(runID, families...); err != nil {
			return struct{}{}, err
		}
		for _, family := range families {
			if err := attempt.AddWholeFamily(runID, family); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	})
	if err := result.Err(); err != nil {
		return 0, err
	}
	revision, _ := result.AcknowledgedRevision(runID)
	return revision, nil
}

func (s *RunForkSQLiteOwner) CaptureSelectedRunForkRevisionForTest(ctx context.Context, runID string, families ...runforkrevision.Family) (int64, error) {
	result := mutationprotocol.RunSQLite(ctx, s.backend, "canonical event fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.candidates, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		effects := runforkrevision.NewEffects()
		if err := effects.Add(runID, families...); err != nil {
			return struct{}{}, err
		}
		for _, family := range families {
			if err := attempt.AddWholeFamily(runID, family); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	})
	if err := result.Err(); err != nil {
		return 0, err
	}
	revision, _ := result.AcknowledgedRevision(runID)
	return revision, nil
}
