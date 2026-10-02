package pipelinepersistence

import (
	"context"
	"database/sql"
	"time"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// These named fixture methods share the ordinary scope/disposition projection.
// The caller owns the exact selected transaction; no owner is reconstructed.
func (s *PipelinePostgresOwner) CommitUnrevisionedEventFixtureFactsTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, at time.Time) error {
	return commitUnrevisionedEventFixtureFactsTx(ctx, tx, eventID, scope, disposition, true, at)
}

func (s *PipelineSQLiteOwner) CommitUnrevisionedEventFixtureFactsTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, at time.Time) error {
	return commitUnrevisionedEventFixtureFactsTx(ctx, tx, eventID, scope, disposition, false, at)
}

func commitUnrevisionedEventFixtureFactsTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, postgres bool, at time.Time) error {
	if err := persistCommittedPipelineScopeTx(ctx, tx, eventID, scope, postgres, at); err != nil {
		return err
	}
	if disposition == nil {
		return nil
	}
	if _, err := persistExactPlatformPipelineReceipt(ctx, tx, eventID, *disposition, postgres, at); err != nil {
		return err
	}
	if disposition.Successful() {
		if postgres {
			return postgresDeliveryAdapter.CommitUnrevisionedFixturePipelineHandoffTx(ctx, tx, eventID)
		}
		return sqliteDeliveryAdapter.CommitUnrevisionedFixturePipelineHandoffTx(ctx, tx, eventID)
	}
	return nil
}

func (s *PipelinePostgresOwner) CommitRevisionedEventFixtureFactsTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, at time.Time) error {
	return commitRevisionedEventFixtureFactsTx(ctx, attempt, eventID, scope, disposition, true, at)
}

func (s *PipelineSQLiteOwner) CommitRevisionedEventFixtureFactsTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, at time.Time) error {
	return commitRevisionedEventFixtureFactsTx(ctx, attempt, eventID, scope, disposition, false, at)
}

func commitRevisionedEventFixtureFactsTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, postgres bool, at time.Time) error {
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := insertCommittedPipelineScopeTx(ctx, tx, attempt, eventID, scope, postgres, at); err != nil {
			return err
		}
		if disposition == nil {
			return nil
		}
		_, err := writeExactPlatformPipelineReceipt(ctx, tx, attempt, eventID, *disposition, postgres, at)
		return err
	}); err != nil {
		return err
	}
	if disposition != nil && disposition.Successful() {
		if postgres {
			return postgresDeliveryAdapter.CommitPipelineHandoff(ctx, attempt, eventID)
		}
		return sqliteDeliveryAdapter.CommitPipelineHandoff(ctx, attempt, eventID)
	}
	return nil
}
