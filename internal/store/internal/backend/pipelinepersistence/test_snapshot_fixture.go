package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/staged"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// InsertSnapshotEventForTest can hold an inserted event before commit, proving
// insertion-sequence/commit inversion without lending SQL or publishing history.
func (s *PipelinePostgresOwner) InsertSnapshotEventForTest(ctx context.Context, record eventrecord.Record, inserted chan<- struct{}, commit <-chan struct{}) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates,
		func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			changed, err := staged.InsertWithinAttempt(ctx, attempt, authoractivity.DialectPostgres, record)
			if err != nil {
				return struct{}{}, err
			}
			if !changed {
				return struct{}{}, fmt.Errorf("snapshot event fixture %s was not inserted", record.EventID)
			}
			if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `
					INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
					SELECT e.event_id, e.run_id, $2, $3, $3 FROM events e WHERE e.event_id = $1::uuid
				`, record.EventID, string(runtimepipelineobligation.ScopeDirect), record.CreatedAt)
				return err
			}); err != nil {
				return struct{}{}, err
			}
			if inserted != nil {
				close(inserted)
			}
			if commit != nil {
				select {
				case <-commit:
				case <-ctx.Done():
					return struct{}{}, ctx.Err()
				}
			}
			return struct{}{}, nil
		})
	return result.Err()
}
