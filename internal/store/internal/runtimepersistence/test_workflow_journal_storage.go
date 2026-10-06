package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

type WorkflowJournalStorage struct {
	StoryCount, StoryHead, RunForkRevisions int64
	MockAttemptStories, Attempts            int64
}

func ReadWorkflowJournalStorageForTest(ctx context.Context, selected any, runID string) (WorkflowJournalStorage, error) {
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return WorkflowJournalStorage{}, fmt.Errorf("journal storage requires exact canonical run identity")
	}
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return WorkflowJournalStorage{}, err
	}
	var evidence WorkflowJournalStorage
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		mode := `json_extract(projection, '$.execution_mode')`
		if dialect == authoractivityfixture.DialectPostgres {
			mode = `projection->>'execution_mode'`
		}
		if err := tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM author_activity_occurrences),
			(SELECT last_sequence FROM author_activity_order WHERE singleton_id=1),
			(SELECT COUNT(*) FROM activity_attempts WHERE run_id=$1),
			(SELECT COUNT(*) FROM author_activity_occurrences WHERE `+mode+`='mock' AND source_owner='activity_attempts')`, runID).
			Scan(&evidence.StoryCount, &evidence.StoryHead, &evidence.Attempts, &evidence.MockAttemptStories); err != nil {
			return err
		}
		var err error
		evidence.RunForkRevisions, err = runforkrevision.CountActivityJournalRevisionsForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return WorkflowJournalStorage{}, err
	}
	return evidence, nil
}
