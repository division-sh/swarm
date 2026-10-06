package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type WorkflowJournalStorage struct {
	StoryCount, StoryHead, RunForkRevisions int64
}

func ReadWorkflowJournalStorageForTest(ctx context.Context, selected any, runID string) (WorkflowJournalStorage, error) {
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return WorkflowJournalStorage{}, fmt.Errorf("journal storage requires exact canonical run identity")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return WorkflowJournalStorage{}, err
	}
	var evidence WorkflowJournalStorage
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM author_activity_occurrences),
			(SELECT last_sequence FROM author_activity_order WHERE singleton_id=1)`).
			Scan(&evidence.StoryCount, &evidence.StoryHead); err != nil {
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
