package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

type ReplyReturnStorageEvidence struct {
	Contexts, EventLinkedDeadLetters int
}

type FanOutPublishedOutcomeStorageEvidence = pipelinepersistence.FanOutPublishedOutcomeStorageEvidence

type SelectedFanOutOutcomeCorrespondence = pipelinepersistence.SelectedFanOutOutcomeCorrespondence

// Observe the original fixed-T storage relation, including NULLs and missing
// lineage/event/delivery rows. This read grants no execution authority.
func ReadSelectedFanOutOutcomeCorrespondenceForTest(ctx context.Context, selected any, runID, kind, sourceEventID, eventID, sourceRunID string) (SelectedFanOutOutcomeCorrespondence, error) {
	var evidence SelectedFanOutOutcomeCorrespondence
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return evidence, err
	}
	for _, id := range []string{runID, sourceEventID, eventID, sourceRunID} {
		if err := validateSelectedForkStorageIdentity(id); err != nil {
			return evidence, err
		}
	}
	if kind != "continued" && kind != "committed" {
		return evidence, fmt.Errorf("selected fan-out correspondence requires continued or committed outcomes")
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadSelectedFanOutOutcomeCorrespondenceForTest(ctx, runID, kind, sourceEventID, eventID, sourceRunID)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadSelectedFanOutOutcomeCorrespondenceForTest(ctx, runID, kind, sourceEventID, eventID, sourceRunID)
	default:
		return SelectedFanOutOutcomeCorrespondence{}, fmt.Errorf("selected fan-out correspondence requires the original native owner")
	}
}

type FanOutProgressIntentStorageEvidence = pipelinepersistence.FanOutProgressIntentStorageEvidence

type FanOutRunProgressStorageEvidence = pipelinepersistence.FanOutRunProgressStorageEvidence

func ReadFanOutRunProgressStorageForTest(ctx context.Context, selected any, runID string) (FanOutRunProgressStorageEvidence, error) {
	var empty FanOutRunProgressStorageEvidence
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return empty, fmt.Errorf("fan-out progress evidence requires an exact canonical run identity")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return empty, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ReadFanOutRunProgressStorageForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ReadFanOutRunProgressStorageForTest(ctx, runID)
	default:
		return empty, fmt.Errorf("fan-out progress evidence requires the original native owner")
	}
}

func ReadFanOutPublishedOutcomeStorageForTest(ctx context.Context, selected any, runID string) ([]FanOutPublishedOutcomeStorageEvidence, error) {
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return nil, fmt.Errorf("fan-out publication evidence requires an exact canonical run identity")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ReadFanOutPublishedOutcomeStorageForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ReadFanOutPublishedOutcomeStorageForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("fan-out publication evidence requires the original native owner")
	}
}

func ReadReplyReturnStorageForTest(ctx context.Context, selected any, runID string) (ReplyReturnStorageEvidence, error) {
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return ReplyReturnStorageEvidence{}, fmt.Errorf("reply return evidence requires an exact canonical run identity")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return ReplyReturnStorageEvidence{}, err
	}
	var evidence ReplyReturnStorageEvidence
	read := func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM reply_contexts WHERE run_id=$1),
			(SELECT COUNT(*) FROM dead_letters d JOIN events e ON e.event_id=d.original_event_id WHERE e.run_id=$1)`, runID).
			Scan(&evidence.Contexts, &evidence.EventLinkedDeadLetters)
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return ReplyReturnStorageEvidence{}, err
	}
	return evidence, nil
}
