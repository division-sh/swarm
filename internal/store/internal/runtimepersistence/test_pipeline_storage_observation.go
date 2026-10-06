package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
)

type ReplyReturnStorageEvidence struct {
	Contexts, EventLinkedDeadLetters int
}

type FanOutPublishedOutcomeStorageEvidence struct {
	Ordinal       int
	EventID, Kind string
	Payload       json.RawMessage
}

type FanOutProgressIntentStorageEvidence struct {
	Cursor, Cardinality      int
	Status, SourceMutationID string
}

type FanOutRunProgressStorageEvidence struct {
	Intents  []FanOutProgressIntentStorageEvidence
	Outcomes int
}

func ReadFanOutRunProgressStorageForTest(ctx context.Context, selected any, runID string) (FanOutRunProgressStorageEvidence, error) {
	var empty FanOutRunProgressStorageEvidence
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return empty, fmt.Errorf("fan-out progress evidence requires an exact canonical run identity")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return empty, err
	}
	var evidence FanOutRunProgressStorageEvidence
	read := func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT cursor,cardinality,status,source_mutation_id FROM fan_out_intents WHERE run_id=$1`, runID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row FanOutProgressIntentStorageEvidence
			if err := rows.Scan(&row.Cursor, &row.Cardinality, &row.Status, &row.SourceMutationID); err != nil {
				return err
			}
			evidence.Intents = append(evidence.Intents, row)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, runID).Scan(&evidence.Outcomes)
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return empty, err
	}
	return evidence, nil
}

func ReadFanOutPublishedOutcomeStorageForTest(ctx context.Context, selected any, runID string) ([]FanOutPublishedOutcomeStorageEvidence, error) {
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return nil, fmt.Errorf("fan-out publication evidence requires an exact canonical run identity")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	var evidence []FanOutPublishedOutcomeStorageEvidence
	read := func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT o.ordinal, o.event_id, e.payload, o.outcome_kind
			FROM fan_out_outcomes o JOIN events e ON e.event_id=o.event_id WHERE o.run_id=$1 ORDER BY o.ordinal`, runID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row FanOutPublishedOutcomeStorageEvidence
			var payload []byte
			if err := rows.Scan(&row.Ordinal, &row.EventID, &payload, &row.Kind); err != nil {
				return err
			}
			row.Payload = append(json.RawMessage(nil), payload...)
			evidence = append(evidence, row)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return nil, err
	}
	return evidence, nil
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
