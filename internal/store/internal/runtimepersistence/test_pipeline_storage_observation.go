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

type SelectedFanOutOutcomeCorrespondence struct {
	Outcomes, ExactContinued int
}

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
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(CASE WHEN o.outcome_kind='continued'
			AND CAST(o.source_event_id AS TEXT)=$3 AND CAST(o.event_id AS TEXT)=$4
			AND o.inherited_disposition IS NULL AND o.failure IS NULL
			AND EXISTS(SELECT 1 FROM run_fork_selected_contract_executions x WHERE x.fork_run_id=o.run_id
				AND x.fork_event_id=o.event_id AND x.source_event_id=o.source_event_id AND x.source_run_id=$5)
			AND EXISTS(SELECT 1 FROM events e WHERE e.event_id=o.event_id AND e.run_id=o.run_id AND e.created_at=o.created_at)
			AND EXISTS(SELECT 1 FROM event_deliveries d WHERE d.event_id=o.event_id AND d.run_id=o.run_id AND d.status='delivered')
			THEN 1 END) FROM fan_out_outcomes o WHERE o.run_id=$1 AND o.outcome_kind=$2`,
			runID, kind, sourceEventID, eventID, sourceRunID).Scan(&evidence.Outcomes, &evidence.ExactContinued)
	})
	if err != nil {
		return SelectedFanOutOutcomeCorrespondence{}, err
	}
	return evidence, nil
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
