package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
)

func requireEventOwnedReferences(ctx context.Context, tx *sql.Tx, postgres bool, record eventrecord.Record) error {
	if sourceID := strings.TrimSpace(record.SourceEventID); sourceID != "" {
		if err := requireSameRunEventReference(ctx, tx, postgres, "causal source", sourceID, record.RunID); err != nil {
			return err
		}
	}
	if referenceID := strings.TrimSpace(record.OperatorReferencedEventID); referenceID != "" {
		if err := requireSameRunEventReference(ctx, tx, postgres, "operator reference", referenceID, record.RunID); err != nil {
			return err
		}
	}
	return nil
}

func requireSameRunEventReference(ctx context.Context, tx *sql.Tx, postgres bool, relation, eventID, runID string) error {
	eventID = strings.TrimSpace(eventID)
	runID = strings.TrimSpace(runID)
	if tx == nil || eventID == "" || runID == "" {
		return fmt.Errorf("%s requires event_id and run_id", relation)
	}
	query := `SELECT COALESCE(CAST(run_id AS TEXT), '') FROM events WHERE event_id = ?`
	if postgres {
		query = `SELECT COALESCE(run_id::text, '') FROM events WHERE event_id = $1::uuid FOR KEY SHARE`
	}
	var actualRunID string
	err := tx.QueryRowContext(ctx, query, eventID).Scan(&actualRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s event %s does not exist", relation, eventID)
	}
	if err != nil {
		return fmt.Errorf("load %s event %s: %w", relation, eventID, err)
	}
	if strings.TrimSpace(actualRunID) != runID {
		return fmt.Errorf("%s event %s belongs to run %s, not run %s", relation, eventID, strings.TrimSpace(actualRunID), runID)
	}
	return nil
}
