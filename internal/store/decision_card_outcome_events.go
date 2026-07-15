package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type decisionCardOutcomeEvent struct {
	eventID       string
	runID         string
	eventName     string
	sourceEventID string
}

func requireDecisionCardOutcomeEvent(ctx context.Context, tx *sql.Tx, expected decisionCardOutcomeEvent, postgres bool) error {
	expected.eventID = strings.TrimSpace(expected.eventID)
	expected.runID = strings.TrimSpace(expected.runID)
	expected.eventName = strings.TrimSpace(expected.eventName)
	expected.sourceEventID = strings.TrimSpace(expected.sourceEventID)
	if tx == nil || expected.eventID == "" || expected.runID == "" || expected.eventName == "" {
		return fmt.Errorf("decision-card outcome event identity is incomplete")
	}
	query := `SELECT COUNT(*) FROM events
		WHERE event_id = ? AND run_id = ? AND event_name = ? AND COALESCE(CAST(source_event_id AS TEXT), '') = ?`
	if postgres {
		query = `SELECT COUNT(*) FROM events
			WHERE event_id = $1::uuid AND run_id = $2::uuid AND event_name = $3 AND COALESCE(source_event_id::text, '') = $4`
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, expected.eventID, expected.runID, expected.eventName, expected.sourceEventID).Scan(&count); err != nil {
		return fmt.Errorf("verify decision-card outcome event: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("decision-card outcome event is not persisted in the active pipeline mutation")
	}
	return nil
}
