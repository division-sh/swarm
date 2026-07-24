package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
)

func insertDecisionRouteObligation(ctx context.Context, tx *sql.Tx, card decisioncard.Card, now time.Time, postgres bool) error {
	query := `INSERT INTO decision_card_route_obligations (
		event_id, card_id, run_id, status, attempt_count, next_attempt_at, created_at, updated_at
	) VALUES (?, ?, ?, 'pending', 0, ?, ?, ?)
	ON CONFLICT (event_id) DO NOTHING`
	if postgres {
		query = numberPostgresPlaceholders(strings.ReplaceAll(query, "?", "$%d"))
	}
	_, err := tx.ExecContext(ctx, query, card.DecisionEventID, card.CardID, card.RunID, now.UTC(), now.UTC(), now.UTC())
	if err != nil {
		return fmt.Errorf("create decision route obligation: %w", err)
	}
	return nil
}
