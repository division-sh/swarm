package decisionpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

func RequireNormalCardControlTx(ctx context.Context, tx *sql.Tx, cardID string, operation runfork.SelectedControl, postgres bool) error {
	if tx == nil {
		return fmt.Errorf("card control requires selected transaction")
	}
	card, err := loadDecisionCard(ctx, tx, cardID, postgres, false)
	if err != nil {
		return err
	}
	return runstate.RequireNormalControlTx(ctx, tx, card.RunID, operation)
}
