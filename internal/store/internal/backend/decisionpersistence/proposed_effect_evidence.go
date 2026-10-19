package decisionpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
)

// ReadProposedEffectEvidenceInTx composes the canonical card and continuation
// decoders in the caller's snapshot. It grants no mutation or dispatch right.
func ReadProposedEffectEvidenceInTx(ctx context.Context, tx *sql.Tx, requestID string, postgres bool) (decisioncard.Card, decisioncard.ProposedEffectContinuation, bool, error) {
	if tx == nil || requestID == "" {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, false, fmt.Errorf("proposed-effect evidence requires transaction and exact request")
	}
	query := `SELECT card_id FROM proposed_effect_continuations WHERE request_event_id = ?`
	if postgres {
		query = `SELECT card_id FROM proposed_effect_continuations WHERE request_event_id = $1::uuid`
	}
	var cardID string
	if err := tx.QueryRowContext(ctx, query, requestID).Scan(&cardID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, false, nil
		}
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, false, err
	}
	card, err := loadDecisionCard(ctx, tx, cardID, postgres, false)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, false, err
	}
	continuation, err := loadProposedEffectContinuation(ctx, tx, cardID, postgres, false)
	if err == nil {
		err = card.Validate()
	}
	if err == nil {
		err = continuation.Validate(card)
	}
	if err == nil && continuation.RequestEventID != requestID {
		err = fmt.Errorf("proposed-effect request identity contradicts its canonical effect")
	}
	return card, continuation, err == nil, err
}
