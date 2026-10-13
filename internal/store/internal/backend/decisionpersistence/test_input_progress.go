package decisionpersistence

import (
	"context"
	"database/sql"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// These component fixtures exercise the canonical progress owner. They do not
// consume an inbound occurrence, decide the card or publish its outcome.
func (s *DecisionPostgresOwner) PreviewInputTextForTest(ctx context.Context, draftID, principalID, text string, at time.Time) (decisioncard.InputFieldProgress, error) {
	if err := s.requireCurrent(); err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	var progress decisioncard.InputFieldProgress
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		progress, err = PreviewInputDraftTextTx(txctx, tx, draftID, principalID, text, at, true)
		return err
	})
	return progress, err
}

func (s *DecisionSQLiteOwner) PreviewInputTextForTest(ctx context.Context, draftID, principalID, text string, at time.Time) (decisioncard.InputFieldProgress, error) {
	if err := s.requireCurrent(); err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	var progress decisioncard.InputFieldProgress
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		progress, err = PreviewInputDraftTextTx(txctx, tx, draftID, principalID, text, at, false)
		return err
	})
	return progress, err
}

func (s *DecisionPostgresOwner) AdvanceInputTextForTest(ctx context.Context, draftID, principalID, text string, at time.Time) (decisioncard.InputFieldProgress, error) {
	result := postgresDecisionMutation(ctx, s, false, func(txctx context.Context, attempt *mutationprotocol.Attempt) (decisioncard.InputFieldProgress, error) {
		return withDecisionSQL(txctx, attempt, func(txctx context.Context, tx *sql.Tx) (decisioncard.InputFieldProgress, error) {
			return AdvanceInputDraftTextTx(txctx, tx, draftID, principalID, text, at, true)
		})
	})
	progress, _ := result.Value()
	return progress, result.Err()
}

func (s *DecisionSQLiteOwner) AdvanceInputTextForTest(ctx context.Context, draftID, principalID, text string, at time.Time) (decisioncard.InputFieldProgress, error) {
	result := sqliteDecisionMutation(ctx, s, "sqlite decision input text fixture", false, func(txctx context.Context, attempt *mutationprotocol.Attempt) (decisioncard.InputFieldProgress, error) {
		return withDecisionSQL(txctx, attempt, func(txctx context.Context, tx *sql.Tx) (decisioncard.InputFieldProgress, error) {
			return AdvanceInputDraftTextTx(txctx, tx, draftID, principalID, text, at, false)
		})
	})
	progress, _ := result.Value()
	return progress, result.Err()
}

func (s *DecisionPostgresOwner) AdvanceInputSkipForTest(ctx context.Context, draftID, principalID string, at time.Time) (decisioncard.InputFieldProgress, error) {
	result := postgresDecisionMutation(ctx, s, false, func(txctx context.Context, attempt *mutationprotocol.Attempt) (decisioncard.InputFieldProgress, error) {
		return withDecisionSQL(txctx, attempt, func(txctx context.Context, tx *sql.Tx) (decisioncard.InputFieldProgress, error) {
			return AdvanceInputDraftSkipTx(txctx, tx, draftID, principalID, at, true)
		})
	})
	progress, _ := result.Value()
	return progress, result.Err()
}

func (s *DecisionSQLiteOwner) AdvanceInputSkipForTest(ctx context.Context, draftID, principalID string, at time.Time) (decisioncard.InputFieldProgress, error) {
	result := sqliteDecisionMutation(ctx, s, "sqlite decision input skip fixture", false, func(txctx context.Context, attempt *mutationprotocol.Attempt) (decisioncard.InputFieldProgress, error) {
		return withDecisionSQL(txctx, attempt, func(txctx context.Context, tx *sql.Tx) (decisioncard.InputFieldProgress, error) {
			return AdvanceInputDraftSkipTx(txctx, tx, draftID, principalID, at, false)
		})
	})
	progress, _ := result.Value()
	return progress, result.Err()
}
