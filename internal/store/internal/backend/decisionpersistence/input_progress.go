package decisionpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

type preparedInputDraftText struct {
	draft    decisioncard.InputDraft
	index    int
	progress decisioncard.InputFieldProgress
}

// PreviewInputDraftTextTx reads the same fenced row and semantic rule used by
// advancement. A preview is never mutation authority; commit must recheck.
func PreviewInputDraftTextTx(ctx context.Context, tx *sql.Tx, draftID, principalID, text string,
	now time.Time, postgres bool) (decisioncard.InputFieldProgress, error) {
	prepared, err := prepareInputDraftProgressTx(ctx, tx, draftID, principalID, text, now, false, false, postgres)
	return prepared.progress, err
}

func PreviewInputDraftSkipTx(ctx context.Context, tx *sql.Tx, draftID, principalID string,
	now time.Time, postgres bool) (decisioncard.InputFieldProgress, decisioncard.InputDraft, error) {
	prepared, err := prepareInputDraftProgressTx(ctx, tx, draftID, principalID, "", now, true, false, postgres)
	return prepared.progress, prepared.draft, err
}

func prepareInputDraftProgressTx(ctx context.Context, tx *sql.Tx, draftID, principalID, text string,
	now time.Time, skip, lock, postgres bool) (preparedInputDraftText, error) {
	if tx == nil || draftID == "" || principalID == "" || now.IsZero() {
		return preparedInputDraftText{}, fmt.Errorf("decision input progress requires transaction, draft, principal and time")
	}
	now = decisioncard.CanonicalTimestamp(now)
	if err := requireActiveDecisionDraftRun(ctx, tx, draftID, postgres); err != nil {
		return preparedInputDraftText{}, err
	}
	draft, err := loadDecisionCardDraftWithLock(ctx, tx, draftID, lock, postgres)
	if err != nil {
		return preparedInputDraftText{}, err
	}
	if draft.PrincipalID != principalID || draft.Status != decisioncard.DraftStatusActive || !draft.ExpiresAt.After(now) {
		return preparedInputDraftText{}, decisioncard.ErrDraftNotAuthority
	}
	var card decisioncard.Card
	if lock {
		card, err = loadPendingDecisionCardMutation(ctx, tx, draft.CardID, postgres)
	} else {
		card, err = loadDecisionCard(ctx, tx, draft.CardID, postgres, false)
		if err == nil {
			err = card.RequirePendingMutation()
		}
	}
	if err != nil {
		return preparedInputDraftText{}, err
	}
	outcome, found := card.Snapshot.Outcomes[draft.Verdict]
	if !found {
		return preparedInputDraftText{}, fmt.Errorf("decision draft verdict is absent from canonical card")
	}
	query := `SELECT input_fields, next_field_index FROM decision_card_input_drafts WHERE input_draft_id=?`
	if postgres {
		query = `SELECT input_fields, next_field_index FROM decision_card_input_drafts WHERE input_draft_id=$1::uuid`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var raw any
	var index int
	if err := tx.QueryRowContext(ctx, query, draftID).Scan(&raw, &index); err != nil {
		return preparedInputDraftText{}, err
	}
	var bytes []byte
	switch value := raw.(type) {
	case []byte:
		bytes = value
	case string:
		bytes = []byte(value)
	default:
		return preparedInputDraftText{}, fmt.Errorf("decision input fields have unsupported storage type %T", raw)
	}
	fields, err := canonicaljson.Decode(bytes)
	if err != nil {
		return preparedInputDraftText{}, fmt.Errorf("decode decision input fields: %w", err)
	}
	if fields.Kind() != semanticvalue.KindObject {
		return preparedInputDraftText{}, fmt.Errorf("decision input fields are not a canonical object")
	}
	var progress decisioncard.InputFieldProgress
	if skip {
		progress, err = decisioncard.SkipInputField(outcome, fields, index)
	} else {
		progress, err = decisioncard.AdvanceInputField(outcome, fields, index, text)
	}
	if err != nil {
		return preparedInputDraftText{}, err
	}
	if progress.Complete {
		if err := decisioncard.ValidateDecision(card, draft.Verdict, progress.Fields); err != nil {
			return preparedInputDraftText{}, fmt.Errorf("%w: %v", decisioncard.ErrInvalidInput, err)
		}
	}
	return preparedInputDraftText{draft: draft, index: index, progress: progress}, nil
}

// AdvanceInputDraftTextTx is the canonical ordered-field mutation. The caller
// must admit the verified inbound occurrence and settle it in this transaction.
func AdvanceInputDraftTextTx(ctx context.Context, tx *sql.Tx, draftID, principalID, text string,
	now time.Time, postgres bool) (decisioncard.InputFieldProgress, error) {
	return advanceInputDraftProgressTx(ctx, tx, draftID, principalID, text, now, false, postgres)
}

func AdvanceInputDraftSkipTx(ctx context.Context, tx *sql.Tx, draftID, principalID string,
	now time.Time, postgres bool) (decisioncard.InputFieldProgress, error) {
	return advanceInputDraftProgressTx(ctx, tx, draftID, principalID, "", now, true, postgres)
}

func advanceInputDraftProgressTx(ctx context.Context, tx *sql.Tx, draftID, principalID, text string,
	now time.Time, skip, postgres bool) (decisioncard.InputFieldProgress, error) {
	prepared, err := prepareInputDraftProgressTx(ctx, tx, draftID, principalID, text, now, skip, true, postgres)
	if err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	now = decisioncard.CanonicalTimestamp(now)
	progress, draft, index := prepared.progress, prepared.draft, prepared.index
	encoded, err := canonicaljson.Encode(progress.Fields)
	if err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	query := `UPDATE decision_card_input_drafts SET input_fields=?, next_field_index=?, updated_at=?
		WHERE input_draft_id=? AND status='active' AND next_field_index=?`
	if postgres {
		query = `UPDATE decision_card_input_drafts SET input_fields=$1::jsonb, next_field_index=$2, updated_at=$3
			WHERE input_draft_id=$4::uuid AND status='active' AND next_field_index=$5`
	}
	result, err := tx.ExecContext(ctx, query, string(encoded), progress.NextFieldIndex, now, draftID, index)
	if err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("count decision input progress updates: %w", err)
	}
	if rows != 1 {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("decision input progress lost current draft: rows=%d", rows)
	}
	_, err = appendDecisionCardChangeDTO(ctx, tx, draft.RunID, draft.CardID, decisioncard.ChangeDraftProgressed, map[string]any{
		"input_draft_id": draftID, "field": progress.AcceptedField, "next_field_index": progress.NextFieldIndex,
	}, now, postgres)
	return progress, err
}
