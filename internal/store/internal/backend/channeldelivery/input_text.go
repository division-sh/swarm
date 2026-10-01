package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	"github.com/google/uuid"
)

func selectChosenDraftTx(ctx context.Context, tx *sql.Tx, text render.PendingText, at time.Time, resolved render.ResolvedAction, postgres bool) (render.InputDraftCandidate, error) {
	var selected render.InputDraftCandidate
	cursor := ""
	for {
		candidates, next, err := ListCurrentInputDraftsTx(ctx, tx, text.Fact, at, cursor, 200, false, postgres)
		if err != nil {
			return render.InputDraftCandidate{}, err
		}
		for _, candidate := range candidates {
			if candidate.DraftID == resolved.Action.DraftID {
				if selected.DraftID != "" || candidate.CardID != resolved.Action.CardID {
					return render.InputDraftCandidate{}, fmt.Errorf("draft choice conflicts with current card")
				}
				selected = candidate
			}
		}
		if next == "" {
			break
		}
		if next == cursor {
			return render.InputDraftCandidate{}, fmt.Errorf("draft choice cursor did not advance")
		}
		cursor = next
	}
	if selected.DraftID == "" {
		return render.InputDraftCandidate{}, fmt.Errorf("draft choice no longer names a current draft: %w", decisioncard.ErrDraftNotAuthority)
	}
	return selected, nil
}

func PreviewCurrentInputDraftTextTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText,
	at time.Time, draftID string, postgres bool) (decisioncard.InputFieldProgress, string, error) {
	_, resolved, err := RequireCurrentInputDraftTx(ctx, tx, text, at, draftID, false, postgres)
	if err != nil {
		return decisioncard.InputFieldProgress{}, "", err
	}
	progress, err := decisionpersistence.PreviewInputDraftTextTx(ctx, tx, draftID, resolved.PrincipalID, text.Text, at, postgres)
	return progress, resolved.PrincipalID, err
}

// RequireChosenInputDraftTx binds a chooser callback to its retained answer
// and reselects the exact draft from current authority in one transaction.
func RequireChosenInputDraftTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction,
	at time.Time, lock, postgres bool) (render.InputDraftCandidate, render.PendingText, render.ResolvedText, error) {
	if tx == nil || at.IsZero() {
		return render.InputDraftCandidate{}, render.PendingText{}, render.ResolvedText{}, fmt.Errorf("draft choice is incomplete")
	}
	state, err := RequireActionIntentTx(ctx, tx, action, postgres, lock)
	if err != nil {
		return render.InputDraftCandidate{}, render.PendingText{}, render.ResolvedText{}, err
	}
	if state != "pending" {
		return render.InputDraftCandidate{}, render.PendingText{}, render.ResolvedText{}, fmt.Errorf("draft choice is already settled")
	}
	var resolved render.ResolvedAction
	var found bool
	if lock {
		resolved, found, err = ResolveActionFactForMutationTx(ctx, tx, action.ActionFact, postgres)
	} else {
		resolved, found, err = ResolveActionFactTx(ctx, tx, action.ActionFact, postgres)
	}
	if err != nil {
		return render.InputDraftCandidate{}, render.PendingText{}, render.ResolvedText{}, err
	}
	if !found || !resolved.CurrentRender || resolved.SourceKind != PlanResponse ||
		resolved.Action.Kind != "select_draft" || uuid.Validate(resolved.Action.DraftID) != nil ||
		uuid.Validate(resolved.Action.CardID) != nil || uuid.Validate(resolved.Action.TextPublicationID) != nil {
		return render.InputDraftCandidate{}, render.PendingText{}, render.ResolvedText{}, fmt.Errorf("draft choice is no longer current")
	}
	if lock {
		if err := LockPrincipalTx(ctx, tx, resolved.PrincipalID, postgres); err != nil {
			return render.InputDraftCandidate{}, render.PendingText{}, render.ResolvedText{}, err
		}
	}
	text, err := LoadChooserTextIntentTx(ctx, tx, resolved.Action.TextPublicationID, postgres, lock)
	if err != nil {
		return render.InputDraftCandidate{}, render.PendingText{}, render.ResolvedText{}, err
	}
	if text.Fact.Interface.Key() != action.Interface.Key() || text.Fact.ExternalAccountRef != action.ExternalAccountRef ||
		text.Fact.ConversationRef != action.ConversationRef || text.Fact.ConversationScope != action.ConversationScope ||
		text.Fact.ReplyToReference != "" {
		return render.InputDraftCandidate{}, render.PendingText{}, render.ResolvedText{}, fmt.Errorf("draft choice answer has different audience")
	}
	bound, current, err := ResolveCurrentTextTx(ctx, tx, text.Fact, postgres)
	if err != nil || !current || bound.PrincipalID != resolved.PrincipalID || bound.BindingRevision != resolved.BindingRevision {
		return render.InputDraftCandidate{}, render.PendingText{}, render.ResolvedText{}, fmt.Errorf("draft choice answer no longer has current principal: %w", err)
	}
	selected, err := selectChosenDraftTx(ctx, tx, text, at, resolved, postgres)
	if err != nil {
		return render.InputDraftCandidate{}, render.PendingText{}, render.ResolvedText{}, err
	}
	return selected, text, bound, nil
}

// AdvancePartialInputDraftTextTx settles an answer and advances one field in
// one transaction. A final answer belongs to the canonical decision mutation.
func AdvancePartialInputDraftTextTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText,
	at time.Time, draftID string, postgres bool) (decisioncard.InputFieldProgress, error) {
	_, resolved, err := RequireCurrentInputDraftTx(ctx, tx, text, at, draftID, true, postgres)
	if err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	preview, err := decisionpersistence.PreviewInputDraftTextTx(ctx, tx, draftID, resolved.PrincipalID, text.Text, at, postgres)
	if err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	if preview.Complete {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("final channel input requires canonical decision mutation")
	}
	progress, err := decisionpersistence.AdvanceInputDraftTextTx(ctx, tx, draftID, resolved.PrincipalID, text.Text, at, postgres)
	if err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	if !progress.Fields.Equal(preview.Fields) || progress.NextFieldIndex != preview.NextFieldIndex {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("channel input progress changed before settlement")
	}
	if err := SettleTextIntentTx(ctx, tx, text, "input_progressed", postgres); err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	return progress, nil
}

func PreviewChosenInputDraftTextTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction,
	at time.Time, postgres bool) (render.InputDraftCandidate, render.PendingText, decisioncard.InputFieldProgress, string, error) {
	candidate, text, bound, err := RequireChosenInputDraftTx(ctx, tx, action, at, false, postgres)
	if err != nil {
		return render.InputDraftCandidate{}, render.PendingText{}, decisioncard.InputFieldProgress{}, "", err
	}
	progress, err := decisionpersistence.PreviewInputDraftTextTx(ctx, tx, candidate.DraftID, bound.PrincipalID, text.Fact.Text, at, postgres)
	return candidate, text, progress, bound.PrincipalID, err
}

func RequireCurrentSkipActionTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction,
	at time.Time, lock, postgres bool) (render.ResolvedAction, decisioncard.InputFieldProgress, decisioncard.InputDraft, error) {
	if tx == nil || at.IsZero() {
		return render.ResolvedAction{}, decisioncard.InputFieldProgress{}, decisioncard.InputDraft{}, fmt.Errorf("channel skip requires exact action time")
	}
	state, err := RequireActionIntentTx(ctx, tx, action, postgres, lock)
	if err != nil {
		return render.ResolvedAction{}, decisioncard.InputFieldProgress{}, decisioncard.InputDraft{}, err
	}
	if state != "pending" {
		return render.ResolvedAction{}, decisioncard.InputFieldProgress{}, decisioncard.InputDraft{}, fmt.Errorf("channel skip is already settled")
	}
	var resolved render.ResolvedAction
	var found bool
	if lock {
		resolved, found, err = ResolveActionFactForMutationTx(ctx, tx, action.ActionFact, postgres)
	} else {
		resolved, found, err = ResolveActionFactTx(ctx, tx, action.ActionFact, postgres)
	}
	if err != nil {
		return render.ResolvedAction{}, decisioncard.InputFieldProgress{}, decisioncard.InputDraft{}, err
	}
	if !found || !resolved.CurrentRender || resolved.SourceKind != PlanCard ||
		resolved.Action.Kind != "skip_input" || uuid.Validate(resolved.Action.DraftID) != nil {
		return render.ResolvedAction{}, decisioncard.InputFieldProgress{}, decisioncard.InputDraft{}, fmt.Errorf("channel skip has no current prompt")
	}
	if lock {
		if err := LockPrincipalTx(ctx, tx, resolved.PrincipalID, postgres); err != nil {
			return render.ResolvedAction{}, decisioncard.InputFieldProgress{}, decisioncard.InputDraft{}, err
		}
	}
	progress, draft, err := decisionpersistence.PreviewInputDraftSkipTx(ctx, tx, resolved.Action.DraftID,
		resolved.PrincipalID, at, postgres)
	if err != nil {
		return render.ResolvedAction{}, decisioncard.InputFieldProgress{}, decisioncard.InputDraft{}, err
	}
	if draft.CardID != resolved.SourceID {
		return render.ResolvedAction{}, decisioncard.InputFieldProgress{}, decisioncard.InputDraft{}, fmt.Errorf("channel skip draft contradicts current card")
	}
	return resolved, progress, draft, nil
}

func AdvancePartialSkipActionTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction,
	at time.Time, postgres bool) (decisioncard.InputFieldProgress, error) {
	resolved, preview, draft, err := RequireCurrentSkipActionTx(ctx, tx, action, at, true, postgres)
	if err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	if preview.Complete {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("final skip requires canonical decision mutation")
	}
	progress, err := decisionpersistence.AdvanceInputDraftSkipTx(ctx, tx, draft.InputDraftID, resolved.PrincipalID, at, postgres)
	if err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	if !progress.Fields.Equal(preview.Fields) || progress.NextFieldIndex != preview.NextFieldIndex {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("channel skip progress changed before settlement")
	}
	if err := SettleAppliedActionIntentTx(ctx, tx, action, render.ActionApplied, postgres); err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	return progress, nil
}

func AdvancePartialChosenInputDraftTextTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction,
	at time.Time, postgres bool) (decisioncard.InputFieldProgress, error) {
	candidate, text, bound, err := RequireChosenInputDraftTx(ctx, tx, action, at, true, postgres)
	if err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	preview, err := decisionpersistence.PreviewInputDraftTextTx(ctx, tx, candidate.DraftID, bound.PrincipalID, text.Fact.Text, at, postgres)
	if err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	if preview.Complete {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("final chosen input requires canonical decision mutation")
	}
	progress, err := decisionpersistence.AdvanceInputDraftTextTx(ctx, tx, candidate.DraftID, bound.PrincipalID, text.Fact.Text, at, postgres)
	if err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	if !progress.Fields.Equal(preview.Fields) || progress.NextFieldIndex != preview.NextFieldIndex {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("chosen input progress changed before settlement")
	}
	if err := SettleAppliedActionIntentTx(ctx, tx, action, render.ActionApplied, postgres); err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	return progress, nil
}
