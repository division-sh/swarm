package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/division-sh/swarm/internal/packs"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

func (s *PostgresStore) SubscribeChannelReconciliation(ctx context.Context) (*render.ReconcileSubscription, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return s.channelChanges.Subscribe(ctx)
}

func (s *SQLiteRuntimeStore) SubscribeChannelReconciliation(ctx context.Context) (*render.ReconcileSubscription, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return s.channelChanges.Subscribe(ctx)
}

// Process composition calls this only after an exact executable publication.
// This observation hint carries no selected-store mutation or send authority.
func (s *PostgresStore) NotifyChannelPublication() error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.channelChanges.PublishAcknowledged(true, render.ReconcileOrdinary|render.ReconcileNative)
}

func (s *SQLiteRuntimeStore) NotifyChannelPublication() error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.channelChanges.PublishAcknowledged(true, render.ReconcileOrdinary|render.ReconcileNative)
}

func (s *PostgresStore) AcknowledgeChannelNotice(ctx context.Context, req apiidempotency.Request, action operatorchannel.InboundAction) (apiidempotency.Completion, bool, error) {
	if s == nil || s.mailboxPostgresOwner == nil {
		return apiidempotency.Completion{}, false, fmt.Errorf("postgres channel notice owner is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return apiidempotency.Completion{}, false, err
	}
	return s.mailboxPostgresOwner.AcknowledgeChannelNotice(ctx, req, action)
}

func (s *SQLiteRuntimeStore) AcknowledgeChannelNotice(ctx context.Context, req apiidempotency.Request, action operatorchannel.InboundAction) (apiidempotency.Completion, bool, error) {
	if s == nil || s.mailboxSQLiteOwner == nil {
		return apiidempotency.Completion{}, false, fmt.Errorf("sqlite channel notice owner is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return apiidempotency.Completion{}, false, err
	}
	return s.mailboxSQLiteOwner.AcknowledgeChannelNotice(ctx, req, action)
}

func (s *PostgresStore) ListCurrentChannelDeliveryPlans(ctx context.Context, cursor string, limit int) ([]render.Candidate, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var plans []channeldelivery.Plan
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		plans, err = channeldelivery.ListCurrentPlans(txctx, tx, cursor, limit, true)
		return err
	})
	return projectDeliveryCandidates(plans), err
}

func (s *PostgresStore) GetCurrentChannelDeliveryPlan(ctx context.Context, deliveryID string) (render.Candidate, bool, error) {
	if s == nil || s.backend == nil {
		return render.Candidate{}, false, fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.Candidate{}, false, err
	}
	var plan channeldelivery.Plan
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		plan, found, err = channeldelivery.LoadCurrentPlan(txctx, tx, deliveryID, true)
		return err
	})
	if err != nil || !found {
		return render.Candidate{}, found, err
	}
	return projectDeliveryCandidates([]channeldelivery.Plan{plan})[0], true, nil
}

func (s *PostgresStore) GetCurrentChannelSentReceipt(ctx context.Context, deliveryID, operationID string) (render.SentReceipt, bool, error) {
	if s == nil || s.backend == nil {
		return render.SentReceipt{}, false, fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.SentReceipt{}, false, err
	}
	var receipt render.SentReceipt
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		receipt, found, err = channeldelivery.ReadCurrentSentReceiptTx(txctx, tx, deliveryID, operationID, true)
		return err
	})
	return receipt, found, err
}

func (s *PostgresStore) CurrentChannelDeliveryActivationID(ctx context.Context) (string, bool, error) {
	if s == nil || s.backend == nil {
		return "", false, fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", false, err
	}
	var id string
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		id, found, err = channeldelivery.CurrentActivationID(txctx, tx, true)
		return err
	})
	return id, found, err
}

func (s *PostgresStore) CurrentChannelCardChangeCursor(ctx context.Context) (int64, bool, error) {
	if s == nil || s.backend == nil {
		return 0, false, fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return 0, false, err
	}
	var cursor int64
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		cursor, found, err = channeldelivery.CurrentCardChangeCursor(txctx, tx)
		return err
	})
	return cursor, found, err
}

func (s *PostgresStore) PlanChangedChannelCard(ctx context.Context, sequence int64, cardID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.PlanChangedCardTx(txctx, tx, sequence, cardID, true)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) ResolveChannelActionFact(ctx context.Context, fact operatorchannel.ActionFact) (render.ResolvedAction, bool, error) {
	if s == nil || s.backend == nil {
		return render.ResolvedAction{}, false, fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.ResolvedAction{}, false, err
	}
	var resolved render.ResolvedAction
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		resolved, found, err = channeldelivery.ResolveActionFactTx(txctx, tx, fact, true)
		return err
	})
	return resolved, found, err
}

func (s *PostgresStore) AdmitChannelReplyAction(ctx context.Context, text operatorchannel.InboundText) (render.PendingAction, bool, error) {
	if s == nil || s.backend == nil {
		return render.PendingAction{}, false, fmt.Errorf("postgres channel action store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.PendingAction{}, false, err
	}
	var action render.PendingAction
	var found bool
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		action, found, changed, err = channeldelivery.AdmitReplyActionTx(txctx, tx, text, true)
		return err
	})
	return action, found, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) ListPendingChannelActions(ctx context.Context, cursor string, limit int) ([]render.PendingAction, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres channel action store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var pending []render.PendingAction
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		pending, err = channeldelivery.ListPendingActionIntents(txctx, tx, cursor, limit, true)
		return err
	})
	return pending, err
}

func (s *PostgresStore) SettleUnappliedChannelAction(ctx context.Context, action operatorchannel.InboundAction, disposition render.ActionDisposition) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres channel action store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.SettleUnappliedActionIntentTx(txctx, tx, action, disposition, true)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) SettleUnsupportedChannelText(ctx context.Context, text operatorchannel.InboundText) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres channel text store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.SettleUnsupportedTextIntentTx(txctx, tx, text, true)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) ListPendingChannelTexts(ctx context.Context, cursor string, limit int) ([]render.PendingText, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres channel text store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var pending []render.PendingText
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		pending, err = channeldelivery.ListPendingTextIntents(txctx, tx, cursor, limit, true)
		return err
	})
	return pending, err
}

func (s *PostgresStore) RejectUnboundChannelText(ctx context.Context, text operatorchannel.InboundText) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres channel text store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.RejectUnboundTextTx(txctx, tx, text, true)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) ResolveCurrentChannelText(ctx context.Context, text operatorchannel.InboundText) (render.ResolvedText, bool, error) {
	if s == nil || s.backend == nil {
		return render.ResolvedText{}, false, fmt.Errorf("postgres channel text store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.ResolvedText{}, false, err
	}
	var resolved render.ResolvedText
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		resolved, found, err = channeldelivery.ResolveCurrentTextTx(txctx, tx, text, true)
		return err
	})
	return resolved, found, err
}

func (s *PostgresStore) HasCurrentChannelInputDraft(ctx context.Context, text operatorchannel.InboundText, at time.Time) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("postgres channel input draft store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		found, err = channeldelivery.HasCurrentBareInputDraftTx(txctx, tx, text, at, true)
		return err
	})
	return found, err
}

func (s *PostgresStore) ListCurrentChannelInputDrafts(ctx context.Context, text operatorchannel.InboundText, at time.Time, cursor string, limit int) ([]render.InputDraftCandidate, string, error) {
	if s == nil || s.backend == nil {
		return nil, "", fmt.Errorf("postgres channel input draft store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, "", err
	}
	var drafts []render.InputDraftCandidate
	var next string
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		drafts, next, err = channeldelivery.ListCurrentInputDraftsTx(txctx, tx, text, at, cursor, limit, true, true)
		return err
	})
	return drafts, next, err
}

func (s *PostgresStore) PreviewCurrentChannelInputDraftText(ctx context.Context, text operatorchannel.InboundText, at time.Time, draftID string) (decisioncard.InputFieldProgress, string, error) {
	if s == nil || s.backend == nil {
		return decisioncard.InputFieldProgress{}, "", fmt.Errorf("postgres channel input draft store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return decisioncard.InputFieldProgress{}, "", err
	}
	var progress decisioncard.InputFieldProgress
	var principalID string
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		progress, principalID, err = channeldelivery.PreviewCurrentInputDraftTextTx(txctx, tx, text, at, draftID, true)
		return err
	})
	return progress, principalID, err
}

func (s *PostgresStore) AdvancePartialChannelInputDraftText(ctx context.Context, text operatorchannel.InboundText, at time.Time, draftID string) (decisioncard.InputFieldProgress, error) {
	if s == nil || s.backend == nil {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("postgres channel input draft store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	var progress decisioncard.InputFieldProgress
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		progress, changed, err = channeldelivery.AdvancePartialInputDraftTextTx(txctx, tx, text, at, draftID, true)
		return err
	})
	return progress, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) PreviewChosenChannelInputDraftText(ctx context.Context, action operatorchannel.InboundAction, at time.Time) (render.InputDraftCandidate, render.PendingText, decisioncard.InputFieldProgress, string, error) {
	if s == nil || s.backend == nil {
		return render.InputDraftCandidate{}, render.PendingText{}, decisioncard.InputFieldProgress{}, "", fmt.Errorf("postgres chosen input store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.InputDraftCandidate{}, render.PendingText{}, decisioncard.InputFieldProgress{}, "", err
	}
	var candidate render.InputDraftCandidate
	var text render.PendingText
	var progress decisioncard.InputFieldProgress
	var principalID string
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		candidate, text, progress, principalID, err = channeldelivery.PreviewChosenInputDraftTextTx(txctx, tx, action, at, true)
		return err
	})
	return candidate, text, progress, principalID, err
}

func (s *PostgresStore) AdvancePartialChosenChannelInputDraftText(ctx context.Context, action operatorchannel.InboundAction, at time.Time) (decisioncard.InputFieldProgress, error) {
	if s == nil || s.backend == nil {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("postgres chosen input store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	var progress decisioncard.InputFieldProgress
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		progress, changed, err = channeldelivery.AdvancePartialChosenInputDraftTextTx(txctx, tx, action, at, true)
		return err
	})
	return progress, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) PreviewChannelInputSkip(ctx context.Context, action operatorchannel.InboundAction, at time.Time) (render.ResolvedAction, decisioncard.InputFieldProgress, decisioncard.InputDraft, error) {
	if s == nil || s.backend == nil {
		return render.ResolvedAction{}, decisioncard.InputFieldProgress{}, decisioncard.InputDraft{}, fmt.Errorf("postgres channel skip store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.ResolvedAction{}, decisioncard.InputFieldProgress{}, decisioncard.InputDraft{}, err
	}
	var resolved render.ResolvedAction
	var progress decisioncard.InputFieldProgress
	var draft decisioncard.InputDraft
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		resolved, progress, draft, err = channeldelivery.RequireCurrentSkipActionTx(txctx, tx, action, at, false, true)
		return err
	})
	return resolved, progress, draft, err
}

func (s *PostgresStore) AdvancePartialChannelInputSkip(ctx context.Context, action operatorchannel.InboundAction, at time.Time) (decisioncard.InputFieldProgress, error) {
	if s == nil || s.backend == nil {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("postgres channel skip store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	var progress decisioncard.InputFieldProgress
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		progress, changed, err = channeldelivery.AdvancePartialSkipActionTx(txctx, tx, action, at, true)
		return err
	})
	return progress, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) ResolveCurrentInboxEntry(ctx context.Context, text operatorchannel.InboundText) (render.ResolvedInboxEntry, bool, error) {
	if s == nil || s.backend == nil {
		return render.ResolvedInboxEntry{}, false, fmt.Errorf("postgres native inbox store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.ResolvedInboxEntry{}, false, err
	}
	var entry render.ResolvedInboxEntry
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		entry, found, err = channeldelivery.ResolveCurrentInboxEntryTx(txctx, tx, text, true)
		return err
	})
	return entry, found, err
}

func (s *PostgresStore) PlanInboxResponse(ctx context.Context, text operatorchannel.InboundText, entry render.ResolvedInboxEntry, fullText string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("postgres channel response store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, changed, err = channeldelivery.PlanInboxResponseTx(txctx, tx, text, entry, fullText, true)
		return err
	})
	return deliveryID, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) PlanChannelTextResponse(ctx context.Context, text operatorchannel.InboundText, fullText, disposition string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("postgres channel text response store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, changed, err = channeldelivery.PlanTextResponseTx(txctx, tx, text, fullText, disposition, true)
		return err
	})
	return deliveryID, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) PlanChannelDraftChooser(ctx context.Context, text operatorchannel.InboundText, at time.Time) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("postgres channel draft chooser store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, changed, err = channeldelivery.PlanDraftChooserTx(txctx, tx, text, at, true)
		return err
	})
	return deliveryID, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) PlanChannelActionResponse(ctx context.Context, action operatorchannel.InboundAction, resolved render.ResolvedAction, inboxText string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("postgres channel response store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, changed, err = channeldelivery.PlanActionResponseTx(txctx, tx, action, resolved, inboxText, true)
		return err
	})
	return deliveryID, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) AdvanceChannelActionPage(ctx context.Context, action operatorchannel.InboundAction, resolved render.ResolvedAction, mode render.ControlPageMode) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.AdvanceActionPageTx(txctx, tx, action, resolved, mode, true)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) PlanManualChannelResend(ctx context.Context, action operatorchannel.InboundAction, resolved render.ResolvedAction) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("postgres manual channel resend store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, changed, err = channeldelivery.PlanManualResendTx(txctx, tx, action, resolved, true)
		return err
	})
	return deliveryID, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) PlanOpenChannelCard(ctx context.Context, cardID string) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	var created bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		created, err = channeldelivery.PlanOpenCardTx(txctx, tx, cardID, true)
		return err
	})
	return created, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && created, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) ListCurrentChannelDeliveryPlans(ctx context.Context, cursor string, limit int) ([]render.Candidate, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var plans []channeldelivery.Plan
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		plans, err = channeldelivery.ListCurrentPlans(txctx, tx, cursor, limit, false)
		return err
	})
	return projectDeliveryCandidates(plans), err
}

func (s *SQLiteRuntimeStore) GetCurrentChannelDeliveryPlan(ctx context.Context, deliveryID string) (render.Candidate, bool, error) {
	if s == nil || s.backend == nil {
		return render.Candidate{}, false, fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.Candidate{}, false, err
	}
	var plan channeldelivery.Plan
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		plan, found, err = channeldelivery.LoadCurrentPlan(txctx, tx, deliveryID, false)
		return err
	})
	if err != nil || !found {
		return render.Candidate{}, found, err
	}
	return projectDeliveryCandidates([]channeldelivery.Plan{plan})[0], true, nil
}

func (s *SQLiteRuntimeStore) GetCurrentChannelSentReceipt(ctx context.Context, deliveryID, operationID string) (render.SentReceipt, bool, error) {
	if s == nil || s.backend == nil {
		return render.SentReceipt{}, false, fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.SentReceipt{}, false, err
	}
	var receipt render.SentReceipt
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		receipt, found, err = channeldelivery.ReadCurrentSentReceiptTx(txctx, tx, deliveryID, operationID, false)
		return err
	})
	return receipt, found, err
}

func projectDeliveryCandidates(plans []channeldelivery.Plan) []render.Candidate {
	candidates := make([]render.Candidate, 0, len(plans))
	for _, plan := range plans {
		candidates = append(candidates, render.Candidate{
			DeliveryID: plan.DeliveryID, SourceKind: plan.SourceKind, SourceID: plan.SourceID,
			RequestActivationID: plan.RequestActivationID,
			BindingRevision:     plan.CurrentBindingRevision,
			Audience: render.Audience{PrincipalID: plan.PrincipalID, InterfaceKey: plan.InterfaceKey,
				DeliveryEpoch: plan.DeliveryEpoch, ExternalAccountRef: plan.ExternalAccountRef,
				ConversationRef: plan.ConversationRef, ConversationScope: plan.ConversationScope},
			State: plan.State, CurrentRenderID: plan.CurrentRenderID, CurrentReceiptID: plan.CurrentReceiptID,
			RecoveryPending: plan.RecoveryPending,
		})
	}
	return candidates
}

func (s *SQLiteRuntimeStore) CurrentChannelDeliveryActivationID(ctx context.Context) (string, bool, error) {
	if s == nil || s.backend == nil {
		return "", false, fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", false, err
	}
	var id string
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		id, found, err = channeldelivery.CurrentActivationID(txctx, tx, false)
		return err
	})
	return id, found, err
}

func (s *SQLiteRuntimeStore) ResolveChannelActionFact(ctx context.Context, fact operatorchannel.ActionFact) (render.ResolvedAction, bool, error) {
	if s == nil || s.backend == nil {
		return render.ResolvedAction{}, false, fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.ResolvedAction{}, false, err
	}
	var resolved render.ResolvedAction
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		resolved, found, err = channeldelivery.ResolveActionFactTx(txctx, tx, fact, false)
		return err
	})
	return resolved, found, err
}

func (s *SQLiteRuntimeStore) AdmitChannelReplyAction(ctx context.Context, text operatorchannel.InboundText) (render.PendingAction, bool, error) {
	if s == nil || s.backend == nil {
		return render.PendingAction{}, false, fmt.Errorf("sqlite channel action store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.PendingAction{}, false, err
	}
	var action render.PendingAction
	var found bool
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "admit channel reply action", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		action, found, changed, err = channeldelivery.AdmitReplyActionTx(txctx, tx, text, false)
		return err
	})
	return action, found, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) ListPendingChannelActions(ctx context.Context, cursor string, limit int) ([]render.PendingAction, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite channel action store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var pending []render.PendingAction
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		pending, err = channeldelivery.ListPendingActionIntents(txctx, tx, cursor, limit, false)
		return err
	})
	return pending, err
}

func (s *SQLiteRuntimeStore) SettleUnappliedChannelAction(ctx context.Context, action operatorchannel.InboundAction, disposition render.ActionDisposition) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite channel action store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "settle channel action without mutation", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.SettleUnappliedActionIntentTx(txctx, tx, action, disposition, false)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) SettleUnsupportedChannelText(ctx context.Context, text operatorchannel.InboundText) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite channel text store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "settle unsupported channel text", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.SettleUnsupportedTextIntentTx(txctx, tx, text, false)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) ListPendingChannelTexts(ctx context.Context, cursor string, limit int) ([]render.PendingText, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite channel text store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var pending []render.PendingText
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		pending, err = channeldelivery.ListPendingTextIntents(txctx, tx, cursor, limit, false)
		return err
	})
	return pending, err
}

func (s *SQLiteRuntimeStore) RejectUnboundChannelText(ctx context.Context, text operatorchannel.InboundText) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite channel text store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "reject unbound channel text", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.RejectUnboundTextTx(txctx, tx, text, false)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) ResolveCurrentChannelText(ctx context.Context, text operatorchannel.InboundText) (render.ResolvedText, bool, error) {
	if s == nil || s.backend == nil {
		return render.ResolvedText{}, false, fmt.Errorf("sqlite channel text store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.ResolvedText{}, false, err
	}
	var resolved render.ResolvedText
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		resolved, found, err = channeldelivery.ResolveCurrentTextTx(txctx, tx, text, false)
		return err
	})
	return resolved, found, err
}

func (s *SQLiteRuntimeStore) HasCurrentChannelInputDraft(ctx context.Context, text operatorchannel.InboundText, at time.Time) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("sqlite channel input draft store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		found, err = channeldelivery.HasCurrentBareInputDraftTx(txctx, tx, text, at, false)
		return err
	})
	return found, err
}

func (s *SQLiteRuntimeStore) ListCurrentChannelInputDrafts(ctx context.Context, text operatorchannel.InboundText, at time.Time, cursor string, limit int) ([]render.InputDraftCandidate, string, error) {
	if s == nil || s.backend == nil {
		return nil, "", fmt.Errorf("sqlite channel input draft store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, "", err
	}
	var drafts []render.InputDraftCandidate
	var next string
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		drafts, next, err = channeldelivery.ListCurrentInputDraftsTx(txctx, tx, text, at, cursor, limit, true, false)
		return err
	})
	return drafts, next, err
}

func (s *SQLiteRuntimeStore) PreviewCurrentChannelInputDraftText(ctx context.Context, text operatorchannel.InboundText, at time.Time, draftID string) (decisioncard.InputFieldProgress, string, error) {
	if s == nil || s.backend == nil {
		return decisioncard.InputFieldProgress{}, "", fmt.Errorf("sqlite channel input draft store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return decisioncard.InputFieldProgress{}, "", err
	}
	var progress decisioncard.InputFieldProgress
	var principalID string
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		progress, principalID, err = channeldelivery.PreviewCurrentInputDraftTextTx(txctx, tx, text, at, draftID, false)
		return err
	})
	return progress, principalID, err
}

func (s *SQLiteRuntimeStore) AdvancePartialChannelInputDraftText(ctx context.Context, text operatorchannel.InboundText, at time.Time, draftID string) (decisioncard.InputFieldProgress, error) {
	if s == nil || s.backend == nil {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("sqlite channel input draft store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	var progress decisioncard.InputFieldProgress
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "advance channel input draft", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		progress, changed, err = channeldelivery.AdvancePartialInputDraftTextTx(txctx, tx, text, at, draftID, false)
		return err
	})
	return progress, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) PreviewChosenChannelInputDraftText(ctx context.Context, action operatorchannel.InboundAction, at time.Time) (render.InputDraftCandidate, render.PendingText, decisioncard.InputFieldProgress, string, error) {
	if s == nil || s.backend == nil {
		return render.InputDraftCandidate{}, render.PendingText{}, decisioncard.InputFieldProgress{}, "", fmt.Errorf("sqlite chosen input store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.InputDraftCandidate{}, render.PendingText{}, decisioncard.InputFieldProgress{}, "", err
	}
	var candidate render.InputDraftCandidate
	var text render.PendingText
	var progress decisioncard.InputFieldProgress
	var principalID string
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		candidate, text, progress, principalID, err = channeldelivery.PreviewChosenInputDraftTextTx(txctx, tx, action, at, false)
		return err
	})
	return candidate, text, progress, principalID, err
}

func (s *SQLiteRuntimeStore) AdvancePartialChosenChannelInputDraftText(ctx context.Context, action operatorchannel.InboundAction, at time.Time) (decisioncard.InputFieldProgress, error) {
	if s == nil || s.backend == nil {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("sqlite chosen input store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	var progress decisioncard.InputFieldProgress
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "advance chosen channel input draft", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		progress, changed, err = channeldelivery.AdvancePartialChosenInputDraftTextTx(txctx, tx, action, at, false)
		return err
	})
	return progress, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) PreviewChannelInputSkip(ctx context.Context, action operatorchannel.InboundAction, at time.Time) (render.ResolvedAction, decisioncard.InputFieldProgress, decisioncard.InputDraft, error) {
	if s == nil || s.backend == nil {
		return render.ResolvedAction{}, decisioncard.InputFieldProgress{}, decisioncard.InputDraft{}, fmt.Errorf("sqlite channel skip store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.ResolvedAction{}, decisioncard.InputFieldProgress{}, decisioncard.InputDraft{}, err
	}
	var resolved render.ResolvedAction
	var progress decisioncard.InputFieldProgress
	var draft decisioncard.InputDraft
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		resolved, progress, draft, err = channeldelivery.RequireCurrentSkipActionTx(txctx, tx, action, at, false, false)
		return err
	})
	return resolved, progress, draft, err
}

func (s *SQLiteRuntimeStore) AdvancePartialChannelInputSkip(ctx context.Context, action operatorchannel.InboundAction, at time.Time) (decisioncard.InputFieldProgress, error) {
	if s == nil || s.backend == nil {
		return decisioncard.InputFieldProgress{}, fmt.Errorf("sqlite channel skip store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return decisioncard.InputFieldProgress{}, err
	}
	var progress decisioncard.InputFieldProgress
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "advance partial channel input skip", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		progress, changed, err = channeldelivery.AdvancePartialSkipActionTx(txctx, tx, action, at, false)
		return err
	})
	return progress, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) ResolveCurrentInboxEntry(ctx context.Context, text operatorchannel.InboundText) (render.ResolvedInboxEntry, bool, error) {
	if s == nil || s.backend == nil {
		return render.ResolvedInboxEntry{}, false, fmt.Errorf("sqlite native inbox store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.ResolvedInboxEntry{}, false, err
	}
	var entry render.ResolvedInboxEntry
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		entry, found, err = channeldelivery.ResolveCurrentInboxEntryTx(txctx, tx, text, false)
		return err
	})
	return entry, found, err
}

func (s *SQLiteRuntimeStore) PlanInboxResponse(ctx context.Context, text operatorchannel.InboundText, entry render.ResolvedInboxEntry, fullText string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("sqlite channel response store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "plan native inbox response", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, changed, err = channeldelivery.PlanInboxResponseTx(txctx, tx, text, entry, fullText, false)
		return err
	})
	return deliveryID, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) PlanChannelTextResponse(ctx context.Context, text operatorchannel.InboundText, fullText, disposition string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("sqlite channel text response store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "plan channel text response", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, changed, err = channeldelivery.PlanTextResponseTx(txctx, tx, text, fullText, disposition, false)
		return err
	})
	return deliveryID, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) PlanChannelDraftChooser(ctx context.Context, text operatorchannel.InboundText, at time.Time) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("sqlite channel draft chooser store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "plan channel draft chooser", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, changed, err = channeldelivery.PlanDraftChooserTx(txctx, tx, text, at, false)
		return err
	})
	return deliveryID, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) PlanChannelActionResponse(ctx context.Context, action operatorchannel.InboundAction, resolved render.ResolvedAction, inboxText string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("sqlite channel response store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "plan channel action response", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, changed, err = channeldelivery.PlanActionResponseTx(txctx, tx, action, resolved, inboxText, false)
		return err
	})
	return deliveryID, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) AdvanceChannelActionPage(ctx context.Context, action operatorchannel.InboundAction, resolved render.ResolvedAction, mode render.ControlPageMode) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "advance channel card action page", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.AdvanceActionPageTx(txctx, tx, action, resolved, mode, false)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) PlanManualChannelResend(ctx context.Context, action operatorchannel.InboundAction, resolved render.ResolvedAction) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("sqlite manual channel resend store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "plan manual channel resend", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, changed, err = channeldelivery.PlanManualResendTx(txctx, tx, action, resolved, false)
		return err
	})
	return deliveryID, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) PlanOpenChannelCard(ctx context.Context, cardID string) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	var created bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "plan open channel card", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		created, err = channeldelivery.PlanOpenCardTx(txctx, tx, cardID, false)
		return err
	})
	return created, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && created, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) CurrentChannelCardChangeCursor(ctx context.Context) (int64, bool, error) {
	if s == nil || s.backend == nil {
		return 0, false, fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return 0, false, err
	}
	var cursor int64
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		cursor, found, err = channeldelivery.CurrentCardChangeCursor(txctx, tx)
		return err
	})
	return cursor, found, err
}

func (s *SQLiteRuntimeStore) PlanChangedChannelCard(ctx context.Context, sequence int64, cardID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "plan changed channel card", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.PlanChangedCardTx(txctx, tx, sequence, cardID, false)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) RejectInboxEntry(ctx context.Context, text operatorchannel.InboundText) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.RejectNativeEntryTx(txctx, tx, text, true)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) RejectInboxEntry(ctx context.Context, text operatorchannel.InboundText) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "reject native inbox entry", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.RejectNativeEntryTx(txctx, tx, text, false)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) FreezeAndPersistChannelRender(ctx context.Context, deliveryID string, bounds packs.PresentationBounds) (render.PreparedRender, error) {
	if s == nil || s.backend == nil {
		return render.PreparedRender{}, fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.PreparedRender{}, err
	}
	var stored render.PreparedRender
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		stored, changed, err = freezeAndPersistChannelRenderTx(txctx, tx, deliveryID, bounds, true)
		return err
	})
	return stored, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) FreezeAndPersistChannelRender(ctx context.Context, deliveryID string, bounds packs.PresentationBounds) (render.PreparedRender, error) {
	if s == nil || s.backend == nil {
		return render.PreparedRender{}, fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.PreparedRender{}, err
	}
	var stored render.PreparedRender
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "freeze channel delivery render", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		stored, changed, err = freezeAndPersistChannelRenderTx(txctx, tx, deliveryID, bounds, false)
		return err
	})
	return stored, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func freezeAndPersistChannelRenderTx(ctx context.Context, tx *sql.Tx, deliveryID string, bounds packs.PresentationBounds, postgres bool) (render.PreparedRender, bool, error) {
	if err := channeldelivery.SetPresentationBoundsTx(ctx, tx, deliveryID, bounds, postgres); err != nil {
		return render.PreparedRender{}, false, err
	}
	plan, found, err := channeldelivery.LoadPlan(ctx, tx, deliveryID, postgres)
	if err != nil {
		return render.PreparedRender{}, false, err
	}
	if !found {
		return render.PreparedRender{}, false, fmt.Errorf("channel delivery plan %s is missing", deliveryID)
	}
	if plan.Bounds.Validate() != nil {
		return render.PreparedRender{}, false, fmt.Errorf("channel card lacks selected action capacity")
	}
	var frozen render.Frozen
	frozen, err = channeldelivery.FreezeCurrentSourceTx(ctx, tx, plan, postgres)
	if err != nil {
		return render.PreparedRender{}, false, err
	}
	id, changed, err := channeldelivery.PersistRenderTx(ctx, tx, deliveryID, frozen, postgres)
	if err != nil {
		return render.PreparedRender{}, false, err
	}
	actions, err := channeldelivery.EnsureRenderActionsTx(ctx, tx, id, frozen, postgres)
	if err != nil {
		return render.PreparedRender{}, false, err
	}
	return render.PreparedRender{RenderID: id, DeliveryID: deliveryID, Frozen: frozen, Actions: actions}, changed, nil
}
