package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

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
	return s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return channeldelivery.SettleUnappliedActionIntentTx(txctx, tx, action, disposition, true)
	})
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

func (s *PostgresStore) ResolveCurrentNativeInboxEntry(ctx context.Context, text operatorchannel.InboundText) (render.ResolvedNativeEntry, bool, error) {
	if s == nil || s.backend == nil {
		return render.ResolvedNativeEntry{}, false, fmt.Errorf("postgres native inbox store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.ResolvedNativeEntry{}, false, err
	}
	var entry render.ResolvedNativeEntry
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		entry, found, err = channeldelivery.ResolveCurrentNativeInboxEntryTx(txctx, tx, text, true)
		return err
	})
	return entry, found, err
}

func (s *PostgresStore) PlanNativeInboxResponse(ctx context.Context, text operatorchannel.InboundText, entry render.ResolvedNativeEntry, fullText string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("postgres channel response store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	err := s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, err = channeldelivery.PlanNativeInboxResponseTx(txctx, tx, text, entry, fullText, true)
		return err
	})
	return deliveryID, err
}

func (s *PostgresStore) PlanChannelActionResponse(ctx context.Context, action operatorchannel.InboundAction, resolved render.ResolvedAction, inboxText string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("postgres channel response store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	err := s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, err = channeldelivery.PlanActionResponseTx(txctx, tx, action, resolved, inboxText, true)
		return err
	})
	return deliveryID, err
}

func (s *PostgresStore) PlanOpenChannelCard(ctx context.Context, cardID string) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	var created bool
	err := s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		created, err = channeldelivery.PlanOpenCardTx(txctx, tx, cardID, true)
		return err
	})
	return created, err
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
	return s.backend.RunTransaction(ctx, "settle channel action without mutation", func(txctx context.Context, tx *sql.Tx) error {
		return channeldelivery.SettleUnappliedActionIntentTx(txctx, tx, action, disposition, false)
	})
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

func (s *SQLiteRuntimeStore) ResolveCurrentNativeInboxEntry(ctx context.Context, text operatorchannel.InboundText) (render.ResolvedNativeEntry, bool, error) {
	if s == nil || s.backend == nil {
		return render.ResolvedNativeEntry{}, false, fmt.Errorf("sqlite native inbox store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.ResolvedNativeEntry{}, false, err
	}
	var entry render.ResolvedNativeEntry
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		entry, found, err = channeldelivery.ResolveCurrentNativeInboxEntryTx(txctx, tx, text, false)
		return err
	})
	return entry, found, err
}

func (s *SQLiteRuntimeStore) PlanNativeInboxResponse(ctx context.Context, text operatorchannel.InboundText, entry render.ResolvedNativeEntry, fullText string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("sqlite channel response store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	err := s.backend.RunTransaction(ctx, "plan native inbox response", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, err = channeldelivery.PlanNativeInboxResponseTx(txctx, tx, text, entry, fullText, false)
		return err
	})
	return deliveryID, err
}

func (s *SQLiteRuntimeStore) PlanChannelActionResponse(ctx context.Context, action operatorchannel.InboundAction, resolved render.ResolvedAction, inboxText string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("sqlite channel response store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	var deliveryID string
	err := s.backend.RunTransaction(ctx, "plan channel action response", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		deliveryID, err = channeldelivery.PlanActionResponseTx(txctx, tx, action, resolved, inboxText, false)
		return err
	})
	return deliveryID, err
}

func (s *SQLiteRuntimeStore) PlanOpenChannelCard(ctx context.Context, cardID string) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	var created bool
	err := s.backend.RunTransaction(ctx, "plan open channel card", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		created, err = channeldelivery.PlanOpenCardTx(txctx, tx, cardID, false)
		return err
	})
	return created, err
}

func (s *PostgresStore) FreezeAndPersistChannelRender(ctx context.Context, deliveryID string) (render.PreparedRender, error) {
	if s == nil || s.backend == nil {
		return render.PreparedRender{}, fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.PreparedRender{}, err
	}
	var stored render.PreparedRender
	err := s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		stored, err = freezeAndPersistChannelRenderTx(txctx, tx, deliveryID, true)
		return err
	})
	return stored, err
}

func (s *SQLiteRuntimeStore) FreezeAndPersistChannelRender(ctx context.Context, deliveryID string) (render.PreparedRender, error) {
	if s == nil || s.backend == nil {
		return render.PreparedRender{}, fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return render.PreparedRender{}, err
	}
	var stored render.PreparedRender
	err := s.backend.RunTransaction(ctx, "freeze channel delivery render", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		stored, err = freezeAndPersistChannelRenderTx(txctx, tx, deliveryID, false)
		return err
	})
	return stored, err
}

func freezeAndPersistChannelRenderTx(ctx context.Context, tx *sql.Tx, deliveryID string, postgres bool) (render.PreparedRender, error) {
	plan, found, err := channeldelivery.LoadPlan(ctx, tx, deliveryID, postgres)
	if err != nil {
		return render.PreparedRender{}, err
	}
	if !found {
		return render.PreparedRender{}, fmt.Errorf("channel delivery plan %s is missing", deliveryID)
	}
	var frozen render.Frozen
	frozen, err = channeldelivery.FreezeCurrentSourceTx(ctx, tx, plan, postgres)
	if err != nil {
		return render.PreparedRender{}, err
	}
	id, _, err := channeldelivery.PersistRenderTx(ctx, tx, deliveryID, frozen, postgres)
	if err != nil {
		return render.PreparedRender{}, err
	}
	actions, err := channeldelivery.EnsureRenderActionsTx(ctx, tx, id, frozen, postgres)
	if err != nil {
		return render.PreparedRender{}, err
	}
	return render.PreparedRender{RenderID: id, DeliveryID: deliveryID, Frozen: frozen, Actions: actions}, nil
}
