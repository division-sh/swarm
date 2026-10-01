package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/google/uuid"
)

// This controls retirement between a refused mutation and its terminal write;
// the chooser itself is produced by the normal selected-store owners.
func proveUnsupportedChooserRetirement(t *testing.T, cards decisioncard.Store, selected selectedChannelDeliveryTestStore,
	runTx func(func(context.Context, *sql.Tx) error) error, authority runtimeeffects.Authority,
	card decisioncard.Card, text operatorchannel.InboundText, now time.Time, postgres bool) {
	t.Helper()
	ctx := testAuthorActivityContext()
	delivery := cards.(render.Store)
	deliver := func(id string, messageID int) render.PreparedRender {
		t.Helper()
		prepared, err := delivery.FreezeAndPersistChannelRender(ctx, id, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64})
		if err != nil {
			t.Fatal(err)
		}
		copy := authority
		copy.ID, err = runtimeeffects.ChannelDeliveryOperationID(id, prepared.RenderID)
		if err != nil {
			t.Fatal(err)
		}
		copy.ChannelDelivery.EffectOperationID = copy.ID
		copy.ChannelDelivery.DeliveryID = id
		copy.ChannelDelivery.RenderID = prepared.RenderID
		copy.ChannelDelivery.RenderHash = prepared.Frozen.Hash
		effectCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(
			testAuthorActivityContextForBundle(copy.ChannelDelivery.BundleHash), copy),
			runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
		handle, err := runtimeeffects.BeginChannelDelivery(effectCtx, []byte(id), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := handle.MarkLaunched(effectCtx); err != nil {
			t.Fatal(err)
		}
		if err := handle.MarkResponseObserved(effectCtx, map[string]any{"provider": "accepted"}); err != nil {
			t.Fatal(err)
		}
		if err := handle.Succeed(effectCtx, map[string]any{"projected_output": map[string]any{"delivery_reference": map[string]any{"id": messageID}}}); err != nil {
			t.Fatal(err)
		}
		return prepared
	}
	for i := 0; i < 2; i++ {
		copy := card
		copy.CardID = uuid.NewString()
		copy.Anchor = newDecisionCardTestStageAnchor("review/check-1", "review", uuid.NewString(), "awaiting_review", uuid.NewString())
		var err error
		copy, err = decisioncard.New(copy)
		if err != nil {
			t.Fatal(err)
		}
		if err := cards.CreateDecisionCard(ctx, copy); err != nil {
			t.Fatal(err)
		}
		if _, err := delivery.PlanOpenChannelCard(ctx, copy.CardID); err != nil {
			t.Fatal(err)
		}
		plans, err := delivery.ListCurrentChannelDeliveryPlans(ctx, "", 100)
		if err != nil {
			t.Fatal(err)
		}
		var id string
		for _, plan := range plans {
			if plan.SourceID == copy.CardID {
				id = plan.DeliveryID
			}
		}
		prepared := deliver(id, 201+i)
		operationID, _ := runtimeeffects.ChannelDeliveryOperationID(id, prepared.RenderID)
		if _, err := DecisionCardDomainForTest(cards).BeginInputForTest(ctx, decisioncard.BeginInputRequest{
			CardID: copy.CardID, Verdict: "revise", PrincipalID: authority.ChannelDelivery.PrincipalID,
			DeliveryReceiptID: operationID, Now: now.Add(time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}
	unrelatedID := text.PublicationID
	text.PublicationID, text.ProviderEventID, text.MessageReference = uuid.NewString(), "retired-chooser-answer", `{"id":203}`
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		return channeldelivery.InsertTextIntentTx(ctx, tx, text, now.Add(2*time.Minute), postgres)
	}); err != nil {
		t.Fatal(err)
	}
	id, err := delivery.PlanChannelDraftChooser(ctx, text, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	prepared := deliver(id, 204)
	action := operatorchannel.InboundAction{ActionFact: operatorchannel.ActionFact{
		Interface: text.Interface, ExternalAccountRef: text.ExternalAccountRef,
		ConversationRef: text.ConversationRef, ConversationScope: text.ConversationScope,
		MessageReference: `{"id":204}`, InteractionRef: "retired-chooser-callback", Token: prepared.Actions[0].Token,
	}, Provider: text.Provider, ProviderEventID: "retired-chooser-callback", PublicationID: uuid.NewString(), ProviderAuthorization: text.ProviderAuthorization}
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		return channeldelivery.InsertActionIntentTx(ctx, tx, action, now.Add(3*time.Minute), postgres)
	}); err != nil {
		t.Fatal(err)
	}
	check := func(ctx context.Context, tx *sql.Tx, wantAction, wantText string) error {
		var actionDisposition, textDisposition string
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(disposition,'') FROM operator_channel_action_intents WHERE publication_id=$1`, action.PublicationID).Scan(&actionDisposition); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT disposition FROM operator_channel_text_intents WHERE publication_id=$1`, text.PublicationID).Scan(&textDisposition); err != nil {
			return err
		}
		if actionDisposition != wantAction || textDisposition != wantText {
			return fmt.Errorf("terminal chooser pair=%q/%q want=%q/%q", actionDisposition, textDisposition, wantAction, wantText)
		}
		var unrelatedState string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM operator_channel_text_intents WHERE publication_id=$1`, unrelatedID).Scan(&unrelatedState); err != nil {
			return err
		}
		if unrelatedState != "pending" {
			return fmt.Errorf("unsupported chooser consumed unrelated text: %s", unrelatedState)
		}
		return nil
	}
	retire := func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE connected_channel_activations SET status='retired',retired_at=$2,retirement_reason='controlled settlement race' WHERE activation_id=$1`, authority.ChannelDelivery.ActivationID, now.Add(4*time.Minute))
		return err
	}
	rollback := errors.New("interrupted unsupported pair settlement")
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		if err := retire(ctx, tx); err != nil {
			return err
		}
		if err := channeldelivery.SettleUnappliedActionIntentTx(ctx, tx, action, render.ActionUnsupported, postgres); err != nil {
			return err
		}
		if err := check(ctx, tx, "unsupported", "unsupported"); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("retirement/settlement rollback: %v", err)
	}
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error { return check(ctx, tx, "", "chooser") }); err != nil {
		t.Fatal(err)
	}
	if err := runTx(retire); err != nil {
		t.Fatal(err)
	}
	foreign := action
	foreign.PublicationID, foreign.ProviderEventID, foreign.InteractionRef, foreign.MessageReference = uuid.NewString(), "foreign-message", "foreign-message", `{"id":999}`
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		return channeldelivery.InsertActionIntentTx(ctx, tx, foreign, now.Add(4*time.Minute), postgres)
	}); err != nil {
		t.Fatal(err)
	}
	if err := delivery.SettleUnappliedChannelAction(ctx, foreign, render.ActionUnsupported); err == nil {
		t.Fatal("foreign receipt consumed the retained answer")
	}
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error { return check(ctx, tx, "", "chooser") }); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := delivery.SettleUnappliedChannelAction(ctx, action, render.ActionUnsupported); err != nil {
			t.Fatal(err)
		}
		if err := runTx(func(ctx context.Context, tx *sql.Tx) error { return check(ctx, tx, "unsupported", "unsupported") }); err != nil {
			t.Fatal(err)
		}
	}
	if _, found, err := selected.ResolveChannelActionFact(ctx, action.ActionFact); err != nil || found {
		t.Fatalf("terminal historical chooser regained mutation authority: %t %v", found, err)
	}
}
