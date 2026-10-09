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
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/google/uuid"
)

func proveChannelReplyTransfer(t *testing.T, selected selectedChannelDeliveryTestStore,
	runTx func(func(context.Context, *sql.Tx) error) error, current func(runtimeeffects.Authority) bool,
	template runtimeeffects.Authority, text operatorchannel.InboundText, control render.Action, postgres bool) {
	t.Helper()
	ctx := context.Background()
	store := selected.(render.Store)
	text.PublicationID, text.ProviderEventID, text.MessageReference = uuid.NewString(), uuid.NewString(), "human-reply"
	text.EntryReference, text.EntryAddress = "", ""
	text.ReplyToReference, text.Text = `{"id":91}`, control.Label
	receivedAt := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name string
		edit func(*operatorchannel.InboundText)
	}{
		{"unquoted", func(f *operatorchannel.InboundText) { f.ReplyToReference = "" }},
		{"foreign quote", func(f *operatorchannel.InboundText) { f.ReplyToReference = `{"id":92}` }},
		{"quoted words are not controls", func(f *operatorchannel.InboundText) { f.Text = "please " + control.Label }},
		{"foreign account", func(f *operatorchannel.InboundText) { f.ExternalAccountRef = "foreign" }},
		{"foreign conversation", func(f *operatorchannel.InboundText) { f.ConversationRef = "foreign" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := text
			probe.PublicationID, probe.ProviderEventID = uuid.NewString(), uuid.NewString()
			test.edit(&probe)
			if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
				return channeldelivery.InsertTextIntentTx(ctx, tx, probe, receivedAt, postgres)
			}); err != nil {
				t.Fatal(err)
			}
			if _, found, err := store.AdmitChannelReplyAction(ctx, probe); found {
				t.Fatalf("non-authoritative text acquired an action, err=%v", err)
			}
			if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
				var state string
				if err := tx.QueryRowContext(ctx, `SELECT state FROM operator_channel_text_intents WHERE publication_id=$1`, probe.PublicationID).Scan(&state); err != nil {
					return err
				}
				if state != "pending" {
					return fmt.Errorf("rejected transfer changed text: %s", state)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		return channeldelivery.InsertTextIntentTx(ctx, tx, text, receivedAt, postgres)
	}); err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("rollback reply transfer")
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		if _, found, _, err := channeldelivery.AdmitReplyActionTx(ctx, tx, text, postgres); err != nil || !found {
			return fmt.Errorf("reply transfer before rollback: %t, %w", found, err)
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	action, found, err := store.AdmitChannelReplyAction(ctx, text)
	if err != nil || !found || action.Fact.Kind != operatorchannel.ActionSourceReply ||
		action.Fact.InteractionRef != "" || action.Fact.Token != control.Token ||
		action.Fact.TextSource != text.TextFact || !action.ReceivedAt.Equal(receivedAt) {
		t.Fatalf("exact reply transfer = %#v, %t, %v", action, found, err)
	}
	replay, found, err := store.AdmitChannelReplyAction(ctx, text)
	if err != nil || !found || replay != action {
		t.Fatalf("reply transfer replay = %#v, %t, %v", replay, found, err)
	}
	forged := action.Fact
	forged.TextSource.MessageReference = "fabricated-source"
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		_, err := channeldelivery.RequireActionIntentTx(ctx, tx, forged, postgres, true)
		return err
	}); err == nil {
		t.Fatal("forged transfer source acquired mutation authority")
	}
	resolved, found, err := store.ResolveChannelActionFact(ctx, action.Fact.ActionFact)
	if err != nil || !found || !resolved.CurrentRender || resolved.Action != control {
		t.Fatalf("reply control = %#v, %t, %v", resolved, found, err)
	}
	deliveryID, err := store.PlanChannelActionResponse(ctx, action.Fact, resolved, "")
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := store.FreezeAndPersistChannelRender(ctx, deliveryID, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64})
	if err != nil {
		t.Fatal(err)
	}
	authority := responseTestAuthority(t, template, deliveryID, prepared)
	if !current(authority) {
		t.Fatal("exact reply navigation lacks first-send response authority")
	}
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		var texts, actions int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_channel_text_intents WHERE publication_id=$1 AND state='settled' AND disposition='control'`, text.PublicationID).Scan(&texts); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_channel_action_intents WHERE publication_id=$1 AND state='settled' AND disposition='navigation'`, text.PublicationID).Scan(&actions); err != nil {
			return err
		}
		if texts != 1 || actions != 1 {
			return fmt.Errorf("reply transfer did not retain exactly one settled responsibility: %d/%d", texts, actions)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
