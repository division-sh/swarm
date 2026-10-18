package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/google/uuid"
)

type cardIntentRecheckKey struct{}
type cardIntentRecheckCut struct {
	publication                 string
	text                        bool
	once                        sync.Once
	fired, preliminary, locking bool
	err                         error
}
type cardIntentRecheckProbe struct {
	native   *sql.DB
	driver   driver.Driver
	location string
}

func (p *cardIntentRecheckProbe) Driver() driver.Driver { return p.driver }
func (p *cardIntentRecheckProbe) Connect(context.Context) (driver.Conn, error) {
	c, err := p.driver.Open(p.location)
	if err != nil {
		return nil, err
	}
	return &cardIntentRecheckConn{Conn: c, probe: p}, nil
}

type cardIntentRecheckConn struct {
	driver.Conn
	probe *cardIntentRecheckProbe
}

func (c *cardIntentRecheckConn) before(ctx context.Context, query string) error {
	cut, ok := ctx.Value(cardIntentRecheckKey{}).(*cardIntentRecheckCut)
	if !ok {
		return nil
	}
	if strings.Contains(query, "FROM operator_channel_action_intents WHERE publication_id=") {
		if strings.HasSuffix(query, " FOR UPDATE") {
			cut.locking = true
		} else {
			cut.preliminary = true
		}
	}
	if query != "SELECT operation_id::text FROM channel_onboarding_operations WHERE operation_id=$1::uuid FOR UPDATE" {
		return nil
	}
	cut.once.Do(func() {
		// An independent fixture transaction changes the exact intent only after
		// the caller's preliminary check. NOWAIT refuses an earlier intent lock.
		tx, err := c.probe.native.BeginTx(ctx, nil)
		if err != nil {
			cut.err = err
			return
		}
		defer tx.Rollback()
		table := "operator_channel_action_intents"
		if cut.text {
			table = "operator_channel_text_intents"
		}
		var id string
		if err := tx.QueryRowContext(ctx, "SELECT publication_id::text FROM "+table+" WHERE publication_id=$1::uuid FOR UPDATE NOWAIT", cut.publication).Scan(&id); err != nil {
			cut.err = err
			return
		}
		result, err := tx.ExecContext(ctx, "UPDATE "+table+" SET provider_authorization=provider_authorization || ':changed' WHERE publication_id=$1::uuid", cut.publication)
		if err != nil {
			cut.err = err
			return
		}
		count, err := result.RowsAffected()
		if err != nil || count != 1 {
			cut.err = fmt.Errorf("intent invalidation did not affect exactly one row")
			return
		}
		cut.err = tx.Commit()
		cut.fired = cut.err == nil
	})
	return cut.err
}
func (c *cardIntentRecheckConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.before(ctx, q); err != nil {
		return nil, err
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
}
func (c *cardIntentRecheckConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.before(ctx, q); err != nil {
		return nil, err
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
}
func (c *cardIntentRecheckConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func proveCardIntentLockingRecheck(t *testing.T, ctx context.Context, cards decisioncard.Store, selected selectedChannelDeliveryTestStore,
	card decisioncard.Card, prepared render.PreparedRender, template operatorchannel.ActionFact, probe *cardIntentRecheckProbe,
	runTx func(func(context.Context, *sql.Tx) error) error) {
	t.Helper()
	var control render.Action
	for _, action := range prepared.Actions {
		if action.Kind == "verdict" && action.Verdict == "revise" {
			control = action
		}
	}
	if control.Token == "" {
		t.Fatal("input control is absent")
	}
	for _, transferred := range []bool{false, true} {
		name := "intent_changed_after_preliminary_read"
		if transferred {
			name = "transferred_text_changed_after_preliminary_read"
		}
		t.Run(name, func(t *testing.T) {
			action := operatorchannel.InboundAction{ActionFact: template, Provider: "telegram", ProviderEventID: uuid.NewString(), PublicationID: uuid.NewString(), ProviderAuthorization: "verified-catalog"}
			action.Token = control.Token
			pending := render.PendingAction{PublicationID: action.PublicationID, Fact: action, ReceivedAt: time.Now().UTC()}
			if transferred {
				text := operatorchannel.InboundText{TextFact: operatorchannel.TextFact{Interface: template.Interface, ExternalAccountRef: template.ExternalAccountRef,
					ConversationRef: template.ConversationRef, ConversationScope: template.ConversationScope, Text: control.Label,
					MessageReference: `{"id":92}`, ReplyToReference: template.MessageReference}, Provider: action.Provider, ProviderEventID: action.ProviderEventID,
					PublicationID: action.PublicationID, ProviderAuthorization: action.ProviderAuthorization}
				if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
					return channeldelivery.InsertTextIntentTx(ctx, tx, text, pending.ReceivedAt, true)
				}); err != nil {
					t.Fatal(err)
				}
				var found bool
				var err error
				pending, found, err = selected.(render.Store).AdmitChannelReplyAction(ctx, text)
				if err != nil || !found {
					t.Fatalf("exact reply transfer: found=%t error=%v", found, err)
				}
			} else if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
				return channeldelivery.InsertActionIntentTx(ctx, tx, action, pending.ReceivedAt, true)
			}); err != nil {
				t.Fatal(err)
			}
			resolved, found, err := selected.ResolveChannelActionFact(ctx, pending.Fact.ActionFact)
			if err != nil || !found || !resolved.CurrentRender {
				t.Fatalf("current input action: found=%t error=%v", found, err)
			}
			input, err := render.PrepareCardAction(pending, resolved, card)
			if err != nil {
				t.Fatal(err)
			}
			before, err := cards.GetDecisionCard(ctx, card.CardID)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := cards.(pipeline.DecisionCardMutationOwner).AcquireDecisionCardMutation(ctx, input.Request, input.Mutation)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := lease.Release(ctx); err != nil {
					t.Error(err)
				}
			}()
			cut := &cardIntentRecheckCut{publication: pending.PublicationID, text: transferred}
			result, err := lease.Commit(context.WithValue(ctx, cardIntentRecheckKey{}, cut), pipeline.DecisionCardMutationCommand{Mutation: input.Mutation})
			want := "verified channel action intent contradicts admitted fact"
			if transferred {
				want = "reply action lacks its exact transferred text intent"
			}
			if !cut.preliminary || !cut.fired || !cut.locking || cut.err != nil || result.Acknowledged || err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("locking recheck: preliminary=%t invalidated=%t locking=%t acknowledged=%t cut=%T error=%v", cut.preliminary, cut.fired, cut.locking, result.Acknowledged, cut.err, err)
			}
			after, err := cards.GetDecisionCard(ctx, card.CardID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("refused input changed the exact card")
			}
			var count int
			if err := probe.native.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_idempotency WHERE method=$1 AND actor_kind=$2 AND actor_id=$3 AND idempotency_key=$4`,
				input.Request.Method, input.Request.Actor.Kind, input.Request.Actor.ID, input.Request.IdempotencyKey).Scan(&count); err != nil || count != 0 {
				t.Fatal("refused input left completion evidence")
			}
		})
	}
}
