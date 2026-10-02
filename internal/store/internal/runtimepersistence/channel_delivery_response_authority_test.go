package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	"github.com/google/uuid"
)

// These are effect-owner fixtures, not public writer/ingress proof. Each
// response variant retains a different settled intent on the same receipt path.
func proveChannelResponseReceiptMatrix(t *testing.T, selected selectedChannelDeliveryTestStore,
	runTx func(func(context.Context, *sql.Tx) error) error, current func(runtimeeffects.Authority) bool,
	template runtimeeffects.Authority, text operatorchannel.InboundText, postgres bool) {
	t.Helper()
	for _, family := range []string{"entry", "teaching", "chooser", "navigation"} {
		t.Run(family, func(t *testing.T) {
			ctx := context.Background()
			text.PublicationID, text.ProviderEventID = uuid.NewString(), uuid.NewString()
			var deliveryID string
			intentTable := "operator_channel_text_intents"
			err := runTx(func(ctx context.Context, tx *sql.Tx) error {
				if err := channeldelivery.InsertTextIntentTx(ctx, tx, text, time.Now(), postgres); err != nil {
					return err
				}
				var err error
				deliveryID, err = channeldelivery.PlanTextResponseTx(ctx, tx, text, "Retained requested response", "teaching", postgres)
				if err != nil {
					return err
				}
				if family == "navigation" {
					action := operatorchannel.InboundAction{
						ActionFact: operatorchannel.ActionFact{Interface: text.Interface, ExternalAccountRef: text.ExternalAccountRef,
							ConversationRef: text.ConversationRef, ConversationScope: text.ConversationScope,
							Token: uuid.NewString(), InteractionRef: "fixture-interaction", MessageReference: text.MessageReference},
						Provider: text.Provider, ProviderEventID: text.ProviderEventID, PublicationID: text.PublicationID,
						ProviderAuthorization: text.ProviderAuthorization,
					}
					if err := channeldelivery.InsertActionIntentTx(ctx, tx, action, time.Now(), postgres); err != nil {
						return err
					}
					if _, err := tx.ExecContext(ctx, `DELETE FROM operator_channel_text_intents WHERE publication_id=$1`, text.PublicationID); err != nil {
						return err
					}
					intentTable = "operator_channel_action_intents"
				}
				_, err = tx.ExecContext(ctx, `UPDATE `+intentTable+` SET state='settled', disposition=$1, settled_at=$2 WHERE publication_id=$3`, family, time.Now(), text.PublicationID)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := selected.FreezeAndPersistChannelRender(ctx, deliveryID, packs.PresentationBounds{Actions: 2, TextRunes: 512, LabelRunes: 24})
			if err != nil {
				t.Fatal(err)
			}
			authority := responseTestAuthority(t, template, deliveryID, prepared)
			if !current(authority) {
				t.Fatal("exact first-send response authority rejected")
			}
			t.Run("contradictory_source_intents", func(t *testing.T) {
				rollback := errors.New("rollback contradictory response intents")
				err := runTx(func(ctx context.Context, tx *sql.Tx) error {
					if intentTable == "operator_channel_action_intents" {
						if err := channeldelivery.InsertTextIntentTx(ctx, tx, text, time.Now(), postgres); err != nil {
							return err
						}
					} else {
						action := operatorchannel.InboundAction{
							ActionFact: operatorchannel.ActionFact{Interface: text.Interface, ExternalAccountRef: text.ExternalAccountRef,
								ConversationRef: text.ConversationRef, ConversationScope: text.ConversationScope,
								Token: uuid.NewString(), InteractionRef: "contradictory-source", MessageReference: text.MessageReference},
							Provider: text.Provider, ProviderEventID: text.ProviderEventID, PublicationID: text.PublicationID,
							ProviderAuthorization: text.ProviderAuthorization,
						}
						if err := channeldelivery.InsertActionIntentTx(ctx, tx, action, time.Now(), postgres); err != nil {
							return err
						}
					}
					if currentTxResponseTest(t, ctx, tx, authority, postgres) {
						return fmt.Errorf("two source intents acquired response authority")
					}
					if err := requireResponseAuthorityTx(selected, ctx, tx, authority); err == nil {
						return fmt.Errorf("two source intents acquired locked effect authority")
					}
					return rollback
				})
				if !errors.Is(err, rollback) {
					t.Fatal(err)
				}
			})
			foreign := authority
			foreign.ChannelDelivery.PreviousReceiptOperationID = uuid.NewString()
			if current(foreign) {
				t.Fatal("first send with missing predecessor admitted")
			}
			t.Run("initial", func(t *testing.T) {
				proveResponseAuthorityNegatives(t, selected, runTx, current, authority, intentTable, text.PublicationID)
			})
			effectCtx, handle := authorizeResponseTestEffect(t, selected, authority)
			if err := handle.MarkLaunched(effectCtx); err != nil {
				t.Fatal(err)
			}
			if err := handle.MarkResponseObserved(effectCtx, map[string]any{"accepted": true}); err != nil {
				t.Fatal(err)
			}
			if err := handle.Succeed(effectCtx, map[string]any{"projected_output": map[string]any{"delivery_reference": map[string]any{"id": 73}}}); err != nil {
				t.Fatal(err)
			}
			previousID := authority.ID
			advance := func(visit int) render.PreparedRender {
				t.Helper()
				if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `UPDATE channel_delivery_plans SET action_page_index=$1 WHERE delivery_id=$2`, visit, deliveryID)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				frozen, err := selected.FreezeAndPersistChannelRender(ctx, deliveryID, prepared.Frozen.Bounds)
				if err != nil {
					t.Fatal(err)
				}
				return frozen
			}
			updated := advance(1)
			edit := responseTestAuthority(t, template, deliveryID, updated)
			edit.ChannelDelivery.PreviousReceiptOperationID = previousID
			if updated.RenderID == prepared.RenderID || !current(edit) {
				t.Fatal("exact sent predecessor from previous render cannot authorize response edit")
			}
			t.Run("edit", func(t *testing.T) {
				proveResponseAuthorityNegatives(t, selected, runTx, current, edit, intentTable, text.PublicationID)
			})
			editCtx, editHandle := authorizeResponseTestEffect(t, selected, edit)
			competing := edit
			competing.ID = uuid.NewString()
			competing.ChannelDelivery.EffectOperationID = competing.ID
			if current(competing) {
				t.Fatal("another active effect did not exclude competing response dispatch")
			}
			// A principal/default transition after authorization still fences launch.
			rollback := errors.New("rollback response prelaunch fence")
			if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `UPDATE channel_delivery_defaults SET delivery_epoch=delivery_epoch+1`); err != nil {
					return err
				}
				if currentTxResponseTest(t, ctx, tx, edit, postgres) {
					return fmt.Errorf("authority loss after authorization was ignored")
				}
				if err := requireResponseAuthorityTx(selected, ctx, tx, edit); err == nil {
					return fmt.Errorf("prelaunch owner admitted authority lost after authorization")
				}
				return rollback
			}); !errors.Is(err, rollback) {
				t.Fatal(err)
			}
			if err := editHandle.MarkLaunched(editCtx); err != nil {
				t.Fatal(err)
			}
			if err := editHandle.MarkResponseObserved(editCtx, map[string]any{"edited": true}); err != nil {
				t.Fatal(err)
			}
			if err := editHandle.Succeed(editCtx, map[string]any{"projected_output": map[string]any{"delivery_receipt": map[string]any{"id": 91}}}); err != nil {
				t.Fatal(err)
			}
			receipt, found, err := selected.GetCurrentChannelSentReceipt(ctx, deliveryID, edit.ID)
			rawReference, encodeErr := json.Marshal(receipt.DeliveryReference)
			if err != nil || encodeErr != nil || !found || string(rawReference) != `{"id":73}` {
				t.Fatalf("edit did not retain original message reference: %#v %t %v", receipt, found, err)
			}
			next := responseTestAuthority(t, template, deliveryID, advance(2))
			next.ChannelDelivery.PreviousReceiptOperationID = previousID
			if current(next) {
				t.Fatal("historical sent predecessor revived after receipt CAS")
			}
			next.ChannelDelivery.PreviousReceiptOperationID = edit.ID
			if !current(next) {
				t.Fatal("exact successor receipt rejected")
			}
		})
	}
}

func responseTestAuthority(t *testing.T, template runtimeeffects.Authority, deliveryID string, prepared render.PreparedRender) runtimeeffects.Authority {
	t.Helper()
	id, err := runtimeeffects.ChannelDeliveryOperationID(deliveryID, prepared.RenderID)
	if err != nil {
		t.Fatal(err)
	}
	template.ID = id
	template.ChannelDelivery.EffectOperationID, template.ChannelDelivery.DeliveryID = id, deliveryID
	template.ChannelDelivery.RenderID, template.ChannelDelivery.RenderHash = prepared.RenderID, prepared.Frozen.Hash
	return template
}

func authorizeResponseTestEffect(t *testing.T, selected selectedChannelDeliveryTestStore, authority runtimeeffects.Authority) (context.Context, *runtimeeffects.Handle) {
	t.Helper()
	ctx := runtimeeffects.WithController(runtimeeffects.WithAuthority(testAuthorActivityContextForBundle(authority.ChannelDelivery.BundleHash), authority),
		runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
	handle, err := runtimeeffects.BeginChannelDelivery(ctx, []byte(authority.ID), nil)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, handle
}

func proveResponseAuthorityNegatives(t *testing.T, selected selectedChannelDeliveryTestStore,
	runTx func(func(context.Context, *sql.Tx) error) error, current func(runtimeeffects.Authority) bool,
	edit runtimeeffects.Authority, intentTable, publicationID string) {
	t.Helper()
	for _, test := range []struct {
		name   string
		change func(*runtimeeffects.ChannelDeliveryAuthority)
	}{
		{"missing_predecessor", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.PreviousReceiptOperationID = "" }},
		{"foreign_predecessor", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.PreviousReceiptOperationID = uuid.NewString() }},
		{"foreign_delivery", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.DeliveryID = uuid.NewString() }},
		{"foreign_render", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.RenderID = uuid.NewString() }},
		{"foreign_render_hash", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.RenderHash = "sha256:foreign" }},
		{"foreign_principal", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.PrincipalID = uuid.NewString() }},
		{"foreign_audience", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.ConversationRef += "-foreign" }},
		{"foreign_account", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.ExternalAccountRef += "-foreign" }},
		{"default_epoch", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.DeliveryEpoch++ }},
		{"binding_revision", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.BindingRevision++ }},
		{"request_activation", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.ActivationID = uuid.NewString() }},
		{"activation_revision", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.ActivationRevision++ }},
		{"publication_generation", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.ContextPublicationGeneration++ }},
		{"plan_generation", func(d *runtimeeffects.ChannelDeliveryAuthority) {
			d.PlanGeneration, _ = plangeneration.FromCanonicalValue("foreign-response")
		}},
		{"target_generation", func(d *runtimeeffects.ChannelDeliveryAuthority) { d.TargetGeneration++ }},
	} {
		if test.name == "missing_predecessor" && edit.ChannelDelivery.PreviousReceiptOperationID == "" {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			foreign := edit
			test.change(&foreign.ChannelDelivery)
			if current(foreign) {
				t.Fatal("contradictory response authority admitted")
			}
			ctx := runtimeeffects.WithController(runtimeeffects.WithAuthority(testAuthorActivityContextForBundle(edit.ChannelDelivery.BundleHash), foreign), runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
			if _, err := runtimeeffects.BeginChannelDelivery(ctx, []byte("forbidden edit"), nil); err == nil {
				t.Fatal("contradictory response acquired managed authorization")
			}
		})
	}
	for _, test := range []struct {
		name, query string
		arg         any
	}{
		{"unsettled_intent", `UPDATE ` + intentTable + ` SET state='pending', disposition=NULL, settled_at=NULL WHERE publication_id=$1`, publicationID},
		{"missing_intent", `DELETE FROM ` + intentTable + ` WHERE publication_id=$1`, publicationID},
		{"retired_binding", `UPDATE operator_channel_bindings SET status='unbound', provider_credential_key=NULL,
			provider_credential_value_seal=NULL, external_account_reference=NULL, conversation_reference=NULL,
			conversation_scope=NULL, source=NULL WHERE interface_key=$1`, edit.ChannelDelivery.InterfaceKey},
		{"retired_activation", `UPDATE connected_channel_activations SET status='retired', retired_at=CURRENT_TIMESTAMP, retirement_reason='test retirement' WHERE activation_id=$1`, edit.ChannelDelivery.ActivationID},
		{"retired_default", `UPDATE channel_delivery_defaults SET state='retired' WHERE principal_id=$1`, edit.ChannelDelivery.PrincipalID},
		{"uncertain_predecessor", `UPDATE channel_delivery_receipts SET state='uncertain', provider_reference=NULL WHERE effect_operation_id=$1`, edit.ChannelDelivery.PreviousReceiptOperationID},
		{"foreign_receipt_delivery", `UPDATE channel_delivery_receipts SET delivery_id=(SELECT delivery_id FROM channel_delivery_plans WHERE source_kind='notice' LIMIT 1) WHERE effect_operation_id=$1`, edit.ChannelDelivery.PreviousReceiptOperationID},
	} {
		if edit.ChannelDelivery.PreviousReceiptOperationID == "" && (test.name == "uncertain_predecessor" || test.name == "foreign_receipt_delivery") {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			rollback := errors.New("rollback response authority negative")
			err := runTx(func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, test.query, test.arg); err != nil {
					return err
				}
				_, postgres := selected.(*PostgresStore)
				if currentTxResponseTest(t, ctx, tx, edit, postgres) {
					return fmt.Errorf("%s was admitted", test.name)
				}
				if err := requireResponseAuthorityTx(selected, ctx, tx, edit); err == nil {
					return fmt.Errorf("%s acquired locked effect authority", test.name)
				}
				return rollback
			})
			if !errors.Is(err, rollback) {
				t.Fatal(err)
			}
		})
	}
}

func requireResponseAuthorityTx(selected selectedChannelDeliveryTestStore, ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	if store, ok := selected.(*PostgresStore); ok {
		return store.effectPostgresOwner.RequireExternalEffectAuthorityTx(ctx, tx, authority, false)
	}
	return selected.(*SQLiteRuntimeStore).effectSQLiteOwner.RequireExternalEffectAuthorityTx(ctx, tx, authority, false)
}

func currentTxResponseTest(t *testing.T, ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, postgres bool) bool {
	t.Helper()
	current := effectpersistence.ExternalEffectAuthorityCurrentSQLite
	if postgres {
		current = effectpersistence.ExternalEffectAuthorityCurrentPostgres
	}
	ok, err := current(ctx, tx, authority)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}
