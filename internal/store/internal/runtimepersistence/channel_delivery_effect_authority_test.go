package runtimepersistence

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type selectedChannelDeliveryTestStore interface {
	channelOnboardingEffectSelectedStore
	InsertMailboxItem(context.Context, runtimetools.MailboxItem) (string, error)
	CurrentChannelDeliveryActivationID(context.Context) (string, bool, error)
	ResolveChannelActionFact(context.Context, operatorchannel.ActionFact) (render.ResolvedAction, bool, error)
}

func TestChannelDeliveryEffectCurrentnessSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"current", "late"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				var selected selectedChannelDeliveryTestStore
				var db interface {
					QueryContext(context.Context, string, ...any) (*sql.Rows, error)
					QueryRowContext(context.Context, string, ...any) *sql.Row
				}
				var runTx func(func(context.Context, *sql.Tx) error) error
				postgres := backend == "postgres"
				if postgres {
					_, raw, cleanup := testutil.StartPostgres(t)
					t.Cleanup(cleanup)
					selected, db = admitTestPostgresStore(t, raw), raw
					runTx = func(fn func(context.Context, *sql.Tx) error) error {
						tx, err := raw.BeginTx(ctx, nil)
						if err != nil {
							return err
						}
						if err := fn(ctx, tx); err != nil {
							_ = tx.Rollback()
							return err
						}
						return tx.Commit()
					}
				} else {
					sqlite := newBootstrappedSQLiteRuntimeStoreForTest(t)
					selected, db = sqlite, sqlite.backend
					runTx = func(fn func(context.Context, *sql.Tx) error) error {
						return sqlite.backend.RunTransaction(ctx, "channel delivery authority test", fn)
					}
				}
				current := func(authority runtimeeffects.Authority) bool {
					t.Helper()
					var ok bool
					var err error
					if postgres {
						ok, err = effectpersistence.ExternalEffectAuthorityCurrentPostgres(ctx, db, authority)
					} else {
						ok, err = effectpersistence.ExternalEffectAuthorityCurrentSQLite(ctx, db, authority)
					}
					if err != nil {
						t.Fatal(err)
					}
					return ok
				}
				now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
				onboarding, activation := selectedChannelConfirmationAuthorityFixture(t, selected, now, runtimeeffects.StateAuthorized)
				principal, err := selected.EnsureOperatorPrincipal(ctx, now)
				if err != nil {
					t.Fatal(err)
				}
				bindingOperationID := onboarding.IdentityOperationID
				bindingOperation, err := selected.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
					OperationID: bindingOperationID, Kind: operatorchannel.OperationConnect,
					PrincipalID: principal.ID, Interface: activation.Interface, ExpectedRevision: 0,
					RequestKeyHash: bindingOperationID, RequestHash: bindingOperationID,
					ProviderCredential: operatorChannelProviderEvidence(), RequestedAt: now,
					ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
				})
				if err != nil {
					t.Fatal(err)
				}
				var claimed operatorchannel.ClaimSettlement
				err = runTx(func(txctx context.Context, tx *sql.Tx) error {
					claim := operatorChannelContractClaim(bindingOperation, operatorchannel.ConversationScopeDirect,
						"account", activation.ConversationRef, uuid.NewString())
					if postgres {
						claimed, err = selected.(*PostgresStore).operatorChannelPostgresOwner.SettleInboundClaimTx(txctx, tx, claim, now.Add(time.Second))
					} else {
						claimed, err = selected.(*SQLiteRuntimeStore).operatorChannelSQLiteOwner.SettleInboundClaimTx(txctx, tx, claim, now.Add(time.Second))
					}
					return err
				})
				if err != nil {
					t.Fatal(err)
				}
				_, binding, err := selected.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
					OperationID: bindingOperationID, PrincipalID: principal.ID, ExpectedRevision: claimed.Operation.Revision,
					Approve: true, ProviderCredentialCurrent: true, ConfirmedAt: now.Add(2 * time.Second),
				})
				if err != nil {
					t.Fatal(err)
				}
				if selectedActivationID, found, err := selected.CurrentChannelDeliveryActivationID(ctx); err != nil || found || selectedActivationID != "" {
					t.Fatalf("pre-success activation = %s, found=%t err=%v", selectedActivationID, found, err)
				}
				noticeContext := []byte(`{"message":"` + strings.Repeat("a", 4000) + `"}`)
				noticeID, err := selected.InsertMailboxItem(ctx, runtimetools.MailboxItem{
					Type: runtimetools.NotifyHumanMailboxItemType, Summary: "Delivery authority proof",
					Context: noticeContext,
				})
				if err != nil {
					t.Fatal(err)
				}
				query := `SELECT delivery_id FROM channel_delivery_plans WHERE source_kind='notice' AND source_id=?`
				if postgres {
					query = `SELECT delivery_id::text FROM channel_delivery_plans WHERE source_kind='notice' AND source_id=$1::uuid`
				}
				var deliveryID string
				if err := db.QueryRowContext(ctx, query, noticeID).Scan(&deliveryID); err != nil {
					t.Fatal(err)
				}
				plan, found, err := channeldelivery.LoadPlan(ctx, db, deliveryID, postgres)
				if err != nil || !found {
					t.Fatalf("plan = %#v, found=%t err=%v", plan, found, err)
				}
				frozen, err := render.FreezeNotice(render.Notice{ID: noticeID, Type: runtimetools.NotifyHumanMailboxItemType,
					Summary: "Delivery authority proof", Priority: "normal", Context: noticeContext},
					render.Audience{PrincipalID: principal.ID, InterfaceKey: plan.InterfaceKey, DeliveryEpoch: plan.DeliveryEpoch,
						ExternalAccountRef: plan.ExternalAccountRef, ConversationRef: plan.ConversationRef, ConversationScope: plan.ConversationScope})
				if err != nil {
					t.Fatal(err)
				}
				altered, err := render.FreezeNotice(render.Notice{ID: noticeID, Type: runtimetools.NotifyHumanMailboxItemType,
					Summary: "Altered delivery", Priority: "normal", Context: noticeContext},
					frozen.Audience)
				if err != nil {
					t.Fatal(err)
				}
				if err := runTx(func(txctx context.Context, tx *sql.Tx) error {
					_, _, err := channeldelivery.PersistRenderTx(txctx, tx, deliveryID, altered, postgres)
					return err
				}); err == nil {
					t.Fatal("altered notice render was admitted")
				}
				var renderID string
				var actions []render.Action
				err = runTx(func(txctx context.Context, tx *sql.Tx) error {
					var persistErr error
					renderID, _, persistErr = channeldelivery.PersistRenderTx(txctx, tx, deliveryID, frozen, postgres)
					if persistErr != nil {
						return persistErr
					}
					actions, persistErr = channeldelivery.EnsureRenderActionsTx(txctx, tx, renderID, frozen, postgres)
					return persistErr
				})
				if err != nil || len(actions) != 1 || actions[0].Kind != "view_full" {
					t.Fatalf("notice actions = %#v, err=%v", actions, err)
				}
				effectOperationID, err := runtimeeffects.ChannelDeliveryOperationID(deliveryID, renderID)
				if err != nil {
					t.Fatal(err)
				}
				authority := runtimeeffects.Authority{
					Kind: runtimeeffects.AuthorityChannelDelivery, ID: effectOperationID,
					ExecutionOwner: "channel-delivery-test", LeaseExpiresAt: time.Now().Add(5 * time.Minute),
					FenceGeneration: activation.Coordinate.ContextPublicationGeneration, ExecutionMode: runtimeeffects.ExecutionModeLive,
					ChannelDelivery: runtimeeffects.ChannelDeliveryAuthority{
						EffectOperationID: effectOperationID, DeliveryID: deliveryID, RenderID: renderID, RenderHash: frozen.Hash,
						PrincipalID: principal.ID, InterfaceKey: binding.Interface.Key(), DeliveryEpoch: plan.DeliveryEpoch,
						BindingRevision: binding.Revision, ExternalAccountRef: plan.ExternalAccountRef, ConversationRef: plan.ConversationRef,
						ActivationID: activation.ActivationID, ActivationRevision: activation.Revision,
						BundleHash: activation.Coordinate.BundleHash, BundleIdentity: activation.Coordinate.BundleIdentity,
						PackInventoryGeneration:      activation.Coordinate.PackInventoryGeneration,
						RuntimeInstanceID:            activation.Coordinate.RuntimeInstanceID,
						ContextPublicationGeneration: activation.Coordinate.ContextPublicationGeneration,
						PlanGeneration:               activation.Coordinate.PlanGeneration, TargetGeneration: activation.Coordinate.TargetGeneration,
					},
				}
				if !authority.Valid() {
					t.Fatal("delivery authority is malformed")
				}
				if current(authority) {
					t.Fatal("activation before successful onboarding admitted delivery")
				}
				effectCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(
					testAuthorActivityContextForBundle(activation.Coordinate.BundleHash), authority),
					runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
				if _, err := runtimeeffects.BeginChannelDelivery(effectCtx, []byte("message"), nil); err == nil {
					t.Fatal("effect authorization before successful onboarding was admitted")
				}
				onboarding, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
					OperationID: onboarding.OperationID, ExpectedRevision: onboarding.Revision,
					Phase: channelonboarding.PhaseSucceeded, Now: now.Add(20 * time.Second),
				})
				if err != nil {
					t.Fatal(err)
				}
				selectedActivationID, found, err := selected.CurrentChannelDeliveryActivationID(ctx)
				if err != nil || !found || selectedActivationID != activation.ActivationID {
					t.Fatalf("selected activation = %s, found=%t err=%v", selectedActivationID, found, err)
				}
				if !current(authority) {
					t.Fatal("succeeded channel activation rejected exact delivery")
				}
				foreignRender := authority
				foreignRender.ChannelDelivery.RenderHash = "sha256:foreign"
				if current(foreignRender) {
					t.Fatal("foreign render admitted")
				}
				handle, err := runtimeeffects.BeginChannelDelivery(effectCtx, []byte("message"), nil)
				if err != nil {
					t.Fatalf("authorize exact delivery: %v", err)
				}
				if err := handle.MarkLaunched(effectCtx); err != nil {
					t.Fatalf("launch exact delivery: %v", err)
				}
				retire := func() {
					t.Helper()
					_, _, err := selected.UnbindOperatorChannel(ctx, operatorchannel.UnbindRequest{
						OperationID: uuid.NewString(), PrincipalID: principal.ID, Interface: binding.Interface,
						ExpectedRevision: binding.Revision, RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(),
						RequestedAt: now.Add(21 * time.Second),
					})
					if err != nil {
						t.Fatal(err)
					}
					if current(authority) {
						t.Fatal("retired default admitted predecessor delivery")
					}
				}
				if mode == "late" {
					retire()
				}
				if err := handle.MarkResponseObserved(effectCtx, map[string]any{"provider": "accepted"}); err != nil {
					t.Fatalf("observe late exact delivery: %v", err)
				}
				if err := handle.Succeed(effectCtx, map[string]any{"projected_output": map[string]any{"delivery_reference": map[string]any{"id": 91}}}); err != nil {
					t.Fatalf("settle late exact delivery: %v", err)
				}
				query = `SELECT state, provider_reference FROM channel_delivery_receipts WHERE effect_operation_id=?`
				if postgres {
					query = `SELECT state, provider_reference FROM channel_delivery_receipts WHERE effect_operation_id=$1::uuid`
				}
				var receiptState, receiptJSON string
				if err := db.QueryRowContext(ctx, query, effectOperationID).Scan(&receiptState, &receiptJSON); err != nil {
					t.Fatal(err)
				}
				if receiptState != "sent" || !strings.Contains(receiptJSON, "delivery_reference") {
					t.Fatalf("late receipt = %s %s", receiptState, receiptJSON)
				}
				fact := operatorchannel.ActionFact{
					Interface: binding.Interface, ExternalAccountRef: plan.ExternalAccountRef,
					ConversationRef: plan.ConversationRef, ConversationScope: plan.ConversationScope,
					MessageReference: `{"id":91}`, InteractionRef: "interaction-1", Token: actions[0].Token,
				}
				if mode == "current" {
					resolved, found, err := selected.ResolveChannelActionFact(ctx, fact)
					if err != nil || !found || !resolved.CurrentRender || resolved.Action.Kind != "view_full" ||
						resolved.DeliveryID != deliveryID || resolved.RenderHash != frozen.Hash ||
						resolved.ReceiptOperationID != effectOperationID {
						t.Fatalf("current delivery action = %#v, found=%t err=%v", resolved, found, err)
					}
					foreign := fact
					foreign.MessageReference = `{"id":92}`
					if _, found, err := selected.ResolveChannelActionFact(ctx, foreign); err != nil || found {
						t.Fatalf("foreign message action resolved: found=%t err=%v", found, err)
					}
					retire()
				}
				if _, found, err := selected.ResolveChannelActionFact(ctx, fact); err != nil || found {
					t.Fatalf("retired delivery action resolved: found=%t err=%v", found, err)
				}
				settledPlan, found, err := channeldelivery.LoadPlan(ctx, db, deliveryID, postgres)
				if err != nil || !found || settledPlan.State != "sent" ||
					settledPlan.CurrentRenderID != renderID || settledPlan.CurrentReceiptID != effectOperationID {
					t.Fatalf("settled delivery plan = %#v, found=%t err=%v", settledPlan, found, err)
				}
				if _, err := runtimeeffects.BeginChannelDelivery(effectCtx, []byte("message"), nil); err == nil {
					t.Fatal("retired settled delivery was authorized for redispatch")
				}
			})
		}
	}
}
