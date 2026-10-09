package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/division-sh/swarm/internal/packs"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type selectedChannelDeliveryTestStore interface {
	channelOnboardingEffectSelectedStore
	SubscribeChannelReconciliation(context.Context) (*render.ReconcileSubscription, error)
	InsertMailboxItem(context.Context, runtimetools.MailboxItem) (string, error)
	CurrentChannelDeliveryActivationID(context.Context) (string, bool, error)
	ResolveChannelActionFact(context.Context, operatorchannel.ActionFact) (render.ResolvedAction, bool, error)
	ResolveCurrentChannelText(context.Context, operatorchannel.InboundText) (render.ResolvedText, bool, error)
	ListCurrentChannelInputDrafts(context.Context, operatorchannel.InboundText, time.Time, string, int) ([]render.InputDraftCandidate, string, error)
	ResolveCurrentInboxEntry(context.Context, operatorchannel.InboundText) (render.ResolvedInboxEntry, bool, error)
	PlanOpenChannelCard(context.Context, string) (bool, error)
	ListCurrentChannelDeliveryPlans(context.Context, string, int) ([]render.Candidate, error)
	GetCurrentChannelSentReceipt(context.Context, string, string) (render.SentReceipt, bool, error)
	FreezeAndPersistChannelRender(context.Context, string, packs.PresentationBounds) (render.PreparedRender, error)
}

func TestChannelDeliveryEffectCurrentnessSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range append([]string{"current", "late", "shared_current", "shared_late", "edit_uncertain", "response_matrix", "reply_matrix", "native_reset_healthy", "native_reset_uncertain", "eligibility_before_authorize", "eligibility_before_launch", "eligibility_after_launch", "eligibility_after_observation", "eligibility_edit", "eligibility_uncertain", "eligibility_recover_authorized", "eligibility_recover_launched", "eligibility_recover_observed"}, channelRecoveryProofModes()...) {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				late := strings.HasSuffix(mode, "late") || mode == "native_reset_uncertain"
				conversationScope := operatorchannel.ConversationScopeDirect
				if strings.HasPrefix(mode, "shared") {
					conversationScope = operatorchannel.ConversationScopeShared
				}
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
					ProviderAuthority: operatorChannelProviderAuthority(), RequestedAt: now,
					ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
				})
				if err != nil {
					t.Fatal(err)
				}
				var claimed operatorchannel.ClaimSettlement
				err = runTx(func(txctx context.Context, tx *sql.Tx) error {
					claim := operatorChannelContractClaim(bindingOperation, conversationScope,
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
					Approve: true, ProviderAuthorityCurrent: true, ConfirmedAt: now.Add(2 * time.Second),
				})
				if err != nil {
					t.Fatal(err)
				}
				if selectedActivationID, found, err := selected.CurrentChannelDeliveryActivationID(ctx); err != nil || found || selectedActivationID != "" {
					t.Fatalf("pre-success activation = %s, found=%t err=%v", selectedActivationID, found, err)
				}
				noticeContext := []byte(`{"message":"` + strings.Repeat("a", 5000) + `"}`)
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
				bounds := packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64}
				frozen, err = render.WithPresentation(frozen, bounds, 0)
				if err != nil {
					t.Fatal(err)
				}
				err = runTx(func(txctx context.Context, tx *sql.Tx) error {
					if err := channeldelivery.SetPresentationBoundsTx(txctx, tx, deliveryID, bounds, postgres); err != nil {
						return err
					}
					var persistErr error
					renderID, _, persistErr = channeldelivery.PersistRenderTx(txctx, tx, deliveryID, frozen, postgres)
					if persistErr != nil {
						return persistErr
					}
					actions, persistErr = channeldelivery.EnsureRenderActionsTx(txctx, tx, renderID, frozen, postgres)
					return persistErr
				})
				if err != nil || len(actions) != 2 || actions[1].Kind != "acknowledge_notice" || actions[0].Kind != "view_full" {
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
				textFact := operatorchannel.InboundText{
					TextFact: operatorchannel.TextFact{Interface: binding.Interface, ExternalAccountRef: "account",
						ConversationRef: activation.ConversationRef, ConversationScope: conversationScope,
						Text: "open inbox", MessageReference: `{"id":12}`},
					Provider: activation.Provider, ProviderEventID: "text-12", PublicationID: uuid.NewString(),
					ProviderAuthorization: "verified-text-auth",
				}
				// Ordinary source discovery is not executable activation authority.
				// The notice remains retained while activation/effect checks refuse it.
				if plans, err := selected.ListCurrentChannelDeliveryPlans(ctx, "", 200); err != nil || len(plans) != 1 || plans[0].DeliveryID != deliveryID {
					t.Fatalf("pre-completion retained plans=%#v err=%v", plans, err)
				}
				if _, found, err := selected.ResolveCurrentChannelText(ctx, textFact); err != nil || found {
					t.Fatalf("pre-completion text current=%t err=%v", found, err)
				}
				completionHints := observeChannelHints(t, selected)
				onboarding, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
					OperationID: onboarding.OperationID, ExpectedRevision: onboarding.Revision,
					Phase: onboarding.Phase, Now: now.Add(19 * time.Second),
				})
				if err != nil {
					t.Fatal(err)
				}
				completionHints.expect(t, 0)
				completion := channelonboarding.AdvanceRequest{
					OperationID: onboarding.OperationID, ExpectedRevision: onboarding.Revision,
					Phase: channelonboarding.PhaseSucceeded, Now: now.Add(20 * time.Second),
				}
				staleCompletion := completion
				staleCompletion.ExpectedRevision--
				if _, err := selected.AdvanceChannelOnboarding(ctx, staleCompletion); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
					t.Fatalf("stale completion error=%v", err)
				}
				completionHints.expect(t, 0)
				onboarding, err = selected.AdvanceChannelOnboarding(ctx, completion)
				if err != nil {
					t.Fatal(err)
				}
				selectedActivationID, found, err := selected.CurrentChannelDeliveryActivationID(ctx)
				if err != nil || !found || selectedActivationID != activation.ActivationID {
					t.Fatalf("selected activation = %s, found=%t err=%v", selectedActivationID, found, err)
				}
				completionHints.expect(t, render.ReconcileOrdinary|render.ReconcileNative)
				completion.ExpectedRevision = onboarding.Revision
				if _, err := selected.AdvanceChannelOnboarding(ctx, completion); !errors.Is(err, channelonboarding.ErrConflict) {
					t.Fatalf("terminal completion error=%v", err)
				}
				completionHints.expect(t, 0)
				completionHints.subscription.Close()
				plans, err := selected.ListCurrentChannelDeliveryPlans(ctx, "", 200)
				if err != nil || len(plans) != 1 || plans[0].DeliveryID != deliveryID {
					t.Fatalf("post-completion current plans=%#v err=%v", plans, err)
				}
				resolvedText, foundText, err := selected.ResolveCurrentChannelText(ctx, textFact)
				if err != nil || !foundText || resolvedText.PrincipalID != principal.ID ||
					resolvedText.InterfaceKey != binding.Interface.Key() || resolvedText.BindingRevision != binding.Revision {
					t.Fatalf("current selected channel text = %#v, found=%t err=%v", resolvedText, foundText, err)
				}
				foreignText := textFact
				foreignText.ExternalAccountRef = "another-account"
				if _, found, err := selected.ResolveCurrentChannelText(ctx, foreignText); err != nil || found {
					t.Fatalf("foreign selected channel text = found:%t err:%v", found, err)
				}
				native := selected.(channelnative.Store)
				entryContractHash, err := channelnative.EntryContractHash(activation.Coordinate.PlanGeneration)
				if err != nil {
					t.Fatal(err)
				}
				admission := channelnative.Admission{
					Provider: activation.Provider, ResourceSlotID: activation.Provider + ":bot_webhook:42",
					ConversationReference: activation.ConversationRef, PrincipalID: principal.ID,
					InterfaceKey: activation.Interface.Key(), BindingRevision: activation.BindingRevision,
					ActivationID: activation.ActivationID, ActivationRevision: activation.Revision,
					ContextPublicationGeneration: int64(activation.Coordinate.ContextPublicationGeneration),
					PackID:                       activation.Interface.ChannelPackID, PackVersion: activation.Interface.ChannelPackVersion,
					PackManifestHash: activation.Interface.ChannelManifestHash,
					PlanGeneration:   activation.Coordinate.PlanGeneration, EntryContractHash: entryContractHash,
				}
				hints := observeChannelHints(t, selected)
				setting, err := native.AttachNativeInboxSetting(ctx, admission)
				expectedInstallID, idErr := channelnative.InstallOperationID(setting.SettingID, setting.Generation)
				if err != nil || idErr != nil || setting.State != "planned" || setting.Generation != 1 ||
					setting.CurrentConsumerCount != 1 || setting.InstallOperationID != expectedInstallID {
					t.Fatalf("attach physical native setting = %#v, %v", setting, err)
				}
				hints.expect(t, render.ReconcileOrdinary|render.ReconcileNative)
				wantScope, wantMember := "chat", ""
				if conversationScope == operatorchannel.ConversationScopeShared {
					wantScope, wantMember = "chat_member", "account"
				}
				if setting.ScopeKind != wantScope || setting.MemberReference != wantMember || setting.EntryCommand == "" {
					t.Fatalf("native setting physical footprint = %#v, want %s/%s", setting, wantScope, wantMember)
				}
				replayedSetting, err := native.AttachNativeInboxSetting(ctx, admission)
				if err != nil || replayedSetting.SettingID != setting.SettingID || replayedSetting.Generation != 1 ||
					replayedSetting.CurrentConsumerCount != 1 || replayedSetting.InstallOperationID != expectedInstallID {
					t.Fatalf("native setting replay = %#v, %v", replayedSetting, err)
				}
				hints.expect(t, 0)
				for _, stale := range []struct {
					name, sqliteUpdate, postgresUpdate string
				}{
					{"onboarding", `UPDATE channel_onboarding_operations SET phase='failed' WHERE operation_id=?`,
						`UPDATE channel_onboarding_operations SET phase='failed' WHERE operation_id=$1::uuid`},
					{"conversation", `UPDATE operator_channel_bindings SET conversation_reference='foreign' WHERE interface_key=?`,
						`UPDATE operator_channel_bindings SET conversation_reference='foreign' WHERE interface_key=$1`},
				} {
					t.Run("native-stale-"+stale.name, func(t *testing.T) {
						rollback := errors.New("rollback native staleness probe")
						probeErr := runTx(func(txctx context.Context, tx *sql.Tx) error {
							query, arg := stale.sqliteUpdate, any(binding.Interface.Key())
							if stale.name == "onboarding" {
								arg = onboarding.OperationID
							}
							if postgres {
								query = stale.postgresUpdate
							}
							if _, err := tx.ExecContext(txctx, query, arg); err != nil {
								return err
							}
							if _, err := channeldelivery.RetireStaleNativeInboxConsumersTx(txctx, tx, postgres); err != nil {
								return err
							}
							query = `SELECT state FROM channel_native_setting_consumers WHERE activation_id=?`
							if postgres {
								query = `SELECT state FROM channel_native_setting_consumers WHERE activation_id=$1::uuid`
							}
							var state string
							if err := tx.QueryRowContext(txctx, query, activation.ActivationID).Scan(&state); err != nil {
								return err
							}
							if state != "retired" {
								return fmt.Errorf("stale native setting consumer remained %q", state)
							}
							return rollback
						})
						if !errors.Is(probeErr, rollback) {
							t.Fatalf("native staleness probe = %v", probeErr)
						}
					})
				}
				foreignAdmission := admission
				foreignAdmission.BindingRevision++
				if _, err := native.AttachNativeInboxSetting(ctx, foreignAdmission); err == nil {
					t.Fatal("native setting accepted contradictory binding revision")
				}
				incompatible := admission
				incompatible.EntryContractHash = "different-native-contract"
				if _, err := native.AttachNativeInboxSetting(ctx, incompatible); err == nil {
					t.Fatal("native setting accepted incompatible contract with a current consumer")
				}
				hints.expect(t, 0)
				if err := native.RetireStaleNativeInboxConsumers(ctx); err != nil {
					t.Fatal(err)
				}
				hints.expect(t, 0)
				hints.subscription.Close()
				nativeAuthority := runtimeeffects.Authority{
					Kind: runtimeeffects.AuthorityChannelNativeSetting, ID: setting.InstallOperationID,
					ExecutionOwner: "channel-native-test", LeaseExpiresAt: time.Now().Add(5 * time.Minute),
					FenceGeneration: uint64(setting.Generation), ExecutionMode: runtimeeffects.ExecutionModeLive,
					ChannelNativeSetting: runtimeeffects.ChannelNativeSettingAuthority{
						EffectOperationID: setting.InstallOperationID, SettingID: setting.SettingID,
						SettingGeneration: setting.Generation, Provider: admission.Provider,
						ResourceSlotID: admission.ResourceSlotID, ConversationRef: admission.ConversationReference,
						ScopeKind: setting.ScopeKind, MemberReference: setting.MemberReference,
						PrincipalID: admission.PrincipalID, EntryContractHash: admission.EntryContractHash,
						EntryCommand: setting.EntryCommand,
						PackID:       admission.PackID, PackVersion: admission.PackVersion, PackManifestHash: admission.PackManifestHash,
						ActivationID: activation.ActivationID, ActivationRevision: activation.Revision,
						BindingRevision: activation.BindingRevision, BundleHash: activation.Coordinate.BundleHash,
						BundleIdentity:               activation.Coordinate.BundleIdentity,
						PackInventoryGeneration:      activation.Coordinate.PackInventoryGeneration,
						RuntimeInstanceID:            activation.Coordinate.RuntimeInstanceID,
						ContextPublicationGeneration: activation.Coordinate.ContextPublicationGeneration,
						PlanGeneration:               activation.Coordinate.PlanGeneration,
						TargetGeneration:             activation.Coordinate.TargetGeneration,
					},
				}
				if !nativeAuthority.Valid() || !current(nativeAuthority) {
					t.Fatal("exact physical native setting authority is not current")
				}
				if strings.HasPrefix(mode, "recovery_") {
					proveChannelRecoveryProjection(t, mode, selected, db, runTx, authority, nativeAuthority, textFact, postgres)
					return
				}
				foreignNative := nativeAuthority
				foreignNative.ChannelNativeSetting.ResourceSlotID += "-foreign"
				if current(foreignNative) {
					t.Fatal("foreign native setting resource was admitted")
				}
				foreignNative = nativeAuthority
				foreignNative.ChannelNativeSetting.EntryContractHash = "foreign"
				if current(foreignNative) {
					t.Fatal("foreign native setting contract was admitted")
				}
				nativeCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(
					testAuthorActivityContextForBundle(activation.Coordinate.BundleHash), nativeAuthority),
					runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
				nativeHandle, err := runtimeeffects.BeginChannelNativeSetting(nativeCtx, []byte("install inbox"), nil)
				if err != nil {
					t.Fatalf("authorize exact physical native setting: %v", err)
				}
				entryText := textFact
				entryText.PublicationID = uuid.NewString()
				entryText.Text = "/" + setting.EntryCommand
				entryText.EntryReference = setting.EntryCommand
				if conversationScope == operatorchannel.ConversationScopeShared {
					entryText.EntryAddress = "SwarmTestBot"
				}
				if err := runTx(func(txctx context.Context, tx *sql.Tx) error {
					return channeldelivery.InsertTextIntentTx(txctx, tx, entryText, now, postgres)
				}); err != nil {
					t.Fatal(err)
				}
				if entry, found, err := selected.ResolveCurrentInboxEntry(ctx, entryText); err != nil || !found {
					t.Fatalf("known uninstalled generation was not discovered: found=%t err=%v", found, err)
				} else if _, err := selected.(render.Store).PlanInboxResponse(ctx, entryText, entry, "must not deliver"); err == nil {
					t.Fatal("unqualified discovery granted response authority")
				}
				forgedEntry := entryText
				forgedEntry.PublicationID = uuid.NewString()
				if _, _, err := selected.ResolveCurrentInboxEntry(ctx, forgedEntry); err == nil {
					t.Fatal("native entry accepted a fact absent from verified inbound intent")
				}
				if late {
					if err := nativeHandle.MarkLaunched(nativeCtx); err != nil {
						t.Fatalf("launch physical native setting: %v", err)
					}
				} else {
					if err := nativeHandle.MarkLaunched(nativeCtx); err != nil {
						t.Fatalf("launch physical native setting: %v", err)
					}
					if err := nativeHandle.MarkResponseObserved(nativeCtx, map[string]any{"provider": "accepted"}); err != nil {
						t.Fatalf("observe native setting: %v", err)
					}
					desired, err := channelnative.DesiredCommands(setting.SettingID, setting.Generation)
					if err != nil {
						t.Fatal(err)
					}
					if err := nativeHandle.Succeed(nativeCtx, map[string]any{"readback_hash": runtimeeffects.Fingerprint(desired)}); err != nil {
						t.Fatalf("settle native setting: %v", err)
					}
					entry, found, err := selected.ResolveCurrentInboxEntry(ctx, entryText)
					if err != nil || !found || entry.PrincipalID != principal.ID || entry.SettingID != setting.SettingID ||
						entry.ActivationID != activation.ActivationID || entry.EntryReference != setting.EntryCommand {
						t.Fatalf("installed native entry = %#v, found=%t err=%v", entry, found, err)
					}
					proveNativeQualificationFences(t, selected, native, admission, setting, onboarding.OperationID, now)
				}
				if strings.HasPrefix(mode, "native_reset_") {
					proveNativeResetTransaction(t, selected, runTx, admission, setting, late, postgres, now)
					return
				}
				if !current(authority) {
					t.Fatal("succeeded channel activation rejected exact delivery")
				}
				if strings.HasPrefix(mode, "eligibility_") {
					proveChannelFirstSendEligibility(t, mode, selected, runTx, current, authority, postgres)
					return
				}
				if mode == "response_matrix" {
					proveChannelResponseReceiptMatrix(t, selected, runTx, current, authority, textFact, postgres)
					return
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
					if current(nativeAuthority) {
						t.Fatal("retired binding admitted predecessor native setting")
					}
					if _, found, err := selected.ResolveCurrentChannelText(ctx, textFact); err != nil || found {
						t.Fatalf("retired binding admitted predecessor text: found=%t err=%v", found, err)
					}
					if _, found, err := selected.ResolveCurrentInboxEntry(ctx, entryText); err != nil || found {
						t.Fatalf("retired binding admitted predecessor native entry: found=%t err=%v", found, err)
					}
					if err := native.RetireStaleNativeInboxConsumers(ctx); err != nil {
						t.Fatal(err)
					}
					var nativeState, consumerState string
					query := `SELECT state FROM channel_native_settings WHERE setting_id=?`
					consumerQuery := `SELECT state FROM channel_native_setting_consumers WHERE activation_id=?`
					if postgres {
						query = `SELECT state FROM channel_native_settings WHERE setting_id=$1::uuid`
						consumerQuery = `SELECT state FROM channel_native_setting_consumers WHERE activation_id=$1::uuid`
					}
					if err := db.QueryRowContext(ctx, query, setting.SettingID).Scan(&nativeState); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRowContext(ctx, consumerQuery, activation.ActivationID).Scan(&consumerState); err != nil {
						t.Fatal(err)
					}
					wantNativeState := "retired"
					if late {
						wantNativeState = "uncertain"
					}
					if nativeState != wantNativeState || consumerState != "retired" {
						t.Fatalf("native setting/consumer after local retirement = %s/%s", nativeState, consumerState)
					}
				}
				if late {
					retire()
					if err := nativeHandle.MarkResponseObserved(nativeCtx, map[string]any{"provider": "accepted"}); err != nil {
						t.Fatalf("observe late native setting: %v", err)
					}
					if err := nativeHandle.Succeed(nativeCtx, map[string]any{"readback_hash": "foreign"}); err == nil {
						t.Fatal("late native setting accepted nonmatching readback")
					}
					desired, err := channelnative.DesiredCommands(setting.SettingID, setting.Generation)
					if err != nil {
						t.Fatal(err)
					}
					if err := nativeHandle.Succeed(nativeCtx, map[string]any{"readback_hash": runtimeeffects.Fingerprint(desired)}); err != nil {
						t.Fatalf("settle exact late native setting: %v", err)
					}
					query := `SELECT state FROM channel_native_settings WHERE setting_id=?`
					if postgres {
						query = `SELECT state FROM channel_native_settings WHERE setting_id=$1::uuid`
					}
					var state string
					if err := db.QueryRowContext(ctx, query, setting.SettingID).Scan(&state); err != nil || state != "retired" {
						t.Fatalf("late native setting after local retirement = %q, %v", state, err)
					}
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
				if !late {
					previous, found, err := selected.GetCurrentChannelSentReceipt(ctx, deliveryID, effectOperationID)
					if err != nil || !found || previous.RenderID != renderID {
						t.Fatalf("current sent receipt = %#v, found=%t err=%v", previous, found, err)
					}
					if mode == "reply_matrix" {
						proveChannelReplyTransfer(t, selected, runTx, current, authority, textFact, actions[0], postgres)
						return
					}
					if err := runTx(func(txctx context.Context, tx *sql.Tx) error {
						query := `UPDATE mailbox SET summary=? WHERE item_id=?`
						if postgres {
							query = `UPDATE mailbox SET summary=$1 WHERE item_id=$2::uuid`
						}
						_, err := tx.ExecContext(txctx, query, "Delivery authority changed", noticeID)
						return err
					}); err != nil {
						t.Fatal(err)
					}
					updated, err := selected.FreezeAndPersistChannelRender(ctx, deliveryID, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64})
					if err != nil || updated.RenderID == renderID {
						t.Fatalf("freeze changed source = %#v, err=%v", updated, err)
					}
					editID, err := runtimeeffects.ChannelDeliveryOperationID(deliveryID, updated.RenderID)
					if err != nil {
						t.Fatal(err)
					}
					editAuthority := authority
					editAuthority.ID = editID
					editAuthority.ChannelDelivery.EffectOperationID = editID
					editAuthority.ChannelDelivery.RenderID = updated.RenderID
					editAuthority.ChannelDelivery.RenderHash = updated.Frozen.Hash
					editAuthority.ChannelDelivery.PreviousReceiptOperationID = effectOperationID
					if !current(editAuthority) {
						t.Fatal("exact edit predecessor was not admitted")
					}
					wrongPredecessor := editAuthority
					wrongPredecessor.ChannelDelivery.PreviousReceiptOperationID = uuid.NewString()
					if current(wrongPredecessor) {
						t.Fatal("foreign edit predecessor was admitted")
					}
					editCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(
						testAuthorActivityContextForBundle(activation.Coordinate.BundleHash), editAuthority),
						runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
					editHandle, err := runtimeeffects.BeginChannelDelivery(editCtx, []byte("edit"), nil)
					if err != nil {
						t.Fatalf("authorize exact edit: %v", err)
					}
					if err := editHandle.MarkLaunched(editCtx); err != nil {
						t.Fatal(err)
					}
					if err := editHandle.MarkResponseObserved(editCtx, map[string]any{"provider": "edited"}); err != nil {
						t.Fatal(err)
					}
					if err := editHandle.Succeed(editCtx, map[string]any{"projected_output": map[string]any{
						"delivery_receipt": map[string]any{"id": 91},
					}}); err != nil {
						t.Fatalf("settle exact edit: %v", err)
					}
					if _, found, err := selected.GetCurrentChannelSentReceipt(ctx, deliveryID, effectOperationID); err != nil || found {
						t.Fatalf("predecessor remained current: found=%t err=%v", found, err)
					}
					editedReceipt, found, err := selected.GetCurrentChannelSentReceipt(ctx, deliveryID, editID)
					if err != nil || !found || editedReceipt.RenderID != updated.RenderID {
						t.Fatalf("edited receipt = %#v, found=%t err=%v", editedReceipt, found, err)
					}
					if ref, valid, err := operatorchannel.OpaqueReference(editedReceipt.DeliveryReference); err != nil || !valid || ref != `{"id":91}` {
						t.Fatalf("edited message reference = %q, valid=%t err=%v", ref, valid, err)
					}
					query := `SELECT provider_reference FROM channel_delivery_receipts WHERE effect_operation_id=?`
					if postgres {
						query = `SELECT provider_reference FROM channel_delivery_receipts WHERE effect_operation_id=$1::uuid`
					}
					var editedJSON string
					if err := db.QueryRowContext(ctx, query, editID).Scan(&editedJSON); err != nil ||
						!strings.Contains(editedJSON, "delivery_reference") || !strings.Contains(editedJSON, "delivery_receipt") {
						t.Fatalf("edited receipt projection = %q, err=%v", editedJSON, err)
					}
					renderID, effectOperationID, frozen, actions = updated.RenderID, editID, updated.Frozen, updated.Actions
					authority, effectCtx = editAuthority, editCtx
					if mode == "edit_uncertain" {
						if err := runTx(func(txctx context.Context, tx *sql.Tx) error {
							query := `UPDATE mailbox SET summary=? WHERE item_id=?`
							if postgres {
								query = `UPDATE mailbox SET summary=$1 WHERE item_id=$2::uuid`
							}
							_, err := tx.ExecContext(txctx, query, "Delivery authority changed again", noticeID)
							return err
						}); err != nil {
							t.Fatal(err)
						}
						latest, err := selected.FreezeAndPersistChannelRender(ctx, deliveryID, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64})
						if err != nil || latest.RenderID == renderID {
							t.Fatalf("freeze second edit = %#v, %v", latest, err)
						}
						uncertainID, err := runtimeeffects.ChannelDeliveryOperationID(deliveryID, latest.RenderID)
						if err != nil {
							t.Fatal(err)
						}
						uncertainAuthority := editAuthority
						uncertainAuthority.ID = uncertainID
						uncertainAuthority.ChannelDelivery.EffectOperationID = uncertainID
						uncertainAuthority.ChannelDelivery.RenderID = latest.RenderID
						uncertainAuthority.ChannelDelivery.RenderHash = latest.Frozen.Hash
						uncertainAuthority.ChannelDelivery.PreviousReceiptOperationID = editID
						uncertainCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(
							testAuthorActivityContextForBundle(activation.Coordinate.BundleHash), uncertainAuthority),
							runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
						uncertainHandle, err := runtimeeffects.BeginChannelDelivery(uncertainCtx, []byte("uncertain edit"), nil)
						if err != nil {
							t.Fatal(err)
						}
						if err := uncertainHandle.MarkLaunched(uncertainCtx); err != nil {
							t.Fatal(err)
						}
						failureErr := failures.New(failures.ClassOutcomeUncertain, "test_channel_edit_uncertain", "test", "settle", nil)
						failure, _ := failures.EnvelopeFromError(failureErr)
						if err := uncertainHandle.Settle(uncertainCtx, runtimeeffects.StateOutcomeUncertain, &failure, nil); err != nil {
							t.Fatal(err)
						}
						uncertainPlan, found, err := channeldelivery.LoadPlan(ctx, db, deliveryID, postgres)
						if err != nil || !found || uncertainPlan.State != "uncertain" || uncertainPlan.CurrentReceiptID != editID ||
							uncertainPlan.CurrentRenderID != latest.RenderID {
							t.Fatalf("uncertain edit retained plan = %#v, found=%t err=%v", uncertainPlan, found, err)
						}
						if current(uncertainAuthority) {
							t.Fatal("uncertain edit retained automatic launch authority")
						}
						if _, err := runtimeeffects.BeginChannelDelivery(uncertainCtx, []byte("uncertain edit"), nil); err == nil {
							t.Fatal("uncertain edit was automatically redispatched")
						}
						return
					}
				}
				fact := operatorchannel.ActionFact{Kind: operatorchannel.ActionSourceCallback,
					Interface: binding.Interface, ExternalAccountRef: plan.ExternalAccountRef,
					ConversationRef: plan.ConversationRef, ConversationScope: plan.ConversationScope,
					MessageReference: `{"id":91}`, InteractionRef: "interaction-1", Token: actions[0].Token,
				}
				if !late {
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
					if err := runTx(func(txctx context.Context, tx *sql.Tx) error {
						return channeldelivery.RequireCardActionTx(txctx, tx, fact, render.CardActionDemand{
							CardID: uuid.NewString(), PrincipalID: principal.ID, Method: "mailbox.decide",
							Verdict: "accept", ReceiptOperationID: effectOperationID, RenderHash: frozen.Hash,
						}, postgres, true)
					}); err == nil || !strings.Contains(err.Error(), "not current card authority") {
						t.Fatalf("notice callback card admission = %v, want semantic rejection", err)
					}
					retire()
				}
				if _, found, err := selected.ResolveChannelActionFact(ctx, fact); err != nil || found {
					t.Fatalf("retired delivery action resolved: found=%t err=%v", found, err)
				}
				if _, found, err := selected.GetCurrentChannelSentReceipt(ctx, deliveryID, effectOperationID); err != nil || found {
					t.Fatalf("retired delivery receipt retained edit authority: found=%t err=%v", found, err)
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

func proveNativeQualificationFences(t *testing.T, selected selectedChannelDeliveryTestStore, native channelnative.Store,
	admission channelnative.Admission, setting channelnative.Setting, operationID string, now time.Time) {
	t.Helper()
	ctx := context.Background()
	hints := observeChannelHints(t, selected)
	localeOwner := selected.(channelonboarding.Store)
	declared, err := localeOwner.SetChannelClientLocale(ctx, channelonboarding.SetClientLocaleRequest{
		OperationID: operationID, PrincipalID: admission.PrincipalID, ExpectedRevision: setting.ClientLocaleRevision,
		Language: "en", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	hints.expect(t, render.ReconcileOrdinary|render.ReconcileNative)
	if _, err := localeOwner.SetChannelClientLocale(ctx, channelonboarding.SetClientLocaleRequest{
		OperationID: operationID, PrincipalID: admission.PrincipalID, ExpectedRevision: setting.ClientLocaleRevision,
		Language: "en", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	hints.expect(t, 0)
	request := channelnative.QualificationRequest{
		SettingID: setting.SettingID, SettingGeneration: setting.Generation, ActivationID: admission.ActivationID,
		ActivationRevision: admission.ActivationRevision, ContextGeneration: admission.ContextPublicationGeneration,
		BindingRevision: admission.BindingRevision, EntryContractHash: admission.EntryContractHash,
		ClientLanguage: declared.ClientLanguage, LocaleRevision: declared.ClientLocaleRevision,
		State: channelnative.QualificationQualified, ReadbackHash: operatorchannel.Hash("qualification-owner-test"), ObservedAt: now,
	}
	for _, test := range []struct {
		name   string
		mutate func(*channelnative.QualificationRequest)
	}{
		{"setting", func(r *channelnative.QualificationRequest) { r.SettingID = uuid.NewString() }},
		{"setting generation", func(r *channelnative.QualificationRequest) { r.SettingGeneration++ }},
		{"activation", func(r *channelnative.QualificationRequest) { r.ActivationID = uuid.NewString() }},
		{"activation revision", func(r *channelnative.QualificationRequest) { r.ActivationRevision++ }},
		{"publication", func(r *channelnative.QualificationRequest) { r.ContextGeneration++ }},
		{"binding", func(r *channelnative.QualificationRequest) { r.BindingRevision++ }},
		{"profile", func(r *channelnative.QualificationRequest) { r.EntryContractHash = "foreign" }},
		{"language", func(r *channelnative.QualificationRequest) { r.ClientLanguage = "fr" }},
		{"locale revision", func(r *channelnative.QualificationRequest) { r.LocaleRevision++ }},
	} {
		t.Run("qualification fence "+test.name, func(t *testing.T) {
			foreign := request
			test.mutate(&foreign)
			if err := native.RecordNativeInboxQualification(ctx, foreign); err == nil {
				t.Fatal("contradictory qualification acquired current authority")
			}
		})
	}
	hints.expect(t, 0)
	if err := native.RecordNativeInboxQualification(ctx, request); err != nil {
		t.Fatal(err)
	}
	hints.expect(t, render.ReconcileOrdinary)
	request.ObservedAt = now.Add(time.Millisecond)
	if err := native.RecordNativeInboxQualification(ctx, request); err != nil {
		t.Fatal(err)
	}
	hints.expect(t, 0)
	qualified, err := native.ReadNativeInboxQualification(ctx, admission.ActivationID)
	if err != nil || qualified.State != channelnative.QualificationQualified || qualified.LocaleRevision != declared.ClientLocaleRevision {
		t.Fatalf("exact qualification=%#v err=%v", qualified, err)
	}
	changed, err := localeOwner.SetChannelClientLocale(ctx, channelonboarding.SetClientLocaleRequest{
		OperationID: operationID, PrincipalID: admission.PrincipalID, ExpectedRevision: declared.ClientLocaleRevision,
		Language: "fr", Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	hints.expect(t, render.ReconcileOrdinary|render.ReconcileNative)
	stale, err := native.ReadNativeInboxQualification(ctx, admission.ActivationID)
	if err != nil || stale.State != channelnative.QualificationStale {
		t.Fatalf("locale change retained predecessor qualification: %#v err=%v", stale, err)
	}
	if err := native.RecordNativeInboxQualification(ctx, request); err == nil {
		t.Fatal("predecessor observation qualified a successor declaration")
	}
	hints.expect(t, 0)
	request.ClientLanguage, request.LocaleRevision = changed.ClientLanguage, changed.ClientLocaleRevision
	request.State, request.ReadbackHash, request.Reason = channelnative.QualificationInvalid, "", "selected locale has conflicting commands"
	request.ObservedAt = now.Add(2 * time.Second)
	if err := native.RecordNativeInboxQualification(ctx, request); err != nil {
		t.Fatal(err)
	}
	hints.expect(t, render.ReconcileOrdinary)
	invalid, err := native.ReadNativeInboxQualification(ctx, admission.ActivationID)
	retained, attachErr := native.AttachNativeInboxSetting(ctx, admission)
	if err != nil || attachErr != nil || invalid.State != channelnative.QualificationInvalid ||
		retained.SettingID != setting.SettingID || retained.Generation != setting.Generation ||
		retained.InstallOperationID != setting.InstallOperationID || retained.State != "installed" {
		t.Fatalf("fresh invalid qualification changed install history: qualification=%#v setting=%#v errors=%v/%v", invalid, retained, err, attachErr)
	}
	hints.expect(t, 0)
}
