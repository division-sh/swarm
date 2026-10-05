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
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	channeldelivery "github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	"github.com/google/uuid"
)

func TestChannelDeliveryOpenCardPlanningUsesCanonicalStatusAndEpochBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			cards, runID := decisionCardTestStore(t, backend)
			selected := cards.(operatorchannel.Store)
			delivery := cards.(render.Store)
			if cursor, current, err := delivery.CurrentChannelCardChangeCursor(ctx); err != nil || current || cursor != 0 {
				t.Fatalf("card change cursor before default = %d, %t, %v", cursor, current, err)
			}
			var settle func(context.Context, operatorchannel.InboundClaim, time.Time) (operatorchannel.ClaimSettlement, error)
			var plan func(string) (bool, error)
			var count func(string) (int, error)
			var list func() ([]render.Candidate, error)
			var freeze func(string) (render.PreparedRender, error)
			var runTx func(func(context.Context, *sql.Tx) error) error
			switch store := cards.(type) {
			case *SQLiteRuntimeStore:
				runTx = func(fn func(context.Context, *sql.Tx) error) error {
					return store.backend.RunTransaction(ctx, "channel responsibility probe", fn)
				}
				settle = func(ctx context.Context, claim operatorchannel.InboundClaim, now time.Time) (operatorchannel.ClaimSettlement, error) {
					var out operatorchannel.ClaimSettlement
					err := store.backend.RunTransaction(ctx, "channel card claim", func(txctx context.Context, tx *sql.Tx) error {
						var err error
						out, err = store.operatorChannelSQLiteOwner.SettleInboundClaimTx(txctx, tx, claim, now)
						return err
					})
					return out, err
				}
				plan = func(id string) (bool, error) {
					var created bool
					err := store.backend.RunTransaction(ctx, "channel card plan", func(txctx context.Context, tx *sql.Tx) error {
						var err error
						created, err = channeldelivery.PlanOpenCardTx(txctx, tx, id, false)
						return err
					})
					return created, err
				}
				count = func(id string) (int, error) {
					var n int
					err := store.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_delivery_plans WHERE source_kind = 'card' AND source_id = ?`, id).Scan(&n)
					return n, err
				}
				list = func() ([]render.Candidate, error) {
					return store.ListCurrentChannelDeliveryPlans(ctx, "", 100)
				}
				freeze = func(id string) (render.PreparedRender, error) {
					return store.FreezeAndPersistChannelRender(ctx, id, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64})
				}
			case *PostgresStore:
				runTx = func(fn func(context.Context, *sql.Tx) error) error {
					return store.backend.RunTransaction(ctx, fn)
				}
				settle = func(ctx context.Context, claim operatorchannel.InboundClaim, now time.Time) (operatorchannel.ClaimSettlement, error) {
					tx, err := store.backend.BeginTx(ctx, nil)
					if err != nil {
						return operatorchannel.ClaimSettlement{}, err
					}
					out, err := store.operatorChannelPostgresOwner.SettleInboundClaimTx(ctx, tx, claim, now)
					if err != nil {
						_ = tx.Rollback()
						return out, err
					}
					return out, tx.Commit()
				}
				plan = func(id string) (bool, error) {
					tx, err := store.backend.BeginTx(ctx, nil)
					if err != nil {
						return false, err
					}
					created, err := channeldelivery.PlanOpenCardTx(ctx, tx, id, true)
					if err != nil {
						_ = tx.Rollback()
						return false, err
					}
					return created, tx.Commit()
				}
				count = func(id string) (int, error) {
					var n int
					err := store.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_delivery_plans WHERE source_kind = 'card' AND source_id = $1::uuid`, id).Scan(&n)
					return n, err
				}
				list = func() ([]render.Candidate, error) {
					return store.ListCurrentChannelDeliveryPlans(ctx, "", 100)
				}
				freeze = func(id string) (render.PreparedRender, error) {
					return store.FreezeAndPersistChannelRender(ctx, id, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64})
				}
			default:
				t.Fatalf("unsupported card store %T", cards)
			}
			now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
			newCard := func() (decisioncard.Card, string, string) {
				t.Helper()
				entityID, activationID := uuid.NewString(), uuid.NewString()
				card, err := decisioncard.New(decisioncard.Card{
					CardID: uuid.NewString(), RunID: runID,
					Anchor:        newDecisionCardTestStageAnchor("launch/review-1", "launch", entityID, "awaiting_review", activationID),
					ExecutionMode: "live", BundleHash: authorActivityTestBundleHash, WorkflowVersion: "1",
					Snapshot: freezeDecisionCardTestSnapshot(t, "launch_review", map[string]any{"summary": "ready"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
						"accept": {Verdict: "accept", AdvancesTo: "operating"},
					}),
					CreatedAt: now,
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := cards.CreateDecisionCard(ctx, card); err != nil {
					t.Fatal(err)
				}
				return card, entityID, activationID
			}
			backlog, backlogEntityID, backlogActivationID := newCard()
			if created, err := plan(backlog.CardID); err != nil || created {
				t.Fatalf("before default plan = %t, %v", created, err)
			}
			principal, err := selected.EnsureOperatorPrincipal(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			identity := operatorChannelContractIdentity("card-plan-" + backend)
			connect := func(kind operatorchannel.OperationKind, revision int64, account, conversation string) operatorchannel.Binding {
				t.Helper()
				now = now.Add(time.Second)
				id := uuid.NewString()
				op, err := selected.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
					OperationID: id, Kind: kind, PrincipalID: principal.ID, Interface: identity,
					ExpectedRevision: revision, RequestKeyHash: id, RequestHash: id,
					ProviderCredential: operatorChannelProviderEvidence(), RequestedAt: now, ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
				})
				if err != nil {
					t.Fatal(err)
				}
				settled, err := settle(ctx, operatorChannelContractClaim(op, operatorchannel.ConversationScopeDirect, account, conversation, uuid.NewString()), now.Add(time.Second))
				if err != nil {
					t.Fatal(err)
				}
				_, binding, err := selected.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
					OperationID: id, PrincipalID: principal.ID, ExpectedRevision: settled.Operation.Revision,
					Approve: true, ProviderCredentialCurrent: true, ConfirmedAt: now.Add(2 * time.Second),
				})
				if err != nil {
					t.Fatal(err)
				}
				return binding
			}
			bound := connect(operatorchannel.OperationConnect, 0, "account-a", "chat-a")
			if cursor, current, err := delivery.CurrentChannelCardChangeCursor(ctx); err != nil || !current || cursor != 0 {
				t.Fatalf("initial card change cursor = %d, %t, %v", cursor, current, err)
			}
			if created, err := plan(backlog.CardID); err != nil || !created {
				t.Fatalf("first backlog plan = %t, %v", created, err)
			}
			changes, err := cards.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 200})
			if err != nil || len(changes) == 0 {
				t.Fatalf("initial card changes = %+v, %v", changes, err)
			}
			sequence := changes[0].Sequence
			if err := delivery.PlanChangedChannelCard(ctx, sequence, uuid.NewString()); err == nil {
				t.Fatal("wrong card identity advanced the channel cursor")
			}
			if cursor, _, err := delivery.CurrentChannelCardChangeCursor(ctx); err != nil || cursor != 0 {
				t.Fatalf("cursor after rejected change = %d, %v", cursor, err)
			}
			if err := delivery.PlanChangedChannelCard(ctx, sequence, backlog.CardID); err != nil {
				t.Fatalf("plan exact card change: %v", err)
			}
			if cursor, _, err := delivery.CurrentChannelCardChangeCursor(ctx); err != nil || cursor != sequence {
				t.Fatalf("cursor after exact planned change = %d, want %d: %v", cursor, sequence, err)
			}
			firstPlans, err := list()
			if err != nil {
				t.Fatal(err)
			}
			var firstDeliveryID string
			for _, item := range firstPlans {
				if item.SourceKind == channeldelivery.PlanCard && item.SourceID == backlog.CardID {
					firstDeliveryID = item.DeliveryID
				}
			}
			if firstDeliveryID == "" {
				t.Fatalf("current card plan missing from scan: %#v", firstPlans)
			}
			firstRender, err := freeze(firstDeliveryID)
			if err != nil || firstRender.Frozen.SourceKind != channeldelivery.PlanCard ||
				len(firstRender.Actions) != 1 || firstRender.Actions[0].Kind != "verdict" ||
				firstRender.Actions[0].Verdict != "accept" || uuid.Validate(firstRender.Actions[0].Token) != nil {
				t.Fatalf("freeze pending card = %#v, %v", firstRender, err)
			}
			repeated, err := freeze(firstDeliveryID)
			if err != nil || repeated.RenderID != firstRender.RenderID ||
				len(repeated.Actions) != 1 || repeated.Actions[0].Token != firstRender.Actions[0].Token {
				t.Fatalf("repeated render = %#v, %v", repeated, err)
			}
			if created, err := plan(backlog.CardID); err != nil || created {
				t.Fatalf("repeated backlog plan = %t, %v", created, err)
			}
			reconnected := connect(operatorchannel.OperationReconnect, bound.Revision, "account-a", "chat-a")
			if created, err := plan(backlog.CardID); err != nil || created {
				t.Fatalf("reconnect plan = %t, %v", created, err)
			}
			reconnectedPlans, err := list()
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range reconnectedPlans {
				if item.DeliveryID == firstDeliveryID && item.BindingRevision != reconnected.Revision {
					t.Fatalf("same-destination reconnect did not renew dispatch binding revision: %#v", item)
				}
			}
			rebound := connect(operatorchannel.OperationRebind, reconnected.Revision, "account-b", "chat-b")
			if created, err := plan(backlog.CardID); err != nil || !created {
				t.Fatalf("rebind plan = %t, %v", created, err)
			}
			if got, err := count(backlog.CardID); err != nil || got != 2 {
				t.Fatalf("backlog plans = %d, %v", got, err)
			}
			currentPlans, err := list()
			if err != nil {
				t.Fatal(err)
			}
			var reboundDeliveryID string
			for _, item := range currentPlans {
				if item.SourceKind == channeldelivery.PlanCard && item.SourceID == backlog.CardID {
					reboundDeliveryID = item.DeliveryID
				}
				if item.DeliveryID == firstDeliveryID {
					t.Fatal("historical delivery epoch was scanned as current")
				}
			}
			if reboundDeliveryID == "" {
				t.Fatalf("rebound card plan missing from scan: %#v", currentPlans)
			}
			if err := cards.SupersedeDecisionCardsForStage(ctx, runID, backlogEntityID, backlogActivationID, "flow moved on", now.Add(4*time.Second)); err != nil {
				t.Fatal(err)
			}
			terminalRender, err := freeze(reboundDeliveryID)
			if err != nil || terminalRender.Frozen.Revision <= firstRender.Frozen.Revision ||
				terminalRender.Frozen.Hash == firstRender.Frozen.Hash {
				t.Fatalf("terminal card render = %#v, %v", terminalRender, err)
			}
			if _, found, err := delivery.GetCurrentChannelDeliveryPlan(ctx, reboundDeliveryID); err != nil || found {
				t.Fatalf("superseded unsent card remains executable: found=%t err=%v", found, err)
			}
			terminalPlans, err := list()
			if err != nil {
				t.Fatal(err)
			}
			for _, candidate := range terminalPlans {
				if candidate.DeliveryID == reboundDeliveryID {
					t.Fatal("superseded unsent card remains in current responsibilities")
				}
			}
			proveChannelCardResponsibility(t, runTx, reboundDeliveryID, backlog.CardID, backend == "postgres", now)
			if _, err := freeze(firstDeliveryID); err == nil {
				t.Fatal("historical card destination accepted a new render")
			}
			terminal, entityID, activationID := newCard()
			if err := cards.SupersedeDecisionCardsForStage(ctx, runID, entityID, activationID, "flow moved on", now.Add(5*time.Second)); err != nil {
				t.Fatal(err)
			}
			changes, err = cards.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{After: sequence, Limit: 200})
			if err != nil || len(changes) == 0 {
				t.Fatalf("terminal card changes = %+v, %v", changes, err)
			}
			terminalSequence := changes[len(changes)-1].Sequence
			if err := delivery.PlanChangedChannelCard(ctx, terminalSequence, terminal.CardID); err != nil {
				t.Fatalf("advance terminal card change: %v", err)
			}
			if cursor, _, err := delivery.CurrentChannelCardChangeCursor(ctx); err != nil || cursor != terminalSequence {
				t.Fatalf("terminal card cursor = %d, want %d: %v", cursor, terminalSequence, err)
			}
			if created, err := plan(terminal.CardID); err != nil || created {
				t.Fatalf("terminal plan = %t, %v", created, err)
			}
			if got, err := count(terminal.CardID); err != nil || got != 0 {
				t.Fatalf("terminal plans = %d, %v", got, err)
			}
			if rebound.Revision <= reconnected.Revision {
				t.Fatalf("rebind did not advance binding revision: %#v", rebound)
			}
			var recoveredCursor int64
			var recoveredCurrent bool
			switch store := cards.(type) {
			case *SQLiteRuntimeStore:
				path := store.Path()
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				fresh, err := NewSQLiteRuntimeStore(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = fresh.Close() })
				if err := fresh.BootstrapSchema(ctx, canonicalSchemaBootstrapTestRequest(t)); err != nil {
					t.Fatal(err)
				}
				recoveredCursor, recoveredCurrent, err = fresh.CurrentChannelCardChangeCursor(ctx)
				if err != nil {
					t.Fatal(err)
				}
			case *PostgresStore:
				fresh, err := store.backend.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer fresh.Close()
				err = fresh.QueryRowContext(ctx, `SELECT card_change_cursor FROM channel_delivery_defaults WHERE singleton_id=1 AND state='current'`).Scan(&recoveredCursor)
				if err != nil {
					t.Fatal(err)
				}
				recoveredCurrent = true
			default:
				t.Fatalf("unsupported card store %T", cards)
			}
			if !recoveredCurrent || recoveredCursor != terminalSequence {
				t.Fatalf("card change cursor after fresh selected-store connection = %d, current=%t, want=%d",
					recoveredCursor, recoveredCurrent, terminalSequence)
			}
		})
	}
}

func proveChannelCardResponsibility(t *testing.T, runTx func(func(context.Context, *sql.Tx) error) error,
	deliveryID, cardID string, postgres bool, now time.Time) {
	t.Helper()
	for _, phase := range []string{"planned", "rendered"} {
		for _, status := range []string{"pending", "deferred", "superseded", "decided", "expired"} {
			t.Run("responsibility/"+phase+"/"+status, func(t *testing.T) {
				rollback := errors.New("rollback responsibility probe")
				err := runTx(func(ctx context.Context, tx *sql.Tx) error {
					storedStatus := status
					var deferred, decidedAt, decisionEvent any
					if status == "deferred" {
						storedStatus, deferred = "pending", now.Add(time.Hour)
					}
					if status == "decided" || status == "expired" {
						decidedAt = now
					}
					if status == "decided" {
						decisionEvent = uuid.NewString()
					}
					query := `UPDATE decision_cards SET status=$1,
						verdict=CASE WHEN $1='decided' THEN 'accept' ELSE NULL END,
						decided_at=$2,
						decision_event_id=$3,
						superseded_reason=CASE WHEN $1='superseded' THEN 'flow moved on' ELSE NULL END,
						deferred_until=$4 WHERE card_id=$5`
					if _, err := tx.ExecContext(ctx, query, storedStatus, decidedAt, decisionEvent, deferred, cardID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(ctx, `UPDATE channel_delivery_plans SET state=$1 WHERE delivery_id=$2`, phase, deliveryID); err != nil {
						return err
					}
					_, found, err := channeldelivery.LoadCurrentPlan(ctx, tx, deliveryID, postgres)
					want := storedStatus == "pending"
					if err != nil || found != want {
						return fmt.Errorf("current get found=%t want=%t: %w", found, want, err)
					}
					cursor, listed := "", false
					for {
						page, err := channeldelivery.ListCurrentPlans(ctx, tx, cursor, 1, postgres)
						if err != nil {
							return err
						}
						if len(page) == 0 {
							break
						}
						if page[0].DeliveryID == deliveryID {
							listed = true
						}
						if page[0].DeliveryID <= cursor {
							return fmt.Errorf("responsibility cursor did not advance")
						}
						cursor = page[0].DeliveryID
					}
					if listed != want {
						return fmt.Errorf("paged list found=%t want=%t", listed, want)
					}
					return rollback
				})
				if !errors.Is(err, rollback) {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestChannelDeliveryCardActionAdmissionSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			cards, runID := decisionCardTestStore(t, backend)
			selected := cards.(selectedChannelDeliveryTestStore)
			postgres := backend == "postgres"
			var runTx func(func(context.Context, *sql.Tx) error) error
			var settleClaim func(operatorchannel.InboundClaim, time.Time) (operatorchannel.ClaimSettlement, error)
			switch store := cards.(type) {
			case *PostgresStore:
				runTx = func(fn func(context.Context, *sql.Tx) error) error {
					return store.backend.RunTransaction(ctx, fn)
				}
				settleClaim = func(claim operatorchannel.InboundClaim, at time.Time) (operatorchannel.ClaimSettlement, error) {
					var out operatorchannel.ClaimSettlement
					err := runTx(func(txctx context.Context, tx *sql.Tx) error {
						var err error
						out, err = store.operatorChannelPostgresOwner.SettleInboundClaimTx(txctx, tx, claim, at)
						return err
					})
					return out, err
				}
			case *SQLiteRuntimeStore:
				runTx = func(fn func(context.Context, *sql.Tx) error) error {
					return store.backend.RunTransaction(ctx, "channel card action admission", fn)
				}
				settleClaim = func(claim operatorchannel.InboundClaim, at time.Time) (operatorchannel.ClaimSettlement, error) {
					var out operatorchannel.ClaimSettlement
					err := runTx(func(txctx context.Context, tx *sql.Tx) error {
						var err error
						out, err = store.operatorChannelSQLiteOwner.SettleInboundClaimTx(txctx, tx, claim, at)
						return err
					})
					return out, err
				}
			default:
				t.Fatalf("unsupported selected card store %T", cards)
			}
			now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
			onboarding, activation := selectedChannelConfirmationAuthorityFixture(t, selected, now, runtimeeffects.StateAuthorized)
			principal, err := selected.EnsureOperatorPrincipal(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			bindingOperation, err := selected.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
				OperationID: onboarding.IdentityOperationID, Kind: operatorchannel.OperationConnect,
				PrincipalID: principal.ID, Interface: activation.Interface, ExpectedRevision: 0,
				RequestKeyHash: onboarding.IdentityOperationID, RequestHash: onboarding.IdentityOperationID,
				ProviderCredential: operatorChannelProviderEvidence(), RequestedAt: now,
				ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
			})
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := settleClaim(operatorChannelContractClaim(bindingOperation, operatorchannel.ConversationScopeDirect,
				"account", activation.ConversationRef, uuid.NewString()), now.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			_, binding, err := selected.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
				OperationID: bindingOperation.OperationID, PrincipalID: principal.ID, ExpectedRevision: claimed.Operation.Revision,
				Approve: true, ProviderCredentialCurrent: true, ConfirmedAt: now.Add(2 * time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}
			onboarding, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
				OperationID: onboarding.OperationID, ExpectedRevision: onboarding.Revision,
				Phase: channelonboarding.PhaseSucceeded, Now: now.Add(20 * time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}
			entityID, stageActivationID := uuid.NewString(), uuid.NewString()
			card, err := decisioncard.New(decisioncard.Card{
				CardID: uuid.NewString(), RunID: runID,
				Anchor:        newDecisionCardTestStageAnchor("review/check-1", "review", entityID, "awaiting_review", stageActivationID),
				ExecutionMode: "live", BundleHash: authorActivityTestBundleHash, WorkflowVersion: "1",
				Snapshot: freezeDecisionCardTestSnapshot(t, "review_check", map[string]any{"summary": "ready"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
					"accept": {Verdict: "accept", AdvancesTo: "done"},
					"revise": {Verdict: "revise", AdvancesTo: "awaiting_review", Input: map[string]runtimecontracts.WorkflowGateInputField{
						"feedback": {Type: "text", Required: true},
					}, InputOrder: []string{"feedback"}},
				}), CreatedAt: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := cards.CreateDecisionCard(ctx, card); err != nil {
				t.Fatal(err)
			}
			created, err := selected.PlanOpenChannelCard(ctx, card.CardID)
			if err != nil || !created {
				t.Fatalf("plan card = %t, %v", created, err)
			}
			plans, err := selected.ListCurrentChannelDeliveryPlans(ctx, "", 100)
			if err != nil {
				t.Fatal(err)
			}
			var candidate render.Candidate
			for _, plan := range plans {
				if plan.SourceID == card.CardID {
					candidate = plan
				}
			}
			if candidate.DeliveryID == "" {
				t.Fatalf("planned card missing: %#v", plans)
			}
			prepared, err := selected.FreezeAndPersistChannelRender(ctx, candidate.DeliveryID, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64})
			if err != nil || len(prepared.Actions) != 2 {
				t.Fatalf("prepared card = %#v, err=%v", prepared, err)
			}
			acceptToken := ""
			for _, action := range prepared.Actions {
				if action.Kind == "verdict" && action.Verdict == "accept" {
					acceptToken = action.Token
				}
			}
			if acceptToken == "" {
				t.Fatalf("prepared card omitted accept: %#v", prepared.Actions)
			}
			operationID, err := runtimeeffects.ChannelDeliveryOperationID(candidate.DeliveryID, prepared.RenderID)
			if err != nil {
				t.Fatal(err)
			}
			authority := runtimeeffects.Authority{
				Kind: runtimeeffects.AuthorityChannelDelivery, ID: operationID, ExecutionOwner: "channel-card-action-test",
				LeaseExpiresAt: time.Now().Add(5 * time.Minute), FenceGeneration: activation.Coordinate.ContextPublicationGeneration,
				ExecutionMode: runtimeeffects.ExecutionModeLive,
				ChannelDelivery: runtimeeffects.ChannelDeliveryAuthority{
					EffectOperationID: operationID, DeliveryID: candidate.DeliveryID, RenderID: prepared.RenderID,
					RenderHash: prepared.Frozen.Hash, PrincipalID: principal.ID, InterfaceKey: binding.Interface.Key(),
					DeliveryEpoch: candidate.Audience.DeliveryEpoch, BindingRevision: binding.Revision,
					ExternalAccountRef: candidate.Audience.ExternalAccountRef, ConversationRef: candidate.Audience.ConversationRef,
					ActivationID: activation.ActivationID, ActivationRevision: activation.Revision,
					BundleHash: activation.Coordinate.BundleHash, BundleIdentity: activation.Coordinate.BundleIdentity,
					PackInventoryGeneration:      activation.Coordinate.PackInventoryGeneration,
					RuntimeInstanceID:            activation.Coordinate.RuntimeInstanceID,
					ContextPublicationGeneration: activation.Coordinate.ContextPublicationGeneration,
					PlanGeneration:               activation.Coordinate.PlanGeneration, TargetGeneration: activation.Coordinate.TargetGeneration,
				},
			}
			if !authority.Valid() {
				t.Fatal("card delivery authority is invalid")
			}
			effectCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(
				testAuthorActivityContextForBundle(activation.Coordinate.BundleHash), authority),
				runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
			handle, err := runtimeeffects.BeginChannelDelivery(effectCtx, []byte("card"), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := handle.MarkLaunched(effectCtx); err != nil {
				t.Fatal(err)
			}
			if err := handle.MarkResponseObserved(effectCtx, map[string]any{"provider": "accepted"}); err != nil {
				t.Fatal(err)
			}
			if err := handle.Succeed(effectCtx, map[string]any{"projected_output": map[string]any{"delivery_reference": map[string]any{"id": 91}}}); err != nil {
				t.Fatal(err)
			}
			fact := operatorchannel.ActionFact{
				Interface: binding.Interface, ExternalAccountRef: candidate.Audience.ExternalAccountRef,
				ConversationRef: candidate.Audience.ConversationRef, ConversationScope: candidate.Audience.ConversationScope,
				MessageReference: `{"id":91}`, InteractionRef: "callback-card-91", Token: acceptToken,
			}
			demand := render.CardActionDemand{CardID: card.CardID, PrincipalID: principal.ID, Method: "mailbox.decide",
				Verdict: "accept", ReceiptOperationID: operationID, RenderHash: prepared.Frozen.Hash}
			require := func(fact operatorchannel.ActionFact, demand render.CardActionDemand) error {
				return runTx(func(txctx context.Context, tx *sql.Tx) error {
					return channeldelivery.RequireCardActionTx(txctx, tx, fact, demand, postgres, true)
				})
			}
			if err := require(fact, demand); err != nil {
				t.Fatalf("exact current card action rejected: %v", err)
			}
			foreign := demand
			foreign.Verdict = "reject"
			if err := require(fact, foreign); err == nil || !strings.Contains(err.Error(), "not current card authority") {
				t.Fatalf("foreign verdict admission = %v", err)
			}
			draft, err := DecisionCardDomainForTest(cards).BeginInputForTest(ctx, decisioncard.BeginInputRequest{
				CardID: card.CardID, Verdict: "revise", PrincipalID: principal.ID,
				DeliveryReceiptID: operationID, Now: now.Add(30 * time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}
			prompted, err := selected.FreezeAndPersistChannelRender(ctx, candidate.DeliveryID, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64})
			if err != nil || !strings.Contains(prompted.Frozen.FullText, "Input: feedback (text) required") ||
				prompted.Frozen.Hash == prepared.Frozen.Hash {
				t.Fatalf("selected active draft prompt = %#v, %v", prompted, err)
			}
			selector, ok := cards.(interface {
				HasCurrentChannelInputDraft(context.Context, operatorchannel.InboundText, time.Time) (bool, error)
			})
			if !ok {
				t.Fatal("selected store lacks bare input classification")
			}
			text := operatorchannel.InboundText{
				TextFact: operatorchannel.TextFact{
					Interface: binding.Interface, ExternalAccountRef: candidate.Audience.ExternalAccountRef,
					ConversationRef: candidate.Audience.ConversationRef, ConversationScope: candidate.Audience.ConversationScope,
					Text: "feedback", MessageReference: `{"id":92}`,
				}, Provider: activation.Provider, ProviderEventID: "bare-input-92",
				PublicationID: uuid.NewString(), ProviderAuthorization: "verified-catalog",
			}
			if found, err := selector.HasCurrentChannelInputDraft(ctx, text, now.Add(time.Minute)); err != nil || !found {
				t.Fatalf("current bare input draft = %t, %v", found, err)
			}
			admitText := func(fact operatorchannel.InboundText) {
				t.Helper()
				if err := runTx(func(txctx context.Context, tx *sql.Tx) error {
					return channeldelivery.InsertTextIntentTx(txctx, tx, fact, now.Add(time.Minute), postgres)
				}); err != nil {
					t.Fatal(err)
				}
			}
			admitText(text)
			if drafts, next, err := selected.ListCurrentChannelInputDrafts(ctx, text, now.Add(time.Minute), "", 2); err != nil || len(drafts) != 1 || drafts[0].CardID != card.CardID || next != "" {
				t.Fatalf("bare draft candidates = %+v next=%q err=%v", drafts, next, err)
			}
			input, ok := cards.(interface {
				PreviewCurrentChannelInputDraftText(context.Context, operatorchannel.InboundText, time.Time, string) (decisioncard.InputFieldProgress, string, error)
				AdvancePartialChannelInputDraftText(context.Context, operatorchannel.InboundText, time.Time, string) (decisioncard.InputFieldProgress, error)
			})
			if !ok {
				t.Fatal("selected store lacks canonical channel input progression")
			}
			preview, actor, err := input.PreviewCurrentChannelInputDraftText(ctx, text, now.Add(time.Minute), draft.InputDraftID)
			if err != nil || actor != principal.ID || !preview.Complete || preview.AcceptedField != "feedback" {
				t.Fatalf("final input preview = %+v actor=%q err=%v", preview, actor, err)
			}
			if _, err := input.AdvancePartialChannelInputDraftText(ctx, text, now.Add(time.Minute), draft.InputDraftID); err == nil || !strings.Contains(err.Error(), "canonical decision") {
				t.Fatalf("partial owner accepted final field: %v", err)
			}
			quoted := text
			quoted.PublicationID = uuid.NewString()
			quoted.ProviderEventID = "quoted-input-93"
			quoted.MessageReference = `{"id":93}`
			quoted.ReplyToReference = `{"id":91}`
			admitText(quoted)
			if drafts, _, err := selected.ListCurrentChannelInputDrafts(ctx, quoted, now.Add(time.Minute), "", 2); err != nil || len(drafts) != 0 {
				t.Fatalf("unsettled prompt retained quoted-input authority: %+v, %v", drafts, err)
			}
			editID, err := runtimeeffects.ChannelDeliveryOperationID(candidate.DeliveryID, prompted.RenderID)
			if err != nil {
				t.Fatal(err)
			}
			editAuthority := authority
			editAuthority.ID = editID
			editAuthority.ChannelDelivery.EffectOperationID = editID
			editAuthority.ChannelDelivery.RenderID = prompted.RenderID
			editAuthority.ChannelDelivery.RenderHash = prompted.Frozen.Hash
			editAuthority.ChannelDelivery.PreviousReceiptOperationID = operationID
			editCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(
				testAuthorActivityContextForBundle(activation.Coordinate.BundleHash), editAuthority),
				runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
			edit, err := runtimeeffects.BeginChannelDelivery(editCtx, []byte("prompt edit"), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := edit.MarkLaunched(editCtx); err != nil {
				t.Fatal(err)
			}
			if err := edit.MarkResponseObserved(editCtx, map[string]any{"provider": "edited"}); err != nil {
				t.Fatal(err)
			}
			if err := edit.Succeed(editCtx, map[string]any{"projected_output": map[string]any{"delivery_receipt": map[string]any{"id": 91}}}); err != nil {
				t.Fatal(err)
			}
			if drafts, next, err := selected.ListCurrentChannelInputDrafts(ctx, quoted, now.Add(time.Minute), "", 2); err != nil || len(drafts) != 1 || drafts[0].CardID != card.CardID || next != "" {
				t.Fatalf("quoted draft candidates = %+v next=%q err=%v", drafts, next, err)
			}
			if preview, actor, err := input.PreviewCurrentChannelInputDraftText(ctx, quoted, now.Add(time.Minute), draft.InputDraftID); err != nil || actor != principal.ID || !preview.Complete {
				t.Fatalf("quoted exact input preview = %+v actor=%q err=%v", preview, actor, err)
			}
			unknownQuote := quoted
			unknownQuote.PublicationID = uuid.NewString()
			unknownQuote.ProviderEventID = "unknown-quote-94"
			unknownQuote.ReplyToReference = `{"id":999}`
			admitText(unknownQuote)
			if drafts, next, err := selected.ListCurrentChannelInputDrafts(ctx, unknownQuote, now.Add(time.Minute), "", 2); err != nil || len(drafts) != 0 || next != "" {
				t.Fatalf("unknown quote fell back to bare draft: %+v next=%q err=%v", drafts, next, err)
			}
			if _, _, err := input.PreviewCurrentChannelInputDraftText(ctx, unknownQuote, now.Add(time.Minute), draft.InputDraftID); err == nil {
				t.Fatal("unknown quote preview fell back to bare draft")
			}
			foreignText := text
			foreignText.ExternalAccountRef = "foreign-account"
			if found, err := selector.HasCurrentChannelInputDraft(ctx, foreignText, now.Add(time.Minute)); err != nil || found {
				t.Fatalf("foreign bare input draft = %t, %v", found, err)
			}
			if found, err := selector.HasCurrentChannelInputDraft(ctx, text, now.Add(16*time.Minute)); err != nil || found {
				t.Fatalf("expired bare input draft = %t, %v", found, err)
			}
			if err := runTx(func(txctx context.Context, tx *sql.Tx) error {
				_, err := decisionpersistence.AdvanceInputDraftTextTx(txctx, tx, draft.InputDraftID, principal.ID,
					"private-answer", now.Add(2*time.Minute), postgres)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			completed, err := selected.FreezeAndPersistChannelRender(ctx, candidate.DeliveryID, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64})
			if err != nil || !strings.Contains(completed.Frozen.FullText, "Input complete; decision pending") ||
				strings.Contains(string(completed.Frozen.Input), "private-answer") || completed.Frozen.Hash == prompted.Frozen.Hash {
				t.Fatalf("selected completed draft render = %#v, %v", completed, err)
			}
			if err := cards.SupersedeDecisionCardsForStage(ctx, runID, entityID, stageActivationID, "moved", now.Add(3*time.Minute)); err != nil {
				t.Fatal(err)
			}
			if found, err := selector.HasCurrentChannelInputDraft(ctx, text, now.Add(4*time.Minute)); err != nil || found {
				t.Fatalf("superseded bare input draft = %t, %v", found, err)
			}
			if _, err := selected.FreezeAndPersistChannelRender(ctx, candidate.DeliveryID, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64}); err != nil {
				t.Fatal(err)
			}
			if err := require(fact, demand); err == nil || !strings.Contains(err.Error(), "not current card authority") {
				t.Fatalf("predecessor render admission = %v", err)
			}
			t.Run("unsupported_chooser_retirement", func(t *testing.T) {
				proveUnsupportedChooserRetirement(t, cards, selected, runTx, authority, card, text, now, postgres)
			})
		})
	}
}
