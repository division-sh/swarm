package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	channeldelivery "github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/google/uuid"
)

func TestChannelDeliveryOpenCardPlanningUsesCanonicalStatusAndEpochBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			cards, runID := decisionCardTestStore(t, backend)
			selected := cards.(operatorchannel.Store)
			var settle func(context.Context, operatorchannel.InboundClaim, time.Time) (operatorchannel.ClaimSettlement, error)
			var plan func(string) (bool, error)
			var count func(string) (int, error)
			switch store := cards.(type) {
			case *SQLiteRuntimeStore:
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
			case *PostgresStore:
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
			backlog, _, _ := newCard()
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
			if created, err := plan(backlog.CardID); err != nil || !created {
				t.Fatalf("first backlog plan = %t, %v", created, err)
			}
			if created, err := plan(backlog.CardID); err != nil || created {
				t.Fatalf("repeated backlog plan = %t, %v", created, err)
			}
			reconnected := connect(operatorchannel.OperationReconnect, bound.Revision, "account-a", "chat-a")
			if created, err := plan(backlog.CardID); err != nil || created {
				t.Fatalf("reconnect plan = %t, %v", created, err)
			}
			rebound := connect(operatorchannel.OperationRebind, reconnected.Revision, "account-b", "chat-b")
			if created, err := plan(backlog.CardID); err != nil || !created {
				t.Fatalf("rebind plan = %t, %v", created, err)
			}
			if got, err := count(backlog.CardID); err != nil || got != 2 {
				t.Fatalf("backlog plans = %d, %v", got, err)
			}
			terminal, entityID, activationID := newCard()
			if err := cards.SupersedeDecisionCardsForStage(ctx, runID, entityID, activationID, "flow moved on", now.Add(5*time.Second)); err != nil {
				t.Fatal(err)
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
		})
	}
}
