package apiv1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestHumanTaskDecisionAcknowledgmentLossReplaysWithoutDuplicateOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			cardStore, humanStore, idempotency, mailbox, workflowStore, db := newHumanTaskAckLossOwners(t, ctx, backend)
			now := time.Date(2026, 7, 14, 14, 0, 0, 0, time.UTC)
			runID := uuid.NewString()
			if backend == "postgres" {
				if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
					t.Fatal(err)
				}
			} else if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
				t.Fatal(err)
			}
			card, continuation := newAPIHumanTaskAckLossCard(t, runID, now)
			if err := humanStore.CreateHumanTaskCard(ctx, card, continuation); err != nil {
				t.Fatalf("create human-task card: %v", err)
			}

			publisher := &humanTaskAckLossPublisher{}
			authority := &humanTaskAckLossAuthority{delegate: workflowStore}
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: OperatorReadHandlers(OperatorReadOptions{
					Now: func() time.Time { return now.Add(time.Minute) }, Ready: func() bool { return true }, Database: fakePinger{},
					Mailbox: mailbox, DecisionCards: cardStore, DecisionAuthority: authority,
					Idempotency: idempotency, Events: publisher,
				}),
			})
			body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"decide","method":"mailbox.decide","params":{"card_id":%q,"verdict":"approve","fields":{},"observed_content_hash":%q,"idempotency_key":"ack-loss"}}`, card.CardID, card.CardContentHash)
			first := rpcCall(t, handler, body)
			if first.Error == nil {
				t.Fatal("planted post-commit acknowledgment loss was not surfaced")
			}
			committed, err := cardStore.GetDecisionCard(ctx, card.CardID)
			if err != nil || committed.Status != decisioncard.StatusDecided || committed.DecisionEventID == "" {
				t.Fatalf("durable card after acknowledgment loss = %#v, %v", committed, err)
			}
			committedContinuation, err := humanStore.LoadHumanTaskContinuation(ctx, card.CardID)
			if err != nil || committedContinuation.State != decisioncard.HumanTaskContinuationDecisionCommitted || committedContinuation.OutcomeEventID != committed.DecisionEventID {
				t.Fatalf("durable continuation after acknowledgment loss = %#v, %v", committedContinuation, err)
			}

			replay := rpcCall(t, handler, body)
			if replay.Error != nil {
				t.Fatalf("acknowledgment-loss replay error = %#v", replay.Error)
			}
			result := asMap(t, replay.Result)
			if result["idempotency_replayed"] != true || result["decision_event_id"] != committed.DecisionEventID {
				t.Fatalf("acknowledgment-loss replay = %#v", result)
			}
			if authority.successfulMutations != 2 || publisher.calls != 1 {
				t.Fatalf("mutation/publish counts = %d/%d, want two API transactions and one semantic publish", authority.successfulMutations, publisher.calls)
			}
			reloaded, err := cardStore.GetDecisionCard(ctx, card.CardID)
			if err != nil || reloaded.DecisionEventID != committed.DecisionEventID || reloaded.Verdict != "approve" {
				t.Fatalf("card changed after replay = %#v, %v", reloaded, err)
			}
		})
	}
}

func newHumanTaskAckLossOwners(
	t *testing.T,
	ctx context.Context,
	backend string,
) (decisioncard.Store, decisioncard.HumanTaskStore, APIIdempotencyStore, MailboxAPIStore, *runtimepipeline.WorkflowInstanceStore, *sql.DB) {
	t.Helper()
	if backend == "postgres" {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		pg := &store.PostgresStore{DB: db}
		return pg, pg, pg, pg, runtimepipeline.NewWorkflowInstanceStore(db), db
	}
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)
	return sqliteStore, sqliteStore, sqliteStore, sqliteStore, workflowStore, sqliteStore.DB
}

func newAPIHumanTaskAckLossCard(t *testing.T, runID string, now time.Time) (decisioncard.Card, decisioncard.HumanTaskContinuation) {
	t.Helper()
	anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
		RequesterAgentID: "requester-agent", OperationID: "provider-turn/tool-call-1", Category: "review",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeFlow, FlowInstance: "provider/instance-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := mustTestDecisionSnapshot("human_task", "Review provider result", nil, map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve"},
		"reject":  {Verdict: "reject", Input: map[string]runtimecontracts.WorkflowGateInputField{"reason": {Type: "text", Required: true}}},
	})
	card, err := decisioncard.New(decisioncard.Card{
		CardID: uuid.NewString(), RunID: runID, Anchor: anchor, Snapshot: snapshot,
		ExecutionMode:    "live",
		BundleHash:       "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		EffectiveCadence: decisioncard.Cadence{InputDraftTTL: "15m", ReminderInterval: "24h"}, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return card, decisioncard.HumanTaskContinuation{
		CardID: card.CardID, RunID: runID, SourceEventID: uuid.NewString(), DeadlineAt: now.Add(24 * time.Hour),
		BudgetBundleHash: card.BundleHash, BudgetLimit: 10,
		BudgetWindowStart: now, BudgetWindowEnd: now.Add(7 * 24 * time.Hour),
		State: decisioncard.HumanTaskContinuationPending, CreatedAt: now, UpdatedAt: now,
	}
}

type humanTaskAckLossAuthority struct {
	delegate            *runtimepipeline.WorkflowInstanceStore
	lost                bool
	successfulMutations int
}

func (a *humanTaskAckLossAuthority) RunPipelineMutation(ctx context.Context, fn func(context.Context) error) error {
	err := a.delegate.RunPipelineMutation(ctx, fn)
	if err != nil {
		return err
	}
	a.successfulMutations++
	if !a.lost {
		a.lost = true
		return errors.New("simulated post-commit acknowledgment loss")
	}
	return nil
}

func (a *humanTaskAckLossAuthority) CommitDecision(ctx context.Context, card decisioncard.Card, eventID string, now time.Time) error {
	return a.delegate.CommitDecision(ctx, card, eventID, now)
}

type humanTaskAckLossPublisher struct{ calls int }

func (p *humanTaskAckLossPublisher) Publish(context.Context, events.Event) error { return nil }

func (p *humanTaskAckLossPublisher) PublishInMutation(context.Context, events.Event) error {
	p.calls++
	return nil
}
