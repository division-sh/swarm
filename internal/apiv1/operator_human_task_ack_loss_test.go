package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestHumanTaskDecisionAcknowledgmentLossReplaysWithoutDuplicateOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := runCompletionSystemNodeBundle(t)
			fact := sourceArtifactFactForTestBundle(t, bundle)
			ctx := testAuthorActivityContextForSource(context.Background(), fact)
			cardStore, humanStore, idempotency, mailbox, workflowStore, db := newHumanTaskAckLossOwners(t, ctx, backend)
			now := time.Date(2026, 7, 14, 14, 0, 0, 0, time.UTC)
			runID := uuid.NewString()
			if backend == "postgres" {
				storetest.RequirePostgresRun(t, ctx, db, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID})
			} else {
				storetest.RequireSQLiteRun(t, ctx, db, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID})
			}
			card, continuation := newAPIHumanTaskAckLossCard(t, runID, fact.BundleHash(), now)
			if err := humanStore.CreateHumanTaskCard(ctx, card, continuation); err != nil {
				t.Fatalf("create human-task card: %v", err)
			}

			authority := &humanTaskAckLossAuthority{delegate: workflowStore}
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: testOperatorHandlers(testOperatorCapabilities{
					Now: func() time.Time { return now.Add(time.Minute) }, Ready: func() bool { return true }, Database: fakePinger{},
					Mailbox: mailbox, DecisionCards: cardStore, DecisionAuthority: authority,
					Idempotency: idempotency,
					Events: bundleScopedFailingEventPublisher{
						failingRunStartPublisher: failingRunStartPublisher{err: errors.New("unexpected human-task API event publication")},
						fact:                     fact,
					},
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
			if authority.successfulMutations != 2 {
				t.Fatalf("successful mutation calls = %d, want two API transactions", authority.successfulMutations)
			}
			query := `SELECT COUNT(*) FROM events WHERE event_id = ?`
			arg := any(committed.DecisionEventID)
			if backend == "postgres" {
				query = `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`
			}
			var eventCount int
			if err := db.QueryRowContext(ctx, query, arg).Scan(&eventCount); err != nil || eventCount != 1 {
				t.Fatalf("durable decision event count = %d, %v; want exactly one", eventCount, err)
			}
			reloaded, err := cardStore.GetDecisionCard(ctx, card.CardID)
			if err != nil || reloaded.DecisionEventID != committed.DecisionEventID || reloaded.Verdict != "approve" {
				t.Fatalf("card changed after replay = %#v, %v", reloaded, err)
			}
		})
	}
}

type bundleScopedFailingEventPublisher struct {
	failingRunStartPublisher
	fact runtimecorrelation.SourceArtifactFact
}

func (p bundleScopedFailingEventPublisher) AdmitSourceArtifactFact(ctx context.Context) (context.Context, error) {
	if err := p.fact.Validate(); err != nil {
		return ctx, err
	}
	return runtimecorrelation.WithSourceArtifactFact(ctx, p.fact), nil
}

func newHumanTaskAckLossOwners(
	t *testing.T,
	ctx context.Context,
	backend string,
) (decisioncard.Store, decisioncard.HumanTaskStore, APIIdempotencyStore, MailboxAPIStore, DecisionCardAuthority, *sql.DB) {
	t.Helper()
	if backend == "postgres" {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		return pg, pg, pg, pg, newHumanTaskAckLossDecisionAuthority(t, db, pg, runtimepipeline.NewWorkflowPersistence(pg), pg, pg), db
	}
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	return sqliteStore, sqliteStore, sqliteStore, sqliteStore,
		newHumanTaskAckLossDecisionAuthority(t, storetest.Database(sqliteStore), sqliteStore, runtimepipeline.NewWorkflowPersistence(sqliteStore), sqliteStore, sqliteStore), storetest.Database(sqliteStore)
}

type humanTaskAckLossPersistence interface {
	runtimebus.EventStore
	apiTestRuntimeMutationOwner
	runtimerunlifecycle.OperationOwner
	PipelineObligations() runtimepipelineobligation.Store
}

func newHumanTaskAckLossDecisionAuthority(
	t *testing.T,
	db *sql.DB,
	selected humanTaskAckLossPersistence,
	persistence runtimepipeline.WorkflowPersistence,
	cards decisioncard.Store,
	humanTasks decisioncard.HumanTaskStore,
) DecisionCardAuthority {
	t.Helper()
	bundle := runCompletionSystemNodeBundle(t)
	source := semanticview.Wrap(bundle)
	eventBus, err := newScopedAPITestEventBus(t, selected, runtimebus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: sourceArtifactFactForTestBundle(t, bundle),
	})
	if err != nil {
		t.Fatalf("construct human-task acknowledgment-loss event bus: %v", err)
	}
	return runtimepipeline.NewPipelineCoordinatorWithOptions(eventBus, completeAPITestDurableWorkflowOptions(t, selected, eventBus, runtimepipeline.PipelineCoordinatorOptions{
		Module:              newRunCompletionSystemNodeModule(t, source),
		Persistence:         persistence,
		DecisionCards:       cards,
		HumanTasks:          humanTasks,
		RunLifecycle:        selected,
		PipelineObligations: selected.PipelineObligations(),
	}))

}

func newAPIHumanTaskAckLossCard(t *testing.T, runID, bundleHash string, now time.Time) (decisioncard.Card, decisioncard.HumanTaskContinuation) {
	t.Helper()
	requesterEntityID := uuid.NewString()
	source := eventtest.RootRoutingSource(requesterEntityID)
	anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
		RequesterAgentID: "requester-agent", OperationID: "provider-turn/tool-call-1", Category: "review",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeFlow, FlowInstance: "provider/instance-a"}, Source: source,
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
		BundleHash:       bundleHash,
		EffectiveCadence: decisioncard.Cadence{InputDraftTTL: "15m", ReminderInterval: "24h"}, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return card, decisioncard.HumanTaskContinuation{
		CardID: card.CardID, RunID: runID, RequesterRoute: source.Route(), SourceEventID: uuid.NewString(), DeadlineAt: now.Add(24 * time.Hour),
		BudgetBundleHash: card.BundleHash, BudgetLimit: 10,
		BudgetWindowStart: now, BudgetWindowEnd: now.Add(7 * 24 * time.Hour),
		State: decisioncard.HumanTaskContinuationPending, CreatedAt: now, UpdatedAt: now,
	}
}

type humanTaskAckLossAuthority struct {
	delegate            DecisionCardAuthority
	lost                bool
	successfulMutations int
}

func (a *humanTaskAckLossAuthority) CommitDecisionCardMutation(
	ctx context.Context,
	idempotency runtimepipeline.DecisionCardMutationIdempotency,
	idempotencyRequest runtimepipeline.DecisionCardMutationIdempotencyRequest,
	mutation runtimepipeline.DecisionCardMutation,
) (json.RawMessage, bool, error) {
	completion, replayed, err := a.delegate.CommitDecisionCardMutation(ctx, idempotency, idempotencyRequest, mutation)
	if err != nil {
		return nil, false, err
	}
	a.successfulMutations++
	if !a.lost {
		a.lost = true
		return nil, false, errors.New("simulated post-commit acknowledgment loss")
	}
	return completion, replayed, nil
}
