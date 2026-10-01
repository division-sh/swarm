package cliapp

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/decisioncardtest"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type factoringMailboxStore interface {
	decisioncard.Store
	decisioncard.HumanTaskStore
	decisioncard.ProposedEffectStore
	apiv1.MailboxAPIStore
}

// Canonical card/continuation writers seed real bounded mailbox reads. This is
// not credited as source-authored creation or the compiled command C11 proof.
func TestScenarioCardFactoringActualContinuationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, anchor := range []string{"human_task", "proposed_effect"} {
			t.Run(backend+"/"+anchor, func(t *testing.T) {
				setCLIAPITestToken(t, "test-token")
				fact, err := correlation.NewSourceArtifactFact(storetest.SemanticFixtureBundleHash)
				if err != nil {
					t.Fatal(err)
				}
				instance := uuid.NewString()
				ctx := correlation.WithSourceArtifactFact(correlation.WithRuntimeInstanceID(context.Background(), instance), fact)
				ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(instance, fact.BundleHash()))
				runID, entity := uuid.NewString(), uuid.NewString()
				var selected factoringMailboxStore
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					storetest.RequireRun(t, ctx, s, storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin()})
					selected = s
				} else {
					_, db, cleanup := testutil.StartPostgres(t)
					t.Cleanup(cleanup)
					s := storetest.AdmitPostgresRuntimeStore(t, db)
					storetest.RequireRun(t, ctx, s, storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin()})
					selected = s
				}
				base := time.Now().UTC().Add(-time.Hour)
				create := func(i int, label string) decisioncard.Card {
					t.Helper()
					return createFactoringMailboxCard(t, ctx, selected, fact, runID, entity, anchor, label, base.Add(time.Duration(i)*time.Second))
				}
				for i := 0; i < 200; i++ {
					create(i, "other")
				}
				target := create(200, "target")
				registry, err := apiv1.LoadRegistry(ResolvePath(RepoRoot(), defaultPlatformSpecPath))
				if err != nil {
					t.Fatal(err)
				}
				handler, err := apiv1.NewHandler(apiv1.Options{Registry: registry, AuthTokens: []string{"test-token"}, Handlers: apiv1.OperatorDecisionCardHandlers(apiv1.DecisionCardHandlerOptions{Cards: selected, ProposedEffects: selected, Mailbox: selected})})
				if err != nil {
					t.Fatal(err)
				}
				server := httptest.NewServer(handler)
				defer server.Close()
				client, err := newCLIAPIClientForTest(t, testRootCommandOptions(server))
				if err != nil {
					t.Fatal(err)
				}
				params := map[string]any{"run_id": runID, "anchor_kind": anchor, "status": "pending", "limit": 200}
				var first mailboxListResult
				if err := client.call(ctx, "mailbox.list", params, &first); err != nil {
					t.Fatal(err)
				}
				if err := validateMailboxListResult(first); err != nil {
					t.Fatal(err)
				}
				if len(first.Items) != 200 || first.NextCursor == "" {
					t.Fatalf("first page=%d cursor=%q", len(first.Items), first.NextCursor)
				}
				for _, item := range first.Items {
					if item.DecisionCard.CardID == target.CardID {
						t.Fatal("target must be past the first page")
					}
				}
				match := map[string]any{"anchor_kind": anchor}
				field := "category"
				if anchor == "proposed_effect" {
					field = "decision"
				}
				match[field] = "target"
				id, hash, err := (scenarioRunner{client: client}).findDecisionCard(ctx, mustScenarioExpressionEvaluator(t, nil), runID, match)
				if err != nil || id != target.CardID || hash != target.CardContentHash {
					t.Fatalf("late exact match id=%s hash=%s err=%v", id, hash, err)
				}
				// Mailbox positions are not anchor-bound. A decision-card cursor has
				// a different wire schema and must not be accepted as a mailbox cursor.
				params["cursor"] = decisioncard.EncodeCursor(target.CreatedAt, target.CardID)
				if err := client.call(ctx, "mailbox.list", params, &first); err == nil {
					t.Fatal("foreign decision-card cursor accepted by mailbox owner")
				}
				// Earlier concurrent insertion does not erase the later exact match.
				create(-1, "target")
				_, _, err = (scenarioRunner{client: client}).findDecisionCard(ctx, mustScenarioExpressionEvaluator(t, nil), runID, match)
				if err == nil || !strings.Contains(err.Error(), "returned 2 items") {
					t.Fatalf("cross-page ambiguity accepted: %v", err)
				}
			})
		}
	}
}

func createFactoringMailboxCard(t *testing.T, ctx context.Context, selected factoringMailboxStore, fact correlation.SourceArtifactFact, run, entity, kind, label string, now time.Time) decisioncard.Card {
	t.Helper()
	source := eventtest.RootRoutingSource(entity)
	outcomes := map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve"}, "reject": {Verdict: "reject"}}
	if kind == "human_task" {
		anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{RequesterAgentID: "requester", OperationID: decisioncardtest.HumanOperation(t, run, uuid.NewString()), Category: label, Scope: decisioncard.Scope{Kind: decisioncard.ScopeFlow, FlowInstance: "provider/one"}, Source: source})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := decisioncard.FreezeSnapshot("human_task", "Review", map[string]any{}, outcomes)
		if err != nil {
			t.Fatal(err)
		}
		card, err := decisioncard.New(decisioncard.Card{CardID: uuid.NewString(), RunID: run, Anchor: anchor, Snapshot: snapshot, ExecutionMode: "live", BundleHash: fact.BundleHash(), CreatedAt: now})
		if err != nil {
			t.Fatal(err)
		}
		continuation := decisioncard.HumanTaskContinuation{CardID: card.CardID, RunID: run, RequesterRoute: source.Route(), SourceEventID: uuid.NewString(), DeadlineAt: now.Add(24 * time.Hour), BudgetBundleHash: fact.BundleHash(), BudgetWindowStart: now, BudgetWindowEnd: now.Add(7 * 24 * time.Hour), State: decisioncard.HumanTaskContinuationPending, CreatedAt: now, UpdatedAt: now}
		if err := selected.CreateHumanTaskCard(ctx, card, continuation); err != nil {
			t.Fatal(err)
		}
		return card
	}
	input, err := canonicaljson.FromGo(map[string]any{"text": "Exact content"})
	if err != nil {
		t.Fatal(err)
	}
	owner := activityidentity.MustNodeOwner(identitytest.RootNode(t, "sender"))
	sourceID := uuid.NewString()
	request := activityidentity.RequestEventID(activityidentity.Fact{RunID: run, SourceEventID: sourceID, EntityID: entity, Owner: owner, ExecutionFlowID: ".", HandlerEventKey: "reply", ActivityID: "send_reply", Tool: "provider_write", Attempt: 1})
	continuation := decisioncard.ProposedEffectContinuation{CardID: decisioncard.ProposedEffectCardID(request, label), RunID: run, RequestEventID: request, ActivityID: "send_reply", Tool: "provider_write", BundleHash: fact.BundleHash(), WorkflowVersion: "1", Input: input, EffectClass: runtimecontracts.ActivityEffectClassNonIdempotentWrite, SuccessEvent: "send_reply.succeeded", FailureEvent: "send_reply.failed", RevisionEvent: "send_reply.revision_requested", RejectedEvent: "send_reply.rejected", RetryMaxAttempts: 1, ForkPolicy: runtimecontracts.ActivityForkRequireConfirmation, EntityID: entity, NodeID: owner.Key(), FlowID: ".", FlowInstance: run, HandlerEventKey: "reply", SourceEventID: sourceID, SourceRunID: run, SourceTaskID: "task", ExecutionMode: "live", State: decisioncard.ProposedEffectPending, CreatedAt: now, UpdatedAt: now}.Canonical()
	effect, err := continuation.EffectValue()
	if err != nil {
		t.Fatal(err)
	}
	continuation.EffectContentHash, err = canonicaljson.HashValue(effect)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{RequestEventID: request, ActivityID: continuation.ActivityID, Decision: label, Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: run, EntityID: entity}, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decisioncard.FreezeSnapshot(label, "Review reply", map[string]any{"input": input.Interface()}, outcomes)
	if err != nil {
		t.Fatal(err)
	}
	card, err := decisioncard.New(decisioncard.Card{CardID: continuation.CardID, RunID: run, Anchor: anchor, Snapshot: snapshot, ExecutionMode: "live", EffectContentHash: continuation.EffectContentHash, BundleHash: fact.BundleHash(), WorkflowVersion: "1", CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.CreateProposedEffectCard(ctx, card, continuation); err != nil {
		t.Fatalf("%s: %v", fmt.Sprint(kind, label), err)
	}
	return card
}
