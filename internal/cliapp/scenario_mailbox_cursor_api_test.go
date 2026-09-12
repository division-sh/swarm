package cliapp

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Canonical writers establish the >200-card dataset; the selector and HTTP API
// are real. This is not source-authored creation, a serve boot or a mutation E2E.
func TestScenarioMailboxActualServerContinuationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			var selected interface {
				decisioncard.Store
				decisioncard.ProposedEffectStore
				apiv1.MailboxAPIStore
			}
			runID := uuid.NewString()
			fact, err := correlation.NewSourceArtifactFact(storetest.SemanticFixtureBundleHash)
			if err != nil {
				t.Fatal(err)
			}
			instance := uuid.NewString()
			ctx := correlation.WithSourceArtifactFact(correlation.WithRuntimeInstanceID(context.Background(), instance), fact)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(instance, fact.BundleHash()))
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
			entity := uuid.NewString()
			create := func(i int, decision string) decisioncard.Card {
				t.Helper()
				source := eventtest.ConcreteTemplateRoutingSource("review", "review/one", entity)
				anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{Route: flowidentity.RouteForInstancePath("review/one"), FlowID: "review", EntityID: entity, Stage: "review", StageActivationID: uuid.NewString(), Source: source})
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := decisioncard.FreezeSnapshot(decision, "Review", map[string]any{}, map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "done"}})
				if err != nil {
					t.Fatal(err)
				}
				card, err := decisioncard.New(decisioncard.Card{CardID: uuid.NewString(), RunID: runID, Anchor: anchor, ExecutionMode: "live", Snapshot: snapshot, BundleHash: fact.BundleHash(), WorkflowVersion: "1", CreatedAt: base.Add(time.Duration(i) * time.Second)})
				if err != nil {
					t.Fatal(err)
				}
				if err := selected.CreateDecisionCard(ctx, card); err != nil {
					t.Fatal(err)
				}
				return card
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
			runner := scenarioRunner{client: client}
			match := map[string]any{"anchor_kind": "stage_gate", "entity_id": entity, "decision": "target"}
			id, hash, err := runner.findDecisionCard(context.Background(), mustScenarioExpressionEvaluator(t, nil), runID, match)
			if err != nil || id != target.CardID || hash != target.CardContentHash {
				t.Fatalf("late target: id=%s hash=%s err=%v", id, hash, err)
			}
			// A new earlier match must not conceal the existing later match.
			create(-1, "target")
			if _, _, err := runner.findDecisionCard(context.Background(), mustScenarioExpressionEvaluator(t, nil), runID, match); err == nil || !strings.Contains(err.Error(), "returned 2 items") {
				t.Fatalf("cross-page ambiguity accepted: %v", err)
			}
		})
	}
}
