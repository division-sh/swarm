package serveapp

import (
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestServedFieldlessGateDeferralBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, fields := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fields=%t", backend, fields), func(t *testing.T) {
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyConstructedGateMutationControl(t, fields))
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "item.received", "bundle_hash": rt.BundleHash,
					"payload": map[string]any{}, "idempotency_key": "fieldless-gate-seed",
				})
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
				waitForkReceiverSourceCompletion(t, rt, seed.RunID)
				var cardID string
				if err := rt.DB.QueryRow(`SELECT card_id FROM decision_cards WHERE run_id=$1 AND status='pending'`, seed.RunID).Scan(&cardID); err != nil {
					t.Fatal(err)
				}
				params := map[string]any{
					"card_id": cardID, "idempotency_key": "fieldless-gate-deferral",
					"until": time.Now().UTC().Add(time.Hour).Truncate(time.Second).Format(time.RFC3339Nano),
				}
				req := mailboxPrincipalRequest(t, rt, "mailbox.defer", params)
				mutation, err := mailboxCardMutation(req, params)
				if err != nil {
					t.Fatal(err)
				}
				ctx := servedControlProofAuthorActivityContext(t, rt)
				if _, replayed, err := rt.Runtime.Pipeline.CommitDecisionCardMutation(ctx, req, mutation); err != nil || replayed {
					t.Fatalf("real constructed gate deferral: replay=%t err=%v", replayed, err)
				}
				var replay map[string]any
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.defer", params, &replay)
				if replay["idempotency_replayed"] != true {
					t.Fatalf("served replay repeated gate deferral: %v", replay)
				}
				requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "mailbox.defer", "fieldless-gate-deferral", 1)
				var fieldRows int
				wantRows := 0
				if fields {
					wantRows = 1
				}
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, seed.RunID).Scan(&fieldRows); err != nil || fieldRows != wantRows {
					t.Fatalf("deferral changed declared field-companion census: rows=%d want=%d err=%v", fieldRows, wantRows, err)
				}
			})
		}
	}
}

func TestServedConstructedDecisionCardDeferralBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedControlProofRuntimeWithFixture(t, backend, func(t *testing.T) string {
				return canonicalrouting.CopyRootIngressServedDecisionControl(t)
			})
			source := rt.Runtime.Pipeline.SemanticSource()
			bundle, found := semanticview.Bundle(source)
			if !found || bundle.SourceArtifact == nil {
				t.Fatal("constructed decision requires its admitted source artifact")
			}
			var owner interface {
				storetest.RunFixtureStore
				decisioncard.Store
			}
			switch backend {
			case servedparity.BackendDefaultSQLite:
				owner = rt.SQLite
			case servedparity.BackendExplicitPostgres:
				owner = rt.Postgres
			default:
				t.Fatalf("unknown decision deferral backend %q", backend)
			}
			runID := uuid.NewString()
			ctx := servedControlProofAuthorActivityContext(t, rt)
			constructionCtx := effects.WithExecutionMode(correlation.WithRunID(ctx, runID), effects.ExecutionModeLive)
			storetest.RequireRun(t, constructionCtx, owner, storetest.RunFixture{
				Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact,
			})
			plan, err := rt.Runtime.Manager.PrepareFlowInstanceActivation(constructionCtx, pipeline.FlowInstanceActivationRequest{
				ContractBundle: source, OccurredAt: time.Now().UTC(),
				Instance: flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, runID, ""),
			})
			if err != nil {
				t.Fatalf("prepare constructed decision: %v", err)
			}
			committed, err := rt.Runtime.Bus.CommitFlowInstanceActivation(constructionCtx, plan)
			if err != nil || !committed.Acknowledged || !committed.Created {
				t.Fatalf("commit constructed decision: acknowledged=%t created=%t err=%v", committed.Acknowledged, committed.Created, err)
			}
			if err := rt.Runtime.Pipeline.FinalizeInitialEntryLifecycle(constructionCtx, committed.Lifecycle); err != nil {
				t.Fatalf("finalize constructed decision: %v", err)
			}
			if len(plan.Lifecycle.GateCards) != 1 {
				t.Fatalf("constructed decision gate cards = %d, want 1", len(plan.Lifecycle.GateCards))
			}
			card, err := owner.GetDecisionCard(constructionCtx, plan.Lifecycle.GateCards[0].Card.CardID)
			if err != nil || card.RunID != runID || card.Status != decisioncard.StatusPending || card.Snapshot.Decision != "launch_review" {
				t.Fatalf("constructed pending decision: card=%+v err=%v", card, err)
			}
			fixture := servedDecisionCardFixture{CardID: card.CardID}
			params := map[string]any{
				"card_id": fixture.CardID, "idempotency_key": "constructor-deferral",
				"until": time.Now().UTC().Add(time.Hour).Truncate(time.Second).Format(time.RFC3339Nano),
			}
			req := mailboxPrincipalRequest(t, rt, "mailbox.defer", params)
			mutation, err := mailboxCardMutation(req, params)
			if err != nil {
				t.Fatal(err)
			}
			if _, replayed, err := rt.Runtime.Pipeline.CommitDecisionCardMutation(ctx, req, mutation); err != nil || replayed {
				t.Fatalf("constructed decision deferral: replay=%t err=%v", replayed, err)
			}
			var replay map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.defer", params, &replay)
			if replay["idempotency_replayed"] != true {
				t.Fatalf("served replay repeated committed deferral: %v", replay)
			}
			requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "mailbox.defer", "constructor-deferral", 1)
		})
	}
}
