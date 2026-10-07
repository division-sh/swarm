package serveapp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deadletters"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue2564DecisionCardReevaluatesSameLeaseAfterRealCASLossBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			name := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				name = "postgres"
			}
			_, start := issue2564ServeHarness(t, name, canonicalrouting.CopyMailboxNoticeCompletion(t), true)
			process, rt := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("serve stop=%d", code)
				}
			})
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "matrix-seed-" + uuid.NewString()})
			rt.waitEntityStage(t, seed.RunID, "", "review")
			rt.waitDeliveries(t, seed.RunID)
			if rt.Runtime == nil {
				t.Fatal("served control proof runtime is required for exact author activity scope")
			}
			runtimeID := strings.TrimSpace(rt.Runtime.Options.RuntimeInstanceID)
			fact := rt.Runtime.Options.SourceArtifactFact
			if runtimeID == "" || fact.BundleHash() == "" || fact.BundleHash() != strings.TrimSpace(rt.BundleHash) {
				t.Fatalf("served control proof scope = runtime %q fact %#v bundle %q", runtimeID, fact, rt.BundleHash)
			}
			if rt.Runtime.WorkOccurrence() == nil {
				t.Fatal("served control proof runtime work occurrence is required")
			}
			if rt.Runtime.Options.ProcessWorkOwner == nil {
				t.Fatal("served control proof process work owner is required")
			}
			ctx := correlation.WithRuntimeInstanceID(context.Background(), runtimeID)
			ctx = correlation.WithSourceArtifactFact(ctx, fact)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, fact.BundleHash()))
			ctx = worklifetime.WithProcess(ctx, rt.Runtime.Options.ProcessWorkOwner)
			ctx = worklifetime.WithOccurrence(ctx, rt.Runtime.WorkOccurrence())
			cardID := storetest.ObservePendingFixtureCard(t, ctx, rt.selected, seed.RunID)
			card, err := rt.persistence.deps.DecisionCards.GetDecisionCard(ctx, cardID)
			if err != nil {
				t.Fatal(err)
			}
			params := map[string]any{"card_id": card.CardID, "idempotency_key": uuid.NewString(), "verdict": "approve", "observed_content_hash": card.CardContentHash}
			raw, err := json.Marshal(map[string]any{"method": "mailbox.decide", "params": params})
			if err != nil {
				t.Fatal(err)
			}
			value, err := canonicaljson.Decode(raw)
			if err != nil {
				t.Fatal(err)
			}
			raw, err = canonicaljson.Encode(value)
			if err != nil {
				t.Fatal(err)
			}
			req := apiidempotency.Request{Method: "mailbox.decide", Actor: apiidempotency.PrincipalActor(storetest.ObserveFixturePrincipal(t, ctx, rt.selected)), ResourceID: cardID, RequestHash: fmt.Sprintf("sha256:%x", sha256.Sum256(raw)), IdempotencyKey: params["idempotency_key"].(string), Now: time.Now().UTC(), TTL: 24 * time.Hour}
			mutation, err := mailboxCardMutation(req, params)
			if err != nil {
				t.Fatal(err)
			}
			losses, attempts := 0, 0
			fault := &mailboxPostCommitFault{beforeCommit: func(ctx context.Context, command pipeline.DecisionCardMutationCommand) error {
				attempts++
				if command.GateState == nil {
					return fmt.Errorf("test did not reach the gate write")
				}
				if losses == 9 {
					return nil
				}
				state := command.GateState
				// This is a fault cut between R1 preparation and the real SQL
				// CAS, not a forged typed error or changed request input.
				if err := storetest.AdvanceGateHeaderRevision(ctx, rt.selected, *state); err != nil {
					return err
				}
				losses++
				return nil
			}}
			bus := &mailboxFaultPublicationBus{EventBus: rt.Runtime.Bus, fault: fault}
			deps := rt.persistence.deps
			coordinator := pipeline.NewPipelineCoordinatorWithOptions(bus, pipeline.PipelineCoordinatorOptions{
				Module: rt.Runtime.Options.WorkflowModule, ExecutionPosture: rt.Runtime.ExecutionPosture,
				ReceiverExecution: eventreceiver.NormalExecution(), DeliveryRuntime: bus, FlowRoutes: bus,
				SourceArtifactFact: fact, WorkOwner: rt.Runtime.WorkOccurrence(),
				Persistence:   pipeline.NewWorkflowPersistence(&mailboxFaultPersistence{WorkflowPersistenceOwner: rt.selected, fault: fault}),
				DeliveryStore: deps.DeliveryStore, DeadLetters: rt.selected.(deadletters.AcknowledgedRecorder), PipelineObligations: deps.PipelineObligations,
				DecisionCards: deps.DecisionCards, ProposedEffects: deps.ProposedEffects, HumanTasks: deps.DecisionCardHumanTasks,
				DecisionCardDraftExpiry: deps.DecisionCardDraftExpiry, HumanTaskExpiry: deps.HumanTaskExpiry,
				RunLifecycle: rt.selected.(runlifecycle.OperationOwner), RunBundleAvailability: deps.RunBundleAvailability,
			})
			if coordinator == nil {
				t.Fatal("real selected-store fault coordinator was not admitted")
			}
			response, replayed, err := coordinator.CommitDecisionCardMutation(ctx, req, mutation)
			if err != nil || replayed || attempts != 10 || losses != 9 {
				t.Fatalf("same request did not survive contention: attempts=%d losses=%d replay=%v err=%v", attempts, losses, replayed, err)
			}
			var result map[string]any
			if err := json.Unmarshal(response, &result); err != nil || result["ok"] != true {
				t.Fatalf("committed response=%s err=%v", response, err)
			}
			evidence := storetest.ObserveCardContention(t, ctx, rt.selected, req.IdempotencyKey, req.ResourceID)
			if evidence.Requests != 1 {
				t.Fatalf("request completion count=%d", evidence.Requests)
			}
			if evidence.DecidedChanges != 1 {
				t.Fatalf("partial or duplicate decision effects=%d", evidence.DecidedChanges)
			}
			var replay map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &replay)
			if replay["idempotency_replayed"] != true {
				t.Fatalf("request lost durable replay: %v", replay)
			}
			delete(replay, "idempotency_replayed")
			if !equalJSONMaps(result, replay) {
				t.Fatalf("replayed different result: original=%v replay=%v", result, replay)
			}
		})
	}
}

func equalJSONMaps(a, b map[string]any) bool {
	left, leftErr := json.Marshal(a)
	right, rightErr := json.Marshal(b)
	return leftErr == nil && rightErr == nil && string(left) == string(right)
}
