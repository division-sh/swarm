package runtimepersistence

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestRunStallAttributionAfterCompletionBothStores(t *testing.T) {
	for _, adapter := range []string{"anthropic_api", "claude_cli"} {
		t.Run(adapter, func(t *testing.T) {
			for _, phase := range []string{"projected", "consumed"} {
				t.Run(phase, func(t *testing.T) {
					eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
						fixture := newCompletionSettlementFixture(t, selected.selected.(completionSettlementTestStore), selected.db, !selected.postgres)
						bundle, err := contracts.LoadWorkflowContractBundleFromArtifact("", sourceartifactfixture.Artifact(), platform.PlatformSpecDisplayPath, contracts.WorkflowContractLoadOptions{})
						if err != nil {
							t.Fatal(err)
						}
						ctx := correlation.WithRunID(fixture.context, fixture.origin.RunID())
						ctx = correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.Fact())
						ctx = effects.WithLifecycleToken(ctx, fixture.authority.Normal)
						constructStaticPublicationSourceFixture(t, ctx, fixture.store.(agentFixtureFlowStore), bundle, "global")
						request := "stall-attribution-" + adapter
						ctx = effects.WithLogicalOperationIdentity(ctx, request)
						handle := beginObservedCompletionForSettlementTest(t, ctx, adapter, request)
						settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, adapter, "", "")
						settlement.ProviderHead = nil
						settlement.Now = settlement.Now.Truncate(time.Microsecond)
						payload, messages := terminalManagedCompletionPayload(t, fixture, settlement)
						var document map[string]json.RawMessage
						if err := json.Unmarshal(payload, &document); err != nil {
							t.Fatal(err)
						}
						var response llm.Response
						if err := json.Unmarshal(document["response"], &response); err != nil {
							t.Fatal(err)
						}
						response.ToolOutputAuthority = &llm.ToolOutputAuthority{ProviderOperationID: handle.Attempt().OperationID, SettledAt: settlement.Now.UTC()}
						document["response"] = mustJSON(t, response)
						document["adapter"] = mustJSON(t, adapter)
						payload = mustJSON(t, document)
						if err := effects.AttachCompletionContinuationEvidence(settlement.Settlement.Evidence, []byte(request), payload); err != nil {
							t.Fatal(err)
						}
						if result, err := handle.SettleCompletion(ctx, settlement); err != nil || !result.Committed {
							t.Fatalf("settle: %+v %v", result, err)
						}
						continuation, ok := handle.CompletionContinuation()
						if !ok {
							t.Fatal("settled completion has no canonical continuation")
						}
						if err := handle.ProjectCompletionConversation(ctx, effects.CompletionConversationProjection{
							SessionID: fixture.sessionID, Identity: settlement.AgentTurn.Identity, Memory: settlement.AgentTurn.Memory,
							ExpectedTurnCount: 0, TurnCount: 1, Messages: mustJSON(t, messages), Payload: continuation.Payload,
						}); err != nil {
							t.Fatal(err)
						}
						if phase == "consumed" {
							if err := handle.ConsumeCompletionResponse(ctx, nil); err != nil {
								t.Fatal(err)
							}
						}
						if _, err := fixture.store.SettleSuccess(ctx, fixture.origin, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
							t.Fatal(err)
						}
						reader := fixture.store.(operatorread.RunReader)
						for read := 0; read < 2; read++ {
							report, err := reader.LoadRunDebugReport(ctx, fixture.origin.RunID(), operatorread.RunDebugQueryOptions{})
							if err != nil {
								t.Fatal(err)
							}
							if report.OperationalFacts == nil {
								t.Fatal("missing snapshot facts")
							}
							var got *operatorread.RunTurnAttribution
							for _, instance := range report.OperationalFacts.Instances {
								if instance.FlowInstance == "global" {
									got = instance.LastTurn
								}
							}
							wantEmission := operatorread.RunEmissionUnknown
							if phase == "consumed" {
								wantEmission = operatorread.RunEmissionNone
							}
							if got == nil || got.DeliveryID != fixture.origin.DeliveryID() || got.AgentID != fixture.agentID || got.Emission != wantEmission || len(got.ProviderTurns) != 1 || len(got.EmittedEvents) != 0 {
								t.Fatalf("exact settled silence attribution = %+v", got)
							}
						}
						requireProviderAttemptCount(t, fixture, 1)
					})
				})
			}
		})
	}
}
