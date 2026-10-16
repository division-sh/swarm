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
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestRunStallAttributionAfterCompletionBothStores(t *testing.T) {
	for _, adapter := range []string{"anthropic_api", "claude_cli"} {
		t.Run(adapter, func(t *testing.T) {
			for _, phase := range []string{"projected", "consumed", "reclaimed", "drained"} {
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
						if phase == "drained" {
							transition := supersedeProviderDrainFixture(t, fixture, manager.AgentLifecycleTerminated)
							if transition.ProviderDrainCount != 1 {
								t.Fatalf("historical drain count = %d", transition.ProviderDrainCount)
							}
						}
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
						if phase != "drained" {
							if err := effects.AttachCompletionContinuationEvidence(settlement.Settlement.Evidence, []byte(request), payload); err != nil {
								t.Fatal(err)
							}
						}
						if result, err := handle.SettleCompletion(ctx, settlement); err != nil || !result.Committed {
							t.Fatalf("settle: %+v %v", result, err)
						}
						continuation, ok := handle.CompletionContinuation()
						if !ok && phase != "drained" {
							t.Fatal("settled completion has no canonical continuation")
						}
						claim := fixture.origin
						if phase == "reclaimed" {
							expireProviderOriginLease(t, fixture, fixture.origin)
							deliveryStore := fixture.store.(deliveryFixtureStore)
							snapshot, err := deliveryStore.Snapshot(ctx, fixture.origin.DeliveryID())
							if err != nil {
								t.Fatal(err)
							}
							reclaimed, err := deliveryStore.ClaimDelivery(ctx, snapshot.Authority, loadCompletionOriginEvent(t, fixture, snapshot.EventID), snapshot.Route)
							if err != nil {
								t.Fatal(err)
							}
							acquired, ok := reclaimed.Acquired()
							if !ok {
								t.Fatalf("reclaim disposition = %s", reclaimed.Disposition)
							}
							claim = acquired.Claim
							successor := fixture.authority
							successor.Normal.Generation++
							successor.FenceGeneration = successor.Normal.Generation
							successor.ExecutionOwner = "stall-attribution-successor"
							successor.Target = effects.UsageTarget{}
							setCompletionFixtureGeneration(t, fixture, int(successor.Normal.Generation))
							ctx = effects.WithLifecycleToken(ctx, successor.Normal)
							ctx = effects.WithAuthority(ctx, successor)
							ctx = managedExecutionStoreTestContext(t, ctx)
							ctx = deliverylifecycle.WithClaim(ctx, claim)
							controller := newCompletionControllerForTest(fixture.store)
							ctx = effects.WithController(ctx, controller)
							originalAttempt := handle.Attempt().AttemptID
							handle, ok, err = controller.RecoverCompletionContinuation(ctx, fixture.sessionID, settlement.AgentTurn.Memory)
							if err != nil || !ok || handle.Attempt().AttemptID != originalAttempt {
								t.Fatalf("recover exact consumed predecessor: found=%t err=%v", ok, err)
							}
							lease, err := fixture.store.(sessions.Registry).Acquire(ctx, settlement.AgentTurn.Identity, fixture.leaseHolder)
							if err != nil {
								t.Fatal(err)
							}
							if err := handle.BindRecoveredContinuationGrant(effects.SessionGrant{SessionID: lease.SessionID, GrantID: lease.GrantID, LockOwner: lease.LockOwner}); err != nil {
								t.Fatal(err)
							}
							continuation, ok = handle.CompletionContinuation()
							if !ok {
								t.Fatal("recovered head lost its continuation")
							}
						}
						if phase != "drained" {
							if err := handle.ProjectCompletionConversation(ctx, effects.CompletionConversationProjection{
								SessionID: fixture.sessionID, Identity: settlement.AgentTurn.Identity, Memory: settlement.AgentTurn.Memory,
								ExpectedTurnCount: 0, TurnCount: 1, Messages: mustJSON(t, messages), Payload: continuation.Payload,
							}); err != nil {
								t.Fatal(err)
							}
						}
						if phase == "consumed" || phase == "reclaimed" {
							if err := handle.ConsumeCompletionResponse(ctx, nil); err != nil {
								t.Fatal(err)
							}
						}
						if phase != "drained" {
							if _, err := fixture.store.SettleSuccess(ctx, claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
								t.Fatal(err)
							}
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
							if phase == "drained" {
								if got != nil || len(report.OperationalFacts.HistoricalTurns) != 1 {
									t.Fatalf("drained turn blamed its replacement: current=%+v historical=%+v", got, report.OperationalFacts.HistoricalTurns)
								}
								got = &report.OperationalFacts.HistoricalTurns[0]
								if !got.Historical || got.EvidenceReason != "historical_drained_completion" {
									t.Fatalf("drained attribution = %+v", got)
								}
							}
							wantEmission := operatorread.RunEmissionUnknown
							if phase == "consumed" || phase == "reclaimed" {
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
