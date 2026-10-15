package runforkexecution

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func proveSelectedQ1SessionSiblingReforkBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, variant := range []string{"sibling_then_refork", "refork_without_sibling"} {
			t.Run(backend+"/"+variant, func(t *testing.T) {
				ctx := runForkTestContext(t)
				var selected startupownership.Store
				var owner SelectedContractExecutionOwner
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					selected, owner = s, selectedContractSQLiteExecutionOwnerForTest(t, s)
				} else {
					s, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)
					selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
				}
				var providerCalls, lookupCalls atomic.Int32
				lookup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					lookupCalls.Add(1)
					_, _ = w.Write([]byte(`{"status":"ready"}`))
				}))
				defer lookup.Close()
				provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var request map[string]any
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request["model"] != "request-A" {
						t.Errorf("fork provider request: model=%v err=%v", request["model"], err)
					}
					response := `{"model":"reported-Q1","choices":[{"message":{"role":"assistant","tool_calls":[{"id":"lookup-1","type":"function","function":{"name":"lookup_data","arguments":"{\"query\":\"status\"}"}}]}}],"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14}}`
					if providerCalls.Add(1)%2 == 0 {
						response = `{"model":"reported-Q1","choices":[{"message":{"role":"assistant","tool_calls":[{"id":"emit-1","type":"function","function":{"name":"emit_task_completed","arguments":"{}"}}]}}],"usage":{"prompt_tokens":13,"completion_tokens":4,"total_tokens":17}}`
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(response))
				}))
				defer provider.Close()
				repo := runForkExecutionRepoRoot(t)
				loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: selectedForkFrameContracts(t, repo, lookup.URL), PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
				loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
				if err != nil {
					t.Fatal(err)
				}
				credentialStore, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := credentialStore.Set(ctx, selection.OpenAICompatibleCredentialEnv, "test-key"); err != nil {
					t.Fatal(err)
				}
				runID, eventID := uuid.NewString(), uuid.NewString()
				at := time.Unix(1700002203, 0).UTC()
				bundle, found := semanticview.Bundle(loaded.Source)
				if !found || bundle.SourceArtifact == nil {
					t.Fatal("missing admitted source artifact")
				}
				storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact, StartedAt: at.Add(-time.Minute)})
				root := flowidentity.Stored(loaded.Source, semanticview.RootExecutionFlowID(loaded.Source), runID, runID, runID, "")
				input := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, "task.assigned", "source-runtime", "", []byte(`{"entity_id":"`+runID+`"}`), 0, runID, events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, runID), runID), eventtest.RootRoutingSource(runID), at, executionmode.Live)
				route := selectedExecutionTestAgentRoute(t, runID, "test-agent", runID)
				route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: root.TemplateID, FlowInstance: root.InstancePath, EntityID: root.EntityID})
				storetest.CommitSemanticEventWithRoutes(t, ctx, selected, input, []events.DeliveryRoute{route}, "subscribed")
				sourceCtx := correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, runID), loaded.SourceArtifactFact)
				command := selectedExecutionSourceFlowCommand(t, sourceCtx, loaded, input, root)
				if committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(sourceCtx, command); err != nil || !committed.Acknowledged {
					t.Fatalf("source flow construction: %+v err=%v", committed, err)
				}
				storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
				reads := selected.(operatorread.ConversationReader)
				sessionIDs, turnIDs := map[string]bool{}, map[string]bool{}
				readTurns := func(id string) operatorread.OperatorConversationTurnListResult {
					t.Helper()
					list, err := reads.ListOperatorConversations(ctx, operatorread.OperatorConversationListOptions{RunID: id, AgentID: "test-agent"})
					if err != nil || len(list.Conversations) != 1 || list.Conversations[0].RunID != id {
						t.Fatalf("fork-local conversation: run=%s list=%+v err=%v", id, list, err)
					}
					page, err := reads.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: list.Conversations[0].SessionID})
					if err != nil || len(page.Turns) != 2 {
						t.Fatalf("fork-local turns: run=%s page=%+v err=%v", id, page, err)
					}
					return page
				}
				execute := func(sourceID, point string) SelectedContractExecutionResult {
					t.Helper()
					callsBefore, lookupBefore := providerCalls.Load(), lookupCalls.Load()
					result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
						SourceRunID: sourceID, At: point, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
						ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
						AgentRuntime: SelectedContractAgentRuntimeOptions{Config: selectedForkAPIProviderConfig(selection.BackendOpenAICompatible, "request-A", provider.URL), ProviderCredentials: credentialStore,
							ProcessCapability: selectedContractTestProcessCapability(t, ctx, selected), QuiescenceTimeout: selectedForkCapabilityProofQuiescenceTimeout},
					})
					if err != nil {
						t.Fatalf("selected session fork: source=%s point=%s provider_delta=%d lookup_delta=%d err=%v", sourceID, point, providerCalls.Load()-callsBefore, lookupCalls.Load()-lookupBefore, err)
					}
					if !result.Activation.Activated || result.Materialization.SourceRunID != sourceID || result.Materialization.ForkRunID == sourceID || result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 {
						t.Fatalf("selected session fork: source=%s point=%s result=%+v err=%v", sourceID, point, result, err)
					}
					if providerCalls.Load()-callsBefore != 2 || lookupCalls.Load()-lookupBefore != 1 {
						t.Fatalf("fork repeated or omitted work: provider=%d lookup=%d", providerCalls.Load()-callsBefore, lookupCalls.Load()-lookupBefore)
					}
					page := readTurns(result.Materialization.ForkRunID)
					if page.Conversation.SessionID == "" || sessionIDs[page.Conversation.SessionID] {
						t.Fatalf("fork reused a source/sibling session: %+v", page.Conversation)
					}
					sessionIDs[page.Conversation.SessionID] = true
					for _, turn := range page.Turns {
						if turn.TurnID == "" || turnIDs[turn.TurnID] || turn.TriggerEventID != result.ForkEvents[0].ForkEventID || turn.CompletionEvidence.Request.RequestedModel != "request-A" || turn.ResolvedModel != "reported-Q1" || !turn.ParseOK || turn.Failure != nil {
							t.Fatalf("fork reused or misattributed completion: %+v", turn)
						}
						turnIDs[turn.TurnID] = true
					}
					return result
				}
				first := execute(runID, eventID)
				firstID, firstPoint := first.Materialization.ForkRunID, first.ForkEvents[0].ForkEventID
				before := readTurns(firstID)
				plan, err := owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: firstID, At: firstPoint})
				if err != nil {
					t.Fatal(err)
				}
				var pending *runfork.RunForkPendingWork
				for i := range plan.PendingWork {
					if plan.PendingWork[i].EventID == firstPoint && plan.PendingWork[i].SubscriberType == "agent" {
						pending = &plan.PendingWork[i]
					}
				}
				if pending == nil || pending.Classification != runfork.RunForkPendingClassificationPending || pending.SourceExecution != (deliverylifecycle.SourceExecutionAtCut{}) {
					t.Fatalf("past publication cut used current session/claim state: %+v", plan.PendingWork)
				}
				current, err := selected.(deliverylifecycle.Store).Snapshot(ctx, pending.DeliveryID)
				if err != nil || current.Status != deliverylifecycle.StatusDelivered || current.ClaimVersion == 0 {
					t.Fatalf("refork source did not actually execute/settle its session input: %+v err=%v", current, err)
				}
				children := int32(2)
				var siblingID string
				if variant == "sibling_then_refork" {
					siblingID = execute(runID, eventID).Materialization.ForkRunID
					children++
				}
				grandchild := execute(firstID, firstPoint)
				if siblingID == firstID || grandchild.Materialization.ForkRunID == firstID || grandchild.Materialization.ForkRunID == siblingID {
					t.Fatal("independent sibling/refork operations shared a child run")
				}
				after := readTurns(firstID)
				if !reflect.DeepEqual(before.Turns, after.Turns) || before.Conversation.SessionID != after.Conversation.SessionID {
					t.Fatalf("refork changed retained source conversation: before=%+v after=%+v", before, after)
				}
				again, err := owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: firstID, At: firstPoint})
				if err != nil || !reflect.DeepEqual(plan.PendingWork, again.PendingWork) || !reflect.DeepEqual(plan.ReplayResumeAdmission, again.ReplayResumeAdmission) {
					t.Fatalf("sibling/refork changed older-cut decisions: before=%+v after=%+v err=%v", plan, again, err)
				}
				if providerCalls.Load() != 2*children || lookupCalls.Load() != children || len(sessionIDs) != int(children) || len(turnIDs) != 2*int(children) {
					t.Fatalf("whole lineage accounting: provider=%d lookup=%d sessions=%d turns=%d", providerCalls.Load(), lookupCalls.Load(), len(sessionIDs), len(turnIDs))
				}
			})
		}
	}
}
