package runforkexecution

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
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
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func assertCompletion2270SelectedCLIReadback(t testing.TB, ctx context.Context, selected startupownership.Store, runID string) {
	t.Helper()
	reads := selected.(operatorread.ConversationReader)
	list, err := reads.ListOperatorConversations(ctx, operatorread.OperatorConversationListOptions{RunID: runID, AgentID: "test-agent"})
	if err != nil || len(list.Conversations) != 1 {
		t.Fatalf("selected CLI conversations: %+v, %v", list, err)
	}
	page, err := reads.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: list.Conversations[0].SessionID})
	if err != nil || len(page.Turns) != 1 {
		t.Fatalf("selected CLI turns: %+v, %v", page, err)
	}
	turn := page.Turns[0]
	if turn.ResolvedModel != "reported-selected-cli" || turn.CompletionEvidence.Request.RequestedModel != "claude-selected-fork" || turn.CompletionEvidence.Request.ModelAlias != "regular" || turn.CompletionEvidence.ProviderReportedCostUSD == nil || *turn.CompletionEvidence.ProviderReportedCostUSD != 0.001 || turn.Tokens == nil || turn.Tokens.Input != 7 || turn.Tokens.Output != 2 {
		t.Fatalf("selected CLI lost independent request/observation/price evidence: %+v", turn)
	}
}

func TestSelectedCompletion2270EvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, protocol := range []struct {
			backend, credential, first, last string
		}{
			{selection.BackendAnthropic, "ANTHROPIC_API_KEY",
				`{"model":"reported-B","usage":{"input_tokens":11,"output_tokens":3},"content":[{"type":"tool_use","id":"lookup-1","name":"lookup_data","input":{"query":"status"}}]}`,
				`{"model":"reported-C","usage":{"input_tokens":13,"output_tokens":4},"content":[{"type":"tool_use","id":"emit-1","name":"emit_task_completed","input":{}}]}`},
			{selection.BackendOpenAICompatible, selection.OpenAICompatibleCredentialEnv,
				`{"model":"reported-B","choices":[{"message":{"role":"assistant","tool_calls":[{"id":"lookup-1","type":"function","function":{"name":"lookup_data","arguments":"{\"query\":\"status\"}"}}]}}],"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14}}`,
				`{"model":"reported-C","choices":[{"message":{"role":"assistant","tool_calls":[{"id":"emit-1","type":"function","function":{"name":"emit_task_completed","arguments":"{}"}}]}}],"usage":{"prompt_tokens":13,"completion_tokens":4,"total_tokens":17}}`},
			{selection.BackendOpenAIResponses, selection.OpenAIResponsesCredentialEnv,
				`{"id":"response-1","model":"reported-B","output":[{"id":"lookup-1","type":"function_call","call_id":"lookup-1","name":"lookup_data","arguments":"{\"query\":\"status\"}"}],"usage":{"input_tokens":11,"output_tokens":3,"total_tokens":14}}`,
				`{"id":"response-2","model":"reported-C","output":[{"id":"emit-1","type":"function_call","call_id":"emit-1","name":"emit_task_completed","arguments":"{}"}],"usage":{"input_tokens":13,"output_tokens":4,"total_tokens":17}}`},
		} {
			t.Run(backend+"/"+protocol.backend, func(t *testing.T) {
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
				lookup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`{"status":"ready"}`))
				}))
				defer lookup.Close()
				root := runForkExecutionRepoRoot(t)
				loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: root, SourceRoot: selectedForkFrameContracts(t, root, lookup.URL), PlatformSpecPath: contracts.DefaultPlatformSpecFile(root)}
				loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
				if err != nil {
					t.Fatal(err)
				}
				var calls atomic.Int32
				provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var request map[string]any
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request["model"] != "request-A" {
						t.Errorf("selected primitive changed admitted request: %v, %v", request, err)
					}
					response := protocol.first
					if calls.Add(1) == 2 {
						response = protocol.last
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(response))
				}))
				defer provider.Close()
				if protocol.backend == selection.BackendAnthropic {
					target, err := url.Parse(provider.URL)
					if err != nil {
						t.Fatal(err)
					}
					previous := http.DefaultTransport
					http.DefaultTransport = selectedForkProviderRedirectTransport{target: target, base: previous}
					defer func() { http.DefaultTransport = previous }()
				}
				credentialStore, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := credentialStore.Set(ctx, protocol.credential, "test-key"); err != nil {
					t.Fatal(err)
				}
				runID, eventID := uuid.NewString(), uuid.NewString()
				at := time.Unix(1700002203, 0).UTC()
				bundle, found := semanticview.Bundle(loaded.Source)
				if !found || bundle.SourceArtifact == nil {
					t.Fatal("missing admitted source artifact")
				}
				storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact, StartedAt: at.Add(-time.Minute)})
				rootInstance := flowidentity.Stored(loaded.Source, semanticview.RootExecutionFlowID(loaded.Source), runID, runID, runID, "")
				event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, "task.assigned", "source-runtime", "", []byte(`{"entity_id":"`+runID+`"}`), 0, runID, events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, runID), runID), eventtest.RootRoutingSource(runID), at, executionmode.Live)
				route := selectedExecutionTestAgentRoute(t, runID, "test-agent", runID)
				route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: rootInstance.TemplateID, FlowInstance: rootInstance.InstancePath, EntityID: rootInstance.EntityID})
				storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, "subscribed")
				sourceCtx := correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, runID), loaded.SourceArtifactFact)
				command := selectedExecutionSourceFlowCommand(t, sourceCtx, loaded, event, rootInstance)
				if committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(sourceCtx, command); err != nil || !committed.Acknowledged {
					t.Fatalf("source flow construction: %+v, %v", committed, err)
				}
				storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
				result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
					SourceRunID: runID, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
					ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
					AgentRuntime: SelectedContractAgentRuntimeOptions{Config: selectedForkAPIProviderConfig(protocol.backend, "request-A", provider.URL), ProviderCredentials: credentialStore,
						ProcessCapability: selectedContractTestProcessCapability(t, ctx, selected), QuiescenceTimeout: selectedForkCapabilityProofQuiescenceTimeout},
				})
				if err != nil {
					t.Fatal(err)
				}
				reads := selected.(operatorread.ConversationReader)
				list, err := reads.ListOperatorConversations(ctx, operatorread.OperatorConversationListOptions{RunID: result.Materialization.ForkRunID, AgentID: "test-agent"})
				if err != nil || len(list.Conversations) != 1 {
					t.Fatalf("selected public conversations: %+v, %v", list, err)
				}
				page, err := reads.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: list.Conversations[0].SessionID})
				if err != nil || len(page.Turns) != 2 || calls.Load() != 2 {
					t.Fatalf("selected turns or repeated primitive: %+v, calls=%d, %v", page, calls.Load(), err)
				}
				observed := map[string]bool{}
				for _, turn := range page.Turns {
					if turn.CompletionEvidence.Request.RequestedModel != "request-A" || turn.CompletionEvidence.Request.BackendProfile != protocol.backend || len(turn.CompletionEvidence.Models) != 1 {
						t.Fatalf("selected evidence contradicted admission: %+v", turn)
					}
					observed[turn.ResolvedModel] = true
				}
				if !observed["reported-B"] || !observed["reported-C"] {
					t.Fatalf("selected rounds collapsed reports: %+v", observed)
				}
			})
		}
	}
}
