package runforkexecution

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642SupportedOneShotReturnsOwnChildReplyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected startupownership.Store
			var owner SelectedContractExecutionOwner
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, owner = s, selectedContractSQLiteExecutionOwnerForTest(t, s)
			} else {
				s := storetest.StartPostgresRuntimeStore(t)
				selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
			}
			repo, ctx := runForkExecutionRepoRoot(t), runForkTestContext(t)
			loader := nativeSelectedCrashLoader{admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: canonicalrouting.CopyOneShotReplySchedule(t), PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}}
			loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{Selection: runfork.RunForkContractSelection{Mode: "selected_contracts"}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if loaded.Cleanup != nil {
					if err := loaded.Cleanup(); err != nil {
						t.Error(err)
					}
				}
			})
			runID, at := uuid.NewString(), time.Now().UTC()
			ctx = effects.WithExecutionMode(correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact), runID), executionmode.Mock)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runForkTestRuntimeInstanceID, loaded.SourceArtifactFact.BundleHash()))
			bundle, _ := semanticview.Bundle(loaded.Source)
			storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin(),
				Artifact: bundle.SourceArtifact, BundleHash: loaded.SourceArtifactFact.BundleHash(), StartedAt: at})
			rt, _ := nodeReplySourceRuntime(t, ctx, selected, owner, loaded, executionposture.MockOnly)
			ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
			created, err := rt.Manager.EnsureFlowInstance(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: loaded.Source,
				Instance: flowidentity.Stored(loaded.Source, ".", runID, runID, "", ""), OccurredAt: at})
			if err != nil || !created {
				t.Fatalf("construct one-shot source: created=%t error=%v", created, err)
			}
			publishOneShotReplyInput(t, ctx, rt.Bus, runID, "request.started", []byte(`{"account_id":"one-shot-account","request_id":"one-shot-request"}`))
			sourceRows, err := storetest.ReadReplyContextRecords(ctx, selected, runID)
			if err != nil || len(sourceRows) != 1 || sourceRows[0].State != replycontext.StateOpen || sourceRows[0].ProviderFlowID != "." {
				t.Fatalf("supported request did not establish one open root return: %+v error=%v", sourceRows, err)
			}
			actor := actors.AgentConfig{ID: "schedule-agent", FlowID: ".", EntityID: runID, Permissions: []string{"schedule"}}
			declaration, found := semanticview.ResolveAgentDeclaration(loaded.Source, actor)
			if !found {
				t.Fatal("schedule actor lacks its exact authored declaration")
			}
			plan, err := declaration.NamePlan()
			if err != nil {
				t.Fatal(err)
			}
			name, err := plan.Materialize()
			if err != nil {
				t.Fatal(err)
			}
			actor.Identity, err = agentidentity.New(runID, name, agentidentity.RootRoute())
			if err != nil {
				t.Fatal(err)
			}
			driver := &publishedJoinSourceDriver{}
			clock, err := genericschedule.NewLifecycle(selected.(genericschedule.Store), driver, rt.Bus, rt.Bus.EngineDispatcher(), nil, executionposture.MockOnly)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := clock.Stop(context.Background()); err != nil {
					t.Error(err)
				}
			})
			executor := tools.NewExecutorWithOptions(rt.Bus, tools.ExecutorOptions{WorkflowSource: loaded.Source, GenericSchedules: clock})
			toolCtx := tools.WithActor(events.WithDeliveryContext(ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: sourceRows[0].ID}}), actor)
			input := map[string]any{"schedule_key": "own-return", "event_type": "provider.replied", "mode": "delay", "delay": "10ms",
				"payload": map[string]any{"account_id": "one-shot-account", "request_id": "one-shot-request", "result": "scheduled-child-only"}}
			answer, err := executor.Execute(toolCtx, "schedule", input)
			if err != nil {
				t.Fatalf("supported one-shot producer: %v", err)
			}
			id := answer.(map[string]any)["activation_id"].(string)
			sourceTimer, present, err := selected.(genericschedule.Store).LoadGenericScheduleActivation(ctx, id)
			if err != nil || !present || sourceTimer.Command.ReplyContext != sourceRows[0].ID || sourceTimer.Status != genericschedule.StatusActive {
				t.Fatalf("native producer dropped its immutable return: %+v present=%t error=%v", sourceTimer, present, err)
			}
			again, err := executor.Execute(toolCtx, "schedule", input)
			if err != nil || !reflect.DeepEqual(answer, again) && again.(map[string]any)["outcome"] != string(genericschedule.AdmissionExactReplay) || again.(map[string]any)["activation_id"] != id {
				t.Fatalf("native exact producer retry changed the timer: first=%+v again=%+v error=%v", answer, again, err)
			}
			publishOneShotReplyInput(t, ctx, rt.Bus, runID, "request.note", []byte(`{"note":"timer-armed-before-fork"}`))
			page, err := selected.(pendingReplyExecutionStore).ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID, EventName: "request.noted"}, Limit: 2})
			if err != nil || len(page.Events) != 1 {
				t.Fatalf("one-shot source has no exact cut: %+v error=%v", page, err)
			}
			before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, runID)
			if err != nil {
				t.Fatal(err)
			}
			manager := workspace.NewHostManager()
			options := workspace.DefaultHostConfig()
			options.WorkspaceRoot = t.TempDir()
			manager.SetConfig(options)
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: runID, At: page.Events[0].EventID, AllowSourceFreeze: true,
				Owner: owner, SourceLoader: loader, ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source), AgentRuntime: SelectedContractAgentRuntimeOptions{
					ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability, Workspace: manager,
					Config: &config.Config{LLM: config.LLMConfig{Backend: selection.BackendClaudeCLI}},
				}})
			if err != nil || !result.Activation.Activated {
				t.Fatalf("one-shot child projection/activation: %+v error=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			wait, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			var replies []replycontext.Record
			for {
				replies, err = storetest.ReadReplyContextRecords(wait, selected, child)
				if err != nil || len(replies) != 1 || replies[0].ID == sourceRows[0].ID {
					t.Fatalf("one-shot child has no own return: %+v error=%v", replies, err)
				}
				if replies[0].State == replycontext.StateTerminal {
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("child timer did not return: %+v error=%v", replies, wait.Err())
				case <-time.After(10 * time.Millisecond):
				}
			}
			outputs, err := selected.(pendingReplyExecutionStore).ListOperatorEvents(wait, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: child, EventName: "requester/request.finished"}, Limit: 2})
			if err != nil || len(outputs.Events) != 1 || outputs.NextCursor != "" {
				t.Fatalf("one-shot did not finish its own requester once: %+v error=%v", outputs, err)
			}
			after, err := storetest.ReadSelectedForkSourceDomain(wait, selected, runID)
			unchanged, present, timerErr := selected.(genericschedule.Store).LoadGenericScheduleActivation(wait, id)
			if err != nil || timerErr != nil || !present || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(sourceTimer, unchanged) {
				t.Fatalf("child one-shot changed source evidence: errors=%v/%v", err, timerErr)
			}
		})
	}
}

func publishOneShotReplyInput(t *testing.T, ctx context.Context, eventBus *bus.EventBus, runID, name string, payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	input := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), events.EventType(name), "one-shot-proof", "", payload,
		0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC(), executionmode.Mock)
	if err := eventBus.PublishAndWait(ctx, input); err != nil {
		t.Fatalf("publish actual one-shot %s input: %v", name, err)
	}
}
