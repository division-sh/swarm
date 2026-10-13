package runforkexecution

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
	for _, variant := range []struct{ backend, dueMode string }{
		{"sqlite", "delay"}, {"sqlite", "absolute"}, {"postgres", "delay"}, {"postgres", "absolute"},
	} {
		t.Run(variant.backend+"/"+variant.dueMode, func(t *testing.T) {
			backend := variant.backend
			var selected startupownership.Store
			var owner SelectedContractExecutionOwner
			var reopen func() startupownership.Store
			var construct func() SelectedContractExecutionOwner
			if backend == "sqlite" {
				s, again := storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background())
				selected, owner = s, selectedContractSQLiteExecutionOwnerForTest(t, s)
				reopen = func() startupownership.Store { s = again(); return s }
				construct = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
			} else {
				s, again := storetest.StartPostgresRuntimeStoreWithReopen(t)
				selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
				reopen = func() startupownership.Store { s = again(); return s }
				construct = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
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
			rt, closeServing := nodeReplySourceRuntime(t, ctx, selected, owner, loaded, executionposture.MockOnly)
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
			if variant.dueMode == "absolute" {
				delete(input, "delay")
				input["mode"], input["at"] = "absolute", time.Now().UTC().Add(10*time.Millisecond).Format(time.RFC3339Nano)
			}
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
			process, found := worklifetime.ProcessFromContext(ctx)
			if !found {
				t.Fatal("one-shot requires its exact process owner")
			}
			baseline := process.ActiveCount()
			contractSelection := runforkadmission.SelectedContractSelection(loaded.Source)
			operation := &runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "one-shot-proof", IdempotencyKey: "own-return",
				TransportHash: "one-shot-proof-transport", SourceRunID: runID, ForkEventID: page.Events[0].EventID,
				TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: contractSelection}
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: runID, At: page.Events[0].EventID, AllowSourceFreeze: true,
				Owner: owner, SourceLoader: loader, ForkOperation: operation, ContractSelection: contractSelection, AgentRuntime: SelectedContractAgentRuntimeOptions{
					ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability, Workspace: manager,
					Config: &config.Config{LLM: config.LLMConfig{Backend: selection.BackendClaudeCLI}},
				}})
			if err != nil || !result.Activation.Activated {
				t.Fatalf("one-shot child projection/activation: %+v error=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			acknowledged, present, err := selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			}).LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !present || acknowledged.Status != runfork.ForkOperationActivated || acknowledged.ForkRunID != child || acknowledged.Result == nil {
				t.Fatalf("one-shot has no exact durable activation acknowledgment: %+v present=%t error=%v", acknowledged, present, err)
			}
			wait, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			var replies []replycontext.Record
			for {
				replies, err = storetest.ReadReplyContextRecords(wait, selected, child)
				if err != nil || len(replies) != 1 || replies[0].ID == sourceRows[0].ID {
					t.Fatalf("one-shot child has no own return: %+v error=%v", replies, err)
				}
				if replies[0].State == replycontext.StateTerminal {
					completed, readErr := selected.(pendingReplyExecutionStore).ListOperatorEvents(wait, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: child, EventName: "requester/request.finished"}, Limit: 2})
					if readErr != nil || len(completed.Events) > 1 || completed.NextCursor != "" {
						t.Fatalf("one-shot requester settlement: %+v error=%v", completed, readErr)
					}
					if len(completed.Events) == 1 {
						settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, child)
						pending, pendingErr := storetest.ReadServedIncompletePipelineHandoffCount(wait, selected, child)
						if err != nil || pendingErr != nil || settlement.DeadLetter != 0 || settlement.Canceled != 0 {
							t.Fatalf("one-shot final handoff: settlement=%+v pending=%d errors=%v/%v", settlement, pending, err, pendingErr)
						}
						if settlement.Delivered > 0 && settlement.Total == settlement.Delivered && pending == 0 {
							break
						}
					}
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
			cleanup, finish := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer finish()
			if err := owner.RetireSelectedContexts(cleanup); err != nil || process.ActiveCount() != baseline {
				t.Fatalf("one-shot selected retirement: work=%d baseline=%d error=%v", process.ActiveCount(), baseline, err)
			}
			after, err := storetest.ReadSelectedForkSourceDomain(wait, selected, runID)
			unchanged, present, timerErr := selected.(genericschedule.Store).LoadGenericScheduleActivation(wait, id)
			if err != nil || timerErr != nil || !present || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(sourceTimer, unchanged) {
				t.Fatalf("child one-shot changed source evidence: errors=%v/%v", err, timerErr)
			}
			childBefore, err := storetest.ReadSelectedForkSourceDomain(cleanup, selected, child)
			if err != nil {
				t.Fatal(err)
			}
			if err := clock.Stop(cleanup); err != nil {
				t.Fatal(err)
			}
			closeServing()
			if err := rt.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if err := owner.ports.contexts.capability.Release(cleanup); err != nil {
				t.Fatal(err)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			selected = reopen()
			restarted := runForkTestContext(t)
			capability := selectedContractTestProcessCapability(t, restarted, selected)
			successor := construct()
			if err := successor.BindSelectedProcess(restarted, process, capability); err != nil {
				t.Fatal(err)
			}
			successorBaseline := process.ActiveCount()
			recovery, recoverDone := context.WithTimeout(restarted, 20*time.Second)
			defer recoverDone()
			entries, err := successor.RecoverSelectedForkContexts(recovery, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{
				SourceLoader: loader, AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability,
					Workspace: manager, Config: &config.Config{LLM: config.LLMConfig{Backend: selection.BackendClaudeCLI}}},
			})
			if err != nil || len(entries) != 1 || entries[0].RunID != child || entries[0].Operation == nil || !reflect.DeepEqual(*entries[0].Operation, acknowledged) {
				t.Fatalf("one-shot successor lost its exact acknowledged child: %+v error=%v", entries, err)
			}
			cleanup2, finish2 := context.WithTimeout(context.WithoutCancel(restarted), 10*time.Second)
			defer finish2()
			if err := successor.RetireSelectedContexts(cleanup2); err != nil || process.ActiveCount() != successorBaseline {
				t.Fatalf("one-shot successor retirement: work=%d baseline=%d error=%v", process.ActiveCount(), successorBaseline, err)
			}
			childAfter, err := storetest.ReadSelectedForkSourceDomain(cleanup2, selected, child)
			sourceAfter, sourceErr := storetest.ReadSelectedForkSourceDomain(cleanup2, selected, runID)
			repliesAfter, replyErr := storetest.ReadReplyContextRecords(cleanup2, selected, child)
			if err != nil || sourceErr != nil || replyErr != nil || !reflect.DeepEqual(before, sourceAfter) || !reflect.DeepEqual(replies, repliesAfter) {
				t.Fatalf("one-shot recovery repeated business work or changed its own reply/source: errors=%v/%v/%v", err, sourceErr, replyErr)
			}
			requireOneShotRecoveryDomain(t, child, actor.Identity.AgentID(), childBefore, childAfter)
		})
	}
}

// A fresh lifecycle takeover records its own diagnostics. Every pre-existing
// row and every business/delivery/receipt/state row must remain unchanged.
func requireOneShotRecoveryDomain(t *testing.T, child, agentID string, before, after map[string]storetest.SelectedForkStorageTableSnapshot) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatal("one-shot recovery changed the domain census")
	}
	for table, original := range before {
		current := after[table]
		if table != "events" {
			if !reflect.DeepEqual(original, current) {
				t.Fatalf("one-shot recovery changed %s", table)
			}
			continue
		}
		if !reflect.DeepEqual(original.Columns, current.Columns) {
			t.Fatal("one-shot recovery changed event storage shape")
		}
		oldRows := make(map[string]bool, len(original.Rows))
		for _, row := range original.Rows {
			oldRows[row] = true
		}
		for _, row := range current.Rows {
			if oldRows[row] {
				delete(oldRows, row)
				continue
			}
			var columns []struct {
				Column string
				Type   string
				Value  any
			}
			if err := json.Unmarshal([]byte(row), &columns); err != nil {
				t.Fatal(err)
			}
			values := make(map[string]any, len(columns))
			for _, column := range columns {
				values[column.Column] = column.Value
				if column.Type == "[]uint8" {
					encoded, ok := column.Value.(string)
					if !ok {
						t.Fatalf("native snapshot byte column %s has inconsistent encoding", column.Column)
					}
					decoded, err := base64.StdEncoding.DecodeString(encoded)
					if err != nil {
						t.Fatal(err)
					}
					values[column.Column] = string(decoded)
				}
			}
			var log struct {
				Details struct {
					EventName string `json:"event_name"`
					RunID     string `json:"run_id"`
					AgentID   string `json:"agent_id"`
				} `json:"details"`
			}
			payload, ok := values["payload"].(string)
			if !ok || json.Unmarshal([]byte(payload), &log) != nil || values["event_class"] != "diagnostic_direct" ||
				values["event_name"] != "platform.runtime_log" || values["run_id"] != child ||
				log.Details.EventName != "platform.agent_lifecycle_transition" || log.Details.RunID != child || log.Details.AgentID != agentID {
				t.Fatalf("one-shot recovery added a non-lifecycle business event: class=%v name=%v run=%v details=%+v", values["event_class"], values["event_name"], values["run_id"], log.Details)
			}
		}
		if len(oldRows) != 0 {
			t.Fatal("one-shot recovery removed or rewrote original events")
		}
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
