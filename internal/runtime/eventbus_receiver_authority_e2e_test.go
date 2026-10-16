package runtime_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type closedReceiverE2EStore interface {
	storetest.RunFixtureStore
	startupRecoveryOrderStore
	swarmruntime.RuntimeLogPersistence
	runtimeeffects.Store
	runtimeeffects.CompletionStore
	runtimeeffects.CompletionHeartbeatStore
	runtimeeffects.RecoveryStore
	managedcapabilities.Persistence
}

func TestManagedEffectAuthorityFollowsActingAgentAcrossNodeChain(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected closedReceiverE2EStore
			if backend == "postgres" {
				selected = storetest.StartPostgresRuntimeStore(t)
			} else {
				selected = storetest.StartSQLiteRuntimeStore(t)
			}

			bundle := loadRuntimeTempBundle(t, closedReceiverAuthorityFixtureFiles())
			source := semanticview.Wrap(bundle)
			fact := runtimeTestSourceArtifactFact(t, source)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(runtimecorrelation.WithSourceArtifactFact(context.Background(), fact), runID)
			ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
				Origin: storetest.ScenarioSetupOrigin(), RunID: runID,
				BundleHash: fact.BundleHash(), Artifact: bundle.SourceArtifact,
			})

			workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
			processOwner := worklifetime.NewProcess()
			modelRuntime := &closedReceiverManagedLLM{
				controller: runtimeeffects.NewCompletionController(selected, selected, selected, nil).WithExecutionPosture(executionposture.Live),
			}
			cfg := &config.Config{
				Runtime: config.RuntimeConfig{MaxConcurrentAgents: 4, EventPollInterval: 5 * time.Millisecond},
				LLM: config.LLMConfig{
					Backend: "anthropic",
					Session: config.LLMSessionConfig{LockTTL: 30 * time.Second, RotateAfterTurns: 8, RotateOnParseFailures: 2},
					Models:  map[string]map[string]string{"regular": {"anthropic": "test-model"}, "cheap": {"anthropic": "test-haiku"}},
				},
			}
			rt, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
				Config:     cfg,
				EventStore: selected, EventBusDurable: externalRuntimeTestDurableDependencies(selected),
				EventPayloadAdmissionBinder: selected, InboundPayloadAdmissionBinder: selected,
				AuthorActivityRegistrars: []swarmruntime.AuthorActivityCatalogRegistrar{selected},
				RunLifecycleCandidates:   selected, WorkflowPersistence: workflowPersistence,
				RuntimeLogStore:          selected,
				ManagerStore:             selected,
				ManagerPersistenceRoles:  externalRuntimeTestSelectedManagerRoles(selected),
				EffectsStore:             selected,
				CompletionStore:          selected,
				CompletionHeartbeatStore: selected,
				EffectsRecoveryStore:     selected,
				ManagedCapabilitiesStore: selected,
				DeliveryStore:            selected,
				PipelineObligations:      selected.PipelineObligations(),
				Options: swarmruntime.RuntimeOptions{
					SelfCheck: false, WorkflowModule: newRuntimeTestWorkflowModule(t, source), LLMRuntime: modelRuntime,
					RuntimeInstanceID: authorActivityTestRuntimeInstanceID, SourceArtifactFact: fact,
					ProcessWorkOwner: processOwner,
				},
			}))
			if err != nil {
				t.Fatalf("NewRuntime: %v", err)
			}
			capability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, rt)
			t.Cleanup(func() {
				if err := closeExternalRuntimeTestGeneration(rt, processOwner, capability); err != nil {
					t.Errorf("close receiver authority generation: %v", err)
				}
			})
			if err := rt.PrepareAuthorActivityCatalog(); err != nil {
				t.Fatalf("PrepareAuthorActivityCatalog: %v", err)
			}
			if err := rt.Start(ctx); err != nil {
				t.Fatalf("Start: %v", err)
			}

			materializeCtx := worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
			constructExternalRuntimeTestRoot(t, materializeCtx, rt.Manager, source)
			entityID := runID
			rootSource := eventtest.RootRoutingSource(entityID)
			root := eventtest.ExistingRunRootIngressWithRoutingSource(
				uuid.NewString(), "task.assigned", "receiver-authority-e2e", "", []byte(`{}`), 0, runID,
				events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), rootSource, time.Now().UTC(),
			)
			publishCtx, cancelPublish := context.WithTimeout(ctx, 10*time.Second)
			defer cancelPublish()
			if err := rt.Bus.PublishAndWait(publishCtx, root); err != nil {
				t.Fatalf("publish receiver authority root: %v", err)
			}

			assertClosedReceiverAuthorityEvidence(t, ctx, selected, backend, runID, root.ID())
			requireClosedReceiverDiagnosticReadback(t, ctx, selected, runID)
		})
	}
}

type closedReceiverManagedLLM struct {
	controller *runtimeeffects.Controller
	mu         sync.Mutex
	calls      map[string]int
}

func (r *closedReceiverManagedLLM) StartSession(ctx context.Context, agentID, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	session := &llm.Session{
		ID: uuid.NewString(), AgentID: strings.TrimSpace(agentID), SystemPrompt: systemPrompt,
		Tools: append([]llm.ToolDefinition(nil), tools...),
	}
	if execution, ok := agentmemory.FromContext(ctx); ok {
		plan := execution.Plan
		session.Memory = plan
		session.MemoryIdentity = execution.Identity.Normalize()
	}
	return session, nil
}

func (*closedReceiverManagedLLM) ProviderContract() llm.ProviderContract {
	return llm.AnthropicAPIProviderContract()
}

func (*closedReceiverManagedLLM) PersistConversationSnapshot(context.Context, *sessions.Lease, *llm.Session) error {
	return nil
}

func (*closedReceiverManagedLLM) PrepareManagedSession(context.Context, *llm.Session) error {
	return nil
}

func (r *closedReceiverManagedLLM) ContinueManagedSession(ctx context.Context, session *llm.Session, call llm.ManagedCall) (*llm.Response, error) {
	message, err := call.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, fmt.Errorf("receiver authority proof requires a session")
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("receiver authority proof requires a managed capability surface")
	}
	observed, err := llm.ObserveAPIRequestCapabilitySurface(surface, session.Tools)
	if err != nil {
		return nil, err
	}
	ctx = managedcapabilities.WithContext(ctx, observed)
	actor, ok := runtimeactors.ActorFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("receiver authority proof requires an actor")
	}
	target := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: observed.Authority.ID,
		RunID: observed.Authority.RunID, AgentID: observed.ActorID, AgentIdentity: observed.ActorIdentity,
		SessionID: observed.Authority.SessionID, Memory: session.Memory,
		FlowInstance: observed.ActorIdentity.FlowInstance(), EntityID: strings.TrimSpace(actor.EffectiveEntityID()),
	}
	ctx = runtimeeffects.WithLogicalOperationIdentitySegment(ctx, "closed-receiver-e2e")
	ctx = runtimeeffects.WithUsageTarget(ctx, target)
	ctx = runtimeeffects.WithController(ctx, r.controller)
	handle, err := runtimeeffects.BeginManagedCompletion(ctx, "anthropic_api", []byte(message.Content), call.Frame(), nil)
	if err != nil {
		return nil, err
	}
	if err := handle.MarkLaunched(ctx); err != nil {
		return nil, err
	}
	if err := handle.MarkResponseObserved(ctx, map[string]any{"agent_id": session.AgentID}); err != nil {
		return nil, err
	}
	if err := handle.Succeed(ctx, map[string]any{"agent_id": session.AgentID}); err != nil {
		return nil, err
	}

	r.mu.Lock()
	if r.calls == nil {
		r.calls = map[string]int{}
	}
	r.calls[session.AgentID]++
	callCount := r.calls[session.AgentID]
	r.mu.Unlock()
	session.Messages = append(session.Messages, message)
	session.TurnCount++
	response := &llm.Response{
		Message:           llm.Message{Role: "assistant"},
		CapabilitySurface: &observed,
		ToolOutputAuthority: &llm.ToolOutputAuthority{
			ProviderOperationID: handle.Attempt().OperationID,
			SettledAt:           time.Now().UTC().Truncate(time.Microsecond),
		},
	}
	if session.AgentID == "upstream-agent" && callCount == 1 {
		response.ToolCalls = []llm.ToolCall{{
			ID: uuid.NewString(), Name: runtimetools.EmitToolName("task.completed"), Arguments: map[string]any{},
		}}
	}
	return response, nil
}

func (r *closedReceiverManagedLLM) ContinueForkChatSession(ctx context.Context, session *llm.Session, call llm.ForkChatCall) (*llm.Response, error) {
	message, err := call.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "noop: " + message.Content}}, nil
}

func assertClosedReceiverAuthorityEvidence(t *testing.T, ctx context.Context, selected any, backend, runID, rootEventID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if closedReceiverEvidenceReady(t, ctx, selected, runID) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for closed receiver evidence on %s: %s", backend, closedReceiverEvidenceDiagnostic(t, ctx, selected, runID))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for closed receiver evidence on %s: %v", backend, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}

	rows, err := storetest.ReadClosedReceiverAuthorityStorage(ctx, selected)
	if err != nil {
		t.Fatalf("query receiver effect evidence: %v", err)
	}
	wantAgents := map[string]bool{"downstream-agent": false, "upstream-agent": false}
	for _, row := range rows {
		agentID, authorityKind, state, surfaceActor, executionKind := row.AgentID, row.AuthorityKind, row.State, row.SurfaceActor, row.ExecutionKind
		if _, ok := wantAgents[agentID]; !ok {
			continue
		}
		if authorityKind != "normal_agent" || state != "settled" || surfaceActor != agentID || executionKind != "normal_agent" {
			t.Fatalf("receiver authority evidence for %s = authority %q state %q surface actor %q execution %q", agentID, authorityKind, state, surfaceActor, executionKind)
		}
		wantAgents[agentID] = true
	}
	for agentID, found := range wantAgents {
		if !found {
			t.Fatalf("missing persisted receiver authority evidence for %s", agentID)
		}
	}

	completed, err := storetest.ReadClosedReceiverEmissionStorage(ctx, selected, runID, "task.completed")
	if err != nil {
		t.Fatalf("load upstream emitted event: %v", err)
	}
	completedID, completedBy, completedType, completedSource := completed.EventID, completed.ProducedBy, completed.ProducedType, completed.SourceEventID
	if completedBy != "upstream-agent" || completedType != "agent" || completedSource != rootEventID {
		t.Fatalf("task.completed lineage = producer %s/%s source %s, want upstream-agent/agent source %s", completedType, completedBy, completedSource, rootEventID)
	}
	finalized, err := storetest.ReadClosedReceiverEmissionStorage(ctx, selected, runID, "task.finalized")
	if err != nil {
		t.Fatalf("load node emitted event: %v", err)
	}
	finalizedID, finalizedBy, finalizedType, finalizedSource := finalized.EventID, finalized.ProducedBy, finalized.ProducedType, finalized.SourceEventID
	bridgeNodeID := identitytest.RootNode(t, "bridge-node").Key()
	if finalizedBy != bridgeNodeID || finalizedType != "node" || finalizedSource != completedID {
		t.Fatalf("task.finalized lineage = producer %s/%s source %s, want %s/node source %s", finalizedType, finalizedBy, finalizedSource, bridgeNodeID, completedID)
	}

	receipts, err := storetest.CountClosedReceiverPipelineSuccessStorage(ctx, selected, runID)
	if err != nil {
		t.Fatalf("count receiver pipeline receipts: %v", err)
	}
	if receipts < 3 {
		t.Fatalf("receiver pipeline receipts = %d, want at least root, node input, and downstream input", receipts)
	}
	_ = finalizedID
}

func closedReceiverEvidenceDiagnostic(t testing.TB, ctx context.Context, selected any, runID string) string {
	t.Helper()
	rows, err := storetest.ReadClosedReceiverDeliveryStorage(ctx, selected, runID)
	if err != nil {
		return "query diagnostic: " + err.Error()
	}
	var facts []string
	for _, row := range rows {
		eventName, flowInstance, entityID, targetRoute := row.EventName, row.FlowInstance, row.EntityID, row.TargetRoute
		subscriberType, subscriberID, status, reason, failure := row.SubscriberType, row.SubscriberID, row.Status, row.Reason, row.Failure
		facts = append(facts, fmt.Sprintf("%s[%s/%s target=%s]:%s/%s:%s:%s:%s", eventName, flowInstance, entityID, targetRoute, subscriberType.String, subscriberID.String, status.String, reason.String, failure.String))
	}
	receipts, err := storetest.ReadClosedReceiverReceiptStorage(ctx, selected, runID)
	if err != nil {
		return strings.Join(facts, ", ") + "; query receipts: " + err.Error()
	}
	for _, row := range receipts {
		eventName, subscriberType, subscriberID, outcome, reason, failure := row.EventName, row.SubscriberType, row.SubscriberID, row.Outcome, row.Reason, row.Failure
		facts = append(facts, fmt.Sprintf("receipt:%s:%s/%s:%s:%s:%s", eventName, subscriberType, subscriberID, outcome, reason, failure))
	}
	agents, err := storetest.ReadClosedReceiverAgentStorage(ctx, selected)
	if err != nil {
		return strings.Join(facts, ", ") + "; query agents: " + err.Error()
	}
	for _, row := range agents {
		agentID, nameOwner, nameSource, routePresence, scope, instanceID, flowInstance, entityID, phase, generation, runMode, status := row.AgentID, row.NameOwner, row.NameSource, row.RoutePresence, row.Scope, row.InstanceID, row.FlowInstance, row.EntityID, row.Phase, row.Generation, row.RunMode, row.Status
		facts = append(facts, fmt.Sprintf("agent:%s:%s/%s/%s/%s/%s/%s/%s:%s/%d/%s/%s", agentID, nameOwner, nameSource, routePresence, scope, instanceID, flowInstance, entityID, phase, generation, runMode, status))
	}
	routeRows, err := storetest.ReadClosedReceiverAgentRouteStorage(ctx, selected, runID)
	if err != nil {
		return strings.Join(facts, ", ") + "; query agent routes: " + err.Error()
	}
	for _, row := range routeRows {
		agentID, owner, source, presence, scope, instanceID, instancePath := row.AgentID, row.Owner, row.Source, row.Presence, row.Scope, row.InstanceID, row.InstancePath
		facts = append(facts, fmt.Sprintf("agent-route:%s:%s/%s/%s/%s/%s/%s", agentID, owner, source, presence, scope, instanceID, instancePath))
	}
	logs, err := storetest.ReadClosedReceiverRuntimeLogStorage(ctx, selected, runID)
	if err != nil {
		return strings.Join(facts, ", ") + "; query runtime logs: " + err.Error()
	}
	for _, payload := range logs {
		facts = append(facts, "runtime-log:"+payload)
	}
	return strings.Join(facts, ", ")
}

func closedReceiverEvidenceReady(t testing.TB, ctx context.Context, selected any, runID string) bool {
	t.Helper()
	bridgeNodeID := identitytest.RootNode(t, "bridge-node").Key()
	observed, err := storetest.ReadClosedReceiverReadinessStorage(ctx, selected, runID, bridgeNodeID)
	if err != nil {
		t.Fatalf("query closed receiver evidence readiness: %v", err)
	}
	actors, eventsCount, deliveries := observed.Actors, observed.Events, observed.Deliveries
	return actors == 2 && eventsCount == 2 && deliveries == 3
}

func requireClosedReceiverDiagnosticReadback(t *testing.T, ctx context.Context, selected any, runID string) {
	t.Helper()
	evidence := closedReceiverEvidenceDiagnostic(t, ctx, selected, runID)
	for _, fragment := range []string{
		"task.assigned[", "task.completed[", "task.finalized[",
		"receipt:task.assigned:platform/pipeline:success:",
		"receipt:task.completed:platform/pipeline:success:",
		"receipt:task.finalized:platform/pipeline:success:",
		"agent:upstream-agent:", "agent:downstream-agent:",
		"agent-route:upstream-agent:", "agent-route:downstream-agent:",
	} {
		if !strings.Contains(evidence, fragment) {
			t.Fatalf("closed receiver diagnostic lost persisted %q: %s", fragment, evidence)
		}
	}
}

func closedReceiverAuthorityFixtureFiles() map[string]string {
	return map[string]string{

		"schema.yaml": `name: closed-receiver-authority
stages:
  pending: {}
  done: {final: true}
pins:
  inputs:
    - task.assigned
`,
		"entities.yaml": "test_entity: {}\n",
		"events.yaml": `task.assigned:
task.completed:
task.finalized:
`,
		"agents.yaml": `upstream-agent:
  intent: {inline: "Emit task.completed for the assigned task."}
  model: regular
  subscriptions: [task.assigned]
  emit_events: [task.completed]
downstream-agent:
  intent: {inline: "Observe the finalized task."}
  model: regular
  subscriptions: [task.finalized]
`,
		"nodes.yaml": `bridge-node:
  execution_type: system_node
  subscribes_to: [task.completed]
  produces: [task.finalized]
  event_handlers:
    task.completed:
      advances_to: done
      emit: task.finalized
`,
	}
}
