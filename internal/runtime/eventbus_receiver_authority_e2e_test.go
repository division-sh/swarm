package runtime_test

import (
	"context"
	"database/sql"
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
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type closedReceiverE2EStore interface {
	startupRecoveryOrderStore
	runtimeeffects.Store
	runtimeeffects.CompletionStore
	runtimeeffects.CompletionHeartbeatStore
	runtimeeffects.RecoveryStore
	managedcapabilities.Persistence
}

func TestManagedEffectAuthorityFollowsActingAgentAcrossNodeChain(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var (
				db       *sql.DB
				selected closedReceiverE2EStore
			)
			if backend == "postgres" {
				_, postgresDB, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				db = postgresDB
				selected = storetest.AdmitPostgresRuntimeStore(t, postgresDB)
			} else {
				sqlite := storetest.StartSQLiteRuntimeStore(t)
				db = storetest.Database(sqlite)
				selected = sqlite
			}

			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			if backend == "postgres" {
				storetest.RequirePostgresRun(t, ctx, db, storetest.RunFixture{
					Origin: storetest.ScenarioSetupOrigin(), RunID: runID,
					BundleHash: authorActivityTestBundleSourceFact.BundleHash(), BundleSource: "ephemeral",
				})
			} else {
				storetest.RequireSQLiteRun(t, ctx, db, storetest.RunFixture{
					Origin: storetest.ScenarioSetupOrigin(), RunID: runID,
					BundleHash: authorActivityTestBundleSourceFact.BundleHash(), BundleSource: "ephemeral",
				})
			}

			bundle := loadRuntimeTempBundle(t, closedReceiverAuthorityFixtureFiles())
			source := semanticview.Wrap(bundle)
			workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
			if backend == "sqlite" {
				workflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
			}
			processOwner := worklifetime.NewProcess()
			t.Cleanup(func() {
				joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				processOwner.Retire()
				if _, err := processOwner.Join(joinCtx); err != nil {
					t.Errorf("join receiver authority runtime: %v", err)
				}
			})
			modelRuntime := &closedReceiverManagedLLM{
				controller: runtimeeffects.NewCompletionController(selected, selected, selected, nil).WithExecutionPosture(executionposture.Live),
			}
			cfg := &config.Config{
				Runtime: config.RuntimeConfig{MaxConcurrentAgents: 4, EventPollInterval: 5 * time.Millisecond},
				LLM: config.LLMConfig{
					Backend:   "anthropic",
					Session:   config.LLMSessionConfig{LockTTL: 30 * time.Second, RotateAfterTurns: 8, RotateOnParseFailures: 2},
					ClaudeAPI: config.ClaudeAPIConfig{DefaultModel: "test-model", HaikuModel: "test-haiku"},
				},
			}
			rt, err := swarmruntime.NewValidationHarnessRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
				Config:     cfg,
				EventStore: selected, EventBusDurable: externalRuntimeTestDurableDependencies(selected),
				EventPayloadValidationBinder: selected, InboundPayloadValidationBinder: selected,
				AuthorActivityRegistrars: []swarmruntime.AuthorActivityCatalogRegistrar{selected},
				RunLifecycleCandidates:   selected, WorkflowPersistence: workflowPersistence,
				ManagerStore:             selected,
				ManagerLifecycleStore:    selected,
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
					RuntimeInstanceID: authorActivityTestRuntimeInstanceID, BundleSourceFact: authorActivityTestBundleSourceFact,
					ProcessWorkOwner: processOwner,
				},
			}))
			if err != nil {
				t.Fatalf("NewRuntime: %v", err)
			}
			t.Cleanup(func() {
				if err := rt.Shutdown(); err != nil {
					t.Errorf("shutdown receiver authority runtime: %v", err)
				}
			})
			if err := rt.PrepareAuthorActivityCatalog(); err != nil {
				t.Fatalf("PrepareAuthorActivityCatalog: %v", err)
			}
			if err := rt.Start(ctx); err != nil {
				t.Fatalf("Start: %v", err)
			}

			entityID := uuid.NewString()
			startedAt := time.Now().UTC().Add(-time.Second)
			materializeCtx := worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
			rootRoute := runID
			if _, err := rt.Pipeline.MaterializeInitialEntry(testLiveExecutionContext(materializeCtx), runtimepipeline.WorkflowInstance{
				InstanceID: rootRoute, StorageRef: rootRoute,
				WorkflowName: bundle.WorkflowName(), WorkflowVersion: bundle.WorkflowVersion(),
				CurrentState: "pending", EnteredStageAt: startedAt, CreatedAt: startedAt,
				Metadata: map[string]any{"flow_path": rootRoute, "instance_id": rootRoute, "entity_id": entityID},
			}, startedAt); err != nil {
				t.Fatalf("materialize receiver authority workflow: %v", err)
			}
			root := eventtest.ExistingRunRootIngress(
				uuid.NewString(), "task.assigned", "receiver-authority-e2e", "", []byte(`{}`), 0, runID,
				events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC(),
			)
			publishCtx, cancelPublish := context.WithTimeout(ctx, 10*time.Second)
			defer cancelPublish()
			if err := rt.Bus.PublishAndWait(publishCtx, root); err != nil {
				t.Fatalf("publish receiver authority root: %v", err)
			}

			assertClosedReceiverAuthorityEvidence(t, ctx, db, backend, runID, root.ID())
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
		plan, err := execution.Plan.Normalize()
		if err != nil {
			return nil, err
		}
		session.Memory = plan
		session.MemoryIdentity = execution.Identity.Normalize()
	}
	return session, nil
}

func (*closedReceiverManagedLLM) ProviderContract() llm.ProviderContract {
	return llm.AnthropicAPIProviderContract()
}

func (*closedReceiverManagedLLM) PersistConversationSnapshot(context.Context, *llm.Session) error {
	return nil
}

func (r *closedReceiverManagedLLM) ContinueSession(ctx context.Context, session *llm.Session, message llm.Message) (*llm.Response, error) {
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
	handle, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte(message.Content), nil)
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
	call := r.calls[session.AgentID]
	r.mu.Unlock()
	session.Messages = append(session.Messages, message)
	session.TurnCount++
	response := &llm.Response{
		Message:           llm.Message{Role: "assistant"},
		CapabilitySurface: &observed,
	}
	if session.AgentID == "upstream-agent" && call == 1 {
		response.ToolCalls = []llm.ToolCall{{
			ID: uuid.NewString(), Name: runtimetools.EmitToolName("task.completed"), Arguments: map[string]any{},
		}}
	}
	return response, nil
}

func assertClosedReceiverAuthorityEvidence(t *testing.T, ctx context.Context, db *sql.DB, backend, runID, rootEventID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if closedReceiverEvidenceReady(t, ctx, db, backend, runID) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for closed receiver evidence on %s: %s", backend, closedReceiverEvidenceDiagnostic(t, ctx, db, backend, runID))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for closed receiver evidence on %s: %v", backend, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}

	placeholder, cast := "?", ""
	if backend == "postgres" {
		placeholder, cast = "$1", "::uuid"
	}
	rows, err := db.QueryContext(ctx, `
		SELECT o.agent_id, o.authority_kind, a.state, s.actor_id, s.execution_kind
		FROM runtime_external_effect_operations o
		JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
		JOIN managed_agent_capability_surfaces s ON s.surface_id = a.capability_surface_id
		ORDER BY o.agent_id
	`)
	if err != nil {
		t.Fatalf("query receiver effect evidence: %v", err)
	}
	defer rows.Close()
	wantAgents := map[string]bool{"downstream-agent": false, "upstream-agent": false}
	for rows.Next() {
		var agentID, authorityKind, state, surfaceActor, executionKind string
		if err := rows.Scan(&agentID, &authorityKind, &state, &surfaceActor, &executionKind); err != nil {
			t.Fatalf("scan receiver effect evidence: %v", err)
		}
		if _, ok := wantAgents[agentID]; !ok {
			continue
		}
		if authorityKind != "normal_agent" || state != "settled" || surfaceActor != agentID || executionKind != "normal_agent" {
			t.Fatalf("receiver authority evidence for %s = authority %q state %q surface actor %q execution %q", agentID, authorityKind, state, surfaceActor, executionKind)
		}
		wantAgents[agentID] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate receiver effect evidence: %v", err)
	}
	for agentID, found := range wantAgents {
		if !found {
			t.Fatalf("missing persisted receiver authority evidence for %s", agentID)
		}
	}

	query := `SELECT event_id, produced_by, produced_by_type, COALESCE(source_event_id, '') FROM events WHERE run_id = ` + placeholder + cast + ` AND event_name = ?`
	args := []any{runID, "task.completed"}
	if backend == "postgres" {
		query = `SELECT event_id::text, produced_by, produced_by_type, COALESCE(source_event_id::text, '') FROM events WHERE run_id = $1::uuid AND event_name = $2`
	}
	var completedID, completedBy, completedType, completedSource string
	if err := db.QueryRowContext(ctx, query, args...).Scan(&completedID, &completedBy, &completedType, &completedSource); err != nil {
		t.Fatalf("load upstream emitted event: %v", err)
	}
	if completedBy != "upstream-agent" || completedType != "agent" || completedSource != rootEventID {
		t.Fatalf("task.completed lineage = producer %s/%s source %s, want upstream-agent/agent source %s", completedType, completedBy, completedSource, rootEventID)
	}
	args[1] = "task.finalized"
	var finalizedID, finalizedBy, finalizedType, finalizedSource string
	if err := db.QueryRowContext(ctx, query, args...).Scan(&finalizedID, &finalizedBy, &finalizedType, &finalizedSource); err != nil {
		t.Fatalf("load node emitted event: %v", err)
	}
	if finalizedBy != "bridge-node" || finalizedType != "node" || finalizedSource != completedID {
		t.Fatalf("task.finalized lineage = producer %s/%s source %s, want bridge-node/node source %s", finalizedType, finalizedBy, finalizedSource, completedID)
	}

	receiptQuery := `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = ? AND r.subscriber_type = 'platform' AND r.subscriber_id = 'pipeline' AND r.outcome = 'success'`
	if backend == "postgres" {
		receiptQuery = `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = $1::uuid AND r.subscriber_type = 'platform' AND r.subscriber_id = 'pipeline' AND r.outcome = 'success'`
	}
	var receipts int
	if err := db.QueryRowContext(ctx, receiptQuery, runID).Scan(&receipts); err != nil {
		t.Fatalf("count receiver pipeline receipts: %v", err)
	}
	if receipts < 3 {
		t.Fatalf("receiver pipeline receipts = %d, want at least root, node input, and downstream input", receipts)
	}
	_ = finalizedID
}

func closedReceiverEvidenceDiagnostic(t testing.TB, ctx context.Context, db *sql.DB, backend, runID string) string {
	t.Helper()
	query := `
		SELECT e.event_name, COALESCE(e.flow_instance, ''), COALESCE(CAST(e.entity_id AS TEXT), ''),
		       CAST(e.target_route AS TEXT), d.subscriber_type, d.subscriber_id, d.status,
		       COALESCE(d.reason_code, ''), COALESCE(CAST(d.failure AS TEXT), '')
		FROM events e
		LEFT JOIN event_deliveries d ON d.event_id = e.event_id
		WHERE e.run_id = ?
		ORDER BY e.created_at, d.subscriber_type, d.subscriber_id`
	args := []any{runID}
	if backend == "postgres" {
		query = strings.Replace(query, "e.run_id = ?", "e.run_id = $1::uuid", 1)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return "query diagnostic: " + err.Error()
	}
	defer rows.Close()
	var facts []string
	for rows.Next() {
		var eventName, flowInstance, entityID, targetRoute string
		var subscriberType, subscriberID, status, reason, failure sql.NullString
		if err := rows.Scan(&eventName, &flowInstance, &entityID, &targetRoute, &subscriberType, &subscriberID, &status, &reason, &failure); err != nil {
			return "scan diagnostic: " + err.Error()
		}
		facts = append(facts, fmt.Sprintf("%s[%s/%s target=%s]:%s/%s:%s:%s:%s", eventName, flowInstance, entityID, targetRoute, subscriberType.String, subscriberID.String, status.String, reason.String, failure.String))
	}
	if err := rows.Err(); err != nil {
		return "iterate diagnostic: " + err.Error()
	}
	return strings.Join(facts, ", ")
}

func closedReceiverEvidenceReady(t testing.TB, ctx context.Context, db *sql.DB, backend, runID string) bool {
	t.Helper()
	query := `
		SELECT
			(SELECT COUNT(DISTINCT o.agent_id)
			 FROM runtime_external_effect_operations o
			 JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
			 WHERE o.agent_id IN ('upstream-agent','downstream-agent') AND a.state = 'settled'),
			(SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name IN ('task.completed','task.finalized')),
			(SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id = d.event_id
			 WHERE e.run_id = ? AND d.status = 'delivered'
			   AND ((d.subscriber_type = 'agent' AND d.subscriber_id IN ('upstream-agent','downstream-agent'))
			     OR (d.subscriber_type = 'node' AND d.subscriber_id = 'bridge-node')))
	`
	if backend == "postgres" {
		query = `
			SELECT
				(SELECT COUNT(DISTINCT o.agent_id)
				 FROM runtime_external_effect_operations o
				 JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
				 WHERE o.agent_id IN ('upstream-agent','downstream-agent') AND a.state = 'settled'),
				(SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name IN ('task.completed','task.finalized')),
				(SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id = d.event_id
				 WHERE e.run_id = $1::uuid AND d.status = 'delivered'
				   AND ((d.subscriber_type = 'agent' AND d.subscriber_id IN ('upstream-agent','downstream-agent'))
				     OR (d.subscriber_type = 'node' AND d.subscriber_id = 'bridge-node')))
		`
	}
	var actors, eventsCount, deliveries int
	args := []any{runID, runID}
	if backend == "postgres" {
		args = []any{runID}
	}
	if err := db.QueryRowContext(ctx, query, args...).Scan(&actors, &eventsCount, &deliveries); err != nil {
		t.Fatalf("query closed receiver evidence readiness: %v", err)
	}
	return actors == 2 && eventsCount == 2 && deliveries == 3
}

func closedReceiverAuthorityFixtureFiles() map[string]string {
	return map[string]string{
		"package.yaml": `name: closed-receiver-authority
version: 1.0.0
platform_version: ">=0.7.0 <0.8.0"
flows: []
`,
		"schema.yaml": `name: closed-receiver-authority
initial_state: pending
terminal_states: [done]
states: [pending, done]
pins:
  inputs:
    events: [task.assigned]
`,
		"events.yaml": `task.assigned: {}
task.completed: {}
task.finalized: {}
`,
		"agents.yaml": `upstream-agent:
  id: upstream-agent
  model: regular
  subscriptions: [task.assigned]
  emit_events: [task.completed]
downstream-agent:
  id: downstream-agent
  model: regular
  subscriptions: [task.finalized]
`,
		"nodes.yaml": `bridge-node:
  id: bridge-node
  execution_type: system_node
  subscribes_to: [task.completed]
  produces: [task.finalized]
  event_handlers:
    task.completed:
      advances_to: done
      emit: task.finalized
`,
		"policy.yaml":                 "{}\n",
		"prompts/upstream-agent.md":   "Emit task.completed.\n",
		"prompts/downstream-agent.md": "Observe task.finalized.\n",
	}
}
