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
				RuntimeLogStore:          selected,
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
	receiptQuery := `
		SELECT e.event_name, r.subscriber_type, r.subscriber_id, r.outcome,
		       COALESCE(r.reason_code, ''), COALESCE(CAST(r.failure AS TEXT), '')
		FROM event_receipts r
		JOIN events e ON e.event_id = r.event_id
		WHERE e.run_id = ?
		ORDER BY e.created_at, r.subscriber_type, r.subscriber_id`
	if backend == "postgres" {
		receiptQuery = strings.Replace(receiptQuery, "e.run_id = ?", "e.run_id = $1::uuid", 1)
	}
	receipts, err := db.QueryContext(ctx, receiptQuery, runID)
	if err != nil {
		return strings.Join(facts, ", ") + "; query receipts: " + err.Error()
	}
	defer receipts.Close()
	for receipts.Next() {
		var eventName, subscriberType, subscriberID, outcome, reason, failure string
		if err := receipts.Scan(&eventName, &subscriberType, &subscriberID, &outcome, &reason, &failure); err != nil {
			return strings.Join(facts, ", ") + "; scan receipts: " + err.Error()
		}
		facts = append(facts, fmt.Sprintf("receipt:%s:%s/%s:%s:%s:%s", eventName, subscriberType, subscriberID, outcome, reason, failure))
	}
	if err := receipts.Err(); err != nil {
		return strings.Join(facts, ", ") + "; iterate receipts: " + err.Error()
	}
	agents, err := db.QueryContext(ctx, `
		SELECT agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		       flow_scope_key, flow_instance_id, flow_instance, COALESCE(CAST(entity_id AS TEXT), ''),
		       lifecycle_phase, lifecycle_generation, lifecycle_run_mode, status
		FROM agents
		WHERE agent_id IN ('upstream-agent', 'downstream-agent')
		ORDER BY agent_id`)
	if err != nil {
		return strings.Join(facts, ", ") + "; query agents: " + err.Error()
	}
	defer agents.Close()
	for agents.Next() {
		var agentID, nameOwner, nameSource, routePresence, scope, instanceID, flowInstance, entityID, phase, runMode, status string
		var generation int64
		if err := agents.Scan(&agentID, &nameOwner, &nameSource, &routePresence, &scope, &instanceID, &flowInstance, &entityID, &phase, &generation, &runMode, &status); err != nil {
			return strings.Join(facts, ", ") + "; scan agents: " + err.Error()
		}
		facts = append(facts, fmt.Sprintf("agent:%s:%s/%s/%s/%s/%s/%s/%s:%s/%d/%s/%s", agentID, nameOwner, nameSource, routePresence, scope, instanceID, flowInstance, entityID, phase, generation, runMode, status))
	}
	if err := agents.Err(); err != nil {
		return strings.Join(facts, ", ") + "; iterate agents: " + err.Error()
	}
	routeRows, err := db.QueryContext(ctx, `
		SELECT d.subscriber_id, d.agent_name_owner, d.agent_name_source,
		       d.agent_route_presence, d.agent_flow_scope_key,
		       d.agent_flow_instance_id, d.agent_flow_instance_path
		FROM event_deliveries d
		JOIN events e ON e.event_id = d.event_id
		WHERE e.run_id = ? AND d.subscriber_type = 'agent'
		ORDER BY d.subscriber_id`, runID)
	if backend == "postgres" {
		routeRows, err = db.QueryContext(ctx, `
			SELECT d.subscriber_id, d.agent_name_owner, d.agent_name_source,
			       d.agent_route_presence, d.agent_flow_scope_key,
			       d.agent_flow_instance_id, d.agent_flow_instance_path
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			WHERE e.run_id = $1::uuid AND d.subscriber_type = 'agent'
			ORDER BY d.subscriber_id`, runID)
	}
	if err != nil {
		return strings.Join(facts, ", ") + "; query agent routes: " + err.Error()
	}
	defer routeRows.Close()
	for routeRows.Next() {
		var agentID, owner, source, presence, scope, instanceID, instancePath string
		if err := routeRows.Scan(&agentID, &owner, &source, &presence, &scope, &instanceID, &instancePath); err != nil {
			return strings.Join(facts, ", ") + "; scan agent routes: " + err.Error()
		}
		facts = append(facts, fmt.Sprintf("agent-route:%s:%s/%s/%s/%s/%s/%s", agentID, owner, source, presence, scope, instanceID, instancePath))
	}
	if err := routeRows.Err(); err != nil {
		return strings.Join(facts, ", ") + "; iterate agent routes: " + err.Error()
	}
	logQuery := `SELECT CAST(payload AS TEXT) FROM events WHERE run_id = ? AND event_name = 'platform.runtime_log' ORDER BY created_at`
	if backend == "postgres" {
		logQuery = `SELECT payload::text FROM events WHERE run_id = $1::uuid AND event_name = 'platform.runtime_log' ORDER BY created_at`
	}
	logs, err := db.QueryContext(ctx, logQuery, runID)
	if err != nil {
		return strings.Join(facts, ", ") + "; query runtime logs: " + err.Error()
	}
	defer logs.Close()
	for logs.Next() {
		var payload string
		if err := logs.Scan(&payload); err != nil {
			return strings.Join(facts, ", ") + "; scan runtime logs: " + err.Error()
		}
		facts = append(facts, "runtime-log:"+payload)
	}
	if err := logs.Err(); err != nil {
		return strings.Join(facts, ", ") + "; iterate runtime logs: " + err.Error()
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
  intent: {inline: "Emit task.completed for the assigned task."}
  model: regular
  subscriptions: [task.assigned]
  emit_events: [task.completed]
downstream-agent:
  id: downstream-agent
  intent: {inline: "Observe the finalized task."}
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
		"policy.yaml": "{}\n",
	}
}
