package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type agentSnapshotTestStore interface {
	storetest.RunFixtureStore
	AgentReadStore
	storetest.AgentFixtureStore
	storetest.ManagedAgentTurnFixtureStore
	sessions.Registry
	ListPendingAgentDeliveryFacts(context.Context, []agentidentity.Identity, time.Time) (map[agentidentity.Identity]operatorread.PendingAgentDeliveryFacts, error)
	ListAgentDeliveryLifecycleFacts(context.Context, []agentidentity.Identity) (map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts, error)
}

type agentSnapshotBackend struct {
	name string
	open func(*testing.T, context.Context) agentSnapshotTestStore
}

func agentSnapshotBackends() []agentSnapshotBackend {
	return []agentSnapshotBackend{
		{
			name: "sqlite",
			open: func(t *testing.T, ctx context.Context) agentSnapshotTestStore {
				return storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T, _ context.Context) agentSnapshotTestStore {
				return storetest.StartPostgresRuntimeStore(t)
			},
		},
	}
}

func TestAgentOperatorSummarySnapshotTerminalizationIsAtomicAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			before := fixture.readSummaryCut(t, storetest.OperatorSnapshotTerminalization(sqliteAgentUsageIdentityForRun(t, fixture.runID, "snapshot-agent-b")))
			if len(before.Agents) != 2 {
				t.Fatalf("snapshot result agent count = %d, want complete before-state count 2", len(before.Agents))
			}
			after := fixture.list(t)
			if len(after.Agents) != 1 {
				t.Fatalf("post-terminalization agent count = %d, want 1", len(after.Agents))
			}
		})
	}
}

func TestAgentOperatorSummarySnapshotActivationIsAtomicAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			identity := sqliteAgentUsageIdentityForRun(t, fixture.runID, "snapshot-agent-b")
			if err := storetest.ApplyOperatorAgentTerminalized(fixture.ctx, fixture.store, identity); err != nil {
				t.Fatal(err)
			}
			before := fixture.readSummaryCut(t, storetest.OperatorSnapshotActivation(identity))
			if len(before.Agents) != 1 {
				t.Fatalf("snapshot result agent count = %d, want complete before-state count 1", len(before.Agents))
			}
			after := fixture.list(t)
			if len(after.Agents) != 2 {
				t.Fatalf("post-activation agent count = %d, want 2", len(after.Agents))
			}
		})
	}
}

func TestAgentOperatorSummarySnapshotSessionReplacementIsAtomicAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			newSessionID := uuid.NewString()
			before := fixture.readSummaryCut(t, storetest.OperatorSnapshotSessionReplacement(fixture.identity, fixture.sessionID, newSessionID))
			detail := requireSnapshotAgent(t, before, "snapshot-agent-a")
			if detail.SessionID != fixture.sessionID {
				t.Fatalf("snapshot session = %q, want complete before-state session %q", detail.SessionID, fixture.sessionID)
			}
			after := requireSnapshotAgent(t, fixture.list(t), "snapshot-agent-a")
			if after.SessionID != newSessionID {
				t.Fatalf("post-replacement session = %q, want %q", after.SessionID, newSessionID)
			}
		})
	}
}

func TestAgentOperatorSummarySnapshotDeliveryFactsAreAtomicAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			delivery := fixture.seedPendingDelivery(t)
			before := fixture.readSummaryCut(t, storetest.OperatorSnapshotDeliverySettlement(delivery.deliveryID))
			beforeAgent := requireSnapshotAgent(t, before, fixture.identity.AgentID())
			if beforeAgent.PendingEvents != 1 {
				t.Fatalf("snapshot pending events = %d, want complete before-state count 1", beforeAgent.PendingEvents)
			}
			afterAgent := requireSnapshotAgent(t, fixture.list(t), fixture.identity.AgentID())
			if afterAgent.PendingEvents != 0 {
				t.Fatalf("post-settlement pending events = %d, want 0", afterAgent.PendingEvents)
			}
		})
	}
}

func TestAgentOperatorSummarySnapshotLatestTurnIsAtomicAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			turnID := fixture.seedTurn(t, "before snapshot")
			before := fixture.readSummaryCut(t, storetest.OperatorSnapshotTurnReplacement(turnID, "after snapshot"))
			beforeAgent := requireSnapshotAgent(t, before, fixture.identity.AgentID())
			if beforeAgent.LiveTurn == nil || beforeAgent.LiveTurn.AssistantVisibleOutput != "before snapshot" {
				t.Fatalf("snapshot latest turn = %#v, want complete before-state output", beforeAgent.LiveTurn)
			}
			afterAgent := requireSnapshotAgent(t, fixture.list(t), fixture.identity.AgentID())
			if afterAgent.LiveTurn == nil || afterAgent.LiveTurn.AssistantVisibleOutput != "after snapshot" {
				t.Fatalf("post-mutation latest turn = %#v, want after-state output", afterAgent.LiveTurn)
			}
		})
	}
}

func TestAgentPendingFactsSnapshotIsAtomicAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			delivery := fixture.seedPendingDelivery(t)
			var before map[agentidentity.Identity]operatorread.PendingAgentDeliveryFacts
			err := storetest.ReadOperatorSnapshotAcrossCut(fixture.ctx, fixture.store, storetest.OperatorSnapshotDeliveryFactsSettlement(delivery.deliveryID), func(ctx context.Context) error {
				var err error
				before, err = fixture.store.ListPendingAgentDeliveryFacts(ctx, []agentidentity.Identity{fixture.identity}, time.Time{})
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := before[fixture.identity].PendingCount; got != 1 {
				t.Fatalf("snapshot pending facts count = %d, want complete before-state count 1", got)
			}
			after, err := fixture.store.ListPendingAgentDeliveryFacts(fixture.ctx, []agentidentity.Identity{fixture.identity}, time.Time{})
			if err != nil {
				t.Fatalf("load %s post-settlement pending facts: %v", backend.name, err)
			}
			if got := after[fixture.identity].PendingCount; got != 0 {
				t.Fatalf("post-settlement pending facts count = %d, want 0", got)
			}
		})
	}
}

func TestAgentLifecycleFactsSnapshotIsAtomicAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			delivery := fixture.seedPendingDelivery(t)
			var before map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts
			err := storetest.ReadOperatorSnapshotAcrossCut(fixture.ctx, fixture.store, storetest.OperatorSnapshotDeliveryFactsSettlement(delivery.deliveryID), func(ctx context.Context) error {
				var err error
				before, err = fixture.store.ListAgentDeliveryLifecycleFacts(ctx, []agentidentity.Identity{fixture.identity})
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := before[fixture.identity]; got.CurrentState != string(runtimedelivery.StateQueued) || got.BlockingLayer != "delivery_queue" {
				t.Fatalf("snapshot lifecycle facts = %#v, want complete queued before-state", got)
			}
			after, err := fixture.store.ListAgentDeliveryLifecycleFacts(fixture.ctx, []agentidentity.Identity{fixture.identity})
			if err != nil {
				t.Fatalf("load %s post-settlement lifecycle facts: %v", backend.name, err)
			}
			if got := after[fixture.identity]; got != (operatorread.AgentDeliveryLifecycleFacts{}) {
				t.Fatalf("post-settlement lifecycle facts = %#v, want empty", got)
			}
		})
	}
}

func TestAgentOperatorDiagnosisSnapshotAggregateToPageIsAtomicAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			delivery := fixture.seedPendingDelivery(t)
			before := fixture.readDiagnosisCut(t, storetest.OperatorSnapshotDeliverySettlement(delivery.deliveryID))
			if before.Queue.PendingCount != 1 || len(before.Queue.PendingDeliveries) != 1 {
				t.Fatalf("snapshot diagnosis queue = %#v, want complete before-state count/page", before.Queue)
			}
			after, err := fixture.store.LoadOperatorAgentDiagnosis(fixture.ctx, fixture.identity, operatorread.OperatorAgentDiagnosisOptions{QueueLimit: 10})
			if err != nil {
				t.Fatalf("load %s post-settlement diagnosis: %v", backend.name, err)
			}
			if after.Queue.PendingCount != 0 || len(after.Queue.PendingDeliveries) != 0 {
				t.Fatalf("post-settlement diagnosis queue = %#v, want empty", after.Queue)
			}
		})
	}
}

func TestAgentOperatorDiagnosisSnapshotReferenceHydrationIsAtomicAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			delivery := fixture.seedPendingDelivery(t)
			before := fixture.readDiagnosisCut(t, storetest.OperatorSnapshotDeliveryCorruption(delivery.deliveryID))
			if before.Queue.PendingCount != 1 || len(before.Queue.PendingDeliveries) != 1 {
				t.Fatalf("snapshot diagnosis delivery hydration = %#v, want complete before-state page", before.Queue)
			}
			if _, err := fixture.store.LoadOperatorAgentDiagnosis(fixture.ctx, fixture.identity, operatorread.OperatorAgentDiagnosisOptions{QueueLimit: 10}); err == nil {
				t.Fatal("post-mutation diagnosis accepted malformed delivery authority")
			}
		})
	}
}

func TestAgentOperatorDiagnosisSnapshotEventHydrationIsAtomicAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			delivery := fixture.seedPendingDelivery(t)
			before := fixture.readDiagnosisCut(t, storetest.OperatorSnapshotEventCorruption(delivery.eventID))
			if before.Queue.PendingCount != 1 || len(before.Queue.PendingDeliveries) != 1 || before.Queue.PendingDeliveries[0].EventName != "snapshot.pending" {
				t.Fatalf("snapshot diagnosis event hydration = %#v, want complete before-state event", before.Queue)
			}
			if _, err := fixture.store.LoadOperatorAgentDiagnosis(fixture.ctx, fixture.identity, operatorread.OperatorAgentDiagnosisOptions{QueueLimit: 10}); err == nil {
				t.Fatal("post-mutation diagnosis accepted malformed event authority")
			}
		})
	}
}

func TestSelectedStoreAgentSnapshotHandlersExecuteAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: testOperatorHandlers(testOperatorCapabilities{
					AgentConversations: fixture.store,
				}),
			})
			for _, tc := range []struct {
				method string
				params string
			}{
				{method: "agent.list", params: `{}`},
				{method: "agent.get", params: fmt.Sprintf(`{"run_id":%q,"agent_id":%q,"flow_instance":%q}`, fixture.identity.RunID, fixture.identity.AgentID(), fixture.identity.FlowInstance())},
				{method: "agent.diagnose", params: fmt.Sprintf(`{"run_id":%q,"agent_id":%q,"flow_instance":%q,"queue_limit":10}`, fixture.identity.RunID, fixture.identity.AgentID(), fixture.identity.FlowInstance())},
			} {
				response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":%q,"params":%s}`, tc.method, tc.method, tc.params))
				if response.Error != nil {
					t.Fatalf("%s %s error = %#v", backend.name, tc.method, response.Error)
				}
			}
		})
	}
}

func TestSelectedStoreAgentSnapshotHandlersPreserveOutputContractAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			turnID := fixture.seedOutputContractTurn(t)
			fixture.setSessionLease(t, "snapshot-lease")
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: testOperatorHandlers(testOperatorCapabilities{
					AgentConversations: fixture.store,
				}),
			})

			list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list","method":"agent.list","params":{}}`)
			if list.Error != nil {
				t.Fatalf("%s agent.list error = %#v", backend.name, list.Error)
			}
			listed := requireAgentSnapshotRPCListItem(t, list.Result, fixture.identity.AgentID())
			if listed["status"] != "idle" || listed["memory"] != true {
				t.Fatalf("%s agent.list canonical output = %#v", backend.name, listed)
			}

			get := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"get","method":"agent.get","params":{"run_id":%q,"agent_id":%q,"flow_instance":%q}}`, fixture.identity.RunID, fixture.identity.AgentID(), fixture.identity.FlowInstance()))
			if get.Error != nil {
				t.Fatalf("%s agent.get error = %#v", backend.name, get.Error)
			}
			detail := asMap(t, get.Result)
			agent := asMap(t, detail["agent"])
			if agent["status"] != "idle" {
				t.Fatalf("%s lease-backed agent status = %#v, want idle", backend.name, agent["status"])
			}
			sessionRef := asMap(t, detail["current_session_ref"])
			if sessionRef["session_id"] != fixture.sessionID || sessionRef["started_at"] == "" {
				t.Fatalf("%s current_session_ref = %#v", backend.name, sessionRef)
			}
			turnRef := asMap(t, detail["last_turn_ref"])
			if turnRef["turn_id"] != turnID || turnRef["parse_ok"] != false || turnRef["completed_at"] == "" {
				t.Fatalf("%s last_turn_ref = %#v", backend.name, turnRef)
			}

			diagnose := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"diagnose","method":"agent.diagnose","params":{"run_id":%q,"agent_id":%q,"flow_instance":%q}}`, fixture.identity.RunID, fixture.identity.AgentID(), fixture.identity.FlowInstance()))
			if diagnose.Error != nil {
				t.Fatalf("%s agent.diagnose error = %#v", backend.name, diagnose.Error)
			}
			diagnosis := asMap(t, diagnose.Result)
			if diagnosis["status"] != "idle" {
				t.Fatalf("%s diagnosis status = %#v, want idle", backend.name, diagnosis["status"])
			}
			if _, ok := diagnosis["delivery_lifecycle"]; ok {
				t.Fatalf("%s diagnosis exposed absent lifecycle: %#v", backend.name, diagnosis["delivery_lifecycle"])
			}
			active := asMap(t, diagnosis["active"])
			if active["turn_id"] != turnID {
				t.Fatalf("%s diagnosis active = %#v", backend.name, active)
			}
			if _, ok := active["task_id"]; ok {
				t.Fatalf("%s diagnosis active exposed absent task_id: %#v", backend.name, active)
			}
			if _, ok := active["entity_id"]; ok {
				t.Fatalf("%s diagnosis active exposed absent entity_id: %#v", backend.name, active)
			}
			lastTool := asMap(t, diagnosis["last_tool_outcome"])
			if lastTool["turn_id"] != turnID || lastTool["tool_name"] != "selected_tool" || lastTool["tool_use_id"] != "toolu-selected" || lastTool["ok"] != false {
				t.Fatalf("%s diagnosis last_tool_outcome = %#v", backend.name, lastTool)
			}
			for _, privateField := range []string{"output", "result"} {
				if _, ok := lastTool[privateField]; ok {
					t.Fatalf("%s diagnosis leaked private last-tool field %q: %#v", backend.name, privateField, lastTool)
				}
			}

			emptyIdentity := sqliteAgentUsageIdentityForRun(t, fixture.runID, "snapshot-agent-b")
			emptyGet := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"empty-get","method":"agent.get","params":{"run_id":%q,"agent_id":%q,"flow_instance":%q}}`, emptyIdentity.RunID, emptyIdentity.AgentID(), emptyIdentity.FlowInstance()))
			if emptyGet.Error != nil {
				t.Fatalf("%s empty agent.get error = %#v", backend.name, emptyGet.Error)
			}
			emptyDetail := asMap(t, emptyGet.Result)
			for _, absent := range []string{"current_session_ref", "last_turn_ref"} {
				if _, ok := emptyDetail[absent]; ok {
					t.Fatalf("%s empty agent.get exposed %s: %#v", backend.name, absent, emptyDetail)
				}
			}
			emptyDiagnose := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"empty-diagnose","method":"agent.diagnose","params":{"run_id":%q,"agent_id":%q,"flow_instance":%q}}`, emptyIdentity.RunID, emptyIdentity.AgentID(), emptyIdentity.FlowInstance()))
			if emptyDiagnose.Error != nil {
				t.Fatalf("%s empty agent.diagnose error = %#v", backend.name, emptyDiagnose.Error)
			}
			emptyDiagnosis := asMap(t, emptyDiagnose.Result)
			for _, absent := range []string{"current_session_ref", "last_turn_ref", "delivery_lifecycle", "runtime_state", "active", "last_tool_outcome"} {
				if _, ok := emptyDiagnosis[absent]; ok {
					t.Fatalf("%s empty diagnosis exposed %s: %#v", backend.name, absent, emptyDiagnosis)
				}
			}
		})
	}
}

func TestAgentOperatorSnapshotRejectsMalformedBaseAuthorityAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			if err := storetest.CorruptOperatorAgentSubscriptions(fixture.ctx, fixture.store, fixture.identity); err != nil {
				t.Fatalf("corrupt %s subscriptions: %v", backend.name, err)
			}
			if _, err := fixture.store.ListOperatorAgents(fixture.ctx, operatorread.OperatorAgentListOptions{}); err == nil || !strings.Contains(err.Error(), "subscriptions must be a json string array") {
				t.Fatalf("malformed %s subscriptions error = %v", backend.name, err)
			}
		})
	}
}

func TestAgentOperatorSnapshotRejectsMalformedRelatedAuthorityAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		for _, tc := range []struct {
			name   string
			mutate func(*testing.T, agentSnapshotBoundaryFixture)
			read   func(agentSnapshotBoundaryFixture) error
		}{
			{
				name: "opaque config",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					if err := storetest.CorruptOperatorAgentConfig(f.ctx, f.store, f.identity); err != nil {
						t.Fatal(err)
					}
				},
			},
			{
				name: "agent identity",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					storetest.RequireMalformedOperatorAgentRoute(t, f.ctx, f.store, f.identity)
				},
			},
			{
				name: "agent status",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					storetest.RequireUnknownOperatorAgentStatus(t, f.ctx, f.store, f.identity)
				},
			},
			{
				name: "session runtime state",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					if err := storetest.CorruptOperatorSessionState(f.ctx, f.store, f.sessionID); err != nil {
						t.Fatal(err)
					}
				},
			},
			{
				name: "turn",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					turnID := f.seedTurn(t, "valid turn")
					if err := storetest.CorruptOperatorTurnBlocks(f.ctx, f.store, turnID); err != nil {
						t.Fatal(err)
					}
				},
			},
			{
				name: "delivery lifecycle",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					delivery := f.seedPendingDelivery(t)
					if err := storetest.CorruptOperatorDeliveryRoute(f.ctx, f.store, delivery.deliveryID); err != nil {
						t.Fatal(err)
					}
				},
			},
			{
				name: "event record",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					delivery := f.seedPendingDelivery(t)
					if err := storetest.CorruptOperatorSnapshotEvent(f.ctx, f.store, delivery.eventID); err != nil {
						t.Fatal(err)
					}
				},
				read: func(f agentSnapshotBoundaryFixture) error {
					_, err := f.store.LoadOperatorAgentDiagnosis(f.ctx, f.identity, operatorread.OperatorAgentDiagnosisOptions{QueueLimit: 10})
					return err
				},
			},
		} {
			t.Run(backend.name+"/"+tc.name, func(t *testing.T) {
				fixture := newAgentSnapshotBoundaryFixture(t, backend)
				tc.mutate(t, fixture)
				var err error
				if tc.read != nil {
					err = tc.read(fixture)
				} else {
					_, err = fixture.store.ListOperatorAgents(fixture.ctx, operatorread.OperatorAgentListOptions{})
				}
				if err == nil {
					t.Fatalf("malformed %s %s authority returned success", backend.name, tc.name)
				}
			})
		}
	}
}

type agentSnapshotBoundaryFixture struct {
	backend   agentSnapshotBackend
	ctx       context.Context
	store     agentSnapshotTestStore
	runID     string
	identity  agentidentity.Identity
	sessionID string
}

func newAgentSnapshotBoundaryFixture(t *testing.T, backend agentSnapshotBackend) agentSnapshotBoundaryFixture {
	t.Helper()
	if err := storetest.InitializeOperatorSnapshotReadBarrier(); err != nil {
		t.Fatal(err)
	}
	ctx := testAuthorActivityContext(context.Background())
	setupStart := time.Now()
	store := backend.open(t, ctx)
	openedAt := time.Now()
	if err := storetest.PrepareOperatorSnapshotReads(ctx, store); err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	startedAt := time.Now().UTC().Add(-time.Minute)
	storetest.RequireRun(t, ctx, store, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: authorActivityTestSourceArtifact, StartedAt: startedAt})
	runAt := time.Now()
	identities := make(map[string]agentidentity.Identity, 2)
	for _, agentID := range []string{"snapshot-agent-a", "snapshot-agent-b"} {
		identity := sqliteAgentUsageIdentityForRun(t, runID, agentID)
		identities[agentID] = identity
		if err := storetest.UpsertStaticAgentFixture(t, ctx, store, runtimemanager.PersistedAgent{
			Config: apiExecutionFixtureConfig(t, "anthropic", runtimeactors.AgentConfig{
				Identity: identity, ID: agentID, Role: "worker", Type: "managed", Model: "regular",
				ExecutionMode: "live", ResolvedLLMBackend: "anthropic", FlowID: "flow",
				FlowPath: identity.FlowInstance(), Memory: agentmemory.Plan{Enabled: true}, Config: json.RawMessage(`{}`),
				Intent: apiTestResolvedIntent(t, agentID, "Prove one atomic operator agent snapshot."),
			}),
			Status: "active", StartedAt: startedAt,
		}); err != nil {
			t.Fatalf("seed %s %s: %v", backend.name, agentID, err)
		}
	}
	ctx = admitAPITestAgentExecution(t, ctx, store, authorActivityTestSourceArtifactFact)
	agentsAt := time.Now()
	fixture := agentSnapshotBoundaryFixture{
		backend: backend, ctx: ctx, store: store, runID: runID,
		identity: identities["snapshot-agent-a"],
	}
	sessionCtx := fixture.sessionAuthorityContext(t)
	lease, err := store.Acquire(sessionCtx, fixture.identity, "snapshot-fixture-setup")
	if err != nil {
		t.Fatalf("issue %s snapshot session: %v", backend.name, err)
	}
	fixture.sessionID = lease.SessionID
	if result, err := store.ReleaseOutcome(sessionCtx, lease); err != nil || !result.Acknowledged {
		t.Fatalf("release %s snapshot setup grant: acknowledged=%v err=%v", backend.name, result.Acknowledged, err)
	}
	t.Logf("snapshot fixture setup: open=%s run=%s agents=%s session=%s", openedAt.Sub(setupStart), runAt.Sub(openedAt), agentsAt.Sub(runAt), time.Since(agentsAt))
	return fixture
}

func (f agentSnapshotBoundaryFixture) sessionAuthorityContext(t *testing.T) context.Context {
	t.Helper()
	state, found, err := f.store.LoadAgentLifecycleState(f.ctx, f.identity)
	if err != nil || !found {
		t.Fatalf("load %s snapshot fixture lifecycle: found=%v err=%v", f.backend.name, found, err)
	}
	return runtimeeffects.WithLifecycleToken(f.ctx, runtimeeffects.LifecycleToken{
		RuntimeEpoch: state.RuntimeEpoch, Identity: f.identity,
		AgentID: f.identity.AgentID(), Generation: state.Generation,
	})
}

func (f agentSnapshotBoundaryFixture) list(t *testing.T) operatorread.OperatorAgentListResult {
	t.Helper()
	result, err := f.store.ListOperatorAgents(f.ctx, operatorread.OperatorAgentListOptions{})
	if err != nil {
		t.Fatalf("list %s snapshot agents: %v", f.backend.name, err)
	}
	return result
}

func (f agentSnapshotBoundaryFixture) setSessionLease(t *testing.T, holder string) {
	t.Helper()
	sessionCtx := f.sessionAuthorityContext(t)
	lease, err := f.store.Acquire(sessionCtx, f.identity, holder)
	if err != nil {
		t.Fatalf("issue %s snapshot output-contract grant: %v", f.backend.name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(sessionCtx), 10*time.Second)
		defer cancel()
		if result, err := f.store.ReleaseOutcome(ctx, lease); err != nil || !result.Acknowledged {
			t.Errorf("release %s snapshot output-contract grant: acknowledged=%v err=%v", f.backend.name, result.Acknowledged, err)
		}
	})
	if lease.SessionID != f.sessionID {
		t.Fatalf("%s output-contract grant changed session: got %q want %q", f.backend.name, lease.SessionID, f.sessionID)
	}
}

type agentSnapshotDeliveryFixture struct {
	deliveryID string
	eventID    string
}

func (f agentSnapshotBoundaryFixture) seedPendingDelivery(t *testing.T) agentSnapshotDeliveryFixture {
	t.Helper()
	createdAt := time.Now().UTC().Add(-30 * time.Second)
	event := eventtest.PersistedProjection(
		uuid.NewString(), "snapshot.pending", "snapshot-gateway", "", json.RawMessage(`{"snapshot":true}`), 0,
		f.runID, "", events.EventEnvelope{}, createdAt,
	)
	route := events.DeliveryRoute{
		Recipient:     events.MustAgentDeliveryRecipient(f.identity.AgentID()),
		AgentIdentity: f.identity,
		Target: events.MustExistingEntityTarget(events.RouteIdentity{
			FlowID: "flow", FlowInstance: f.identity.FlowInstance(), EntityID: uuid.NewString(),
		}),
	}
	storetest.CommitSemanticEventWithRoutes(t, f.ctx, f.store, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
	deliveryID, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatalf("derive %s snapshot delivery ID: %v", f.backend.name, err)
	}
	return agentSnapshotDeliveryFixture{deliveryID: deliveryID, eventID: event.ID()}
}

func (f agentSnapshotBoundaryFixture) seedTurn(t *testing.T, output string) string {
	t.Helper()
	return f.seedTurnWithOutputContract(t, "snapshot-turn", "", true, []runtimellm.TurnBlock{{
		Kind: "turn_summary", Data: json.RawMessage(fmt.Sprintf(`{"assistant_visible_output":%q,"outcome":"completed"}`, output)),
	}})
}

func (f agentSnapshotBoundaryFixture) seedOutputContractTurn(t *testing.T) string {
	t.Helper()
	return f.seedTurnWithOutputContract(t, "", "", false, []runtimellm.TurnBlock{
		{Kind: "tool_result", ToolName: "selected_tool", Output: json.RawMessage(`{"secret":"private-provider-output"}`), Data: json.RawMessage(`{"tool_use_id":"toolu-selected"}`)},
		{Kind: "turn_summary", Data: json.RawMessage(`{"assistant_visible_output":"public summary","outcome":"failed"}`)},
	})
}

func (f agentSnapshotBoundaryFixture) seedTurnWithOutputContract(t *testing.T, taskID, entityID string, parseOK bool, blocks []runtimellm.TurnBlock) string {
	t.Helper()
	eventID := uuid.NewString()
	createdAt := time.Now().UTC().Add(-20 * time.Second)
	event := storetest.InsertNativeExistingRunRootEvent(
		t, f.ctx, f.store, eventID, f.runID, events.EventType("snapshot.turn"),
		eventtest.Producer(events.EventProducerExternal, "snapshot-turn"), []byte(`{}`),
		events.EventEnvelope{Scope: events.EventScopeGlobal}, createdAt.Add(-time.Second),
	)
	turnID := uuid.NewString()
	storetest.PersistManagedAgentTurnFixture(t, f.ctx, storetest.ManagedAgentTurnFixture{
		Store: f.store, Selected: f.store, Identity: f.identity, RunID: f.runID,
		SessionID: f.sessionID, TurnID: turnID, Memory: agentmemory.Plan{Enabled: true}, Event: event,
		TaskID: taskID, EntityID: entityID, ParseOK: parseOK, Latency: time.Millisecond, CreatedAt: createdAt,
		TurnBlocks: blocks,
	})
	return turnID
}

func (f agentSnapshotBoundaryFixture) readSummaryCut(t *testing.T, cut storetest.OperatorSnapshotCut) operatorread.OperatorAgentListResult {
	t.Helper()
	var out operatorread.OperatorAgentListResult
	err := storetest.ReadOperatorSnapshotAcrossCut(f.ctx, f.store, cut, func(ctx context.Context) error {
		var err error
		out, err = f.store.ListOperatorAgents(ctx, operatorread.OperatorAgentListOptions{})
		return err
	})
	if err != nil {
		t.Fatalf("read atomic %s operator summary: %v", f.backend.name, err)
	}
	return out
}

func (f agentSnapshotBoundaryFixture) readDiagnosisCut(t *testing.T, cut storetest.OperatorSnapshotCut) operatorread.OperatorAgentDiagnosis {
	t.Helper()
	var out operatorread.OperatorAgentDiagnosis
	err := storetest.ReadOperatorSnapshotAcrossCut(f.ctx, f.store, cut, func(ctx context.Context) error {
		var err error
		out, err = f.store.LoadOperatorAgentDiagnosis(ctx, f.identity, operatorread.OperatorAgentDiagnosisOptions{QueueLimit: 10})
		return err
	})
	if err != nil {
		t.Fatalf("read atomic %s operator diagnosis: %v", f.backend.name, err)
	}
	return out
}

func requireSnapshotAgent(t *testing.T, result operatorread.OperatorAgentListResult, agentID string) operatorread.OperatorAgentSummary {
	t.Helper()
	for _, agent := range result.Agents {
		if agent.AgentID == agentID {
			return agent
		}
	}
	t.Fatalf("agent %s missing from snapshot: %#v", agentID, result.Agents)
	return operatorread.OperatorAgentSummary{}
}

func requireAgentSnapshotRPCListItem(t *testing.T, result any, agentID string) map[string]any {
	t.Helper()
	items, ok := asMap(t, result)["agents"].([]any)
	if !ok {
		t.Fatalf("agent.list agents = %#v, want array", asMap(t, result)["agents"])
	}
	for _, item := range items {
		agent := asMap(t, item)
		if agent["agent_id"] == agentID {
			return agent
		}
	}
	t.Fatalf("agent.list missing %s: %#v", agentID, items)
	return nil
}
