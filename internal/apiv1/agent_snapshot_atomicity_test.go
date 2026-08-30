package apiv1

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	modernsqlite "modernc.org/sqlite"
)

type agentSnapshotTestStore interface {
	AgentReadStore
	storetest.AgentFixtureStore
	storetest.ManagedAgentTurnFixtureStore
	ListPendingAgentDeliveryFacts(context.Context, []agentidentity.Identity, time.Time) (map[agentidentity.Identity]operatorread.PendingAgentDeliveryFacts, error)
	ListAgentDeliveryLifecycleFacts(context.Context, []agentidentity.Identity) (map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts, error)
}

type agentSnapshotBackend struct {
	name   string
	sqlite bool
	open   func(*testing.T, context.Context) (agentSnapshotTestStore, *sql.DB)
}

type sqliteAgentSnapshotBarrier struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
}

var (
	sqliteSnapshotBarrierID       atomic.Int64
	sqliteSnapshotBarrierRegistry sync.Map
	sqliteSnapshotFunctionOnce    sync.Once
	sqliteSnapshotFunctionErr     error
)

func requireSQLiteAgentSnapshotFunction(t *testing.T) {
	t.Helper()
	sqliteSnapshotFunctionOnce.Do(func() {
		sqliteSnapshotFunctionErr = modernsqlite.RegisterScalarFunction("agent_snapshot_barrier", 1, func(_ *modernsqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			id, ok := args[0].(int64)
			if !ok {
				return nil, fmt.Errorf("agent snapshot barrier id has type %T", args[0])
			}
			value, ok := sqliteSnapshotBarrierRegistry.Load(id)
			if !ok {
				return nil, fmt.Errorf("agent snapshot barrier %d is not registered", id)
			}
			barrier := value.(*sqliteAgentSnapshotBarrier)
			barrier.enteredOnce.Do(func() { close(barrier.entered) })
			<-barrier.release
			return int64(1), nil
		})
	})
	if sqliteSnapshotFunctionErr != nil {
		t.Fatalf("register sqlite agent snapshot barrier function: %v", sqliteSnapshotFunctionErr)
	}
}

func agentSnapshotBackends() []agentSnapshotBackend {
	return []agentSnapshotBackend{
		{
			name: "sqlite", sqlite: true,
			open: func(t *testing.T, ctx context.Context) (agentSnapshotTestStore, *sql.DB) {
				selected := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				return selected, storetest.DatabaseForTest(selected)
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T, _ context.Context) (agentSnapshotTestStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
	}
}

func TestAgentOperatorSummarySnapshotTerminalizationIsAtomicAcrossBackends(t *testing.T) {
	for _, backend := range agentSnapshotBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newAgentSnapshotBoundaryFixture(t, backend)
			before := fixture.readAcrossSessionBoundary(t, func(exec agentSnapshotMutationExec) {
				fixture.setAgentStatus(t, exec, "snapshot-agent-b", "terminated")
			})
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
			fixture.setAgentStatus(t, fixture.db, "snapshot-agent-b", "terminated")
			before := fixture.readAcrossSessionBoundary(t, func(exec agentSnapshotMutationExec) {
				fixture.setAgentStatus(t, exec, "snapshot-agent-b", "active")
			})
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
			before := fixture.readAcrossSessionBoundary(t, func(exec agentSnapshotMutationExec) {
				fixture.replaceSession(t, exec, newSessionID)
			})
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
			before := fixture.readAcrossExternalTableBoundary(t, "agent_sessions", func(exec agentSnapshotMutationExec) {
				fixture.settlePendingSnapshotDelivery(t, exec, delivery.deliveryID)
			})
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
			before := fixture.readAcrossExternalTableBoundary(t, "agent_turns", func(exec agentSnapshotMutationExec) {
				fixture.replaceTurnSummary(t, exec, turnID, "after snapshot")
			})
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
			fixture.readDeliveryWrapperAcrossBoundary(t, func() error {
				var err error
				before, err = fixture.store.ListPendingAgentDeliveryFacts(fixture.ctx, []agentidentity.Identity{fixture.identity}, time.Time{})
				return err
			}, func(exec agentSnapshotMutationExec, table string) {
				fixture.settlePendingSnapshotDeliveryInTable(t, exec, table, delivery.deliveryID)
			})
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
			fixture.readDeliveryWrapperAcrossBoundary(t, func() error {
				var err error
				before, err = fixture.store.ListAgentDeliveryLifecycleFacts(fixture.ctx, []agentidentity.Identity{fixture.identity})
				return err
			}, func(exec agentSnapshotMutationExec, table string) {
				fixture.settlePendingSnapshotDeliveryInTable(t, exec, table, delivery.deliveryID)
			})
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
			before := fixture.readDiagnosisAcrossSessionBoundary(t, func(exec agentSnapshotMutationExec) {
				fixture.settlePendingSnapshotDelivery(t, exec, delivery.deliveryID)
			})
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
			before := fixture.readDiagnosisAcrossSessionBoundary(t, func(exec agentSnapshotMutationExec) {
				fixture.updateJSONWith(t, exec, "event_deliveries", "delivery_target_route", `{}`, "delivery_id", delivery.deliveryID)
			})
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
			before := fixture.readDiagnosisAcrossSessionBoundary(t, func(exec agentSnapshotMutationExec) {
				fixture.execDialect(t, exec,
					`UPDATE events SET payload_bytes=? WHERE event_id=?`,
					`UPDATE events SET payload_bytes=$1 WHERE event_id=$2::uuid`,
					[]byte{0}, delivery.eventID)
			})
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
				{method: "agent.get", params: fmt.Sprintf(`{"agent_id":%q,"flow_instance":%q}`, fixture.identity.AgentID(), fixture.identity.FlowInstance())},
				{method: "agent.diagnose", params: fmt.Sprintf(`{"agent_id":%q,"flow_instance":%q,"queue_limit":10}`, fixture.identity.AgentID(), fixture.identity.FlowInstance())},
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
			fixture.setSessionLease(t, "snapshot-lease")
			turnID := fixture.seedOutputContractTurn(t)
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
			if listed["status"] != "idle" || listed["memory"] != true || listed["memory_source"] != "authored" {
				t.Fatalf("%s agent.list canonical output = %#v", backend.name, listed)
			}

			get := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"get","method":"agent.get","params":{"agent_id":%q,"flow_instance":%q}}`, fixture.identity.AgentID(), fixture.identity.FlowInstance()))
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

			diagnose := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"diagnose","method":"agent.diagnose","params":{"agent_id":%q,"flow_instance":%q}}`, fixture.identity.AgentID(), fixture.identity.FlowInstance()))
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

			emptyIdentity := sqliteAgentUsageIdentity(t, "snapshot-agent-b")
			emptyGet := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"empty-get","method":"agent.get","params":{"agent_id":%q,"flow_instance":%q}}`, emptyIdentity.AgentID(), emptyIdentity.FlowInstance()))
			if emptyGet.Error != nil {
				t.Fatalf("%s empty agent.get error = %#v", backend.name, emptyGet.Error)
			}
			emptyDetail := asMap(t, emptyGet.Result)
			for _, absent := range []string{"current_session_ref", "last_turn_ref"} {
				if _, ok := emptyDetail[absent]; ok {
					t.Fatalf("%s empty agent.get exposed %s: %#v", backend.name, absent, emptyDetail)
				}
			}
			emptyDiagnose := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"empty-diagnose","method":"agent.diagnose","params":{"agent_id":%q,"flow_instance":%q}}`, emptyIdentity.AgentID(), emptyIdentity.FlowInstance()))
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
			query := `UPDATE agents SET subscriptions='{}' WHERE agent_id=?`
			if !backend.sqlite {
				query = `UPDATE agents SET subscriptions='{}'::jsonb WHERE agent_id=$1`
			}
			if _, err := fixture.db.ExecContext(fixture.ctx, query, "snapshot-agent-a"); err != nil {
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
					f.updateJSON(t, "agents", "config", `[]`, "agent_id", f.identity.AgentID())
				},
			},
			{
				name: "memory plan",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					f.withRelaxedCheckConstraints(t, "agents", "memory_source", func(exec agentSnapshotMutationExec) {
						f.execDialect(t, exec,
							`UPDATE agents SET memory_enabled=1, memory_source='platform_default' WHERE agent_id=?`,
							`UPDATE agents SET memory_enabled=TRUE, memory_source='platform_default' WHERE agent_id=$1`,
							f.identity.AgentID())
					})
				},
			},
			{
				name: "agent identity",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					f.withRelaxedAgentIdentityConstraints(t, func(exec agentSnapshotMutationExec) {
						f.execDialect(t, exec,
							`UPDATE agents SET flow_scope_key='' WHERE agent_id=?`,
							`UPDATE agents SET flow_scope_key='' WHERE agent_id=$1`,
							f.identity.AgentID())
					})
				},
			},
			{
				name: "agent status",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					f.withRelaxedCheckConstraints(t, "agents", "status", func(exec agentSnapshotMutationExec) {
						f.execDialect(t, exec,
							`UPDATE agents SET status='unknown' WHERE agent_id=?`,
							`UPDATE agents SET status='unknown' WHERE agent_id=$1`,
							f.identity.AgentID())
					})
				},
			},
			{
				name: "session runtime state",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					f.updateJSON(t, "agent_sessions", "runtime_state", `"invalid"`, "session_id", f.sessionID)
				},
			},
			{
				name: "turn",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					turnID := f.seedTurn(t, "valid turn")
					f.updateJSON(t, "agent_turns", "turn_blocks", `{}`, "turn_id", turnID)
				},
			},
			{
				name: "delivery lifecycle",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					delivery := f.seedPendingDelivery(t)
					f.updateJSON(t, "event_deliveries", "delivery_target_route", `{}`, "delivery_id", delivery.deliveryID)
				},
			},
			{
				name: "event record",
				mutate: func(t *testing.T, f agentSnapshotBoundaryFixture) {
					delivery := f.seedPendingDelivery(t)
					f.execDialect(t, f.db,
						`UPDATE events SET payload_bytes=? WHERE event_id=?`,
						`UPDATE events SET payload_bytes=$1 WHERE event_id=$2::uuid`,
						[]byte{0}, delivery.eventID)
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

type agentSnapshotMutationExec interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type agentSnapshotBoundaryFixture struct {
	backend   agentSnapshotBackend
	ctx       context.Context
	store     agentSnapshotTestStore
	db        *sql.DB
	runID     string
	identity  agentidentity.Identity
	sessionID string
}

func newAgentSnapshotBoundaryFixture(t *testing.T, backend agentSnapshotBackend) agentSnapshotBoundaryFixture {
	t.Helper()
	if backend.sqlite {
		requireSQLiteAgentSnapshotFunction(t)
	}
	ctx := testAuthorActivityContext(context.Background())
	store, db := backend.open(t, ctx)
	if backend.sqlite {
		if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
			t.Fatalf("enable sqlite WAL: %v", err)
		}
	}
	runID := uuid.NewString()
	startedAt := time.Now().UTC().Add(-time.Minute)
	if backend.sqlite {
		storetest.RequireSQLiteRun(t, ctx, db, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: authorActivityTestSourceArtifact, StartedAt: startedAt})
	} else {
		storetest.RequirePostgresRun(t, ctx, db, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: authorActivityTestSourceArtifact, StartedAt: startedAt})
	}
	identities := make(map[string]agentidentity.Identity, 2)
	for _, agentID := range []string{"snapshot-agent-a", "snapshot-agent-b"} {
		identity := sqliteAgentUsageIdentity(t, agentID)
		identities[agentID] = identity
		if err := storetest.UpsertStaticAgentFixture(t, ctx, store, runtimemanager.PersistedAgent{
			Config: runtimeactors.AgentConfig{
				Identity: identity, ID: agentID, Role: "worker", Type: "managed", Model: "regular",
				ExecutionMode: "live", ResolvedLLMBackend: "anthropic", FlowID: "flow",
				FlowPath: identity.FlowInstance(), Memory: agentmemory.Authored(true), Config: json.RawMessage(`{}`),
				Intent: apiTestResolvedIntent(t, agentID, "Prove one atomic operator agent snapshot."),
			},
			Status: "active", StartedAt: startedAt,
		}); err != nil {
			t.Fatalf("seed %s %s: %v", backend.name, agentID, err)
		}
	}
	fixture := agentSnapshotBoundaryFixture{
		backend: backend, ctx: ctx, store: store, db: db, runID: runID,
		identity: identities["snapshot-agent-a"], sessionID: uuid.NewString(),
	}
	fixture.insertSession(t, db, fixture.sessionID)
	return fixture
}

func (f agentSnapshotBoundaryFixture) list(t *testing.T) operatorread.OperatorAgentListResult {
	t.Helper()
	result, err := f.store.ListOperatorAgents(f.ctx, operatorread.OperatorAgentListOptions{})
	if err != nil {
		t.Fatalf("list %s snapshot agents: %v", f.backend.name, err)
	}
	return result
}

func (f agentSnapshotBoundaryFixture) execDialect(t *testing.T, exec agentSnapshotMutationExec, sqliteQuery, postgresQuery string, args ...any) {
	t.Helper()
	query := sqliteQuery
	if !f.backend.sqlite {
		query = postgresQuery
	}
	result, err := exec.ExecContext(f.ctx, query, args...)
	if err != nil {
		t.Fatalf("mutate %s snapshot authority: %v", f.backend.name, err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		t.Fatalf("mutate %s snapshot authority changed %d rows, want 1", f.backend.name, changed)
	}
}

func (f agentSnapshotBoundaryFixture) updateJSON(t *testing.T, table, column, raw, keyColumn, key string) {
	t.Helper()
	f.updateJSONWith(t, f.db, table, column, raw, keyColumn, key)
}

func (f agentSnapshotBoundaryFixture) updateJSONWith(t *testing.T, exec agentSnapshotMutationExec, table, column, raw, keyColumn, key string) {
	t.Helper()
	f.execDialect(t, exec,
		fmt.Sprintf(`UPDATE %s SET %s=? WHERE %s=?`, table, column, keyColumn),
		fmt.Sprintf(`UPDATE %s SET %s=$1::jsonb WHERE %s::text=$2`, table, column, keyColumn),
		raw, key,
	)
}

func (f agentSnapshotBoundaryFixture) withRelaxedCheckConstraints(t *testing.T, table, definitionFragment string, mutate func(agentSnapshotMutationExec)) {
	t.Helper()
	if f.backend.sqlite {
		conn, err := f.db.Conn(f.ctx)
		if err != nil {
			t.Fatalf("acquire sqlite hostile-authority connection: %v", err)
		}
		defer conn.Close()
		if _, err := conn.ExecContext(f.ctx, `PRAGMA ignore_check_constraints=ON`); err != nil {
			t.Fatalf("relax sqlite check constraints: %v", err)
		}
		defer conn.ExecContext(f.ctx, `PRAGMA ignore_check_constraints=OFF`)
		mutate(conn)
		return
	}
	rows, err := f.db.QueryContext(f.ctx, `
		SELECT conname
		FROM pg_constraint
		WHERE conrelid=$1::regclass AND contype='c' AND pg_get_constraintdef(oid) ILIKE $2
		ORDER BY conname`, table, "%"+definitionFragment+"%")
	if err != nil {
		t.Fatalf("list postgres %s hostile-authority constraints: %v", table, err)
	}
	var constraints []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			t.Fatalf("scan postgres %s hostile-authority constraint: %v", table, err)
		}
		constraints = append(constraints, name)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close postgres %s hostile-authority constraints: %v", table, err)
	}
	if len(constraints) == 0 {
		t.Fatalf("postgres %s has no check constraint containing %q", table, definitionFragment)
	}
	for _, name := range constraints {
		quoted := strings.ReplaceAll(name, `"`, `""`)
		if _, err := f.db.ExecContext(f.ctx, fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT "%s"`, table, quoted)); err != nil {
			t.Fatalf("drop postgres %s hostile-authority constraint %s: %v", table, name, err)
		}
	}
	mutate(f.db)
}

func (f agentSnapshotBoundaryFixture) withRelaxedAgentIdentityConstraints(t *testing.T, mutate func(agentSnapshotMutationExec)) {
	t.Helper()
	if f.backend.sqlite {
		conn, err := f.db.Conn(f.ctx)
		if err != nil {
			t.Fatalf("acquire sqlite hostile-identity connection: %v", err)
		}
		defer conn.Close()
		if _, err := conn.ExecContext(f.ctx, `PRAGMA foreign_keys=OFF`); err != nil {
			t.Fatalf("relax sqlite foreign keys: %v", err)
		}
		if _, err := conn.ExecContext(f.ctx, `PRAGMA ignore_check_constraints=ON`); err != nil {
			t.Fatalf("relax sqlite identity checks: %v", err)
		}
		defer conn.ExecContext(f.ctx, `PRAGMA foreign_keys=ON`)
		defer conn.ExecContext(f.ctx, `PRAGMA ignore_check_constraints=OFF`)
		mutate(conn)
		return
	}
	rows, err := f.db.QueryContext(f.ctx, `
		SELECT conrelid::regclass::text, conname
		FROM pg_constraint
		WHERE confrelid='agents'::regclass AND contype='f'
		ORDER BY conrelid::regclass::text, conname`)
	if err != nil {
		t.Fatalf("list postgres agent identity foreign keys: %v", err)
	}
	type constraint struct{ table, name string }
	var constraints []constraint
	for rows.Next() {
		var item constraint
		if err := rows.Scan(&item.table, &item.name); err != nil {
			_ = rows.Close()
			t.Fatalf("scan postgres agent identity foreign key: %v", err)
		}
		constraints = append(constraints, item)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close postgres agent identity foreign keys: %v", err)
	}
	for _, item := range constraints {
		table := strings.ReplaceAll(item.table, `"`, `""`)
		name := strings.ReplaceAll(item.name, `"`, `""`)
		if _, err := f.db.ExecContext(f.ctx, fmt.Sprintf(`ALTER TABLE "%s" DROP CONSTRAINT "%s"`, table, name)); err != nil {
			t.Fatalf("drop postgres agent identity foreign key %s.%s: %v", item.table, item.name, err)
		}
	}
	f.withRelaxedCheckConstraints(t, "agents", "flow_scope_key", mutate)
}

func (f agentSnapshotBoundaryFixture) setAgentStatus(t *testing.T, exec agentSnapshotMutationExec, agentID, status string) {
	t.Helper()
	query := `UPDATE agents SET status=? WHERE agent_id=?`
	args := []any{status, agentID}
	if !f.backend.sqlite {
		query = `UPDATE agents SET status=$1 WHERE agent_id=$2`
	}
	if _, err := exec.ExecContext(f.ctx, query, args...); err != nil {
		t.Fatalf("set %s agent %s status: %v", f.backend.name, agentID, err)
	}
}

func (f agentSnapshotBoundaryFixture) insertSession(t *testing.T, exec agentSnapshotMutationExec, sessionID string) {
	t.Helper()
	fields, err := f.identity.StorageFields()
	if err != nil {
		t.Fatal(err)
	}
	args := []any{sessionID, f.runID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, time.Now().UTC()}
	query := `INSERT INTO agent_sessions (
		session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
		conversation, turn_count, runtime_state, status, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored', '[]', 0, '{}', 'active', ?, ?)`
	args = append(args, args[len(args)-1])
	if !f.backend.sqlite {
		query = `INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored', '[]'::jsonb, 0, '{}'::jsonb, 'active', $10, $11)`
	}
	if _, err := exec.ExecContext(f.ctx, query, args...); err != nil {
		t.Fatalf("insert %s agent session: %v", f.backend.name, err)
	}
}

func (f agentSnapshotBoundaryFixture) setSessionLease(t *testing.T, holder string) {
	t.Helper()
	expiresAt := time.Now().UTC().Add(time.Hour)
	f.execDialect(t, f.db,
		`UPDATE agent_sessions SET lease_holder=?, lease_expires_at=? WHERE session_id=?`,
		`UPDATE agent_sessions SET lease_holder=$1, lease_expires_at=$2 WHERE session_id=$3::uuid`,
		holder, expiresAt, f.sessionID,
	)
}

func (f agentSnapshotBoundaryFixture) replaceSession(t *testing.T, exec agentSnapshotMutationExec, successorID string) {
	t.Helper()
	table := "agent_sessions"
	if f.backend.sqlite {
		table = "agent_sessions_snapshot_source"
	}
	now := time.Now().UTC()
	query := fmt.Sprintf(`UPDATE %s SET status='terminated', termination_reason='normal', terminated_at=?, updated_at=? WHERE session_id=?`, table)
	args := []any{now, now, f.sessionID}
	if !f.backend.sqlite {
		query = `UPDATE agent_sessions SET status='terminated', termination_reason='normal', terminated_at=$1, updated_at=$2 WHERE session_id=$3::uuid`
	}
	if _, err := exec.ExecContext(f.ctx, query, args...); err != nil {
		t.Fatalf("terminate %s predecessor session: %v", f.backend.name, err)
	}
	if f.backend.sqlite {
		f.insertSQLiteRenamedSession(t, exec, successorID)
		return
	}
	f.insertSession(t, exec, successorID)
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
	event := storetest.InsertExistingRunRootEventRecord(
		t, f.ctx, f.db, authoractivityfixture.Dialect(f.backend.name), eventID, f.runID, events.EventType("snapshot.turn"),
		eventtest.Producer(events.EventProducerExternal, "snapshot-turn"), []byte(`{}`),
		events.EventEnvelope{Scope: events.EventScopeGlobal}, createdAt.Add(-time.Second),
	)
	turnID := uuid.NewString()
	storetest.PersistManagedAgentTurnFixture(t, f.ctx, storetest.ManagedAgentTurnFixture{
		Store: f.store, Selected: f.store, Identity: f.identity, RunID: f.runID,
		SessionID: f.sessionID, TurnID: turnID, Memory: agentmemory.Authored(true), Event: event,
		TaskID: taskID, EntityID: entityID, ParseOK: parseOK, Latency: time.Millisecond, CreatedAt: createdAt,
		TurnBlocks: blocks,
	})
	return turnID
}

func (f agentSnapshotBoundaryFixture) replaceTurnSummary(t *testing.T, exec agentSnapshotMutationExec, turnID, output string) {
	t.Helper()
	table := "agent_turns"
	if f.backend.sqlite {
		table = "agent_turns_snapshot_source"
	}
	raw := fmt.Sprintf(`[{"kind":"turn_summary","data":{"assistant_visible_output":%q,"outcome":"completed"}}]`, output)
	query := fmt.Sprintf(`UPDATE %s SET turn_blocks=? WHERE turn_id=?`, table)
	args := []any{raw, turnID}
	if !f.backend.sqlite {
		query = `UPDATE agent_turns SET turn_blocks=$1::jsonb WHERE turn_id=$2::uuid`
	}
	result, err := exec.ExecContext(f.ctx, query, args...)
	if err != nil {
		t.Fatalf("replace %s snapshot turn summary: %v", f.backend.name, err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		t.Fatalf("replace %s snapshot turn summary changed %d rows, want 1", f.backend.name, changed)
	}
}

func (f agentSnapshotBoundaryFixture) settlePendingSnapshotDelivery(t *testing.T, exec agentSnapshotMutationExec, deliveryID string) {
	t.Helper()
	f.settlePendingSnapshotDeliveryInTable(t, exec, "event_deliveries", deliveryID)
}

func (f agentSnapshotBoundaryFixture) settlePendingSnapshotDeliveryInTable(t *testing.T, exec agentSnapshotMutationExec, table, deliveryID string) {
	t.Helper()
	now := time.Now().UTC()
	query := fmt.Sprintf(`UPDATE %s
		SET status='delivered', next_eligible_at=NULL, settled_at=?, updated_at=?
		WHERE delivery_id=? AND status='pending'`, table)
	args := []any{now, now, deliveryID}
	if !f.backend.sqlite {
		query = fmt.Sprintf(`UPDATE %s
			SET status='delivered', next_eligible_at=NULL, settled_at=$1, updated_at=$1
			WHERE delivery_id=$2::uuid AND status='pending'`, table)
		args = []any{now, deliveryID}
	}
	result, err := exec.ExecContext(f.ctx, query, args...)
	if err != nil {
		t.Fatalf("settle %s pending snapshot delivery: %v", f.backend.name, err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		t.Fatalf("settle %s pending snapshot delivery changed %d rows, want 1", f.backend.name, changed)
	}
}

func (f agentSnapshotBoundaryFixture) readDeliveryWrapperAcrossBoundary(t *testing.T, read func() error, mutate func(agentSnapshotMutationExec, string)) {
	t.Helper()
	if f.backend.sqlite {
		id := sqliteSnapshotBarrierID.Add(1)
		barrier := &sqliteAgentSnapshotBarrier{entered: make(chan struct{}), release: make(chan struct{})}
		sqliteSnapshotBarrierRegistry.Store(id, barrier)
		t.Cleanup(func() { sqliteSnapshotBarrierRegistry.Delete(id) })
		const source = "event_deliveries_snapshot_source"
		if _, err := f.db.ExecContext(f.ctx, `ALTER TABLE event_deliveries RENAME TO event_deliveries_snapshot_source`); err != nil {
			t.Fatalf("rename sqlite delivery snapshot table: %v", err)
		}
		if _, err := f.db.ExecContext(f.ctx, fmt.Sprintf(`CREATE VIEW event_deliveries AS SELECT * FROM %s WHERE agent_snapshot_barrier(%d)=1`, source, id)); err != nil {
			t.Fatalf("create sqlite delivery snapshot barrier view: %v", err)
		}
		resultCh := make(chan error, 1)
		go func() { resultCh <- read() }()
		select {
		case <-barrier.entered:
		case <-time.After(10 * time.Second):
			close(barrier.release)
			t.Fatal("sqlite wrapper did not reach the delivery snapshot boundary")
		}
		mutate(f.db, source)
		close(barrier.release)
		receiveAgentSnapshotWrapper(t, resultCh)
		return
	}

	blocker, err := f.db.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.ExecContext(f.ctx, `LOCK TABLE event_deliveries IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock postgres delivery wrapper boundary: %v", err)
	}
	resultCh := make(chan error, 1)
	go func() { resultCh <- read() }()
	waitForPostgresAgentSnapshotReadBlock(t, f.ctx, f.db, "event_deliveries")
	mutate(blocker, "event_deliveries")
	if err := blocker.Commit(); err != nil {
		t.Fatalf("commit postgres delivery wrapper boundary: %v", err)
	}
	receiveAgentSnapshotWrapper(t, resultCh)
}

func receiveAgentSnapshotWrapper(t *testing.T, resultCh <-chan error) {
	t.Helper()
	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("read atomic agent wrapper snapshot: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("agent wrapper snapshot read did not complete")
	}
}

func (f agentSnapshotBoundaryFixture) insertSQLiteRenamedSession(t *testing.T, exec agentSnapshotMutationExec, sessionID string) {
	t.Helper()
	fields, err := f.identity.StorageFields()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := exec.ExecContext(f.ctx, `INSERT INTO agent_sessions_snapshot_source (
		session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
		conversation, turn_count, runtime_state, status, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored', '[]', 0, '{}', 'active', ?, ?)`,
		sessionID, f.runID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, now, now); err != nil {
		t.Fatalf("insert sqlite successor session: %v", err)
	}
}

func (f agentSnapshotBoundaryFixture) readAcrossSessionBoundary(t *testing.T, mutate func(agentSnapshotMutationExec)) operatorread.OperatorAgentListResult {
	t.Helper()
	if f.backend.sqlite {
		return f.readAcrossSQLiteSessionBoundary(t, mutate)
	}
	return f.readAcrossPostgresSessionBoundary(t, mutate)
}

func (f agentSnapshotBoundaryFixture) readAcrossPostgresSessionBoundary(t *testing.T, mutate func(agentSnapshotMutationExec)) operatorread.OperatorAgentListResult {
	t.Helper()
	blocker, err := f.db.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.ExecContext(f.ctx, `LOCK TABLE agent_sessions IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock postgres session boundary: %v", err)
	}
	resultCh := make(chan agentSnapshotReadResult, 1)
	go func() {
		result, readErr := f.store.ListOperatorAgents(f.ctx, operatorread.OperatorAgentListOptions{})
		resultCh <- agentSnapshotReadResult{result: result, err: readErr}
	}()
	waitForPostgresSessionReadBlock(t, f.ctx, f.db)
	mutate(blocker)
	if err := blocker.Commit(); err != nil {
		t.Fatalf("commit postgres boundary mutation: %v", err)
	}
	return receiveAgentSnapshotRead(t, resultCh)
}

func (f agentSnapshotBoundaryFixture) readAcrossSQLiteSessionBoundary(t *testing.T, mutate func(agentSnapshotMutationExec)) operatorread.OperatorAgentListResult {
	t.Helper()
	id := sqliteSnapshotBarrierID.Add(1)
	barrier := &sqliteAgentSnapshotBarrier{entered: make(chan struct{}), release: make(chan struct{})}
	sqliteSnapshotBarrierRegistry.Store(id, barrier)
	t.Cleanup(func() { sqliteSnapshotBarrierRegistry.Delete(id) })
	if _, err := f.db.ExecContext(f.ctx, `ALTER TABLE agent_sessions RENAME TO agent_sessions_snapshot_source`); err != nil {
		t.Fatalf("rename sqlite session table: %v", err)
	}
	if _, err := f.db.ExecContext(f.ctx, fmt.Sprintf(`CREATE VIEW agent_sessions AS SELECT * FROM agent_sessions_snapshot_source WHERE agent_snapshot_barrier(%d)=1`, id)); err != nil {
		t.Fatalf("create sqlite session barrier view: %v", err)
	}
	resultCh := make(chan agentSnapshotReadResult, 1)
	go func() {
		result, readErr := f.store.ListOperatorAgents(f.ctx, operatorread.OperatorAgentListOptions{})
		resultCh <- agentSnapshotReadResult{result: result, err: readErr}
	}()
	select {
	case <-barrier.entered:
	case <-time.After(10 * time.Second):
		close(barrier.release)
		t.Fatal("sqlite agent snapshot did not reach the session boundary")
	}
	mutate(f.db)
	close(barrier.release)
	return receiveAgentSnapshotRead(t, resultCh)
}

func (f agentSnapshotBoundaryFixture) readDiagnosisAcrossSessionBoundary(t *testing.T, mutate func(agentSnapshotMutationExec)) operatorread.OperatorAgentDiagnosis {
	t.Helper()
	if f.backend.sqlite {
		return f.readDiagnosisAcrossSQLiteSessionBoundary(t, mutate)
	}
	blocker, err := f.db.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.ExecContext(f.ctx, `LOCK TABLE agent_sessions IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock postgres diagnosis session boundary: %v", err)
	}
	resultCh := make(chan agentSnapshotDiagnosisReadResult, 1)
	go func() {
		result, readErr := f.store.LoadOperatorAgentDiagnosis(f.ctx, f.identity, operatorread.OperatorAgentDiagnosisOptions{QueueLimit: 10})
		resultCh <- agentSnapshotDiagnosisReadResult{result: result, err: readErr}
	}()
	waitForPostgresSessionReadBlock(t, f.ctx, f.db)
	mutate(blocker)
	if err := blocker.Commit(); err != nil {
		t.Fatalf("commit postgres diagnosis boundary mutation: %v", err)
	}
	return receiveAgentSnapshotDiagnosis(t, resultCh)
}

func (f agentSnapshotBoundaryFixture) readDiagnosisAcrossSQLiteSessionBoundary(t *testing.T, mutate func(agentSnapshotMutationExec)) operatorread.OperatorAgentDiagnosis {
	t.Helper()
	id := sqliteSnapshotBarrierID.Add(1)
	barrier := &sqliteAgentSnapshotBarrier{entered: make(chan struct{}), release: make(chan struct{})}
	sqliteSnapshotBarrierRegistry.Store(id, barrier)
	t.Cleanup(func() { sqliteSnapshotBarrierRegistry.Delete(id) })
	if _, err := f.db.ExecContext(f.ctx, `ALTER TABLE agent_sessions RENAME TO agent_sessions_snapshot_source`); err != nil {
		t.Fatalf("rename sqlite diagnosis session table: %v", err)
	}
	if _, err := f.db.ExecContext(f.ctx, fmt.Sprintf(`CREATE VIEW agent_sessions AS SELECT * FROM agent_sessions_snapshot_source WHERE agent_snapshot_barrier(%d)=1`, id)); err != nil {
		t.Fatalf("create sqlite diagnosis session barrier view: %v", err)
	}
	resultCh := make(chan agentSnapshotDiagnosisReadResult, 1)
	go func() {
		result, readErr := f.store.LoadOperatorAgentDiagnosis(f.ctx, f.identity, operatorread.OperatorAgentDiagnosisOptions{QueueLimit: 10})
		resultCh <- agentSnapshotDiagnosisReadResult{result: result, err: readErr}
	}()
	select {
	case <-barrier.entered:
	case <-time.After(10 * time.Second):
		close(barrier.release)
		t.Fatal("sqlite agent diagnosis did not reach the session boundary")
	}
	mutate(f.db)
	close(barrier.release)
	return receiveAgentSnapshotDiagnosis(t, resultCh)
}

func (f agentSnapshotBoundaryFixture) readAcrossExternalTableBoundary(t *testing.T, table string, mutate func(agentSnapshotMutationExec)) operatorread.OperatorAgentListResult {
	t.Helper()
	if f.backend.sqlite {
		return f.readAcrossSQLiteExternalTableBoundary(t, table, mutate)
	}
	return f.readAcrossPostgresExternalTableBoundary(t, table, mutate)
}

func (f agentSnapshotBoundaryFixture) readAcrossPostgresExternalTableBoundary(t *testing.T, table string, mutate func(agentSnapshotMutationExec)) operatorread.OperatorAgentListResult {
	t.Helper()
	blocker, err := f.db.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.ExecContext(f.ctx, fmt.Sprintf(`LOCK TABLE %s IN ACCESS EXCLUSIVE MODE`, table)); err != nil {
		t.Fatalf("lock postgres %s snapshot boundary: %v", table, err)
	}
	resultCh := make(chan agentSnapshotReadResult, 1)
	go func() {
		result, readErr := f.store.ListOperatorAgents(f.ctx, operatorread.OperatorAgentListOptions{})
		resultCh <- agentSnapshotReadResult{result: result, err: readErr}
	}()
	waitForPostgresAgentSnapshotReadBlock(t, f.ctx, f.db, table)
	mutate(blocker)
	if err := blocker.Commit(); err != nil {
		t.Fatalf("commit postgres %s snapshot boundary: %v", table, err)
	}
	return receiveAgentSnapshotRead(t, resultCh)
}

func (f agentSnapshotBoundaryFixture) readAcrossSQLiteExternalTableBoundary(t *testing.T, table string, mutate func(agentSnapshotMutationExec)) operatorread.OperatorAgentListResult {
	t.Helper()
	id := sqliteSnapshotBarrierID.Add(1)
	barrier := &sqliteAgentSnapshotBarrier{entered: make(chan struct{}), release: make(chan struct{})}
	sqliteSnapshotBarrierRegistry.Store(id, barrier)
	t.Cleanup(func() { sqliteSnapshotBarrierRegistry.Delete(id) })
	source := table + "_snapshot_source"
	if _, err := f.db.ExecContext(f.ctx, fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, table, source)); err != nil {
		t.Fatalf("rename sqlite %s snapshot table: %v", table, err)
	}
	if _, err := f.db.ExecContext(f.ctx, fmt.Sprintf(`CREATE VIEW %s AS SELECT * FROM %s WHERE agent_snapshot_barrier(%d)=1`, table, source, id)); err != nil {
		t.Fatalf("create sqlite %s snapshot barrier view: %v", table, err)
	}
	resultCh := make(chan agentSnapshotReadResult, 1)
	go func() {
		result, readErr := f.store.ListOperatorAgents(f.ctx, operatorread.OperatorAgentListOptions{})
		resultCh <- agentSnapshotReadResult{result: result, err: readErr}
	}()
	select {
	case <-barrier.entered:
	case <-time.After(10 * time.Second):
		close(barrier.release)
		t.Fatalf("sqlite agent snapshot did not reach the %s boundary", table)
	}
	mutate(f.db)
	close(barrier.release)
	return receiveAgentSnapshotRead(t, resultCh)
}

type agentSnapshotReadResult struct {
	result operatorread.OperatorAgentListResult
	err    error
}

type agentSnapshotDiagnosisReadResult struct {
	result operatorread.OperatorAgentDiagnosis
	err    error
}

func receiveAgentSnapshotRead(t *testing.T, resultCh <-chan agentSnapshotReadResult) operatorread.OperatorAgentListResult {
	t.Helper()
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("read atomic agent snapshot: %v", result.err)
		}
		return result.result
	case <-time.After(10 * time.Second):
		t.Fatal("agent snapshot read did not complete")
		return operatorread.OperatorAgentListResult{}
	}
}

func receiveAgentSnapshotDiagnosis(t *testing.T, resultCh <-chan agentSnapshotDiagnosisReadResult) operatorread.OperatorAgentDiagnosis {
	t.Helper()
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("read atomic agent diagnosis snapshot: %v", result.err)
		}
		return result.result
	case <-time.After(10 * time.Second):
		t.Fatal("agent diagnosis snapshot read did not complete")
		return operatorread.OperatorAgentDiagnosis{}
	}
}

func waitForPostgresSessionReadBlock(t *testing.T, ctx context.Context, db *sql.DB) {
	waitForPostgresAgentSnapshotReadBlock(t, ctx, db, "agent_sessions")
}

func waitForPostgresAgentSnapshotReadBlock(t *testing.T, ctx context.Context, db *sql.DB, table string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var blocked bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname=current_database()
				  AND pid<>pg_backend_pid()
				  AND wait_event_type='Lock'
				  AND query LIKE $1
			)`, "%"+table+"%").Scan(&blocked)
		if err != nil {
			t.Fatalf("inspect postgres session boundary: %v", err)
		}
		if blocked {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("postgres agent snapshot did not block at the %s boundary", table)
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
