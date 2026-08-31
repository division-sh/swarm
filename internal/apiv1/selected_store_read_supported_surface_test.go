package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSelectedStoreRunReadHandlersExecuteAcrossBackends(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T, context.Context) (RunReadStore, string)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T, ctx context.Context) (RunReadStore, string) {
				selected := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				runID := uuid.NewString()
				storetest.RequireSQLiteRun(t, ctx, storetest.DatabaseForTest(selected), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: time.Now().UTC().Add(-time.Minute)})
				return selected, runID
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T, ctx context.Context) (RunReadStore, string) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := storetest.AdmitPostgresRuntimeStore(t, db)
				runID := uuid.NewString()
				storetest.RequirePostgresRun(t, ctx, db, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: time.Now().UTC().Add(-time.Minute)})
				return selected, runID
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			selected, runID := backend.open(t, ctx)
			handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorHandlers(testOperatorCapabilities{Runs: selected})})
			for _, tc := range []struct {
				method string
				params string
			}{
				{method: "run.get", params: fmt.Sprintf(`{"run_id":%q}`, runID)},
				{method: "run.list", params: `{}`},
				{method: "run.diagnose", params: fmt.Sprintf(`{"run_id":%q}`, runID)},
			} {
				resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":%q,"params":%s}`, tc.method, tc.method, tc.params))
				if resp.Error != nil {
					t.Fatalf("%s %s error = %#v", backend.name, tc.method, resp.Error)
				}
			}
		})
	}
}

func TestPostgresAgentConversationOwnerBacksSupportedAPISurface(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	agentID := "agent-operator-read"
	sessionID := uuid.NewString()
	turnID := uuid.NewString()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	identity := sqliteAgentUsageIdentityForRun(t, runID, agentID)
	intent := apiTestResolvedIntent(t, agentID, "Provide operator-read proof for the selected PostgreSQL store.")
	fields, err := identity.StorageFields()
	if err != nil {
		t.Fatalf("operator read identity fields: %v", err)
	}
	storetest.RequirePostgresRun(t, ctx, db, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: base.Add(-time.Hour)})
	if err := storetest.UpsertStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			Identity: identity, ID: agentID, Role: "researcher", Type: "managed", Model: "cheap",
			ExecutionMode: "live", ResolvedLLMBackend: "anthropic", FlowPath: "flow/a", Intent: intent,
		},
		Status: "active", StartedAt: base.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("upsert postgres operator-read agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored',
			'[{"role":"assistant","content":"ready"}]'::jsonb, 0, '{}'::jsonb, 'active', $10, $11
		)
	`, sessionID, runID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, base.Add(-5*time.Minute), base); err != nil {
		t.Fatalf("seed postgres session: %v", err)
	}
	storetest.InsertExistingRunRootEventRecord(t, ctx, db, "postgres", eventID, runID, events.EventType("operator.read"),
		eventtest.Producer(events.EventProducerExternal, "operator-read-fixture"), []byte(`{}`),
		events.EventEnvelope{Scope: events.EventScopeGlobal}, base.Add(-4*time.Minute))
	storetest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{
		Store: selected, Selected: selected, Identity: identity, RunID: runID, SessionID: sessionID, TurnID: turnID,
		Memory: agentmemory.Authored(true), Event: storetest.LoadCanonicalEventRecord(t, ctx, selected, eventID),
		TaskID: "task-operator-read", ParseOK: true, Latency: 10 * time.Millisecond, CreatedAt: base,
	})
	if _, err := selected.ListOperatorAgents(ctx, operatorread.OperatorAgentListOptions{}); err != nil {
		t.Fatalf("direct postgres agent list: %v", err)
	}

	handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorHandlers(testOperatorCapabilities{
		AgentConversations:     selected,
		AgentDeliveryLifecycle: selected,
		AgentUsage:             selected,
	})})
	for _, tc := range []struct {
		method string
		params string
	}{
		{method: "agent.list", params: `{}`},
		{method: "agent.get", params: fmt.Sprintf(`{"run_id":%q,"agent_id":%q}`, runID, agentID)},
		{method: "agent.diagnose", params: fmt.Sprintf(`{"run_id":%q,"agent_id":%q,"queue_limit":10}`, runID, agentID)},
		{method: "agent.delivery_diagnostics", params: fmt.Sprintf(`{"run_id":%q,"agent_id":%q,"failure_limit":10,"dead_letter_limit":10}`, runID, agentID)},
		{method: "agent.delivery_lifecycle", params: fmt.Sprintf(`{"run_id":%q,"agent_id":%q,"limit":10}`, runID, agentID)},
		{method: "agent.usage", params: fmt.Sprintf(`{"run_id":%q,"agent_id":%q}`, runID, agentID)},
		{method: "conversation.list", params: `{}`},
		{method: "conversation.list_turns", params: fmt.Sprintf(`{"session_id":%q}`, sessionID)},
		{method: "conversation.get_turn", params: fmt.Sprintf(`{"session_id":%q,"turn_id":%q}`, sessionID, turnID)},
	} {
		resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":%q,"params":%s}`, tc.method, tc.method, tc.params))
		if resp.Error != nil {
			t.Fatalf("%s postgres error = %#v", tc.method, resp.Error)
		}
		assertConversationFrameSupportedSurface(t, tc.method, resp.Result, turnID)
	}
}

func assertConversationFrameSupportedSurface(t *testing.T, method string, result any, turnID string) {
	t.Helper()
	switch method {
	case "conversation.get_turn":
		frame := asMap(t, asMap(t, result)["frame"])
		wantKeys := map[string]bool{"version": true, "frame_id": true, "content_hash": true, "turn_kind": true}
		if len(frame) != len(wantKeys) {
			t.Fatalf("conversation.get_turn frame keys = %v, want exact initial-turn safe whitelist", frame)
		}
		for key := range frame {
			if !wantKeys[key] {
				t.Fatalf("conversation.get_turn exposed non-whitelisted frame key %q", key)
			}
		}
		if frame["version"] != "agent-execution-frame.v1" || frame["frame_id"] != "agent-frame:v1:"+turnID {
			t.Fatalf("conversation.get_turn frame identity = %#v", frame)
		}
	case "conversation.list", "conversation.list_turns":
		if jsonValueHasKey(result, "frame") {
			t.Fatalf("%s exposed historical frame detail: %#v", method, result)
		}
	}
}

func jsonValueHasKey(value any, want string) bool {
	switch typed := value.(type) {
	case map[string]any:
		if _, ok := typed[want]; ok {
			return true
		}
		for _, child := range typed {
			if jsonValueHasKey(child, want) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if jsonValueHasKey(child, want) {
				return true
			}
		}
	}
	return false
}

func TestPostgresBundleCatalogOwnerBacksSupportedAPISurface(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	bundleHash := "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	parsedJSON, err := json.Marshal(map[string]any{
		"projection_version": "swarm.bundle.catalog.v2",
		"agents":             []map[string]any{apiTestCatalogAgentDefinition(t, "bundle-agent", "Handle the bundle catalog proof.")},
	})
	if err != nil {
		t.Fatalf("encode postgres bundle catalog projection: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json, data_blob, metadata, ingested_at)
		VALUES ($1, $2, $3::jsonb, NULL, '{"source":"postgres-test"}'::jsonb, $4)
	`, bundleHash, `agents:
  bundle-agent:
    intent:
      inline: Handle the bundle catalog proof.
    role: worker
    model: regular
    type: managed
`, parsedJSON, time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed postgres bundle catalog: %v", err)
	}
	handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorHandlers(testOperatorCapabilities{BundleCatalog: selected})})
	for _, tc := range []struct {
		method string
		params string
	}{
		{method: "bundle.list", params: `{}`},
		{method: "bundle.get", params: fmt.Sprintf(`{"bundle_hash":%q}`, bundleHash)},
		{method: "bundle.agents", params: fmt.Sprintf(`{"bundle_hash":%q}`, bundleHash)},
	} {
		resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":%q,"params":%s}`, tc.method, tc.method, tc.params))
		if resp.Error != nil {
			t.Fatalf("%s postgres error = %#v", tc.method, resp.Error)
		}
	}
}
