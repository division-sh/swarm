package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	storepkg "github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestSQLiteAgentConversationOwnerBacksSupportedAPISurface(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newSQLiteAgentUsageStoreFixture(t, ctx)
	agentID := "agent-operator-read"
	sessionID := uuid.NewString()
	turnID := uuid.NewString()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	identity := sqliteAgentUsageIdentity(t, agentID)
	identityFields, err := identity.StorageFields()
	if err != nil {
		t.Fatalf("operator read identity fields: %v", err)
	}

	seedSQLiteAgentUsageAgent(t, ctx, sqliteStore, agentID)
	storetest.RequireSQLiteRun(t, ctx, storetest.DatabaseForTest(sqliteStore), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: base.Add(-time.Hour)})
	if _, err := storetest.DatabaseForTest(sqliteStore).ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored',
			'[{"role":"assistant","content":"ready"}]', 0, '{}', 'active', ?, ?
		)
	`, sessionID, runID, identityFields.AgentID, identityFields.NameOwner, identityFields.NameSource,
		identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID,
		identityFields.FlowInstancePath, base.Add(-5*time.Minute), base); err != nil {
		t.Fatalf("seed sqlite session: %v", err)
	}
	storetest.InsertExistingRunRootEventRecord(
		t, ctx, storetest.DatabaseForTest(sqliteStore), "sqlite", eventID, runID, events.EventType("operator.read"),
		eventtest.Producer(events.EventProducerExternal, "operator-read-fixture"), []byte(`{}`),
		events.EventEnvelope{Scope: events.EventScopeGlobal}, base.Add(-4*time.Minute),
	)
	storetest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{
		Store: sqliteStore, Selected: sqliteStore, Identity: identity, RunID: runID, SessionID: sessionID, TurnID: turnID,
		Memory: agentmemory.Authored(true), Event: storetest.LoadCanonicalEventRecord(t, ctx, sqliteStore, eventID),
		TaskID: "task-operator-read", ParseOK: true, Latency: 10 * time.Millisecond, CreatedAt: base,
	})

	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations: sqliteStore,
		}),
	})

	for _, tc := range []struct {
		method string
		params string
	}{
		{method: "agent.list", params: `{}`},
		{method: "agent.get", params: fmt.Sprintf(`{"agent_id":%q}`, agentID)},
		{method: "agent.diagnose", params: fmt.Sprintf(`{"agent_id":%q,"queue_limit":10}`, agentID)},
		{method: "agent.delivery_diagnostics", params: fmt.Sprintf(`{"agent_id":%q,"failure_limit":10,"dead_letter_limit":10}`, agentID)},
		{method: "conversation.list", params: `{}`},
		{method: "conversation.list_turns", params: fmt.Sprintf(`{"session_id":%q}`, sessionID)},
		{method: "conversation.get_turn", params: fmt.Sprintf(`{"session_id":%q,"turn_id":%q}`, sessionID, turnID)},
	} {
		t.Run(tc.method, func(t *testing.T) {
			resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":%q,"params":%s}`, tc.method, tc.method, tc.params))
			if resp.Error != nil {
				t.Fatalf("%s sqlite error = %#v", tc.method, resp.Error)
			}
			assertConversationFrameSupportedSurface(t, tc.method, resp.Result, turnID)
		})
	}
}

func TestSQLiteBundleCatalogOwnerBacksSupportedAPISurface(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newSQLiteAgentUsageStoreFixture(t, ctx)
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	parsedJSON, err := json.Marshal(map[string]any{"projection_version": "swarm.bundle.catalog.v2", "agents": []map[string]any{
		apiTestCatalogAgentDefinition(t, "bundle-agent", "Bundle agent intent."),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storetest.DatabaseForTest(sqliteStore).ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json, data_blob, metadata, ingested_at)
		VALUES (?, ?, ?, NULL, '{"source":"sqlite-test"}', ?)
	`, bundleHash, `agents:
  bundle-agent:
    role: worker
    model: regular
    type: managed
	`, string(parsedJSON), time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed sqlite bundle catalog: %v", err)
	}
	if _, err := sqliteStore.ListBundleCatalog(ctx, bundlecatalog.ListOptions{}); err != nil {
		t.Fatalf("list sqlite bundle catalog through selected-store owner: %v", err)
	}

	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			BundleCatalog: sqliteStore,
		}),
	})

	for _, tc := range []struct {
		method string
		params string
	}{
		{method: "bundle.list", params: `{}`},
		{method: "bundle.get", params: fmt.Sprintf(`{"bundle_hash":%q}`, bundleHash)},
		{method: "bundle.agents", params: fmt.Sprintf(`{"bundle_hash":%q}`, bundleHash)},
	} {
		t.Run(tc.method, func(t *testing.T) {
			resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":%q,"params":%s}`, tc.method, tc.method, tc.params))
			if resp.Error != nil {
				t.Fatalf("%s sqlite error = %#v", tc.method, resp.Error)
			}
		})
	}
}

var _ AgentReadStore = (*storepkg.SQLiteRuntimeStore)(nil)
var _ ConversationReadStore = (*storepkg.SQLiteRuntimeStore)(nil)
var _ BundleCatalogReadStore = (*storepkg.SQLiteRuntimeStore)(nil)
