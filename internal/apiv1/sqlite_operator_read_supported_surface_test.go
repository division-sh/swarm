package apiv1

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
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

	seedSQLiteAgentUsageAgent(t, ctx, sqliteStore, agentID)
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, base.Add(-time.Hour)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES (
			?, ?, ?, 'flow/operator-read', 1, 'authored',
			'[{"role":"assistant","content":"ready"}]', 1, '{}', 'active', ?, ?
		)
	`, sessionID, runID, agentID, base.Add(-5*time.Minute), base); err != nil {
		t.Fatalf("seed sqlite session: %v", err)
	}
	storetest.InsertExistingRunRootEventRecord(
		t, ctx, sqliteStore.DB, "sqlite", eventID, runID, events.EventType("operator.read"),
		eventtest.Producer(events.EventProducerExternal, "operator-read-fixture"), []byte(`{}`),
		events.EventEnvelope{Scope: events.EventScopeGlobal}, base.Add(-4*time.Minute),
	)
	capabilitySurfaceID := seedSQLiteOperatorReadCapabilitySurface(t, ctx, sqliteStore, runID, turnID, sessionID, agentID, "session")
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, capability_surface_id, tool_calls,
			emitted_events,
			request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count, failure, execution_mode, created_at
		) VALUES (
			?, ?, ?, ?, 'flow/operator-read', 1, 'authored', NULL,
			?, 'operator.read', 'task-operator-read', ?, '[]',
			'[]',
			'{}', '{}', '[]', 1, 10, 0, NULL, 'live', ?
		)
	`, turnID, runID, agentID, sessionID, eventID, capabilitySurfaceID, base); err != nil {
		t.Fatalf("seed sqlite turn: %v", err)
	}

	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
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
		})
	}
}

func seedSQLiteOperatorReadCapabilitySurface(t testing.TB, ctx context.Context, sqliteStore *storepkg.SQLiteRuntimeStore, runID, turnID, sessionID, agentID, runtimeMode string) string {
	t.Helper()
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorID: agentID, RuntimeMode: runtimeMode, Provider: "test", Transport: "api", ProviderContract: "sqlite-operator-read-test",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: turnID,
			ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: "sqlite-operator-read-runtime",
			RunID: runID, SessionID: sessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build sqlite operator-read capability surface: %v", err)
	}
	if err := sqliteStore.SaveManagedCapabilitySurface(ctx, surface); err != nil {
		t.Fatalf("persist sqlite operator-read capability surface: %v", err)
	}
	return surface.ID
}

func TestSQLiteBundleCatalogOwnerBacksSupportedAPISurface(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newSQLiteAgentUsageStoreFixture(t, ctx)
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json, data_blob, metadata, ingested_at)
		VALUES (?, ?, '{}', NULL, '{"source":"sqlite-test"}', ?)
	`, bundleHash, `agents:
  bundle-agent:
    role: worker
    model: regular
    type: managed
`, time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed sqlite bundle catalog: %v", err)
	}

	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
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

var _ AgentConversationReadStore = (*storepkg.SQLiteRuntimeStore)(nil)
var _ BundleCatalogReadStore = (*storepkg.SQLiteRuntimeStore)(nil)
