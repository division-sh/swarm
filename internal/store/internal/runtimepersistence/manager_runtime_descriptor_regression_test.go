package runtimepersistence

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestPersistedAgentProjectionRejectsLiveDescriptorWithMockArtifact(t *testing.T) {
	identity := testAgentIdentity(t, "live-agent-with-inactive-artifact", "")
	_, err := projectPersistedAgentConfig(withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
		ID: "live-agent-with-inactive-artifact", Identity: identity, Role: "reviewer", Model: "regular", LLMBackend: "anthropic",
		ResolvedLLMBackend: "anthropic", ExecutionMode: runtimeeffects.ExecutionModeLive, Memory: agentmemory.Plan{},
		Mock: mockperformance.Performance{Kind: mockperformance.KindPython, Module: "mocks/reviewer.py", Source: []byte("def handle(input): return {'text': 'mock'}\n"), Digest: "sha256:test"},
	}), "")
	if err == nil || !strings.Contains(err.Error(), "live runtime descriptor cannot carry a mock performance artifact") {
		t.Fatalf("projectPersistedAgentConfig error = %v, want live/mock artifact conflict", err)
	}
}

func TestW5PersistedMemoryPlanPreservesEnablementAndRootRefusal(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		identity := testAgentIdentity(t, "agent-memory", "review/one")
		cfg := withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
			ExecutionMode: "live", ID: "agent-memory", Identity: identity,
			Role: "reviewer", Model: "regular", LLMBackend: "anthropic", ResolvedLLMBackend: "anthropic",
			FlowPath: "review/one", Memory: agentmemory.Plan{Enabled: enabled},
		})
		row, err := projectPersistedAgentConfig(cfg, "")
		if err != nil {
			t.Fatal(err)
		}
		restored, err := hydratePersistedAgentConfig(row)
		if err != nil || restored.Memory != cfg.Memory || restored.Identity != cfg.Identity {
			t.Fatalf("enablement/identity round trip: %+v, %v", restored, err)
		}
	}
	root := withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
		ExecutionMode: "live", ID: "root-memory", Identity: testAgentIdentity(t, "root-memory", ""),
		Role: "reviewer", Model: "regular", Memory: agentmemory.Plan{Enabled: true},
	})
	if _, err := projectPersistedAgentConfig(root, ""); err == nil || !strings.Contains(err.Error(), "flow-instance owner") {
		t.Fatalf("root memory must fail closed: %v", err)
	}
}

func TestW5FreshMemorySchemaHasNoSourceBothStores(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, postgresDB, _ := testutil.StartPostgres(t)
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			for _, table := range []string{"agents", "agent_sessions", "agent_turns", "agent_conversation_audits"} {
				var count int
				var err error
				if backend == "postgres" {
					err = postgresDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 AND column_name='memory_source'`, table).Scan(&count)
				} else {
					err = sqliteStore.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info(?) WHERE name='memory_source'`, table).Scan(&count)
				}
				if err != nil || count != 0 {
					t.Fatalf("%s %s retains memory source: %d, %v", backend, table, count, err)
				}
				if backend == "postgres" {
					err = postgresDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 AND column_name='memory_enabled'`, table).Scan(&count)
				} else {
					err = sqliteStore.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info(?) WHERE name='memory_enabled'`, table).Scan(&count)
				}
				if err != nil || count != 1 {
					t.Fatalf("%s %s lost enablement: %d, %v", backend, table, count, err)
				}
			}
		})
	}
}

func TestManagerStore_LoadAgents_FailsClosedOnMalformedRuntimeDescriptor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		runtimeDescriptor  string
		wantErrorSubstring string
	}{
		{
			name:               "non object runtime descriptor",
			runtimeDescriptor:  `[]`,
			wantErrorSubstring: `invalid runtime_descriptor: decode runtime_descriptor: json: cannot unmarshal array into Go value of type map[string]json.RawMessage`,
		},
		{
			name:               "unsupported runtime descriptor keys",
			runtimeDescriptor:  `{"type":"review-worker","legacy_scope":"global"}`,
			wantErrorSubstring: `invalid runtime_descriptor: runtime_descriptor contains unsupported keys: legacy_scope`,
		},
		{
			name:               "wrong runtime descriptor field types",
			runtimeDescriptor:  `{"type":1}`,
			wantErrorSubstring: `invalid runtime_descriptor: decode runtime_descriptor: json: cannot unmarshal number into Go struct field PersistedAgentRuntimeDescriptor.type of type string`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, db, _ := testutil.StartPostgres(t)
			pg := admitTestPostgresStore(t, db)
			ctx := testAuthorActivityContext()
			identityFields, err := agentIdentityFields(testAgentIdentity(t, "agent-malformed-runtime-descriptor", ""))
			if err != nil {
				t.Fatal(err)
			}
			requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{
				Origin: semanticScenarioSetupRunOriginForTest(), RunID: identityFields.RunID,
			})

			if _, err := db.ExecContext(ctx, `
				INSERT INTO agents (
					agent_id, agent_name_owner, agent_name_source, agent_route_presence,
					flow_scope_key, flow_instance_id, flow_instance,
					role, model, llm_backend, memory_enabled,
					parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
					runtime_descriptor, status,
					lifecycle_process_authority_id, lifecycle_process_owner_id,
					lifecycle_process_boot_id, lifecycle_generation_grant_id,
					lifecycle_bundle_hash,
					lifecycle_runtime_instance_id, lifecycle_runtime_generation,
					topology_authority_kind, topology_admission, execution_lifetime, run_id
				) VALUES (
					$1, $2, $3, $4, $5, $6, $7,
					'reviewer', 'regular', 'anthropic', FALSE,
					NULL, NULL, '{"config":{},"receiver_config":null}'::jsonb, '["review.ready"]'::jsonb, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
					$8::jsonb, 'active',
					'00000000-0000-4000-8000-000000000001'::uuid, 'runtime-descriptor-test',
					'00000000-0000-4000-8000-000000000002'::uuid, '00000000-0000-4000-8000-000000000003'::uuid,
					'bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
					'00000000-0000-4000-8000-000000000004'::uuid, 1,
					'static_declaration_plan', $9::jsonb, 'durable_managed', $10::uuid
				)
			`, identityFields.AgentID, identityFields.NameOwner, identityFields.NameSource, identityFields.RoutePresence,
				identityFields.FlowScopeKey, identityFields.FlowInstanceID, identityFields.FlowInstancePath,
				tt.runtimeDescriptor, testAgentTopologyJSON(t), identityFields.RunID); err != nil {
				t.Fatalf("seed agent row: %v", err)
			}

			_, err = pg.LoadAgents(ctx)
			if err == nil || !strings.Contains(err.Error(), tt.wantErrorSubstring) {
				t.Fatalf("LoadAgents error = %v, want substring %q", err, tt.wantErrorSubstring)
			}
		})
	}
}
