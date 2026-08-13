package runtimepersistence

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestPersistedAgentProjectionRejectsLiveDescriptorWithMockArtifact(t *testing.T) {
	identity := testAgentIdentity(t, "live-agent-with-inactive-artifact", "")
	_, err := projectPersistedAgentConfig(withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
		ID: "live-agent-with-inactive-artifact", Identity: identity, Role: "reviewer", Model: "regular", LLMBackend: "anthropic",
		ResolvedLLMBackend: "anthropic", ExecutionMode: runtimeeffects.ExecutionModeLive, Memory: agentmemory.PlatformDefault(),
		Mock: mockperformance.Performance{Kind: mockperformance.KindPython, Module: "mocks/reviewer.py", Source: []byte("def handle(input): return {'text': 'mock'}\n"), Digest: "sha256:test"},
	}), "")
	if err == nil || !strings.Contains(err.Error(), "live runtime descriptor cannot carry a mock performance artifact") {
		t.Fatalf("projectPersistedAgentConfig error = %v, want live/mock artifact conflict", err)
	}
}

func TestPersistedAgentProjectionRejectsEnabledPlatformDefaultMemory(t *testing.T) {
	illegal := agentmemory.Plan{Enabled: true, Source: agentmemory.SourcePlatformDefault}
	identity := testAgentIdentity(t, "agent-invalid-memory", "review/one")
	_, err := projectPersistedAgentConfig(runtimeactors.AgentConfig{ExecutionMode: "live", ID: "agent-invalid-memory",
		Identity: identity,
		Role:     "reviewer",
		Model:    "regular",
		FlowPath: "review/one",
		Memory:   illegal,
	}, "")
	if err == nil || !strings.Contains(err.Error(), `requires source "authored"`) {
		t.Fatalf("projectPersistedAgentConfig error = %v, want authored-source requirement", err)
	}

	_, err = hydratePersistedAgentConfig(persistedAgentProjection{
		Identity:          mustStorageFields(t, identity),
		AgentID:           "agent-invalid-memory",
		FlowInstance:      "review/one",
		Role:              "reviewer",
		Model:             "regular",
		LLMBackend:        "anthropic",
		MemoryEnabled:     true,
		MemorySource:      string(agentmemory.SourcePlatformDefault),
		ConfigJSON:        []byte(`{}`),
		RuntimeDescriptor: []byte(`{"type":"generic"}`),
	})
	if err == nil || !strings.Contains(err.Error(), `requires source "authored"`) {
		t.Fatalf("hydratePersistedAgentConfig error = %v, want authored-source requirement", err)
	}
}

func TestFreshAgentsSchemaRejectsEnabledPlatformDefaultMemory(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, postgresDB, _ := testutil.StartPostgres(t)
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)

	for _, backend := range []struct {
		name string
		exec func() error
	}{
		{
			name: "postgres",
			exec: func() error {
				_, err := postgresDB.ExecContext(ctx, `
					INSERT INTO agents (
						agent_id, agent_name_owner, agent_name_source, agent_route_presence,
						flow_scope_key, flow_instance_id, flow_instance,
						role, model, memory_enabled, memory_source,
						topology_authority_kind, topology_admission, execution_lifetime
					)
					VALUES ('invalid-memory-postgres', 'schema-negative-test', 'runtime_created', 'present',
						'review', 'one', 'review/one', 'reviewer', 'regular', TRUE, 'platform_default',
						'static_declaration_plan', $1::jsonb, 'durable_managed')
				`, testAgentTopologyJSON(t))
				return err
			},
		},
		{
			name: "sqlite",
			exec: func() error {
				_, err := sqliteStore.backend.ExecContext(ctx, `
					INSERT INTO agents (
						agent_id, agent_name_owner, agent_name_source, agent_route_presence,
						flow_scope_key, flow_instance_id, flow_instance,
						role, model, memory_enabled, memory_source,
						topology_authority_kind, topology_admission, execution_lifetime
					)
					VALUES ('invalid-memory-sqlite', 'schema-negative-test', 'runtime_created', 'present',
						'review', 'one', 'review/one', 'reviewer', 'regular', 1, 'platform_default',
						'static_declaration_plan', ?, 'durable_managed')
				`, testAgentTopologyJSON(t))
				return err
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			if err := backend.exec(); err == nil {
				t.Fatal("fresh agents schema accepted memory enabled with platform-default provenance")
			}
		})
	}
}

func mustStorageFields(t testing.TB, identity agentidentity.Identity) agentidentity.StorageFields {
	t.Helper()
	fields, err := identity.StorageFields()
	if err != nil {
		t.Fatalf("agent identity storage fields: %v", err)
	}
	return fields
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

			if _, err := db.ExecContext(ctx, `
				INSERT INTO agents (
					agent_id, agent_name_owner, agent_name_source, agent_route_presence,
					flow_scope_key, flow_instance_id, flow_instance,
					role, model, llm_backend, memory_enabled, memory_source,
					parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
					runtime_descriptor, status,
					topology_authority_kind, topology_admission, execution_lifetime
				) VALUES (
					$1, $2, $3, $4, $5, $6, $7,
					'reviewer', 'regular', 'anthropic', FALSE, 'platform_default',
					NULL, NULL, '{}'::jsonb, '["review.ready"]'::jsonb, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
					$8::jsonb, 'active',
					'static_declaration_plan', $9::jsonb, 'durable_managed'
				)
			`, identityFields.AgentID, identityFields.NameOwner, identityFields.NameSource, identityFields.RoutePresence,
				identityFields.FlowScopeKey, identityFields.FlowInstanceID, identityFields.FlowInstancePath,
				tt.runtimeDescriptor, testAgentTopologyJSON(t)); err != nil {
				t.Fatalf("seed agent row: %v", err)
			}

			_, err = pg.LoadAgents(ctx)
			if err == nil || !strings.Contains(err.Error(), tt.wantErrorSubstring) {
				t.Fatalf("LoadAgents error = %v, want substring %q", err, tt.wantErrorSubstring)
			}
		})
	}
}
