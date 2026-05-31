package store

import (
	"context"
	"strings"
	"testing"

	"swarm/internal/testutil"
)

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
			runtimeDescriptor:  `{"type":"review-worker","mode":"review","legacy_scope":"global"}`,
			wantErrorSubstring: `invalid runtime_descriptor: runtime_descriptor contains unsupported keys: legacy_scope`,
		},
		{
			name:               "wrong runtime descriptor field types",
			runtimeDescriptor:  `{"type":1,"mode":"review"}`,
			wantErrorSubstring: `invalid runtime_descriptor: decode runtime_descriptor: json: cannot unmarshal number into Go struct field persistedAgentRuntimeDescriptor.type of type string`,
		},
		{
			name:               "unsupported session scope authority",
			runtimeDescriptor:  `{"type":"review-worker","session_scope_authority":"authored"}`,
			wantErrorSubstring: `invalid runtime_descriptor: unsupported session_scope_authority "authored"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, db, _ := testutil.StartPostgres(t)
			pg := &PostgresStore{DB: db}
			ctx := context.Background()

			if err := pg.ensureSchemaCompatibilityColumns(ctx); err != nil {
				t.Fatalf("ensureSchemaCompatibilityColumns: %v", err)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO agents (
					agent_id, flow_instance, role, model, llm_backend, conversation_mode,
					parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
					runtime_descriptor, status
				) VALUES (
					$1, '', 'reviewer', 'sonnet', 'anthropic', 'task',
					NULL, NULL, '{}'::jsonb, '["review.ready"]'::jsonb, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
					$2::jsonb, 'active'
				)
			`, "agent-malformed-runtime-descriptor", tt.runtimeDescriptor); err != nil {
				t.Fatalf("seed agent row: %v", err)
			}

			_, err := pg.LoadAgents(ctx)
			if err == nil || !strings.Contains(err.Error(), tt.wantErrorSubstring) {
				t.Fatalf("LoadAgents error = %v, want substring %q", err, tt.wantErrorSubstring)
			}
		})
	}
}
