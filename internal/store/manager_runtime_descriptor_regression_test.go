package store

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
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
			runtimeDescriptor:  `{"type":"review-worker","legacy_scope":"global"}`,
			wantErrorSubstring: `invalid runtime_descriptor: runtime_descriptor contains unsupported keys: legacy_scope`,
		},
		{
			name:               "wrong runtime descriptor field types",
			runtimeDescriptor:  `{"type":1}`,
			wantErrorSubstring: `invalid runtime_descriptor: decode runtime_descriptor: json: cannot unmarshal number into Go struct field persistedAgentRuntimeDescriptor.type of type string`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, db, _ := testutil.StartPostgres(t)
			pg := &PostgresStore{DB: db}
			ctx := context.Background()

			if _, err := db.ExecContext(ctx, `
				INSERT INTO agents (
					agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source,
					parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
					runtime_descriptor, status
				) VALUES (
					$1, NULL, 'reviewer', 'regular', 'anthropic', FALSE, 'platform_default',
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
