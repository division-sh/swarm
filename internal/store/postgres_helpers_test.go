package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/config"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemanager "swarm/internal/runtime/manager"
	"swarm/internal/testutil"
)

func portFromDSN(t *testing.T, dsn string) int {
	t.Helper()
	for _, part := range strings.Fields(dsn) {
		if strings.HasPrefix(part, "port=") {
			var n int
			fmtSscanf(strings.TrimPrefix(part, "port="), &n)
			if n > 0 {
				return n
			}
		}
	}
	t.Fatalf("port not found in dsn: %q", dsn)
	return 0
}

func dbNameFromDSN(t *testing.T, dsn string) string {
	t.Helper()
	for _, part := range strings.Fields(dsn) {
		if strings.HasPrefix(part, "dbname=") {
			return strings.TrimPrefix(part, "dbname=")
		}
	}
	t.Fatalf("dbname not found in dsn: %q", dsn)
	return ""
}

// Small local helper to avoid importing fmt (keeps this file tiny in coverage terms).
func fmtSscanf(s string, out *int) {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	*out = n
}

func TestPostgresStore_HelpersAndDescriptors(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	port := portFromDSN(t, dsn)
	dbName := dbNameFromDSN(t, dsn)

	cfg := config.DatabaseConfig{
		Host:     "127.0.0.1",
		Port:     port,
		Name:     dbName,
		User:     "postgres",
		Password: "postgres",
		SSLMode:  "disable",
		PoolSize: 5,
	}
	gotDSN := DSNFromConfig(cfg)
	if !strings.Contains(gotDSN, "host=127.0.0.1") || !strings.Contains(gotDSN, "dbname="+dbName) {
		t.Fatalf("unexpected dsn: %q", gotDSN)
	}
	pg, err := NewPostgresStore(gotDSN)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	ctx := context.Background()
	if err := pg.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	entityID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('testco', 'test', 'static', '{"instance_kind":"entity","workflow_version":"v1"}'::jsonb, 'active', now())
	`); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO entity_state (
			entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, 'testco', 'default', 'testco', 'TestCo', 'active',
			'{}'::jsonb, '{"users_total":10,"mrr":1234}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, entityID); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}

	// Active agent descriptors.
	_ = pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ID: "a", Role: "a", Mode: "global", Type: "stub", FlowPath: "review/inst-1", EntityID: entityID, Config: []byte(`{}`)},
		Status: "active", HiredBy: "test", StartedAt: time.Now().UTC(),
	})
	_ = pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ID: "t", Role: "t", Mode: "global", Type: "stub", Config: []byte(`{}`)},
		Status: "terminated", HiredBy: "test", StartedAt: time.Now().UTC(),
	})
	descriptors, err := pg.ListActiveAgentDescriptors(ctx)
	if err != nil || len(descriptors) == 0 {
		t.Fatalf("ListActiveAgentDescriptors err=%v descriptors=%v", err, descriptors)
	}
	if got := descriptors[0].AgentID; got != "a" {
		t.Fatalf("descriptor agent_id = %q, want a", got)
	}
	if got := descriptors[0].EntityID; got != entityID {
		t.Fatalf("descriptor entity_id = %q, want %q", got, entityID)
	}
	if got := descriptors[0].FlowInstance; got != "review/inst-1" {
		t.Fatalf("descriptor flow_instance = %q, want review/inst-1", got)
	}

}
