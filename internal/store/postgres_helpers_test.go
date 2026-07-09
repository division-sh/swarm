package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func TestDSNFromConfigQuotesKeywordValues(t *testing.T) {
	cfg := config.DatabaseConfig{
		Host:    "127.0.0.1",
		Port:    5432,
		Name:    "swarm db",
		User:    "swarm user",
		SSLMode: "disable",
	}
	password := `has space 'quote' \slash user=other`

	dsn := DSNFromConfig(cfg, password)
	for _, want := range []string{
		`host='127.0.0.1'`,
		`dbname='swarm db'`,
		`sslmode='disable'`,
		`user='swarm user'`,
		`password='has space \'quote\' \\slash user=other'`,
	} {
		if !strings.Contains(dsn, want) {
			t.Fatalf("DSNFromConfig() = %q, want substring %q", dsn, want)
		}
	}
	if _, err := pq.NewConnector(dsn); err != nil {
		t.Fatalf("pq.NewConnector(%q): %v", dsn, err)
	}
}

func TestDSNFromConfigPinsDefaultNonSecretKeywordsAgainstPGEnv(t *testing.T) {
	t.Setenv("PGHOST", "env-host")
	t.Setenv("PGPORT", "15432")
	t.Setenv("PGDATABASE", "env-db")
	t.Setenv("PGUSER", "env-user")
	t.Setenv("PGSSLMODE", "require")

	dsn := DSNFromConfig(config.DatabaseConfig{}, "secret")
	for _, want := range []string{
		`host='127.0.0.1'`,
		"port=5432",
		`dbname='swarm'`,
		`sslmode='disable'`,
		`user='postgres'`,
	} {
		if !strings.Contains(dsn, want) {
			t.Fatalf("DSNFromConfig() = %q, want pinned default keyword %q", dsn, want)
		}
	}
	for _, notWant := range []string{"env-host", "15432", "env-db", "env-user", "require"} {
		if strings.Contains(dsn, notWant) {
			t.Fatalf("DSNFromConfig() = %q, leaked PG env value %q", dsn, notWant)
		}
	}
	if _, err := pq.NewConnector(dsn); err != nil {
		t.Fatalf("pq.NewConnector(%q): %v", dsn, err)
	}
}

func TestPostgresStore_HelpersAndDescriptors(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	parsed, err := pq.NewConfig(dsn)
	if err != nil {
		t.Fatalf("parse canonical test Postgres DSN: %v", err)
	}

	cfg := config.DatabaseConfig{
		Host:     parsed.Host,
		Port:     int(parsed.Port),
		Name:     parsed.Database,
		User:     parsed.User,
		SSLMode:  string(parsed.SSLMode),
		PoolSize: 5,
	}
	gotDSN := DSNFromConfig(cfg, parsed.Password)
	if !strings.Contains(gotDSN, "host='"+parsed.Host+"'") || !strings.Contains(gotDSN, "dbname='"+parsed.Database+"'") {
		t.Fatalf("unexpected dsn: %q", gotDSN)
	}
	pg, err := NewPostgresStore(gotDSN)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	const runID = "55555555-5555-5555-5555-555555555555"
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	if err := pg.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	entityID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('testco', 'test', 'static', '{"instance_kind":"entity","workflow_version":"v1"}'::jsonb, 'active', now())
	`); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'testco', 'default', 'testco', 'TestCo', 'active',
			'{}'::jsonb, '{"users_total":10,"mrr":1234}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, runID, entityID); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}

	// Active agent descriptors.
	_ = pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ID: "a", Role: "a", Mode: "global", Type: "stub", Model: "regular", FlowPath: "review/inst-1", EntityID: entityID, Config: []byte(`{}`)},
		Status: "active", HiredBy: "test", StartedAt: time.Now().UTC(),
	})
	_ = pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ID: "t", Role: "t", Mode: "global", Type: "stub", Model: "regular", Config: []byte(`{}`)},
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
