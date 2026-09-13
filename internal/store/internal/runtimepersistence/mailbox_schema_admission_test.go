package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestMailboxTokenOnlySchemaRejectedBeforeRuntimeAdmissionBothStores(t *testing.T) {
	for _, dialect := range []SchemaDialect{SchemaDialectSQLite, SchemaDialectPostgres} {
		for _, populated := range []bool{false, true} {
			name := string(dialect) + "/empty"
			if populated {
				name = string(dialect) + "/populated"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				current := canonicalSchemaBootstrapTestRequest(t)
				legacy := canonicalSchemaBootstrapTestRequest(t)
				legacy.Origin.SwarmVersion = "token-only-mailbox"
				found := false
				for i, plan := range legacy.PlatformPlans {
					if plan.TableName == "api_idempotency" {
						found = true
						// Exact pre-#2420 table shape, not an alias or migration path.
						legacy.PlatformPlans[i] = SchemaTableDDL{
							TableName: "api_idempotency", SchemaKind: "platform_spec", ColumnCount: 8,
							Statements: []string{
								`CREATE TABLE IF NOT EXISTS api_idempotency (
    method TEXT NOT NULL,
    actor_token_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    response JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (method, actor_token_id, idempotency_key)
)`,
								`CREATE INDEX IF NOT EXISTS idx_api_idempotency_expires ON api_idempotency (expires_at)`,
							},
						}
					}
				}
				if !found {
					t.Fatal("canonical schema omitted api_idempotency")
				}
				var db *sql.DB
				var bootstrap func(context.Context, SchemaBootstrapRequest) error
				var runtimeProbe func() error
				if dialect == SchemaDialectSQLite {
					path := filepath.Join(t.TempDir(), "token-only.db")
					old, err := NewSQLiteRuntimeStore(path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = old.Close() })
					if err := old.BootstrapSchema(ctx, legacy); err != nil {
						t.Fatal(err)
					}
					db = old.backend.ConstructionHandle()
					fresh, err := NewSQLiteRuntimeStore(path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = fresh.Close() })
					bootstrap = fresh.BootstrapSchema
					runtimeProbe = func() error {
						_, err := fresh.ListActiveAgentDescriptors(ctx, unacceptedAdmissionEventID)
						return err
					}
				} else {
					_, pg, cleanup := testutil.StartEmptyPostgres(t)
					t.Cleanup(cleanup)
					db = pg
					old := newPostgresStoreWithBackend(mustPostgresBackend(db))
					if err := old.BootstrapSchema(ctx, legacy); err != nil {
						t.Fatal(err)
					}
					fresh := newPostgresStoreWithBackend(mustPostgresBackend(db))
					bootstrap = fresh.BootstrapSchema
					runtimeProbe = func() error {
						_, err := fresh.ListActiveAgentDescriptors(ctx, unacceptedAdmissionEventID)
						return err
					}
				}
				if populated {
					if _, err := db.Exec(`INSERT INTO api_idempotency
(method,actor_token_id,idempotency_key,request_hash,resource_id,response,created_at,expires_at)
VALUES ('mailbox.decide','old-token','key','hash','card','{"original":true}','2026-01-01T00:00:00Z','2027-01-01T00:00:00Z')`); err != nil {
						t.Fatal(err)
					}
				}
				current.StatePlans = generatedProbeStatePlans()
				var incompatible *SchemaCompatibilityError
				if err := bootstrap(ctx, current); !errors.As(err, &incompatible) {
					t.Fatalf("old actor schema admission = %v", err)
				}
				for _, column := range []string{"api_idempotency", "actor_kind", "actor_id", "actor_token_id"} {
					if !strings.Contains(strings.Join(incompatible.Drift, ";"), column) {
						t.Fatalf("missing %s diagnostic: %v", column, incompatible.Drift)
					}
				}
				if err := runtimeProbe(); err == nil || !strings.Contains(err.Error(), "schema is unaccepted") {
					t.Fatalf("runtime admitted rejected store: %v", err)
				}
				assertSchemaTableExists(t, dialect, db, "generated_probe_state", false)
				var count int
				if err := db.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE actor_token_id='old-token' AND request_hash='hash' AND resource_id='card'`).Scan(&count); err != nil {
					t.Fatal(err)
				}
				want := 0
				if populated {
					want = 1
				}
				if count != want {
					t.Fatalf("old completion changed: count=%d want=%d", count, want)
				}
			})
		}
	}
}
