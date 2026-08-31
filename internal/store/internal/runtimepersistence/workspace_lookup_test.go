package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimeworkspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestWorkspaceLookupPrivateAdapterParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			runID := uuid.NewString()
			entityID := uuid.NewString()
			entityFlow := "workspace-entity-" + uuid.NewString()
			sharedFlow := "workspace-shared-" + uuid.NewString()

			var lookup workspaceLookupTestStore
			var db *sql.DB
			switch backend {
			case "sqlite":
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				lookup = store
				db = store.backend.ConstructionHandle()
				requireRunFixtureForTest(t, ctx, store, semanticRunFixture{
					Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID,
					State: runtimerunlifecycle.StateRunning, StartedAt: time.Now().UTC(),
				})
			case "postgres":
				_, postgresDB, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store := newPostgresStoreWithBackend(mustPostgresBackend(postgresDB))
				lookup = store
				db = postgresDB
				requireRunFixtureForTest(t, ctx, store, semanticRunFixture{
					Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID,
					State: runtimerunlifecycle.StateRunning, StartedAt: time.Now().UTC(),
				})
			default:
				t.Fatalf("unknown backend %q", backend)
			}

			seedWorkspaceLookupFacts(t, ctx, db, backend, runID, entityID, entityFlow, sharedFlow)

			got, err := lookup.LookupWorkspaceEntity(ctx, runtimecurrentstate.Identity{RunID: runID, EntityID: entityID})
			if err != nil {
				t.Fatalf("LookupWorkspaceEntity: %v", err)
			}
			if got.Slug != "customer-one" {
				t.Fatalf("workspace slug = %q, want customer-one", got.Slug)
			}

			containers, err := lookup.ListRuntimeWorkspaceContainers(ctx, runID)
			if err != nil {
				t.Fatalf("ListRuntimeWorkspaceContainers: %v", err)
			}
			if want := []string{"customer-one", "customer-two"}; !reflect.DeepEqual(containers.EntitySlugs, want) {
				t.Fatalf("workspace container slugs = %#v, want %#v", containers.EntitySlugs, want)
			}

			_, err = lookup.LookupWorkspaceEntity(ctx, runtimecurrentstate.Identity{RunID: runID, EntityID: uuid.NewString()})
			if err == nil || !strings.Contains(err.Error(), "is not present") {
				t.Fatalf("missing workspace entity error = %v, want fail-closed absence", err)
			}
		})
	}
}

type workspaceLookupTestStore interface {
	LookupWorkspaceEntity(context.Context, runtimecurrentstate.Identity) (runtimeworkspace.WorkspaceEntityLookup, error)
	ListRuntimeWorkspaceContainers(context.Context, string) (runtimeworkspace.RuntimeWorkspaceContainerSet, error)
}

func seedWorkspaceLookupFacts(t *testing.T, ctx context.Context, db *sql.DB, backend, runID, entityID, entityFlow, sharedFlow string) {
	t.Helper()
	now := time.Now().UTC()
	flowQuery := `INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at) VALUES (?, ?, 'workspace', 'static', ?, 'active', ?)`
	entityQuery := `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, slug, name, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at) VALUES (?, ?, ?, 'default', ?, ?, 'active', '{}', '{}', '{}', 1, ?, ?, ?)`
	if backend == "postgres" {
		flowQuery = `INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at) VALUES ($1::uuid, $2, 'workspace', 'static', $3::jsonb, 'active', $4)`
		entityQuery = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, slug, name, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, 'default', $4, $5, 'active', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, $6, $7, $8)`
	}
	for _, flow := range []struct {
		id     string
		config string
	}{{entityFlow, `{"instance_kind":"entity"}`}, {sharedFlow, `{"instance_kind":"shared"}`}} {
		if _, err := db.ExecContext(ctx, flowQuery, runID, flow.id, flow.config, now); err != nil {
			t.Fatalf("seed %s flow instance: %v", backend, err)
		}
	}
	for _, entity := range []struct {
		id, flow, slug string
	}{{entityID, entityFlow, "customer-one"}, {uuid.NewString(), entityFlow, "customer-two"}, {uuid.NewString(), sharedFlow, "shared-ignored"}} {
		if _, err := db.ExecContext(ctx, entityQuery, runID, entity.id, entity.flow, entity.slug, entity.slug, now, now, now); err != nil {
			t.Fatalf("seed %s workspace entity: %v", backend, err)
		}
	}
}
