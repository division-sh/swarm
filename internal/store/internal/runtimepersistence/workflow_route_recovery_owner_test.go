package runtimepersistence_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimeworkflowroute "github.com/division-sh/swarm/internal/runtime/workflowroute"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type activeWorkflowRouteReader interface {
	LoadActiveWorkflowRoute(context.Context, string) (runtimeworkflowroute.RecoveryRecord, error)
	runtimerunlifecycle.OperationOwner
	runtimerunlifecycle.CandidateStore
}

func TestActiveWorkflowRouteRecoveryUsesOnlyCurrentRunOwnersOnBothStores(t *testing.T) {
	tests := []struct {
		name   string
		open   func(*testing.T) (activeWorkflowRouteReader, *sql.DB)
		sqlite bool
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (activeWorkflowRouteReader, *sql.DB) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected, storetest.Database(selected)
			},
			sqlite: true,
		},
		{
			name: "postgres",
			open: func(t *testing.T) (activeWorkflowRouteReader, *sql.DB) {
				_, db, _ := testutil.StartPostgres(t)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			selected, db := tc.open(t)
			ctx := testAuthorActivityContext()
			retiredRunID := uuid.NewString()
			currentRunID := uuid.NewString()
			ambiguousRunID := uuid.NewString()
			for _, fixture := range []storetest.RunFixture{
				{Origin: storetest.ScenarioSetupOrigin(), RunID: retiredRunID, State: runtimerunlifecycle.StateCancelled},
				{Origin: storetest.ScenarioSetupOrigin(), RunID: currentRunID, State: runtimerunlifecycle.StateRunning},
				{Origin: storetest.ScenarioSetupOrigin(), RunID: ambiguousRunID, State: runtimerunlifecycle.StatePaused},
			} {
				storetest.RequireRun(t, ctx, selected, fixture)
			}

			instancePath := "review/" + uuid.NewString()
			retiredEntityID := uuid.NewString()
			currentEntityID := uuid.NewString()
			ambiguousEntityID := uuid.NewString()
			config := `{"workflow_version":"1","instance_id":"one","flow_path":"` + instancePath + `"}`
			if tc.sqlite {
				if _, err := db.ExecContext(ctx, `
					INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
					VALUES (?, 'review', 'template', ?, 'active', CURRENT_TIMESTAMP)
				`, instancePath, config); err != nil {
					t.Fatalf("seed SQLite flow instance: %v", err)
				}
			} else if _, err := db.ExecContext(ctx, `
				INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
				VALUES ($1, 'review', 'template', $2::jsonb, 'active', NOW())
			`, instancePath, config); err != nil {
				t.Fatalf("seed Postgres flow instance: %v", err)
			}

			insertOwner := func(runID, entityID string) {
				t.Helper()
				query := "INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state) VALUES (?, ?, ?, 'review_entity', 'active')"
				if !tc.sqlite {
					query = "INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state) VALUES ($1::uuid, $2::uuid, $3, 'review_entity', 'active')"
				}
				if _, err := db.ExecContext(ctx, query, runID, entityID, instancePath); err != nil {
					t.Fatalf("seed route owner %s/%s: %v", runID, entityID, err)
				}
			}

			insertOwner(retiredRunID, retiredEntityID)
			insertOwner(currentRunID, currentEntityID)
			record, err := selected.LoadActiveWorkflowRoute(ctx, instancePath)
			if err != nil {
				t.Fatalf("recover current owner with retired predecessor: %v", err)
			}
			if record.EntityID != currentEntityID {
				t.Fatalf("recovered entity_id = %q, want current owner %q instead of retired owner %q", record.EntityID, currentEntityID, retiredEntityID)
			}

			insertOwner(ambiguousRunID, ambiguousEntityID)
			if _, err := selected.LoadActiveWorkflowRoute(ctx, instancePath); err == nil || !strings.Contains(err.Error(), "exactly one current persisted entity owner") {
				t.Fatalf("ambiguous current owner error = %v", err)
			}

			for _, runID := range []string{currentRunID, ambiguousRunID} {
				if _, _, err := storetest.TerminalizeRun(ctx, selected, runtimerunlifecycle.TerminalRequest{
					RunID: runID, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC(),
				}); err != nil {
					t.Fatalf("retire current route owner run %s: %v", runID, err)
				}
			}
			if _, err := selected.LoadActiveWorkflowRoute(ctx, instancePath); err == nil || !strings.Contains(err.Error(), "exactly one current persisted entity owner") {
				t.Fatalf("missing current owner error = %v", err)
			}
		})
	}
}

func TestEntityStateSchemaRequiresExplicitNonblankEntityContractOnBothStores(t *testing.T) {
	tests := []struct {
		name   string
		open   func(*testing.T) (activeWorkflowRouteReader, *sql.DB)
		sqlite bool
	}{
		{name: "sqlite", sqlite: true, open: func(t *testing.T) (activeWorkflowRouteReader, *sql.DB) {
			selected := storetest.StartSQLiteRuntimeStore(t)
			return selected, storetest.Database(selected)
		}},
		{name: "postgres", open: func(t *testing.T) (activeWorkflowRouteReader, *sql.DB) {
			_, db, _ := testutil.StartPostgres(t)
			return storetest.AdmitPostgresRuntimeStore(t, db), db
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			selected, db := tc.open(t)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
				Origin: storetest.ScenarioSetupOrigin(), RunID: runID, State: runtimerunlifecycle.StateRunning,
			})
			omitted := `INSERT INTO entity_state (run_id, entity_id, flow_instance, current_state) VALUES (?, ?, 'review/one', 'active')`
			blank := `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state) VALUES (?, ?, 'review/two', '   ', 'active')`
			if !tc.sqlite {
				omitted = `INSERT INTO entity_state (run_id, entity_id, flow_instance, current_state) VALUES ($1::uuid, $2::uuid, 'review/one', 'active')`
				blank = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state) VALUES ($1::uuid, $2::uuid, 'review/two', '   ', 'active')`
			}
			if _, err := db.ExecContext(ctx, omitted, runID, uuid.NewString()); err == nil {
				t.Fatal("fresh schema accepted omitted entity_type")
			}
			if _, err := db.ExecContext(ctx, blank, runID, uuid.NewString()); err == nil {
				t.Fatal("fresh schema accepted blank entity_type")
			}
		})
	}
}
