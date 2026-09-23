package tools_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type postCommitFaultCreateStore struct {
	runtimetools.EntityPersistence
	fault  error
	reject bool
	writes int
}

func (s *postCommitFaultCreateStore) CreateEntity(ctx context.Context, rec runtimetools.EntityCreateRecord) (runtimetools.EntityCreateResult, error) {
	if s.reject {
		return runtimetools.EntityCreateResult{}, s.fault
	}
	result, err := s.EntityPersistence.CreateEntity(ctx, rec)
	if err != nil || !result.Acknowledged {
		return result, err
	}
	s.writes++
	return result, s.fault
}

func TestCreateEntityAcknowledgedErrorReturnsCanonicalIDWithoutRetryOnBothStores(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live", ID: "tester", Type: "internal", Role: "operator",
		Tools: []string{"create_entity", "get_entity"},
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := loadWave1EntityToolBundle(t, actor, "review", "accounts", "", "accounts:\n  status: text\n")
			var base runtimetools.EntityPersistence
			query := `SELECT COUNT(*) FROM entity_state WHERE run_id = ? AND entity_id = ?`
			mutationQuery := `SELECT COUNT(*) FROM entity_mutations WHERE run_id = ? AND entity_id = ?`
			if backend == "sqlite" {
				selected := newSQLiteRuntimeToolStoreForTest(t)
				ensureSQLiteEntityToolTestRun(t, selected)
				base = selected
			} else {
				selected := newPostgresHumanTaskToolStoreForTest(t)
				ensureEntityToolTestRun(t, storetest.DatabaseForTest(selected))
				base = selected
				query = `SELECT COUNT(*) FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
				mutationQuery = `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid AND entity_id = $2::uuid`
			}
			fault := errors.Join(errors.New("independent post-commit cleanup failed"), errors.New("SQL connection password=private-token"))
			store := &postCommitFaultCreateStore{EntityPersistence: base, fault: fault}
			ctx := runtimetools.WithActor(runtimecorrelation.WithRunID(unmanagedToolTestContext(), entityToolTestRunID), actor)
			bus := &entityToolRuntimeLogBus{}
			exec := runtimetools.NewExecutorWithOptions(bus, runtimetools.ExecutorOptions{
				EntityStore: store, WorkflowSource: semanticview.Wrap(bundle), AllowInternalLegacyEntityTools: true,
			})
			out, err := exec.Execute(ctx, "create_entity", map[string]any{
				"flow_instance": "review/inst-1", "fields": map[string]any{"status": "open"},
			})
			if err != nil {
				t.Fatalf("acknowledged create reported retryable error: %v", err)
			}
			response, ok := out.(map[string]any)
			if !ok || response["status"] != "committed_with_post_commit_error" || response["write_committed"] != true || response["retry_write"] != false || response["post_commit_error_code"] != "entity_create_post_commit_failure" {
				t.Fatalf("acknowledged create response = %#v", out)
			}
			entityID, ok := response["entity_id"].(string)
			if !ok {
				t.Fatalf("missing created ID: %#v", response)
			}
			if _, err := uuid.Parse(entityID); err != nil {
				t.Fatalf("noncanonical created ID %q: %v", entityID, err)
			}
			encoded, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "private-token") || strings.Contains(string(encoded), "independent post-commit") {
				t.Fatalf("tool response exposed backend cause: %#v", response)
			}
			var diagnosticFound bool
			for _, entry := range bus.logs {
				if entry.Action == "entity_create_post_commit_failure" {
					diagnosticFound = true
					detail, ok := entry.Detail.(map[string]any)
					if !ok || entry.EntityID != entityID || detail["post_commit_error"] != fault.Error() {
						t.Fatalf("internal create diagnostic = %#v", entry)
					}
				}
			}
			if !diagnosticFound {
				t.Fatal("missing internal post-commit diagnostic")
			}
			db := storetest.DatabaseForTest(base)
			for _, statement := range []string{query, mutationQuery} {
				var count int
				if err := db.QueryRowContext(ctx, statement, entityToolTestRunID, entityID).Scan(&count); err != nil || count == 0 {
					t.Fatalf("committed create readback count = %d, error = %v", count, err)
				}
			}
			if store.writes != 1 {
				t.Fatalf("create attempts = %d, want one", store.writes)
			}
			store.reject = true
			out, err = exec.Execute(ctx, "create_entity", map[string]any{
				"flow_instance": "review/inst-1", "fields": map[string]any{"status": "other"},
			})
			if out != nil || !errors.Is(err, fault) || !strings.Contains(err.Error(), "write_failed") || store.writes != 1 {
				t.Fatalf("unacknowledged create = %#v, %v; writes = %d", out, err, store.writes)
			}
			allRowsQuery := `SELECT COUNT(*) FROM entity_state WHERE run_id = ?`
			if backend == "postgres" {
				allRowsQuery = `SELECT COUNT(*) FROM entity_state WHERE run_id = $1::uuid`
			}
			var totalRows int
			if err := db.QueryRowContext(ctx, allRowsQuery, entityToolTestRunID).Scan(&totalRows); err != nil || totalRows != 1 {
				t.Fatalf("entity rows after unacknowledged create = %d, error = %v; want one committed row", totalRows, err)
			}
		})
	}
}
