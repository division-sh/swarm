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
)

type postCommitFaultEntityStore struct {
	runtimetools.EntityPersistence
	fault  error
	reject bool
	writes int
}

func (s *postCommitFaultEntityStore) SaveEntityField(ctx context.Context, update runtimetools.EntityFieldUpdate) (runtimetools.EntityFieldWriteResult, error) {
	if s.reject {
		return runtimetools.EntityFieldWriteResult{}, s.fault
	}
	result, err := s.EntityPersistence.SaveEntityField(ctx, update)
	if err != nil || !result.Acknowledged {
		return result, err
	}
	s.writes++
	return result, s.fault
}

func TestSaveEntityFieldAcknowledgedErrorReturnsCommittedToolResponseOnBothStores(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live", ID: "tester", Type: "internal", Role: "operator",
		Tools: []string{"create_entity", "get_entity", "save_entity_field"},
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := loadWave1EntityToolBundle(t, actor, "review", "accounts", "", "accounts:\n  status: text\n")
			var base runtimetools.EntityPersistence
			query := `SELECT revision FROM entity_state WHERE run_id = ? AND entity_id = ?`
			mutationQuery := `SELECT COUNT(*) FROM entity_mutations WHERE run_id = ? AND entity_id = ?`
			if backend == "sqlite" {
				selected := newSQLiteRuntimeToolStoreForTest(t)
				ensureSQLiteEntityToolTestRun(t, selected)
				base = selected
			} else {
				selected := newPostgresHumanTaskToolStoreForTest(t)
				ensureEntityToolTestRun(t, storetest.DatabaseForTest(selected))
				base = selected
				query = `SELECT revision FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
				mutationQuery = `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid AND entity_id = $2::uuid`
			}
			fault := errors.Join(errors.New("independent post-commit cleanup failed"), errors.New("SQL connection password=private-token"))
			store := &postCommitFaultEntityStore{EntityPersistence: base, fault: fault}
			ctx := runtimetools.WithActor(runtimecorrelation.WithRunID(unmanagedToolTestContext(), entityToolTestRunID), actor)
			bus := &entityToolRuntimeLogBus{}
			exec := runtimetools.NewExecutorWithOptions(bus, runtimetools.ExecutorOptions{
				EntityStore: store, WorkflowSource: semanticview.Wrap(bundle), AllowInternalLegacyEntityTools: true,
			})
			entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
				"flow_instance": "review/inst-1", "fields": map[string]any{"status": "open"},
			})
			db := storetest.DatabaseForTest(base)
			var before, baselineRevision int
			if err := db.QueryRowContext(ctx, mutationQuery, entityToolTestRunID, entityID).Scan(&before); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, query, entityToolTestRunID, entityID).Scan(&baselineRevision); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Execute(ctx, "save_entity_field", map[string]any{
				"entity_id": entityID, "field": "status", "value": "closed",
			})
			if err != nil {
				t.Fatalf("acknowledged write reported a retryable tool error: %v", err)
			}
			response, ok := out.(map[string]any)
			if !ok || response["status"] != "committed_with_post_commit_error" || response["write_committed"] != true || response["retry_write"] != false || response["post_commit_error_code"] != "entity_field_write_post_commit_failure" {
				t.Fatalf("acknowledged tool response = %#v", out)
			}
			encoded, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			if _, leaked := response["post_commit_error"]; leaked || strings.Contains(string(encoded), "password=private-token") || strings.Contains(string(encoded), "independent post-commit cleanup failed") {
				t.Fatalf("tool response exposed backend detail: %#v", response)
			}
			var diagnosticFound bool
			for _, entry := range bus.logs {
				if entry.Action != "entity_field_write_post_commit_failure" {
					encodedEntry, err := json.Marshal(entry)
					if err != nil {
						t.Fatal(err)
					}
					if strings.Contains(string(encodedEntry), "password=private-token") || strings.Contains(string(encodedEntry), "independent post-commit cleanup failed") {
						t.Fatalf("ordinary tool diagnostic exposed backend detail: %#v", entry)
					}
					continue
				}
				diagnosticFound = true
				detail, ok := entry.Detail.(map[string]any)
				if !ok || detail["post_commit_error"] != fault.Error() || detail["revision"] != response["revision"] {
					t.Fatalf("internal diagnostic lost joined cause or revision: %#v", entry)
				}
			}
			if !diagnosticFound {
				t.Fatal("acknowledged post-commit failure was not logged internally")
			}
			var revision, count int
			if err := db.QueryRowContext(ctx, query, entityToolTestRunID, entityID).Scan(&revision); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, mutationQuery, entityToolTestRunID, entityID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if response["revision"] != revision || revision != baselineRevision+1 || count != before+1 || store.writes != 1 {
				t.Fatalf("committed revision = %#v, persisted revision = %d, mutations = %d, writes = %d", response["revision"], revision, count, store.writes)
			}
			store.reject = true
			out, err = exec.Execute(ctx, "save_entity_field", map[string]any{
				"entity_id": entityID, "field": "status", "value": "other",
			})
			if out != nil || !errors.Is(err, fault) || !strings.Contains(err.Error(), "write_failed") {
				t.Fatalf("unacknowledged tool result = %#v, %v; want write_failed without revision", out, err)
			}
			if err := db.QueryRowContext(ctx, mutationQuery, entityToolTestRunID, entityID).Scan(&count); err != nil || count != before+1 {
				t.Fatalf("unacknowledged write mutations = %d, error = %v", count, err)
			}
		})
	}
}
