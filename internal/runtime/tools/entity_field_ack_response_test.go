package tools_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type postCommitFaultEntityWriter struct {
	pipeline.EntityFieldMutationWriter
	fault  error
	reject bool
	writes int
}

func (s *postCommitFaultEntityWriter) ApplyEntityFieldMutation(ctx context.Context, update pipeline.EntityFieldMutation) (pipeline.EntityFieldMutationResult, error) {
	if s.reject {
		return pipeline.EntityFieldMutationResult{}, s.fault
	}
	result, err := s.EntityFieldMutationWriter.ApplyEntityFieldMutation(ctx, update)
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
		for _, operation := range []string{"set", "append"} {
			t.Run(backend+"/"+operation, func(t *testing.T) {
				bundle := loadWave1EntityToolBundle(t, actor, "review", "accounts", "", "accounts:\n  status: text\n  items: list<text>\n")
				var base runtimetools.EntityPersistence
				var sourceCtx context.Context
				query := `SELECT revision FROM entity_state WHERE run_id = ? AND entity_id = ?`
				mutationQuery := `SELECT COUNT(*) FROM entity_mutations WHERE run_id = ? AND entity_id = ?`
				if backend == "sqlite" {
					selected := newSQLiteRuntimeToolStoreForTest(t)
					sourceCtx = seedEntityToolSourceRun(t, selected, bundle)
					base = selected
				} else {
					selected := newPostgresHumanTaskToolStoreForTest(t)
					sourceCtx = seedEntityToolSourceRun(t, selected, bundle)
					base = selected
					query = `SELECT revision FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
					mutationQuery = `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid AND entity_id = $2::uuid`
				}
				fault := errors.Join(errors.New("independent post-commit cleanup failed"), errors.New("SQL connection password=private-token"))
				fixture := sourceCtx.Value(entityToolImportFixtureKey{}).(entityToolImportFixture)
				writer := &postCommitFaultEntityWriter{EntityFieldMutationWriter: fixture.pipeline, fault: fault}
				actor := entityToolFixtureActor(t, fixture.source, actor, entityToolTestRunID, "review", "review/inst-1")
				ctx := runtimetools.WithActor(sourceCtx, actor)
				bus := &entityToolRuntimeLogBus{}
				exec := runtimetools.NewExecutorWithOptions(bus, runtimetools.ExecutorOptions{
					EntityStore: base, EntityWriter: writer, WorkflowSource: semanticview.Wrap(bundle), AllowInternalLegacyEntityTools: true,
				})
				entityID := seedImportedEntityForToolTest(t, ctx, map[string]any{
					"flow_instance": "review/inst-1", "fields": map[string]any{"status": "open", "items": []any{"equal"}},
				})
				db := storetest.DatabaseForTest(base)
				var before, baselineRevision int
				if err := db.QueryRowContext(ctx, mutationQuery, entityToolTestRunID, entityID).Scan(&before); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRowContext(ctx, query, entityToolTestRunID, entityID).Scan(&baselineRevision); err != nil {
					t.Fatal(err)
				}
				input := map[string]any{
					"entity_id": entityID, "field": "status", "value": "closed",
				}
				if operation == "append" {
					input["op"], input["field"], input["value"] = "append", "items", "equal"
				}
				out, err := exec.Execute(ctx, "save_entity_field", input)
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
				if response["revision"] != revision || revision != baselineRevision+1 || count != before+1 || writer.writes != 1 {
					t.Fatalf("committed revision = %#v, persisted revision = %d, mutations = %d, writes = %d", response["revision"], revision, count, writer.writes)
				}
				row, found, err := base.LoadEntityState(ctx, runtimetools.EntityIdentity{RunID: entityToolTestRunID, EntityID: entityID})
				if err != nil || !found {
					t.Fatalf("committed readback: found=%v err=%v", found, err)
				}
				if operation == "append" && !reflect.DeepEqual(row["fields"].(map[string]any)["items"], []any{"equal", "equal"}) {
					t.Fatalf("acknowledged append was repeated or lost multiplicity: %#v", row["fields"])
				}
				writer.reject = true
				input["value"] = "other"
				out, err = exec.Execute(ctx, "save_entity_field", input)
				if out != nil || !errors.Is(err, fault) || !strings.Contains(err.Error(), "write_failed") {
					t.Fatalf("unacknowledged tool result = %#v, %v; want write_failed without revision", out, err)
				}
				if err := db.QueryRowContext(ctx, mutationQuery, entityToolTestRunID, entityID).Scan(&count); err != nil || count != before+1 {
					t.Fatalf("unacknowledged write mutations = %d, error = %v", count, err)
				}
				after, found, err := base.LoadEntityState(ctx, runtimetools.EntityIdentity{RunID: entityToolTestRunID, EntityID: entityID})
				if err != nil || !found || !reflect.DeepEqual(row, after) {
					t.Fatalf("unacknowledged write changed state: %#v, %v", after, err)
				}
			})
		}
	}
}
