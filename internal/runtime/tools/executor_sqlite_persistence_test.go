package tools_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type humanTaskToolStore interface {
	runtimetools.HumanTaskPersistence
	runtimetools.MailboxPersistence
	GetV1MailboxItem(ctx context.Context, id string) (store.MailboxV1ItemDetail, error)
}

type allowHumanTaskAuthority struct{}

func (allowHumanTaskAuthority) CanonicalRole(role string) string { return strings.TrimSpace(role) }
func (allowHumanTaskAuthority) ProducerRoles() []string          { return nil }
func (allowHumanTaskAuthority) ProducerEventsForRole(string) []string {
	return nil
}
func (allowHumanTaskAuthority) HasMessageAuthority(actor, target models.AgentConfig) bool {
	return false
}
func (allowHumanTaskAuthority) AuthorizeRouting(actor, target models.AgentConfig, status string) error {
	return nil
}
func (allowHumanTaskAuthority) AuthorizeManagement(actor, target models.AgentConfig) error {
	return nil
}
func (allowHumanTaskAuthority) AuthorizeMailboxSend(actor models.AgentConfig) error {
	return nil
}
func (allowHumanTaskAuthority) CanDecideHumanTasks(role string) bool { return true }

func TestEntityTools_SQLiteBackendNeutralEntityPersistence(t *testing.T) {
	actor := models.AgentConfig{
		ID:    "tester",
		Type:  "internal",
		Role:  "operator",
		Tools: []string{"create_entity", "get_entity", "save_entity_field", "query_entities", "query_metrics", "search_entities"},
	}
	bundle := loadWave1EntityToolBundle(t, actor, "review", "accounts", `
types: {}
`, `
accounts:
  status: text
  score: numeric
  priority: integer
`)
	sqliteStore := newSQLiteRuntimeToolStoreForTest(t)
	ensureSQLiteEntityToolTestRun(t, sqliteStore)
	ctx := runtimetools.WithActor(runtimecorrelation.WithRunID(context.Background(), entityToolTestRunID), actor)
	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		EntityStore:                    sqliteStore,
		WorkflowSource:                 semanticview.Wrap(bundle),
		AllowInternalLegacyEntityTools: true,
	})

	entityID := mustCreateEntityID(t, ctx, exec, map[string]any{
		"flow_instance": "review/inst-1",
		"name":          "Acme",
		"fields": map[string]any{
			"status":   "open",
			"score":    42.0,
			"priority": 3,
		},
	})
	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "status",
		"value":     "closed",
	}); err != nil {
		t.Fatalf("sqlite save_entity_field: %v", err)
	}
	searchOut, err := exec.Execute(ctx, "search_entities", map[string]any{
		"flow_instance": "review/inst-1",
		"current_state": "queued",
		"filter":        map[string]any{"status": "closed"},
	})
	if err != nil {
		t.Fatalf("sqlite search_entities: %v", err)
	}
	searchResult := searchOut.(map[string]any)
	if got := len(searchResult["results"].([]map[string]any)); got != 1 {
		t.Fatalf("sqlite search result count = %d, want 1", got)
	}
	queryOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"filter": `status == "closed"`,
		"select": []string{"status"},
	})
	if err != nil {
		t.Fatalf("sqlite query_entities: %v", err)
	}
	if got := len(queryOut.(map[string]any)["results"].([]map[string]any)); got != 1 {
		t.Fatalf("sqlite query result count = %d, want 1", got)
	}
	metricOut, err := exec.Execute(ctx, "query_metrics", map[string]any{
		"metric": "sum",
		"field":  "score",
	})
	if err != nil {
		t.Fatalf("sqlite query_metrics: %v", err)
	}
	if got := testNumericValue(metricOut.(map[string]any)["value"]); got != 42.0 {
		t.Fatalf("sqlite metric sum = %v, want 42", got)
	}
	var mutationCount int
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM entity_mutations
		WHERE run_id = ? AND entity_id = ?
	`, entityToolTestRunID, entityID).Scan(&mutationCount); err != nil {
		t.Fatalf("count sqlite entity mutations: %v", err)
	}
	if mutationCount < 2 {
		t.Fatalf("sqlite entity mutation count = %d, want create and update mutations", mutationCount)
	}
}

func TestSQLiteEntityPersistence_MarshalsStructuredFilterValues(t *testing.T) {
	sqliteStore := newSQLiteRuntimeToolStoreForTest(t)
	ensureSQLiteEntityToolTestRun(t, sqliteStore)
	ctx := runtimecorrelation.WithRunID(context.Background(), entityToolTestRunID)
	entityID := uuid.NewString()
	if err := sqliteStore.CreateEntity(ctx, runtimetools.EntityCreateRecord{
		RunID:        entityToolTestRunID,
		EntityID:     entityID,
		FlowInstance: "review/inst-structured",
		EntityType:   "account",
		CurrentState: "queued",
		FieldsJSON: json.RawMessage(`{
			"business_brief":{"summary":"validated"},
			"tags":["alpha","beta"]
		}`),
		CreatedAt: time.Now().UTC(),
		Writer: runtimetools.EntityMutationWriter{
			Type:        "platform",
			ID:          "sqlite-structured-filter-test",
			HandlerStep: "seed",
		},
	}); err != nil {
		t.Fatalf("seed sqlite structured entity: %v", err)
	}

	rows, err := sqliteStore.QueryEntityStates(ctx, runtimetools.EntityStateQuery{
		RunID: entityToolTestRunID,
		FieldEquals: []runtimetools.EntityFieldEquals{
			{Path: "business_brief", Value: map[string]any{"summary": "validated"}},
			{Path: "tags", Value: []any{"alpha", "beta"}},
		},
	})
	if err != nil {
		t.Fatalf("query sqlite structured field filters: %v", err)
	}
	if len(rows) != 1 || strings.TrimSpace(asString(rows[0]["entity_id"])) != entityID {
		t.Fatalf("structured filter rows = %#v, want seeded entity %s", rows, entityID)
	}
}

func TestRoleScopedEntityTools_SQLiteCurrentEntityPersistence(t *testing.T) {
	actor := models.AgentConfig{ID: "validation-orchestrator", Role: "validation_orchestrator", Tools: []string{"save_entity_field"}}
	bundle := loadRoleScopedEntityToolBundle(t, actor, true)
	sqliteStore := newSQLiteRuntimeToolStoreForTest(t)
	ensureSQLiteEntityToolTestRun(t, sqliteStore)
	ctx := runtimecorrelation.WithRunID(context.Background(), entityToolTestRunID)
	entityID := uuid.NewString()
	if err := sqliteStore.CreateEntity(ctx, runtimetools.EntityCreateRecord{
		RunID:        entityToolTestRunID,
		EntityID:     entityID,
		FlowInstance: "validation/inst-1",
		EntityType:   "validation_case",
		CurrentState: "queued",
		FieldsJSON:   json.RawMessage(`{"status":"open","business_brief":{"summary":"before","confidence":1}}`),
		CreatedAt:    time.Now().UTC(),
		Writer: runtimetools.EntityMutationWriter{
			Type:        "platform",
			ID:          "sqlite-role-scoped-test",
			HandlerStep: "seed",
		},
	}); err != nil {
		t.Fatalf("seed sqlite role-scoped entity: %v", err)
	}
	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		EntityStore:       sqliteStore,
		WorkflowSource:    semanticview.Wrap(bundle),
		AuthorityProvider: allowHumanTaskAuthority{},
	})
	currentCtx := runtimetools.WithActor(runtimebus.WithInboundEvent(ctx, eventtest.RootIngress(
		"evt-current",
		events.EventType("validation.started"),
		"",
		"",
		nil,
		0,
		entityToolTestRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "validation/inst-1"),
		time.Time{},
	)), actor)

	names := roleScopedToolDefinitionMap(exec.ToolDefinitionsForActorInContext(currentCtx, actor))
	if _, ok := names["read_validation_case"]; !ok {
		t.Fatalf("sqlite current entity did not enable generated read tool: %#v", sortedRoleScopedToolNames(names))
	}
	if _, err := exec.Execute(currentCtx, "save_validation_case_business_brief", map[string]any{
		"value": map[string]any{"summary": "after", "confidence": 9},
	}); err != nil {
		t.Fatalf("sqlite save_validation_case_business_brief: %v", err)
	}
	got, err := exec.Execute(currentCtx, "read_validation_case_business_brief", map[string]any{})
	if err != nil {
		t.Fatalf("sqlite read_validation_case_business_brief: %v", err)
	}
	brief, ok := got.(map[string]any)
	if !ok || strings.TrimSpace(asString(brief["summary"])) != "after" {
		t.Fatalf("sqlite role-scoped brief = %#v, want updated summary", got)
	}
}

func TestHumanTaskTools_BackendNeutralPersistence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store humanTaskToolStore
	}{
		{name: "postgres", store: newPostgresHumanTaskToolStoreForTest(t)},
		{name: "sqlite", store: newSQLiteRuntimeToolStoreForTest(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{Extensions: map[string]any{
				"budget": map[string]any{
					"human_tasks": map[string]any{
						"max_tasks_per_week": 1,
						"budget_reset":       "monday",
					},
				},
			}}
			exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
				Config:            cfg,
				HumanTaskStore:    tc.store,
				AuthorityProvider: allowHumanTaskAuthority{},
			})
			requester := models.AgentConfig{ID: "requester", Role: "worker", EntityID: uuid.NewString()}
			decider := models.AgentConfig{ID: "decider", Role: "operator"}

			firstID := createHumanTaskWithExecutor(t, exec, requester)
			firstItem, err := tc.store.GetMailboxItem(context.Background(), firstID)
			if err != nil {
				t.Fatalf("get first human task: %v", err)
			}
			if firstItem.Type != "human_task" || firstItem.EntityID != requester.EntityID {
				t.Fatalf("first human task item = %#v", firstItem)
			}
			if _, err := exec.ExecHumanTaskDecideDirect(context.Background(), decider, map[string]any{
				"task_id":  firstID,
				"decision": "approve",
				"reason":   "ok",
			}); err != nil {
				t.Fatalf("approve first human task: %v", err)
			}

			secondID := createHumanTaskWithExecutor(t, exec, requester)
			out, err := exec.ExecHumanTaskDecideDirect(context.Background(), decider, map[string]any{
				"task_id":  secondID,
				"decision": "approve",
			})
			if err != nil {
				t.Fatalf("budget-gated approve second human task: %v", err)
			}
			if got := strings.TrimSpace(asString(out.(map[string]any)["status"])); got != "deferred" {
				t.Fatalf("second human task status = %q, want deferred", got)
			}
			assertHumanTaskDeferredProjection(t, tc.store, secondID)
			requeueCount, err := tc.store.HumanTaskRequeueCount(context.Background(), secondID)
			if err != nil {
				t.Fatalf("load second human task requeue count: %v", err)
			}
			if requeueCount != 1 {
				t.Fatalf("second human task requeue count = %d, want 1", requeueCount)
			}
			explicitID := createHumanTaskWithExecutor(t, exec, requester)
			explicitUntil := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
			out, err = exec.ExecHumanTaskDecideDirect(context.Background(), decider, map[string]any{
				"task_id":      explicitID,
				"decision":     "defer",
				"reason":       "wait for operator context",
				"requeue_date": explicitUntil.Format(time.RFC3339),
			})
			if err != nil {
				t.Fatalf("explicit defer human task: %v", err)
			}
			if got := strings.TrimSpace(asString(out.(map[string]any)["status"])); got != "deferred" {
				t.Fatalf("explicit human task status = %q, want deferred", got)
			}
			assertHumanTaskDeferredProjection(t, tc.store, explicitID)
			missingDateID := createHumanTaskWithExecutor(t, exec, requester)
			if _, err := exec.ExecHumanTaskDecideDirect(context.Background(), decider, map[string]any{
				"task_id":  missingDateID,
				"decision": "defer",
			}); err == nil {
				t.Fatal("defer human task without requeue_date error = nil")
			}
			out, err = exec.ExecHumanTaskDecideDirect(context.Background(), decider, map[string]any{
				"task_id":  secondID,
				"decision": "approve",
			})
			if err != nil {
				t.Fatalf("approve requeued human task: %v", err)
			}
			if got := strings.TrimSpace(asString(out.(map[string]any)["status"])); got != "approved" {
				t.Fatalf("requeued human task status = %q, want approved", got)
			}
		})
	}
}

func assertHumanTaskDeferredProjection(t *testing.T, store humanTaskToolStore, taskID string) {
	t.Helper()
	detail, err := store.GetV1MailboxItem(context.Background(), taskID)
	if err != nil {
		t.Fatalf("get v1 deferred human task: %v", err)
	}
	if detail.Item.Status != "deferred" || detail.Item.Decision != "" || detail.Item.DeferredUntil == "" {
		t.Fatalf("deferred human task projection = %#v, want status deferred, no terminal decision, deferred_until set", detail.Item)
	}
	if _, err := time.Parse(time.RFC3339Nano, detail.Item.DeferredUntil); err != nil {
		t.Fatalf("deferred_until %q is not RFC3339Nano: %v", detail.Item.DeferredUntil, err)
	}
	if len(detail.History) < 2 || detail.History[len(detail.History)-1].Action != "deferred" {
		t.Fatalf("deferred human task history = %#v", detail.History)
	}
}

func createHumanTaskWithExecutor(t *testing.T, exec *runtimetools.Executor, actor models.AgentConfig) string {
	t.Helper()
	out, err := exec.ExecHumanTaskRequestDirect(context.Background(), actor, map[string]any{
		"category":       "review",
		"description":    "Needs human review",
		"expected_value": "approval",
		"priority":       "high",
		"talking_points": []string{"one", "two"},
	})
	if err != nil {
		t.Fatalf("human_task_request: %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("human_task_request output = %#v", out)
	}
	taskID := strings.TrimSpace(asString(result["task_id"]))
	if taskID == "" {
		t.Fatalf("human_task_request task_id missing in %#v", out)
	}
	return taskID
}

func newPostgresHumanTaskToolStoreForTest(t *testing.T) *store.PostgresStore {
	t.Helper()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	return &store.PostgresStore{DB: db}
}

func newSQLiteRuntimeToolStoreForTest(t *testing.T) *store.SQLiteRuntimeStore {
	t.Helper()
	raw, err := os.ReadFile(runtimecontracts.DefaultPlatformSpecFile(runtimepipeline.WorkflowRepoRoot()))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("unmarshal platform spec: %v", err)
	}
	plans, err := store.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	sqliteStore, err := store.NewSQLiteRuntimeStore(filepath.Join(t.TempDir(), "dev.db"))
	if err != nil {
		t.Fatalf("NewSQLiteRuntimeStore: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite runtime store: %v", err)
		}
	})
	if err := sqliteStore.EnsureSchemaTables(context.Background(), plans); err != nil {
		t.Fatalf("EnsureSchemaTables: %v", err)
	}
	return sqliteStore
}

func ensureSQLiteEntityToolTestRun(t *testing.T, sqliteStore *store.SQLiteRuntimeStore) {
	t.Helper()
	if _, err := sqliteStore.DB.ExecContext(context.Background(), `
		INSERT INTO runs (run_id, status, bundle_source, started_at)
		VALUES (?, 'running', 'legacy', ?)
		ON CONFLICT(run_id) DO NOTHING
	`, entityToolTestRunID, time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite entity tool test run: %v", err)
	}
}
