package tools_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

type humanTaskToolStore interface {
	decisioncard.Store
	decisioncard.HumanTaskStore
	runtimereplycontext.Store
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
func (allowHumanTaskAuthority) AuthorizeMailboxSend(actor models.AgentConfig) error {
	return nil
}

func TestEntityTools_SQLiteBackendNeutralEntityPersistence(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "tester",
		Type:          "internal",
		Role:          "operator",
		Tools:         []string{"create_entity", "get_entity", "save_entity_field", "query_entities", "query_metrics", "search_entities"},
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
	ctx := runtimetools.WithActor(runtimecorrelation.WithRunID(unmanagedToolTestContext(), entityToolTestRunID), actor)
	exec := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{
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
	if _, err := storetest.DatabaseForTest(sqliteStore).ExecContext(ctx, `
		UPDATE entity_state
		SET bookkeeping = '{"private_fact":"must-not-leak"}'
		WHERE run_id = ? AND entity_id = ?
	`, entityToolTestRunID, entityID); err != nil {
		t.Fatalf("inject sqlite hostile bookkeeping: %v", err)
	}
	getOut, err := exec.Execute(ctx, "get_entity", map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatalf("sqlite get_entity: %v", err)
	}
	if _, exists := getOut.(map[string]any)["bookkeeping"]; exists {
		t.Fatalf("sqlite get_entity exposed platform bookkeeping: %#v", getOut)
	}
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
	if _, exists := searchResult["results"].([]map[string]any)[0]["bookkeeping"]; exists {
		t.Fatalf("sqlite search_entities exposed platform bookkeeping: %#v", searchResult)
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
	wholeQueryOut, err := exec.Execute(ctx, "query_entities", map[string]any{
		"filter": `status == "closed"`,
	})
	if err != nil {
		t.Fatalf("sqlite whole-row query_entities: %v", err)
	}
	wholeRows := wholeQueryOut.(map[string]any)["results"].([]map[string]any)
	if len(wholeRows) != 1 {
		t.Fatalf("sqlite whole query results = %#v", wholeRows)
	}
	if _, exists := wholeRows[0]["bookkeeping"]; exists {
		t.Fatalf("sqlite query_entities exposed platform bookkeeping: %#v", wholeRows[0])
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
	if err := storetest.DatabaseForTest(sqliteStore).QueryRowContext(ctx, `
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
	ctx := runtimecorrelation.WithRunID(unmanagedToolTestContext(), entityToolTestRunID)
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
	actor := models.AgentConfig{ExecutionMode: "live", ID: "validation-orchestrator", Role: "validation_orchestrator", Tools: []string{"save_entity_field"}}
	bundle := loadRoleScopedEntityToolBundle(t, actor, true)
	sqliteStore := newSQLiteRuntimeToolStoreForTest(t)
	ensureSQLiteEntityToolTestRun(t, sqliteStore)
	ctx := runtimecorrelation.WithRunID(unmanagedToolTestContext(), entityToolTestRunID)
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
	if _, err := storetest.DatabaseForTest(sqliteStore).ExecContext(ctx, `
		UPDATE entity_state
		SET bookkeeping = '{"private_fact":"must-not-leak"}'
		WHERE run_id = ? AND entity_id = ?
	`, entityToolTestRunID, entityID); err != nil {
		t.Fatalf("inject sqlite role-scoped hostile bookkeeping: %v", err)
	}
	exec := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{
		EntityStore:       sqliteStore,
		WorkflowSource:    semanticview.Wrap(bundle),
		AuthorityProvider: allowHumanTaskAuthority{},
	})
	currentCtx := runtimetools.WithActor(runtimebus.WithInboundEvent(ctx, eventtest.RunCreatingRootIngress(
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
	whole, err := exec.Execute(currentCtx, "read_validation_case", map[string]any{})
	if err != nil {
		t.Fatalf("sqlite read_validation_case with hostile bookkeeping: %v", err)
	}
	if _, exists := whole.(map[string]any)["bookkeeping"]; exists {
		t.Fatalf("sqlite role-scoped read exposed platform bookkeeping: %#v", whole)
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

func TestHumanTaskRequestCreatesTypedCardAndContinuationForImportedAgentOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store humanTaskToolStore
	}{
		{name: "postgres", store: newPostgresHumanTaskToolStoreForTest(t)},
		{name: "sqlite", store: newSQLiteRuntimeToolStoreForTest(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const flowPath = "gateway/provider"
			cfg := &config.Config{Extensions: map[string]any{
				"budget": map[string]any{"human_tasks": map[string]any{
					"max_tasks_per_week": 3, "budget_reset": "monday", "auto_expire_hours": 48,
				}},
			}}
			requester := models.AgentConfig{
				ExecutionMode: "live",
				ID:            "requester",
				Role:          "worker", FlowID: "provider", FlowPath: flowPath, EntityID: uuid.NewString(),
				Tools: []string{"human_task_request"}, Permissions: []string{"human_task_request"},
			}
			bundle := loadWave1EntityToolBundle(t, requester, "provider", "provider_record", "types: {}\n", "provider_record:\n  status: text\n")
			bundle.FlowTree.ByID["provider"].Path = flowPath
			bundle.FlowTree.ByID["provider"].Paths.PackageKey = "provider-package"
			source := semanticview.Wrap(bundle)
			declarations := semanticview.AgentDeclarations(source)
			if len(declarations) != 1 {
				t.Fatalf("requester declarations = %#v, want one", declarations)
			}
			plan, err := semanticview.ScopedAgentNamePlan(source, declarations[0])
			if err != nil {
				t.Fatalf("requester declaration name: %v", err)
			}
			requester.Identity = agentidentitytest.Declared(t, plan.AgentID, plan.OwnerURI, flowPath, "provider", flowPath)
			exec := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{
				Config: cfg, HumanTaskStore: tc.store, AuthorityProvider: allowHumanTaskAuthority{}, WorkflowSource: source,
			})
			ctx, replyContextID, sourceEventID := seedReplyToolContext(t, tc.store)
			ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, "provider-turn/tool-call-1")
			ctx = runtimecorrelation.WithBundleSourceFact(ctx, authorActivityTestBundleSourceFact)
			ctx = runtimetools.WithActor(ctx, requester)
			input := map[string]any{
				"scope": "flow", "category": "review", "description": "Review provider response",
				"talking_points": []string{"Check source evidence"}, "expected_value": "approval", "priority": "high",
			}
			created, err := exec.Execute(ctx, "human_task_request", input)
			if err != nil {
				t.Fatalf("human_task_request: %v", err)
			}
			cardID := strings.TrimSpace(asString(created.(map[string]any)["card_id"]))
			card, err := tc.store.GetDecisionCard(context.Background(), cardID)
			if err != nil {
				t.Fatalf("GetDecisionCard: %v", err)
			}
			anchor, err := card.Anchor.HumanTask()
			if err != nil {
				t.Fatal(err)
			}
			if anchor.RequesterAgentID != requester.ID || anchor.OperationID != "provider-turn/tool-call-1" || anchor.Scope.Kind != decisioncard.ScopeFlow || anchor.Scope.FlowInstance != "provider" {
				t.Fatalf("human-task anchor = %#v", anchor)
			}
			wantSourceRoute := events.RouteIdentity{FlowID: "provider", FlowInstance: flowPath, EntityID: requester.EntityID}
			if got := anchor.Source.Route().Normalized(); got != wantSourceRoute {
				t.Fatalf("human-task routing source = %#v, want %#v", got, wantSourceRoute)
			}
			continuation, err := tc.store.LoadHumanTaskContinuation(context.Background(), cardID)
			if err != nil {
				t.Fatalf("LoadHumanTaskContinuation: %v", err)
			}
			if continuation.ReplyContextID != replyContextID || continuation.SourceEventID != sourceEventID || continuation.State != decisioncard.HumanTaskContinuationPending {
				t.Fatalf("human-task continuation = %#v", continuation)
			}
			if got := continuation.RequesterRoute.Normalized(); got != (events.RouteIdentity{FlowID: "provider", FlowInstance: flowPath, EntityID: requester.EntityID}) {
				t.Fatalf("human-task requester route = %#v", got)
			}
			if got := continuation.DeadlineAt.Sub(card.CreatedAt); got != 48*time.Hour {
				t.Fatalf("default expiry = %s, want 48h", got)
			}

			replayed, err := exec.Execute(ctx, "human_task_request", input)
			if err != nil || strings.TrimSpace(asString(replayed.(map[string]any)["card_id"])) != cardID {
				t.Fatalf("idempotent replay = %#v, %v", replayed, err)
			}
			changed := map[string]any{}
			for key, value := range input {
				changed[key] = value
			}
			changed["description"] = "Changed request under the same operation"
			if _, err := exec.Execute(ctx, "human_task_request", changed); err == nil {
				t.Fatal("changed content under the same operation identity was accepted")
			}

			forkCtx, _, _ := seedReplyToolContext(t, tc.store)
			forkCtx = runtimeeffects.WithLogicalOperationIdentity(forkCtx, "provider-turn/tool-call-1")
			forkCtx = runtimecorrelation.WithBundleSourceFact(forkCtx, authorActivityTestBundleSourceFact)
			forkCtx = runtimetools.WithActor(forkCtx, requester)
			forked, err := exec.Execute(forkCtx, "human_task_request", input)
			if err != nil {
				t.Fatalf("fork-local human_task_request: %v", err)
			}
			if forkedCardID := strings.TrimSpace(asString(forked.(map[string]any)["card_id"])); forkedCardID == "" || forkedCardID == cardID {
				t.Fatalf("fork-local card id = %q, source card id = %q", forkedCardID, cardID)
			}
		})
	}
}

func seedReplyToolContext(t *testing.T, persistence humanTaskToolStore) (context.Context, string, string) {
	t.Helper()
	runID := uuid.NewString()
	requestEventID := uuid.NewString()
	// Force precision that Postgres cannot retain so idempotent replay proves
	// admission canonicalizes before snapshot and selected-store persistence.
	now := time.Now().UTC().Truncate(time.Microsecond).Add(789 * time.Nanosecond)
	switch typed := persistence.(type) {
	case *store.PostgresStore:
		runlifecyclefixture.RequirePostgres(t, unmanagedToolTestContext(), storetest.DatabaseForTest(typed), runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, StartedAt: now, BundleHash: authorActivityTestBundleHash, BundleSource: authorActivityTestBundleSource})
	case *store.SQLiteRuntimeStore:
		runlifecyclefixture.RequireSQLite(t, unmanagedToolTestContext(), storetest.DatabaseForTest(typed), runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, StartedAt: now, BundleHash: authorActivityTestBundleHash, BundleSource: authorActivityTestBundleSource})
	default:
		t.Fatalf("unsupported reply tool store %T", persistence)
	}
	storetest.CommitSemanticEvent(t, unmanagedToolTestContext(), persistence, eventtest.PersistedChildForProducer(
		requestEventID, events.EventType("provider.requested"),
		eventtest.Producer(events.EventProducerNode, "requester"), "", []byte(`{}`), 0,
		runID, eventtest.UUID("provider-request-parent:"+requestEventID), events.EventEnvelope{Scope: events.EventScopeGlobal}, now,
	))
	record := runtimereplycontext.Record{
		RunID:                runID,
		RequestEventID:       requestEventID,
		RequesterFlowID:      "requester",
		RequestOutputPin:     "provider_requested",
		ReplyInputPin:        "provider_replied",
		ProviderFlowID:       "provider",
		ProviderInputPin:     "provider_requested",
		ProviderOutputPin:    "provider_replied",
		Origin:               events.RouteIdentity{FlowID: "requester", FlowInstance: "requester/account-a", EntityID: uuid.NewString()},
		RequestCorrelationID: requestEventID,
		State:                runtimereplycontext.StateOpen,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	record.ID = runtimereplycontext.DeterministicID(record.RequestEventID, record.RequesterFlowID, record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID, record.Origin)
	if err := persistence.CreateReplyContext(unmanagedToolTestContext(), record); err != nil {
		t.Fatalf("seed reply tool context: %v", err)
	}
	inbound := eventtest.RunCreatingRootIngress(
		requestEventID,
		events.EventType("provider.requested"),
		"",
		"",
		nil,
		0,
		runID,
		"",
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "provider", FlowInstance: "provider", EntityID: record.Origin.EntityID}),
		now,
	)
	ctx := runtimebus.WithInboundEvent(runtimecorrelation.WithRunID(unmanagedToolTestContext(), runID), inbound)
	ctx = events.WithDeliveryContext(ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: record.ID}})
	return ctx, record.ID, requestEventID
}

func newPostgresHumanTaskToolStoreForTest(t *testing.T) *store.PostgresStore {
	t.Helper()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	return storetest.AdmitPostgresRuntimeStore(t, db)
}

func newSQLiteRuntimeToolStoreForTest(t *testing.T) *store.SQLiteRuntimeStore {
	t.Helper()
	source, err := yamlsource.LoadFile(runtimecontracts.DefaultPlatformSpecFile(runtimepipeline.WorkflowRepoRoot()))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := source.Decode(&spec); err != nil {
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
	if err := sqliteStore.BootstrapSchema(unmanagedToolTestContext(), store.SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin:        store.RuntimeStoreOrigin{SwarmVersion: "tools-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatalf("BootstrapSchema: %v", err)
	}
	return sqliteStore
}

func ensureSQLiteEntityToolTestRun(t *testing.T, sqliteStore *store.SQLiteRuntimeStore) {
	t.Helper()
	runlifecyclefixture.RequireSQLite(t, unmanagedToolTestContext(), storetest.DatabaseForTest(sqliteStore), runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: entityToolTestRunID, StartedAt: time.Now().UTC(), BundleHash: authorActivityTestBundleHash, BundleSource: authorActivityTestBundleSource})
}
