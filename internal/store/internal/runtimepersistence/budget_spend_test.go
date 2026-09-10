package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSQLiteRuntimeStoreBudgetSpendPersistence(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	activeEntity := uuid.NewString()
	terminalEntity := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Second)
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	seedSQLiteBudgetRun(t, ctx, store, runID, now)
	seedSQLiteBudgetEntity(t, ctx, store, runID, activeEntity, "flow/active", "active", now)
	seedSQLiteBudgetEntity(t, ctx, store, runID, terminalEntity, "flow/done", "done", now)
	activeIdentity := mustTestAgentIdentityForRun(runID, "agent-1", "flow/active")
	globalIdentity := mustTestAgentIdentityForRun(runID, "agent-global", "global")
	seedTestAgentRow(t, ctx, store.backend.ConstructionHandle(), false, activeIdentity, "active")
	seedTestAgentRow(t, ctx, store.backend.ConstructionHandle(), false, globalIdentity, "active")

	if err := store.RecordSpend(ctx, budgetspend.SpendRecord{
		ExecutionMode:   "live",
		EntityID:        activeEntity,
		FlowInstance:    "flow/active",
		AgentID:         "agent-1",
		AgentIdentity:   activeIdentity,
		Model:           "claude-sonnet",
		ModelAlias:      "regular",
		BackendProfile:  "anthropic",
		Provider:        "anthropic",
		Transport:       "api",
		ResolvedModel:   "claude-sonnet",
		InputTokens:     10,
		OutputTokens:    4,
		CostUSD:         1.25,
		InvocationType:  "api",
		UsageAccounting: "exact",
		RecordedAt:      now,
	}); err != nil {
		t.Fatalf("RecordSpend(entity): %v", err)
	}
	if err := store.RecordSpend(ctx, budgetspend.SpendRecord{
		ExecutionMode:   "live",
		FlowInstance:    "global",
		AgentID:         "agent-global",
		AgentIdentity:   globalIdentity,
		Model:           "claude-cli",
		ModelAlias:      "regular",
		BackendProfile:  "claude_cli",
		Provider:        "claude",
		Transport:       "cli",
		ResolvedModel:   "claude-cli",
		InputTokens:     8,
		OutputTokens:    2,
		CostUSD:         0.75,
		InvocationType:  "cli_test",
		UsageAccounting: "estimated",
		RecordedAt:      now,
	}); err != nil {
		t.Fatalf("RecordSpend(global): %v", err)
	}

	flow, err := store.ResolveFlowInstance(ctx, runID, activeEntity)
	if err != nil {
		t.Fatalf("ResolveFlowInstance: %v", err)
	}
	if flow != "flow/active" {
		t.Fatalf("flow instance = %q, want flow/active", flow)
	}
	targets, err := store.ListBudgetProjectionTargets(ctx, []string{"done"})
	if err != nil {
		t.Fatalf("ListBudgetProjectionTargets: %v", err)
	}
	wantTargets := []budgetspend.ProjectionTarget{{RunID: runID, EntityID: activeEntity}}
	if !reflect.DeepEqual(targets, wantTargets) {
		t.Fatalf("budget projection targets = %#v, want %#v", targets, wantTargets)
	}

	since := now.Add(-time.Hour)
	system, err := store.SumSpendUSD(ctx, budgetspend.SpendQuery{Scope: budgetspend.ScopeSystem, Since: since})
	if err != nil {
		t.Fatalf("SumSpendUSD(system): %v", err)
	}
	global, err := store.SumSpendUSD(ctx, budgetspend.SpendQuery{Scope: budgetspend.ScopeGlobal, Since: since})
	if err != nil {
		t.Fatalf("SumSpendUSD(global): %v", err)
	}
	entity, err := store.SumSpendUSD(ctx, budgetspend.SpendQuery{Scope: budgetspend.ScopeEntity, EntityID: activeEntity, Since: since})
	if err != nil {
		t.Fatalf("SumSpendUSD(entity): %v", err)
	}
	if system != 2.0 || global != 0.75 || entity != 1.25 {
		t.Fatalf("spend sums system=%v global=%v entity=%v, want 2.0/0.75/1.25", system, global, entity)
	}

	var exactRows, estimatedRows int
	if err := store.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM spend_ledger WHERE usage_accounting = 'exact'`).Scan(&exactRows); err != nil {
		t.Fatalf("count exact rows: %v", err)
	}
	if err := store.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM spend_ledger WHERE usage_accounting = 'estimated'`).Scan(&estimatedRows); err != nil {
		t.Fatalf("count estimated rows: %v", err)
	}
	if exactRows != 1 || estimatedRows != 1 {
		t.Fatalf("usage accounting rows exact=%d estimated=%d, want 1/1", exactRows, estimatedRows)
	}
}

func TestPostgresStoreBudgetSpendPersistenceQueries(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := admitTestPostgresStore(t, db)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	entityID := uuid.NewString()
	recordedAt := time.Now().UTC().Truncate(time.Second)

	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: recordedAt})
	for _, entity := range []struct {
		id, flow, state string
	}{{entityID, "flow/1", "active"}, {uuid.NewString(), "flow/done", "done"}} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
				gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
			) VALUES ($1::uuid, $2::uuid, $3, 'budget_entity', $2, $2, $4,
				'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, $5, $5, $5)
		`, runID, entity.id, entity.flow, entity.state, recordedAt); err != nil {
			t.Fatalf("seed postgres budget entity %s: %v", entity.id, err)
		}
	}
	identity := mustTestAgentIdentityForRun(runID, "agent-1", "flow/1")
	seedTestAgentRow(t, ctx, db, true, identity, "active")
	fields, err := identity.StorageFields()
	if err != nil {
		t.Fatal(err)
	}
	record := budgetspend.SpendRecord{
		ExecutionMode:   "live",
		EntityID:        entityID,
		FlowInstance:    "flow/1",
		AgentID:         "agent-1",
		AgentIdentity:   identity,
		Model:           "claude-sonnet",
		ModelAlias:      "regular",
		BackendProfile:  "anthropic",
		Provider:        "anthropic",
		Transport:       "api",
		ResolvedModel:   "claude-sonnet",
		InputTokens:     10,
		OutputTokens:    4,
		CostUSD:         1.25,
		InvocationType:  "anthropic",
		UsageAccounting: "exact",
		RecordedAt:      recordedAt,
	}
	if err := pg.RecordSpend(ctx, record); err != nil {
		t.Fatalf("RecordSpend: %v", err)
	}

	var got budgetspend.SpendRecord
	var gotFields agentidentity.StorageFields
	if err := db.QueryRowContext(ctx, `
		SELECT execution_mode, run_id::text, entity_id::text, flow_instance, agent_id,
			agent_name_owner, agent_name_source, agent_route_presence, agent_flow_scope_key, agent_flow_instance_id,
			model, model_alias, backend_profile, provider, transport, resolved_model,
			input_tokens, output_tokens, cost_usd, invocation_type, usage_accounting, created_at
		FROM spend_ledger WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, runID, entityID).Scan(
		&got.ExecutionMode, &gotFields.RunID, &got.EntityID, &got.FlowInstance, &got.AgentID,
		&gotFields.NameOwner, &gotFields.NameSource, &gotFields.RoutePresence, &gotFields.FlowScopeKey, &gotFields.FlowInstanceID,
		&got.Model, &got.ModelAlias, &got.BackendProfile, &got.Provider, &got.Transport, &got.ResolvedModel,
		&got.InputTokens, &got.OutputTokens, &got.CostUSD, &got.InvocationType, &got.UsageAccounting, &got.RecordedAt,
	); err != nil {
		t.Fatalf("read persisted spend: %v", err)
	}
	gotFields.AgentID, gotFields.FlowInstancePath = got.AgentID, got.FlowInstance
	if gotFields != fields {
		t.Fatalf("persisted agent identity = %#v, want %#v", gotFields, fields)
	}
	got.AgentIdentity, err = agentidentity.FromStorageFields(gotFields)
	if err != nil {
		t.Fatalf("read persisted agent identity: %v", err)
	}
	got.RecordedAt = got.RecordedAt.UTC()
	if !reflect.DeepEqual(got, record) {
		t.Fatalf("persisted spend = %#v, want %#v", got, record)
	}

	flow, err := pg.ResolveFlowInstance(ctx, runID, entityID)
	if err != nil {
		t.Fatalf("ResolveFlowInstance: %v", err)
	}
	if flow != "flow/1" {
		t.Fatalf("flow = %q, want flow/1", flow)
	}

	targets, err := pg.ListBudgetProjectionTargets(ctx, []string{"done"})
	if err != nil {
		t.Fatalf("ListBudgetProjectionTargets: %v", err)
	}
	wantTargets := []budgetspend.ProjectionTarget{{RunID: runID, EntityID: entityID}}
	if !reflect.DeepEqual(targets, wantTargets) {
		t.Fatalf("budget projection targets = %#v, want %#v", targets, wantTargets)
	}

	since := recordedAt.Add(-time.Hour)
	spent, err := pg.SumSpendUSD(ctx, budgetspend.SpendQuery{Scope: budgetspend.ScopeEntity, EntityID: entityID, Since: since})
	if err != nil {
		t.Fatalf("SumSpendUSD: %v", err)
	}
	if spent != 1.25 {
		t.Fatalf("spent = %v, want 1.25", spent)
	}
}

func seedSQLiteBudgetEntity(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, runID, entityID, flowInstance, state string, at time.Time) {
	t.Helper()
	if _, err := store.backend.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (?, ?, ?, 'budget_entity', ?, ?, ?, '{}', '{}', '{}', 1, ?, ?, ?)
	`, runID, entityID, flowInstance, entityID, entityID, state, at, at, at); err != nil {
		t.Fatalf("seed sqlite budget entity %s: %v", entityID, err)
	}
}

func seedSQLiteBudgetRun(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, runID string, at time.Time) {
	t.Helper()
	requireRunFixtureForTest(t, ctx, NewSQLiteRuntimeStoreForTest(store.backend.ConstructionHandle()), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: at})
}
