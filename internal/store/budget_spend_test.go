package store

import (
	"context"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
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

	if err := store.RecordSpend(ctx, budgetspend.SpendRecord{
		ExecutionMode:   "live",
		EntityID:        activeEntity,
		FlowInstance:    "flow/active",
		AgentID:         "agent-1",
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
	if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM spend_ledger WHERE usage_accounting = 'exact'`).Scan(&exactRows); err != nil {
		t.Fatalf("count exact rows: %v", err)
	}
	if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM spend_ledger WHERE usage_accounting = 'estimated'`).Scan(&estimatedRows); err != nil {
		t.Fatalf("count estimated rows: %v", err)
	}
	if exactRows != 1 || estimatedRows != 1 {
		t.Fatalf("usage accounting rows exact=%d estimated=%d, want 1/1", exactRows, estimatedRows)
	}
}

func TestPostgresStoreBudgetSpendPersistenceQueries(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	pg := &PostgresStore{DB: db}
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	entityID := uuid.NewString()
	recordedAt := time.Now().UTC().Truncate(time.Second)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(status, '') FROM runs WHERE run_id = $1::uuid FOR UPDATE")).
		WithArgs(runID).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("running"))
	mock.ExpectQuery("SELECT EXISTS \\(SELECT 1 FROM entity_state").
		WithArgs(runID, entityID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO spend_ledger")).
		WithArgs("live", entityID, "flow/1", "agent-1", "claude-sonnet", "regular", "anthropic", "anthropic", "api", "claude-sonnet", 10, 4, 1.25, "anthropic", "exact", recordedAt).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := pg.RecordSpend(ctx, budgetspend.SpendRecord{
		ExecutionMode:   "live",
		EntityID:        entityID,
		FlowInstance:    "flow/1",
		AgentID:         "agent-1",
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
	}); err != nil {
		t.Fatalf("RecordSpend: %v", err)
	}

	mock.ExpectQuery("FROM entity_state").
		WithArgs(runID, entityID).
		WillReturnRows(sqlmock.NewRows([]string{"flow_instance"}).AddRow("flow/1"))
	flow, err := pg.ResolveFlowInstance(ctx, runID, entityID)
	if err != nil {
		t.Fatalf("ResolveFlowInstance: %v", err)
	}
	if flow != "flow/1" {
		t.Fatalf("flow = %q, want flow/1", flow)
	}

	mock.ExpectQuery("SELECT es.run_id::text, es.entity_id::text").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"run_id", "entity_id"}).AddRow(runID, entityID))
	targets, err := pg.ListBudgetProjectionTargets(ctx, []string{"done"})
	if err != nil {
		t.Fatalf("ListBudgetProjectionTargets: %v", err)
	}
	wantTargets := []budgetspend.ProjectionTarget{{RunID: runID, EntityID: entityID}}
	if !reflect.DeepEqual(targets, wantTargets) {
		t.Fatalf("budget projection targets = %#v, want %#v", targets, wantTargets)
	}

	since := recordedAt.Add(-time.Hour)
	mock.ExpectQuery("FROM spend_ledger").
		WithArgs(entityID, since, false).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(1.25))
	spent, err := pg.SumSpendUSD(ctx, budgetspend.SpendQuery{Scope: budgetspend.ScopeEntity, EntityID: entityID, Since: since})
	if err != nil {
		t.Fatalf("SumSpendUSD: %v", err)
	}
	if spent != 1.25 {
		t.Fatalf("spent = %v, want 1.25", spent)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func seedSQLiteBudgetEntity(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, runID, entityID, flowInstance, state string, at time.Time) {
	t.Helper()
	if _, err := store.DB.ExecContext(ctx, `
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
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, runID, at); err != nil {
		t.Fatalf("seed sqlite budget run %s: %v", runID, err)
	}
}
