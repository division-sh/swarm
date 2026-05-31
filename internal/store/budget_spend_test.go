package store

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"swarm/internal/runtime/budgetspend"
	runtimecorrelation "swarm/internal/runtime/correlation"
)

func TestSQLiteRuntimeStoreBudgetSpendPersistence(t *testing.T) {
	ctx := context.Background()
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
		EntityID:        activeEntity,
		FlowInstance:    "flow/active",
		AgentID:         "agent-1",
		Model:           "claude-sonnet",
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
		FlowInstance:    "global",
		AgentID:         "agent-global",
		Model:           "claude-cli",
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
	active, err := store.ListActiveEntityIDs(ctx, runID, []string{"done"})
	if err != nil {
		t.Fatalf("ListActiveEntityIDs: %v", err)
	}
	if len(active) != 1 || active[0] != activeEntity {
		t.Fatalf("active entities = %#v, want only %s", active, activeEntity)
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
	ctx := context.Background()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	recordedAt := time.Now().UTC().Truncate(time.Second)

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO spend_ledger")).
		WithArgs(entityID, "flow/1", "agent-1", "claude-sonnet", 10, 4, 1.25, "api", "exact", recordedAt).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := pg.RecordSpend(ctx, budgetspend.SpendRecord{
		EntityID:        entityID,
		FlowInstance:    "flow/1",
		AgentID:         "agent-1",
		Model:           "claude-sonnet",
		InputTokens:     10,
		OutputTokens:    4,
		CostUSD:         1.25,
		InvocationType:  "api",
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

	mock.ExpectQuery("SELECT entity_id::text").
		WithArgs(runID, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"entity_id"}).AddRow(entityID))
	active, err := pg.ListActiveEntityIDs(ctx, runID, []string{"done"})
	if err != nil {
		t.Fatalf("ListActiveEntityIDs: %v", err)
	}
	if len(active) != 1 || active[0] != entityID {
		t.Fatalf("active entities = %#v, want %s", active, entityID)
	}

	since := recordedAt.Add(-time.Hour)
	mock.ExpectQuery("FROM spend_ledger").
		WithArgs(entityID, since).
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
