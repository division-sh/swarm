package runtime_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type budgetRecoveryParityStore interface {
	budgetspend.Store
	runtimebus.EventStore
	runtimemanager.ManagerPersistence
	runtimemanager.AgentLifecyclePersistence
	runtimetools.MailboxPersistence
}

func TestCompletionBudgetRecoveryProjectionParity(t *testing.T) {
	type backend struct {
		name  string
		start func(*testing.T) (budgetRecoveryParityStore, *sql.DB, bool)
	}
	backends := []backend{
		{
			name: "sqlite",
			start: func(t *testing.T) (budgetRecoveryParityStore, *sql.DB, bool) {
				s := storetest.StartSQLiteRuntimeStore(t)
				return s, s.DB, false
			},
		},
		{
			name: "postgres",
			start: func(t *testing.T) (budgetRecoveryParityStore, *sql.DB, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return &store.PostgresStore{DB: db}, db, true
			},
		},
	}

	for _, tc := range backends {
		t.Run(tc.name, func(t *testing.T) {
			selected, db, postgres := tc.start(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)
			runA, runB := uuid.NewString(), uuid.NewString()
			entityA, entityB, terminalEntity := uuid.NewString(), uuid.NewString(), uuid.NewString()
			seedBudgetRecoveryRun(t, ctx, db, postgres, runA, now)
			seedBudgetRecoveryRun(t, ctx, db, postgres, runB, now.Add(time.Second))
			seedBudgetRecoveryEntity(t, ctx, db, postgres, runA, entityA, "active", now)
			seedBudgetRecoveryEntity(t, ctx, db, postgres, runB, entityB, "active", now.Add(time.Second))
			seedBudgetRecoveryEntity(t, ctx, db, postgres, runB, terminalEntity, "done", now.Add(2*time.Second))

			for _, seed := range []struct {
				runID  string
				record budgetspend.SpendRecord
			}{
				{runID: runA, record: budgetRecoverySpend(entityA, "flow/a", 9.5, now)},
				{runID: runB, record: budgetRecoverySpend(entityB, "flow/b", 9.5, now)},
				{runID: runB, record: budgetRecoverySpend(terminalEntity, "flow/done", 9.5, now)},
				{record: budgetRecoverySpend("", "global", 9.5, now)},
			} {
				spendCtx := ctx
				if seed.runID != "" {
					spendCtx = runtimecorrelation.WithRunID(spendCtx, seed.runID)
				}
				if err := selected.RecordSpend(spendCtx, seed.record); err != nil {
					t.Fatalf("seed retained spend: %v", err)
				}
			}

			bus, err := runtimebus.NewEventBus(selected)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{TerminalStages: []string{"done"}},
				Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
					"budget_warning_percent":   {Value: 50},
					"budget_throttle_percent":  {Value: 75},
					"budget_emergency_percent": {Value: 90},
				}},
			})
			tracker := runtimepkg.NewBudgetTracker(selected, bus, &config.Config{Extensions: map[string]any{
				"budget": map[string]any{
					"system_monthly_cap":     40,
					"global_monthly_cap":     10,
					"per_entity_monthly_cap": 10,
				},
			}}, selected, nil, source)
			manager := runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
				LifecycleStore: selected,
				SemanticSource: source,
				Budget:         tracker,
			}, selected)

			if _, err := manager.RecoverWithStartupReplayDiagnostics(ctx); err != nil {
				t.Fatalf("RecoverWithStartupReplayDiagnostics with process context: %v", err)
			}
			assertRecoveredBudgetState(t, tracker, entityA, entityB, terminalEntity)
			assertBudgetRecoverySideEffects(t, ctx, db, 4, 4)

			if _, err := manager.RecoverWithStartupReplayDiagnostics(ctx); err != nil {
				t.Fatalf("repeated RecoverWithStartupReplayDiagnostics with process context: %v", err)
			}
			assertRecoveredBudgetState(t, tracker, entityA, entityB, terminalEntity)
			assertBudgetRecoverySideEffects(t, ctx, db, 4, 4)
		})
	}
}

func budgetRecoverySpend(entityID, flowInstance string, cost float64, at time.Time) budgetspend.SpendRecord {
	return budgetspend.SpendRecord{
		ExecutionMode:   "live",
		EntityID:        entityID,
		FlowInstance:    flowInstance,
		AgentID:         "budget-recovery-agent",
		Model:           "test-model",
		ModelAlias:      "regular",
		BackendProfile:  "test",
		Provider:        "test",
		Transport:       "test",
		ResolvedModel:   "test-model",
		CostUSD:         cost,
		InvocationType:  "completion",
		UsageAccounting: "exact",
		RecordedAt:      at,
	}
}

func seedBudgetRecoveryRun(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, runID string, at time.Time) {
	t.Helper()
	query := `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`
	if postgres {
		query = `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`
	}
	if _, err := db.ExecContext(ctx, query, runID, at); err != nil {
		t.Fatalf("seed run %s: %v", runID, err)
	}
}

func seedBudgetRecoveryEntity(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, runID, entityID, state string, at time.Time) {
	t.Helper()
	query := `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (?, ?, ?, 'budget_recovery', ?, '{}', '{}', '{}', 1, ?, ?, ?)
	`
	args := []any{runID, entityID, "flow/" + entityID, state, at, at, at}
	if postgres {
		query = `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, current_state,
				gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
			) VALUES ($1::uuid, $2::uuid, $3, 'budget_recovery', $4, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, $5, $6, $7)
		`
	}
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("seed entity %s for run %s: %v", entityID, runID, err)
	}
}

func assertRecoveredBudgetState(t *testing.T, tracker *runtimepkg.BudgetTracker, entityA, entityB, terminalEntity string) {
	t.Helper()
	wants := map[string]string{
		"system":   tracker.CurrentState("system", ""),
		"global":   tracker.CurrentState("global", ""),
		"entity_a": tracker.CurrentState("entity", entityA),
		"entity_b": tracker.CurrentState("entity", entityB),
	}
	for name, got := range wants {
		if got != "emergency" {
			t.Fatalf("recovered %s state = %q, want emergency", name, got)
		}
	}
	if got := tracker.CurrentState("entity", terminalEntity); got != "ok" {
		t.Fatalf("terminal entity recovery state = %q, want unprojected ok", got)
	}
	if !tracker.IsEntityEmergency(entityA) || !tracker.IsEntityThrottle(entityB) {
		t.Fatalf("manager suppression guard was not refreshed: entityA emergency=%v entityB throttle=%v", tracker.IsEntityEmergency(entityA), tracker.IsEntityThrottle(entityB))
	}
}

func assertBudgetRecoverySideEffects(t *testing.T, ctx context.Context, db *sql.DB, wantEvents, wantMailbox int) {
	t.Helper()
	for table, want := range map[string]int{"events": wantEvents, "mailbox": wantMailbox} {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		if table == "events" {
			query += " WHERE event_name = 'platform.budget_threshold_crossed'"
		}
		var got int
		if err := db.QueryRowContext(ctx, query).Scan(&got); err != nil {
			t.Fatalf("count %s recovery side effects: %v", table, err)
		}
		if got != want {
			t.Fatalf("%s recovery side effects = %d, want %d", table, got, want)
		}
	}
}
