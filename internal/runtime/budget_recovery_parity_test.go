package runtime_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type budgetRecoveryParityStore interface {
	budgetspend.Store
	runtimebus.EventStore
	runtimemanager.ManagerPersistence
	storetest.AgentFixtureStore
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
				return s, storetest.Database(s), false
			},
		},
		{
			name: "postgres",
			start: func(t *testing.T) (budgetRecoveryParityStore, *sql.DB, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db, true
			},
		},
	}

	for _, tc := range backends {
		t.Run(tc.name, func(t *testing.T) {
			selected, db, postgres := tc.start(t)
			ctx := testAuthorActivityContext(context.Background())
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
				{runID: runA, record: budgetRecoverySpend(t, entityA, "flow/a", 9.5, now)},
				{runID: runB, record: budgetRecoverySpend(t, entityB, "flow/b", 9.5, now)},
				{runID: runB, record: budgetRecoverySpend(t, terminalEntity, "flow/done", 9.5, now)},
				{record: budgetRecoverySpend(t, "", "global", 9.5, now)},
			} {
				spendCtx := ctx
				if seed.runID != "" {
					spendCtx = runtimecorrelation.WithRunID(spendCtx, seed.runID)
				}
				if err := selected.RecordSpend(spendCtx, seed.record); err != nil {
					t.Fatalf("seed retained spend: %v", err)
				}
			}

			bus, err := newRuntimeTestEventBus(t, selected)
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
			}}, selected, nil, source, executionposture.Live)
			manager := runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
				BaseContext:    ctx,
				LifecycleStore: storetest.AgentLifecycleFixture(selected),
				SemanticSource: source,
				Budget:         tracker,
				WorkOwner:      runtimeTestEventBusWorkOwner(t, bus), ReceiverExecution: eventreceiver.NormalExecution(),
			}, selected)
			coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: authorActivityTestBundleSourceFact.BundleHash(), BundleSource: "ephemeral"}
			plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, nil)
			if err != nil {
				t.Fatalf("construct budget recovery topology plan: %v", err)
			}
			capability, err := selected.AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
				OwnerID: "budget-recovery-test", BootID: uuid.NewString(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
			})
			if err != nil {
				t.Fatalf("acquire budget recovery process capability: %v", err)
			}
			if _, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{
				OperationID: uuid.NewString(), Plan: plan,
			}); err != nil {
				t.Fatalf("install budget recovery source set: %v", err)
			}
			grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
				BundleHash: coordinate.BundleHash, BundleSource: coordinate.BundleSource,
				RuntimeInstanceID: authorActivityTestRuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
			})
			if err != nil {
				t.Fatalf("issue budget recovery generation grant: %v", err)
			}
			topologyAdmission, err := runtimeagenttopology.StaticAdmission(plan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged)
			if err != nil {
				t.Fatalf("construct budget recovery topology admission: %v", err)
			}
			if err := manager.InstallStartupTopology(grant, topologyAdmission, plan); err != nil {
				t.Fatalf("install budget recovery manager topology: %v", err)
			}
			if err := manager.ReconcileStaticTopologyForStartup(ctx, source); err != nil {
				t.Fatalf("reconcile budget recovery manager topology: %v", err)
			}
			t.Cleanup(func() { _ = capability.Release(context.Background()) })

			admission, err := managedexecution.New(managedexecution.KindNormalRuntime, "budget-recovery-test", 1, "", "budget-recovery-actors", "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", nil)
			if err != nil {
				t.Fatalf("managedexecution.New: %v", err)
			}
			managedCtx := managedexecution.WithAdmission(ctx, admission)
			if _, err := manager.RecoverWithStartupReplayDiagnostics(managedCtx); err != nil {
				t.Fatalf("RecoverWithStartupReplayDiagnostics with process context: %v", err)
			}
			assertRecoveredBudgetState(t, tracker, entityA, entityB, terminalEntity)
			assertBudgetRecoverySideEffects(t, ctx, db, 4, 4)

			if _, err := manager.RecoverWithStartupReplayDiagnostics(managedCtx); err != nil {
				t.Fatalf("repeated RecoverWithStartupReplayDiagnostics with process context: %v", err)
			}
			assertRecoveredBudgetState(t, tracker, entityA, entityB, terminalEntity)
			assertBudgetRecoverySideEffects(t, ctx, db, 4, 4)
		})
	}
}

func budgetRecoverySpend(t *testing.T, entityID, flowInstance string, cost float64, at time.Time) budgetspend.SpendRecord {
	t.Helper()
	scopeKey, instanceID, found := strings.Cut(flowInstance, "/")
	if !found {
		instanceID = scopeKey
	}
	return budgetspend.SpendRecord{
		ExecutionMode:   "live",
		EntityID:        entityID,
		FlowInstance:    flowInstance,
		AgentID:         "budget-recovery-agent",
		AgentIdentity:   agentidentitytest.Runtime(t, "budget-recovery-agent", "budget-recovery-test", scopeKey, instanceID, flowInstance),
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
	if postgres {
		runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, StartedAt: at})
	} else {
		runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, StartedAt: at})
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
