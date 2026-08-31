package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func TestPostgresStore_ApplyDestructiveResetCleanup_DeletesRunScopedRowsAndPreservesBoundaries(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })
	ctx := testAuthorActivityContext()
	seed := seedDestructiveResetCleanupRows(t, ctx, pg)

	now := time.Date(2026, 5, 16, 18, 30, 0, 0, time.UTC)
	result, err := pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
			Plan:          cleanupPlanForRunIDs(seed.RunA, seed.RunB),
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-30 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("ApplyDestructiveResetCleanup: %v", err)
	}
	if result.DryRun {
		t.Fatal("cleanup result DryRun = true, want false")
	}
	if len(result.RunIDs) != 2 {
		t.Fatalf("cleanup run IDs = %#v, want two runs", result.RunIDs)
	}
	assertCleanupTableResult(t, result, "runs", 2, 2)
	assertCleanupTableResult(t, result, "events", 6, 6)
	assertCleanupTableResult(t, result, "event_receipts", 1, 1)
	assertCleanupTableResult(t, result, "dead_letters", 2, 2)
	assertCleanupTableResult(t, result, "event_delivery_handler_rule_selections", 3, 3)
	assertCleanupTableResult(t, result, "fan_out_obligation_barriers", 1, 1)
	assertCleanupTableResult(t, result, "fan_out_outcomes", 1, 1)
	assertCleanupTableResult(t, result, "fan_out_intents", 1, 1)
	assertCleanupTableResult(t, result, "timers", 3, 3)
	assertCleanupTableResult(t, result, "conversation_forks", 1, 1)
	assertCleanupTableResult(t, result, "human_task_continuations", 1, 1)
	assertCleanupTableResult(t, result, "decision_cards", 1, 1)
	assertCleanupTableResult(t, result, "routing_rules", 1, 1)
	assertCleanupTableResult(t, result, "agents", 1, 1)
	assertCleanupTableResult(t, result, "flow_instances", 1, 1)
	assertCleanupTableResult(t, result, "mailbox", 0, 0)
	assertCleanupTableResult(t, result, "generated_entity_tables", 0, 0)

	for _, table := range []string{
		"runs",
		"event_delivery_handler_rule_selections",
		"event_deliveries",
		"fan_out_obligation_barriers",
		"fan_out_outcomes",
		"fan_out_intents",
		"run_fork_delivery_event_replays",
		"run_fork_selected_contract_executions",
		"run_fork_selected_contract_branch_divergences",
		"run_fork_selected_contract_route_recoveries",
		"run_fork_selected_contract_bindings",
		"agent_turns",
		"agent_conversation_audits",
		"agent_sessions",
		"agents",
		"human_task_continuations",
		"decision_cards",
		"entity_mutations",
		"entity_state",
		"flow_instances",
		"run_control_state",
	} {
		if got := countRows(t, ctx, pg, table); got != 0 {
			t.Fatalf("%s rows after cleanup = %d, want 0", table, got)
		}
	}
	if got := countRows(t, ctx, pg, "conversation_forks"); got != 1 {
		t.Fatalf("conversation_forks rows after cleanup = %d, want 1 preserved row without source_run_id", got)
	}
	for _, table := range []string{"events", "event_receipts", "dead_letters", "timers"} {
		if got := countRows(t, ctx, pg, table); got != 1 {
			t.Fatalf("%s rows after cleanup = %d, want 1 preserved global row", table, got)
		}
	}
	for _, table := range []string{
		"runtime_store_metadata",
		"api_idempotency",
		"runtime_ingress_state",
		"routing_rules",
		"mailbox",
		"generated_entity_fixture",
		"generated_node_state_fixture",
	} {
		if got := countRows(t, ctx, pg, table); got != 1 {
			t.Fatalf("%s rows after cleanup = %d, want 1 preserved row", table, got)
		}
	}
	if got := countRows(t, ctx, pg, "spend_ledger"); got != 2 {
		t.Fatalf("spend_ledger rows after cleanup = %d, want 2 preserved immutable rows", got)
	}
	var retainedSpendRuns int
	if err := pg.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM spend_ledger WHERE run_id = ANY($1::uuid[])`, pq.Array(result.RunIDs)).Scan(&retainedSpendRuns); err != nil {
		t.Fatalf("count retained run-scoped spend evidence: %v", err)
	}
	if retainedSpendRuns != 2 {
		t.Fatalf("retained run-scoped spend evidence = %d, want 2", retainedSpendRuns)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_DryRunCountsWithoutMutation(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })
	ctx := testAuthorActivityContext()
	seed := seedDestructiveResetCleanupRows(t, ctx, pg)

	now := time.Date(2026, 5, 16, 18, 35, 0, 0, time.UTC)
	result, err := pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			DryRun:        true,
			PlannedAt:     now.Add(-time.Minute),
			Plan:          cleanupPlanForRunIDs(seed.RunA, seed.RunB),
		},
	})
	if err != nil {
		t.Fatalf("ApplyDestructiveResetCleanup dry-run: %v", err)
	}
	if !result.DryRun {
		t.Fatal("cleanup result DryRun = false, want true")
	}
	assertCleanupTableResult(t, result, "runs", 2, 0)
	assertCleanupTableResult(t, result, "events", 6, 0)
	assertCleanupTableResult(t, result, "fan_out_obligation_barriers", 1, 0)
	assertCleanupTableResult(t, result, "fan_out_outcomes", 1, 0)
	assertCleanupTableResult(t, result, "fan_out_intents", 1, 0)
	assertCleanupTableResult(t, result, "conversation_forks", 1, 0)
	assertCleanupTableResult(t, result, "human_task_continuations", 1, 0)
	assertCleanupTableResult(t, result, "decision_cards", 1, 0)
	assertCleanupTableResult(t, result, "mailbox", 0, 0)
	if got := countRows(t, ctx, pg, "runs"); got != 2 {
		t.Fatalf("runs after dry-run = %d, want 2", got)
	}
	if got := countRows(t, ctx, pg, "events"); got != 7 {
		t.Fatalf("events after dry-run = %d, want 7 including preserved no-run event", got)
	}
	if got := countRows(t, ctx, pg, "fan_out_obligation_barriers"); got != 1 {
		t.Fatalf("fan_out_obligation_barriers after dry-run = %d, want preserved", got)
	}
	if got := countRows(t, ctx, pg, "fan_out_intents"); got != 1 {
		t.Fatalf("fan_out_intents after dry-run = %d, want preserved", got)
	}
	if got := countRows(t, ctx, pg, "fan_out_outcomes"); got != 1 {
		t.Fatalf("fan_out_outcomes after dry-run = %d, want preserved", got)
	}
	if got := countRows(t, ctx, pg, "mailbox"); got != 1 {
		t.Fatalf("mailbox after dry-run = %d, want preserved", got)
	}
	if got := countRows(t, ctx, pg, "human_task_continuations"); got != 1 {
		t.Fatalf("human_task_continuations after dry-run = %d, want 1", got)
	}
	if got := countRows(t, ctx, pg, "decision_cards"); got != 1 {
		t.Fatalf("decision_cards after dry-run = %d, want 1", got)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_ProviderAuthorityMatrix(t *testing.T) {
	for _, candidate := range []struct {
		name     string
		drained  bool
		terminal bool
	}{
		{name: "terminal_current", terminal: true},
		{name: "terminal_drain", drained: true, terminal: true},
		{name: "pending_current"},
		{name: "pending_drain", drained: true},
	} {
		t.Run(candidate.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			store := admitTestPostgresStore(t, db)
			fixture := newCompletionSettlementFixture(t, store, db, false)
			ctx := providerDrainContext(t, fixture, "destructive-reset-"+candidate.name)
			handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", candidate.name)
			if candidate.drained {
				transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
				if transition.ProviderDrainCount != 1 {
					t.Fatalf("provider drain transition=%+v, want one drain", transition)
				}
			}
			if candidate.terminal {
				settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "provider-head-current", "provider-head-after-reset")
				if candidate.drained {
					settlement.ProviderHead = nil
				}
				if _, err := handle.SettleCompletion(ctx, settlement); err != nil {
					t.Fatalf("settle provider attempt before reset: %v", err)
				}
				if !candidate.drained {
					deliveryStore, ok := fixture.store.(runtimedelivery.Store)
					if !ok {
						t.Fatalf("completion store %T does not expose delivery settlement", fixture.store)
					}
					if _, err := deliveryStore.SettleSuccess(testAuthorActivityContext(), fixture.origin, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatalf("settle current provider origin before reset: %v", err)
					}
				}
			}

			now := time.Now().UTC().Add(time.Minute)
			request := destructivereset.CleanupRequest{
				ActorTokenID: "operator-token", RequestedAt: now,
				Result: destructivereset.Result{
					OperationName: destructivereset.DefaultOperationName,
					PlannedAt:     now.Add(-time.Minute), Plan: cleanupPlanForRunIDs(fixture.authority.Target.RunID),
				},
				Quiescence: destructivereset.QuiescenceResult{
					OperationName: destructivereset.DefaultOperationName, AppliedAt: now.Add(-30 * time.Second),
				},
			}
			_, err := store.ApplyDestructiveResetCleanup(testAuthorActivityContext(), request)
			if !candidate.terminal {
				if !errors.Is(err, destructivereset.ErrInvalidRequest) || !strings.Contains(err.Error(), "nonterminal provider authority") {
					t.Fatalf("pending provider cleanup error=%v, want provider-authority refusal", err)
				}
				requireExternalAttemptState(t, fixture.db, false, handle.Attempt().AttemptID, runtimeeffects.StateResponseObserved)
				if candidate.drained {
					requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "pending")
				}
				return
			}
			if err != nil {
				t.Fatalf("clean terminal provider evidence: %v", err)
			}
			for table, column := range map[string]string{
				"runs":                            "run_id",
				"agents":                          "run_id",
				"agent_sessions":                  "run_id",
				"flow_instances":                  "run_id",
				"flow_instance_runtime_readiness": "run_id",
			} {
				var count int
				if err := db.QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE `+column+`=$1::uuid`, fixture.authority.Target.RunID).Scan(&count); err != nil || count != 0 {
					t.Fatalf("terminal cleanup %s rows=%d err=%v, want 0", table, count, err)
				}
			}
			var attempts int
			if err := db.QueryRow(`SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE attempt_id=$1::uuid`, handle.Attempt().AttemptID).Scan(&attempts); err != nil || attempts != 1 {
				t.Fatalf("preserved provider attempts=%d err=%v, want 1", attempts, err)
			}
			if candidate.drained {
				requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
			}
			var deliveryAttempts int
			if err := db.QueryRow(`SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1::uuid`, fixture.origin.DeliveryID()).Scan(&deliveryAttempts); err != nil || deliveryAttempts != 0 {
				t.Fatalf("origin delivery attempts after cleanup=%d err=%v, want 0", deliveryAttempts, err)
			}
		})
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_RejectsExecutingDirectiveAuthority(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })
	ctx := testAuthorActivityContext()
	seed := seedDestructiveResetCleanupRows(t, ctx, pg)
	now := time.Date(2026, 7, 10, 18, 0, 0, 0, time.UTC)
	request := destructiveResetDirectiveReservation(t, seed.RunA, "reset-active", "reset-active-hash", now.Add(-2*time.Minute))
	reserved, err := pg.ReserveDirectiveOperation(ctx, request)
	if err != nil {
		t.Fatalf("ReserveDirectiveOperation: %v", err)
	}
	if _, err := admitDirectiveExecutionForTest(ctx, pg, reserved.Operation.OperationID, "reset-active-owner", now.Add(-time.Minute), time.Hour); err != nil {
		t.Fatalf("AdmitDirectiveExecution: %v", err)
	}

	_, err = pg.ApplyDestructiveResetCleanup(ctx, destructiveResetCleanupRequest(seed.RunA, seed.RunB, now))
	if !errors.Is(err, destructivereset.ErrInvalidRequest) || !strings.Contains(err.Error(), "retained agent directive authority") || !strings.Contains(err.Error(), "state=executing") {
		t.Fatalf("ApplyDestructiveResetCleanup error = %v, want executing directive authority refusal", err)
	}
	persisted, ok, err := pg.LoadDirectiveOperation(ctx, reserved.Operation.OperationID)
	if err != nil || !ok || persisted.State != runtimeagentcontrol.DirectiveOperationExecuting {
		t.Fatalf("operation after refused cleanup = %#v ok=%v err=%v", persisted, ok, err)
	}
	replayRequest := destructiveResetDirectiveReservation(t, seed.RunA, "reset-active", "reset-active-hash", now)
	replay, err := pg.ReserveDirectiveOperation(ctx, replayRequest)
	if err != nil || replay.Created || replay.Operation.OperationID != reserved.Operation.OperationID {
		t.Fatalf("same-key reservation after refused cleanup = %#v err=%v", replay, err)
	}
	if got := countRows(t, ctx, pg, "runs"); got != 2 {
		t.Fatalf("runs after refused cleanup = %d, want 2", got)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_RetainsTerminalDirectiveAuthorityUntilExpiry(t *testing.T) {
	for _, terminalState := range []runtimeagentcontrol.DirectiveOperationState{
		runtimeagentcontrol.DirectiveOperationSucceeded,
		runtimeagentcontrol.DirectiveOperationFailed,
	} {
		t.Run(string(terminalState), func(t *testing.T) {
			dsn, _, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			pg, err := NewPostgresStore(dsn)
			if err != nil {
				t.Fatalf("NewPostgresStore: %v", err)
			}
			bootstrapTestPostgresStore(t, pg)
			t.Cleanup(func() { _ = pg.backend.Close() })
			ctx := testAuthorActivityContext()
			seed := seedDestructiveResetCleanupRows(t, ctx, pg)
			now := time.Date(2026, 7, 10, 19, 0, 0, 0, time.UTC)
			request := destructiveResetDirectiveReservation(t, seed.RunA, "reset-terminal", "reset-terminal-hash", now)
			reserved, err := pg.ReserveDirectiveOperation(ctx, request)
			if err != nil {
				t.Fatalf("ReserveDirectiveOperation: %v", err)
			}
			const ownerID = "reset-terminal-owner"
			if _, err := admitDirectiveExecutionForTest(ctx, pg, reserved.Operation.OperationID, ownerID, now.Add(time.Second), time.Minute); err != nil {
				t.Fatalf("AdmitDirectiveExecution: %v", err)
			}
			switch terminalState {
			case runtimeagentcontrol.DirectiveOperationSucceeded:
				if _, err := pg.RecordDirectiveExecuted(ctx, reserved.Operation.OperationID, ownerID, directiveOperationResponseForTest(reserved.Operation), now.Add(2*time.Second)); err != nil {
					t.Fatalf("RecordDirectiveExecuted: %v", err)
				}
				if _, err := pg.FinalizeDirectiveSuccess(ctx, reserved.Operation.OperationID, now.Add(3*time.Second), 24*time.Hour); err != nil {
					t.Fatalf("FinalizeDirectiveSuccess: %v", err)
				}
			case runtimeagentcontrol.DirectiveOperationFailed:
				failure := runtimeagentcontrol.DirectiveBoardStepFailure(errors.New("injected failure"))
				if _, err := pg.FinalizeDirectiveFailure(ctx, reserved.Operation.OperationID, ownerID, failure, now.Add(3*time.Second), 24*time.Hour); err != nil {
					t.Fatalf("FinalizeDirectiveFailure: %v", err)
				}
			}

			_, err = pg.ApplyDestructiveResetCleanup(ctx, destructiveResetCleanupRequest(seed.RunA, seed.RunB, now.Add(time.Hour)))
			if !errors.Is(err, destructivereset.ErrInvalidRequest) || !strings.Contains(err.Error(), "state="+string(terminalState)) {
				t.Fatalf("ApplyDestructiveResetCleanup before expiry error = %v", err)
			}
			replayRequest := destructiveResetDirectiveReservation(t, seed.RunA, "reset-terminal", "reset-terminal-hash", now.Add(time.Hour))
			replay, err := pg.ReserveDirectiveOperation(ctx, replayRequest)
			if err != nil || replay.Created || replay.Operation.OperationID != reserved.Operation.OperationID {
				t.Fatalf("same-key replay before expiry = %#v err=%v", replay, err)
			}
			conflictRequest := destructiveResetDirectiveReservation(t, seed.RunA, "reset-terminal", "changed-hash", now.Add(time.Hour))
			if _, err := pg.ReserveDirectiveOperation(ctx, conflictRequest); !errors.Is(err, runtimeagentcontrol.ErrDirectiveIdempotencyConflict) {
				t.Fatalf("changed-hash replay before expiry error = %v, want conflict", err)
			}

			result, err := pg.ApplyDestructiveResetCleanup(ctx, destructiveResetCleanupRequest(seed.RunA, seed.RunB, now.Add(25*time.Hour)))
			if err != nil {
				t.Fatalf("ApplyDestructiveResetCleanup after expiry: %v", err)
			}
			if len(result.RunIDs) != 2 {
				t.Fatalf("cleanup run IDs = %#v", result.RunIDs)
			}
			if _, ok, err := pg.LoadDirectiveOperation(ctx, reserved.Operation.OperationID); err != nil || ok {
				t.Fatalf("expired operation after cleanup ok=%v err=%v", ok, err)
			}
			if got := countRows(t, ctx, pg, "runs"); got != 0 {
				t.Fatalf("runs after post-expiry cleanup = %d, want 0", got)
			}
		})
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_IncludeBundlesDeletesBundleCatalog(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })
	ctx := testAuthorActivityContext()
	seed := seedDestructiveResetCleanupRows(t, ctx, pg)
	seedDestructiveResetBundleRows(t, ctx, pg)

	now := time.Date(2026, 5, 16, 18, 37, 0, 0, time.UTC)
	result, err := pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName:  destructivereset.DefaultOperationName,
			IncludeBundles: true,
			PlannedAt:      now.Add(-time.Minute),
			Plan:           cleanupPlanForRunIDsIncludingBundles(seed.RunA, seed.RunB),
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-30 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("ApplyDestructiveResetCleanup include_bundles=true: %v", err)
	}
	if !result.IncludeBundles {
		t.Fatalf("cleanup IncludeBundles = false, want true")
	}
	assertCleanupTableResult(t, result, "bundles", 2, 2)
	if got := countRows(t, ctx, pg, "bundles"); got != 0 {
		t.Fatalf("bundles after include_bundles=true cleanup = %d, want 0", got)
	}
	if got := countRows(t, ctx, pg, "runs"); got != 0 {
		t.Fatalf("runs after include_bundles=true cleanup = %d, want existing cleanup still applied", got)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_ExcludeBundlesPreservesBundleCatalog(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })
	ctx := testAuthorActivityContext()
	seed := seedDestructiveResetCleanupRows(t, ctx, pg)
	seedDestructiveResetBundleRows(t, ctx, pg)

	now := time.Date(2026, 5, 16, 18, 38, 0, 0, time.UTC)
	result, err := pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName:  destructivereset.DefaultOperationName,
			IncludeBundles: false,
			PlannedAt:      now.Add(-time.Minute),
			Plan:           cleanupPlanForRunIDs(seed.RunA, seed.RunB),
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-30 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("ApplyDestructiveResetCleanup include_bundles=false: %v", err)
	}
	if result.IncludeBundles {
		t.Fatalf("cleanup IncludeBundles = true, want false")
	}
	assertCleanupTablePreserved(t, result, "bundles", 2)
	if got := countRows(t, ctx, pg, "bundles"); got != 2 {
		t.Fatalf("bundles after include_bundles=false cleanup = %d, want preserved", got)
	}
	if got := countRows(t, ctx, pg, "runs"); got != 0 {
		t.Fatalf("runs after include_bundles=false cleanup = %d, want existing cleanup still applied", got)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_DryRunIncludeBundlesCountsWithoutMutation(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })
	ctx := testAuthorActivityContext()
	seed := seedDestructiveResetCleanupRows(t, ctx, pg)
	seedDestructiveResetBundleRows(t, ctx, pg)

	now := time.Date(2026, 5, 16, 18, 39, 0, 0, time.UTC)
	result, err := pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName:  destructivereset.DefaultOperationName,
			DryRun:         true,
			IncludeBundles: true,
			PlannedAt:      now.Add(-time.Minute),
			Plan:           cleanupPlanForRunIDsIncludingBundles(seed.RunA, seed.RunB),
		},
	})
	if err != nil {
		t.Fatalf("ApplyDestructiveResetCleanup dry-run include_bundles=true: %v", err)
	}
	assertCleanupTableResult(t, result, "bundles", 2, 0)
	if got := countRows(t, ctx, pg, "bundles"); got != 2 {
		t.Fatalf("bundles after dry-run include_bundles=true = %d, want preserved", got)
	}
	if got := countRows(t, ctx, pg, "runs"); got != 2 {
		t.Fatalf("runs after dry-run include_bundles=true = %d, want preserved", got)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_IncludeBundlesRejectsOutOfPlanPersistedRun(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })
	ctx := testAuthorActivityContext()
	seedDestructiveResetBundleRows(t, ctx, pg)
	outOfPlanRun := uuid.NewString()
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(),
		RunID: outOfPlanRun, State: storerunlifecycle.StateCompleted,
		BundleHash:   destructiveResetCleanupBundleHashA,
		BundleSource: storerunlifecycle.BundleSourcePersisted,
	})

	now := time.Date(2026, 5, 16, 18, 40, 0, 0, time.UTC)
	_, err = pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName:  destructivereset.DefaultOperationName,
			IncludeBundles: true,
			PlannedAt:      now.Add(-time.Minute),
			Plan:           cleanupPlanForRunIDsIncludingBundles(),
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-30 * time.Second),
		},
	})
	if !errors.Is(err, destructivereset.ErrInvalidRequest) {
		t.Fatalf("include_bundles=true out-of-plan error = %v, want ErrInvalidRequest", err)
	}
	if got := countRows(t, ctx, pg, "bundles"); got != 2 {
		t.Fatalf("bundles after rejected include_bundles=true cleanup = %d, want rollback/preserved", got)
	}
	if got := countRowsWhere(t, ctx, pg, "runs", `run_id = $1::uuid`, outOfPlanRun); got != 1 {
		t.Fatalf("out-of-plan run rows after rejected cleanup = %d, want preserved", got)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_DoesNotDeleteRunsCreatedAfterPlan(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })
	ctx := testAuthorActivityContext()
	seedDestructiveResetCleanupRows(t, ctx, pg)
	plan, err := (destructivereset.InventoryPlanner{Reader: pg}).BuildPlan(ctx, destructivereset.Request{ActorTokenID: "operator-token", IncludeBundles: false, IncludeBundlesSet: true})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if !plan.CleanupRunSetKnown || len(plan.CleanupRuns) != 2 {
		t.Fatalf("planned cleanup runs = known:%v %#v, want two-run snapshot", plan.CleanupRunSetKnown, plan.CleanupRuns)
	}
	lateRun := uuid.NewString()
	lateEvent := uuid.NewString()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(pg.backend.ConstructionHandle())), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: lateRun})
	lateSemanticEvent := eventtest.PersistedProjectionForProducer(
		lateEvent, events.EventType("late.event"), eventtest.Producer(events.EventProducerExternal, "cleanup-late-ingress"), "",
		[]byte(`{}`), 0, lateRun, "", events.EventEnvelope{Scope: events.EventScopeGlobal}, time.Now().UTC(),
	)
	if err := insertCanonicalEventRecordFixture(ctx, pg, lateSemanticEvent); err != nil {
		t.Fatalf("seed late event: %v", err)
	}

	now := time.Date(2026, 5, 16, 18, 42, 0, 0, time.UTC)
	result, err := pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
			Plan:          plan,
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-30 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("ApplyDestructiveResetCleanup: %v", err)
	}
	if len(result.RunIDs) != 2 {
		t.Fatalf("cleanup run IDs = %#v, want only planned runs", result.RunIDs)
	}
	if got := countRowsWhere(t, ctx, pg, "runs", `run_id = $1::uuid`, lateRun); got != 1 {
		t.Fatalf("late run rows after cleanup = %d, want preserved", got)
	}
	if got := countRowsWhere(t, ctx, pg, "events", `event_id = $1::uuid`, lateEvent); got != 1 {
		t.Fatalf("late event rows after cleanup = %d, want preserved", got)
	}
}

func TestPostgresStore_DestructiveResetPlanCapturesManagedContainersBeforeCleanup(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })
	ctx := testAuthorActivityContext()
	seedDestructiveResetCleanupRows(t, ctx, pg)
	containerRefs := []destructivereset.ContainerRef{{
		Name:          "swarm-agent-agent-a",
		Kind:          "agent",
		Action:        destructivereset.ContainerActionStop,
		ResetEligible: true,
		RunID:         "11111111-1111-1111-1111-111111111111",
		AgentIdentity: mustTestAgentIdentityForRun("11111111-1111-1111-1111-111111111111", "agent-a", "reset/agent-a"),
	}}
	plan, err := (destructivereset.InventoryPlanner{Reader: destructivereset.CompositeInventoryReader{
		Reader:     pg,
		Containers: managedContainerInventoryFunc(func(context.Context) ([]destructivereset.ContainerRef, error) { return containerRefs, nil }),
	}}).BuildPlan(ctx, destructivereset.Request{ActorTokenID: "operator-token", IncludeBundles: false, IncludeBundlesSet: true})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(plan.EntityContainers) != 1 || plan.EntityContainers[0].Name != "swarm-agent-agent-a" {
		t.Fatalf("planned entity containers = %#v, want managed container snapshot", plan.EntityContainers)
	}
	now := time.Date(2026, 5, 16, 18, 45, 0, 0, time.UTC)
	if _, err := pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
			Plan:          plan,
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-30 * time.Second),
		},
	}); err != nil {
		t.Fatalf("ApplyDestructiveResetCleanup: %v", err)
	}
	if len(plan.EntityContainers) != 1 || plan.EntityContainers[0].Name != "swarm-agent-agent-a" {
		t.Fatalf("plan entity containers after cleanup = %#v, want immutable pre-cleanup snapshot", plan.EntityContainers)
	}
	if got := countRows(t, ctx, pg, "entity_state"); got != 0 {
		t.Fatalf("entity_state after cleanup = %d, want deleted while plan still carries container refs", got)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_SeversPreservedReferencesWhenDependentForkIncluded(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	lateRunID := deterministicRunForkMaterializationID(runID, eventID)
	preservedRunID := uuid.NewString()
	lateMutationID := uuid.NewString()
	activeSessionID := uuid.NewString()
	predecessorSessionID := uuid.NewString()
	var cleanupTimerID, preservedTimerID string
	replyContextID := "reply-v1:cleanup-" + uuid.NewString()
	mailboxID := uuid.NewString()
	entityID := uuid.NewString()
	agentIdentity := mustTestAgentIdentityForRun(runID, "agent-a", "cleanup")
	agentFields, err := agentIdentityFields(agentIdentity)
	if err != nil {
		t.Fatalf("agent identity fields: %v", err)
	}
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(pg.backend.ConstructionHandle())), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(),
		RunID: preservedRunID, State: storerunlifecycle.StateCompleted,
	})
	registerTestAuthorActivityCatalog(t, pg)
	seedTestAgentRow(t, ctx, pg.backend.ConstructionHandle(), true, agentIdentity, "active")
	preservedAgentIdentity := mustTestAgentIdentityForRun(preservedRunID, "agent-a", "cleanup")
	preservedAgentFields := testAgentIdentityStorageFields(t, preservedAgentIdentity)
	seedTestAgentRow(t, ctx, pg.backend.ConstructionHandle(), true, preservedAgentIdentity, "terminated")
	cleanupSemanticEvent := eventtest.PersistedProjectionForProducer(
		eventID, events.EventType("batch.contract"), eventtest.Producer(events.EventProducerExternal, "cleanup-reference-ingress"), "",
		[]byte(`{}`), 0, runID, "", events.EventEnvelope{Scope: events.EventScopeGlobal}, time.Now().UTC(),
	)
	if err := commitSemanticEventFixture(ctx, pg, cleanupSemanticEvent); err != nil {
		t.Fatalf("seed cleanup event: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source, status
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored', 'active')
	`, activeSessionID, runID, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource,
		agentFields.RoutePresence, agentFields.FlowScopeKey, agentFields.FlowInstanceID, agentFields.FlowInstancePath); err != nil {
		t.Fatalf("seed active session: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source, status, termination_reason, terminated_at, successor_session_id
		) VALUES (
			$1::uuid, $3::uuid, $4, $5, $6, $7, $8, $9, $10,
			TRUE, 'authored', 'terminated', 'cancelled', now(), $2::uuid
		)
	`, predecessorSessionID, activeSessionID, preservedRunID, preservedAgentFields.AgentID, preservedAgentFields.NameOwner, preservedAgentFields.NameSource,
		preservedAgentFields.RoutePresence, preservedAgentFields.FlowScopeKey, preservedAgentFields.FlowInstanceID, preservedAgentFields.FlowInstancePath); err != nil {
		t.Fatalf("seed preserved predecessor session: %v", err)
	}
	captureRunForkTestRevision(t, pg.backend.ConstructionHandle(), runID)
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
		SourceRunID: runID,
		At:          eventID,
	})
	if err != nil {
		t.Fatalf("materialize dependent fork: %v", err)
	}
	if materialized.ForkRunID != lateRunID {
		t.Fatalf("dependent fork run_id = %s, want %s", materialized.ForkRunID, lateRunID)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO entity_mutations (mutation_id, run_id, entity_id, domain, path, caused_by_event, writer_type, writer_id)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'authored_field', 'status', $4::uuid, 'platform', 'test')
	`, lateMutationID, lateRunID, entityID, eventID); err != nil {
		t.Fatalf("seed preserved late mutation: %v", err)
	}
	cleanupTimer := admitGenericScheduleFixture(t, ctx, pg, testRootGenericScheduleCommand(
		t, runID, entityID, "cleanup-timer", runtimegenericschedule.DelayDue(time.Hour),
	))
	cleanupTimerID = cleanupTimer.ID
	preservedTimer := admitGenericScheduleFixture(t, ctx, pg, testGlobalGenericScheduleCommand(
		"preserved-source-timer", runtimegenericschedule.EveryDue(time.Hour),
	))
	preservedTimerID = preservedTimer.ID
	if _, err := pg.backend.ExecContext(ctx, `UPDATE timers SET source_timer_id = $1::uuid WHERE timer_id = $2::uuid`, cleanupTimerID, preservedTimerID); err != nil {
		t.Fatalf("link preserved source timer: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO reply_contexts (
			reply_context_id, run_id, request_event_id, requester_flow_id, request_output_pin,
			reply_input_pin, provider_flow_id, provider_input_pin, provider_output_pin,
			origin_route, request_correlation_id, state
		)
		VALUES (
			$1, $2::uuid, $3::uuid, 'requester', 'provider_requested', 'provider_replied',
			'provider', 'provider_requested', 'provider_replied',
			'{"flow_id":"requester","flow_instance":"requester/a"}'::jsonb, $3::text, 'open'
		)
	`, replyContextID, runID, eventID); err != nil {
		t.Fatalf("seed cleanup reply context: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO mailbox (item_id, item_type, source_event_id, from_agent, summary, reply_context_id)
		VALUES ($1::uuid, 'review_notice', $2::uuid, 'agent-a', 'preserved reply mailbox', $3)
	`, mailboxID, eventID, replyContextID); err != nil {
		t.Fatalf("seed preserved reply mailbox: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO runtime_ingress_state (status, controlled_by, transition_event_id)
		VALUES ('running', 'test', $1::uuid)
		ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, controlled_by = EXCLUDED.controlled_by, transition_event_id = EXCLUDED.transition_event_id
	`, eventID); err != nil {
		t.Fatalf("seed runtime ingress transition event: %v", err)
	}

	now := time.Date(2026, 5, 16, 18, 45, 0, 0, time.UTC)
	_, err = pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
			Plan:          cleanupPlanForRunIDs(runID, lateRunID),
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-30 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("ApplyDestructiveResetCleanup: %v", err)
	}
	if got := countRowsWhere(t, ctx, pg, "runs", `run_id = $1::uuid`, runID); got != 0 {
		t.Fatalf("cleanup run rows after cleanup = %d, want deleted", got)
	}
	if got := countRowsWhere(t, ctx, pg, "events", `event_id = $1::uuid`, eventID); got != 0 {
		t.Fatalf("cleanup event rows after cleanup = %d, want deleted", got)
	}
	if got := countRowsWhere(t, ctx, pg, "agent_sessions", `session_id = $1::uuid`, activeSessionID); got != 0 {
		t.Fatalf("cleanup session rows after cleanup = %d, want deleted", got)
	}
	if got := countRowsWhere(t, ctx, pg, "timers", `timer_id = $1::uuid`, cleanupTimerID); got != 0 {
		t.Fatalf("cleanup timer rows after cleanup = %d, want deleted", got)
	}
	var predecessorSuccessor sql.NullString
	if err := pg.backend.QueryRowContext(ctx, `
		SELECT successor_session_id::text
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, predecessorSessionID).Scan(&predecessorSuccessor); err != nil {
		t.Fatalf("read preserved predecessor successor: %v", err)
	}
	if predecessorSuccessor.Valid {
		t.Fatalf("preserved predecessor successor_session_id = %q, want NULL", predecessorSuccessor.String)
	}
	if got := countRowsWhere(t, ctx, pg, "runs", `run_id = $1::uuid`, lateRunID); got != 0 {
		t.Fatalf("dependent fork rows after cleanup = %d, want deleted with source", got)
	}
	var transitionEvent sql.NullString
	if err := pg.backend.QueryRowContext(ctx, `SELECT transition_event_id::text FROM runtime_ingress_state WHERE id = 1`).Scan(&transitionEvent); err != nil {
		t.Fatalf("read runtime ingress transition event: %v", err)
	}
	if transitionEvent.Valid {
		t.Fatalf("runtime ingress transition_event_id = %q, want NULL", transitionEvent.String)
	}
	if got := countRowsWhere(t, ctx, pg, "entity_mutations", `mutation_id = $1::uuid`, lateMutationID); got != 0 {
		t.Fatalf("dependent fork mutation rows after cleanup = %d, want deleted with fork", got)
	}
	var sourceTimer sql.NullString
	if err := pg.backend.QueryRowContext(ctx, `
		SELECT source_timer_id::text
		FROM timers
		WHERE timer_id = $1::uuid
	`, preservedTimerID).Scan(&sourceTimer); err != nil {
		t.Fatalf("read preserved timer source: %v", err)
	}
	if sourceTimer.Valid {
		t.Fatalf("preserved timer source_timer_id = %q, want NULL", sourceTimer.String)
	}
	if got := countRowsWhere(t, ctx, pg, "reply_contexts", `reply_context_id = $1`, replyContextID); got != 0 {
		t.Fatalf("cleanup reply context rows = %d, want deleted", got)
	}
	var mailboxReplyContext sql.NullString
	if err := pg.backend.QueryRowContext(ctx, `SELECT reply_context_id FROM mailbox WHERE item_id = $1::uuid`, mailboxID).Scan(&mailboxReplyContext); err != nil {
		t.Fatalf("read preserved mailbox reply context: %v", err)
	}
	if mailboxReplyContext.Valid {
		t.Fatalf("preserved mailbox reply_context_id = %q, want NULL", mailboxReplyContext.String)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_DeletesForkLineageRowsByLinkedEventsAndDeliveries(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })
	ctx := testAuthorActivityContext()
	cleanupRunID := uuid.NewString()
	preservedSourceRunID := uuid.NewString()
	preservedForkRunID := uuid.NewString()
	cleanupEventID := uuid.NewString()
	preservedSourceEventID := uuid.NewString()
	entityID := uuid.NewString()
	for _, runID := range []string{cleanupRunID, preservedSourceRunID, preservedForkRunID} {
		requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	}
	seededAt := time.Date(2026, 5, 16, 19, 0, 0, 0, time.UTC)
	entityEnvelope := events.EventEnvelope{EntityID: entityID, FlowInstance: "flow/a", Scope: events.EventScopeEntity}
	preservedSourceEvent := eventtest.PersistedProjectionForProducer(
		preservedSourceEventID, events.EventType("source.event"), eventtest.Producer(events.EventProducerExternal, "cleanup-lineage-source"), "",
		[]byte(`{}`), 0, preservedSourceRunID, "", entityEnvelope, seededAt,
	)
	if err := insertCanonicalEventRecordFixture(ctx, pg, preservedSourceEvent); err != nil {
		t.Fatalf("seed preserved source event: %v", err)
	}
	sourceRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-a")}
	cleanupDelivery := seedAgentDeliveryStateFixture(t, ctx, pg, preservedSourceEvent, sourceRoute, "queued", nil)
	cleanupDeliveryID := cleanupDelivery.DeliveryID
	if err := commitDeliveryReplayEventFixture(
		ctx, pg, preservedSourceEvent, cleanupRunID, cleanupDeliveryID, "", "agent", "agent-a", seededAt.Add(time.Second),
	); err != nil {
		t.Fatalf("seed delivery replay: %v", err)
	}
	selectedLineage, err := events.NewSelectedForkLineage(
		cleanupRunID, preservedSourceRunID, preservedSourceEventID,
		runfork.RunForkSelectedContractExecutionLineageOwner, "", executionmode.Live,
	)
	if err != nil {
		t.Fatalf("construct selected-fork lineage: %v", err)
	}
	selectedEvent := eventtest.SelectedForkReplay(
		cleanupEventID, events.EventType("source.event"), eventtest.Producer(events.EventProducerPlatform, "cleanup-selected-contract"), "",
		[]byte(`{}`), 0, selectedLineage, entityEnvelope, seededAt.Add(2*time.Second),
	)
	if err := commitSelectedForkEventFixture(ctx, pg, selectedEvent, runfork.RunForkSelectedContractExecutionLineage{
		ForkRunID: cleanupRunID, SourceRunID: preservedSourceRunID, SourceEventID: preservedSourceEventID,
		ForkEventID: cleanupEventID, EventName: "source.event",
		SelectionAuthority: runfork.RunForkSelectedContractExecutionLineageOwner, CreatedAt: seededAt.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("seed selected-contract event: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_bindings (fork_run_id, source_run_id, fork_event_id, mode, contracts_root, workflow_name, workflow_version)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'selected_contracts', '/contracts', 'wf', 'v1')
	`, cleanupRunID, preservedSourceRunID, cleanupEventID); err != nil {
		t.Fatalf("seed selected binding: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_branch_divergences (fork_run_id, source_run_id, fork_event_id, owner, policy, source_run_status_at_activation, source_run_status_after_activation)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'test', 'selected_contract_source_advanced_branch', 'running', 'completed')
	`, cleanupRunID, preservedSourceRunID, cleanupEventID); err != nil {
		t.Fatalf("seed selected branch divergence: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_route_recoveries (
			fork_run_id, source_run_id, fork_event_id, owner, runtime_recovery_owner, mode, contracts_root, workflow_name, workflow_version,
			route_topology_owner, recipient_planning_owner, frontier_evidence_fingerprint, route_topology_fingerprint,
			recipient_planning_fingerprint, route_topology, recipient_planning
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'test', 'test', 'selected_contracts', '/contracts', 'wf', 'v1',
			'topology', 'recipients', 'frontier', 'route', 'recipient', '{}'::jsonb, '{}'::jsonb
		)
	`, cleanupRunID, preservedSourceRunID, cleanupEventID); err != nil {
		t.Fatalf("seed selected route recovery: %v", err)
	}

	now := time.Date(2026, 5, 16, 19, 5, 0, 0, time.UTC)
	result, err := pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
			Plan:          cleanupPlanForRunIDs(cleanupRunID),
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-30 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("ApplyDestructiveResetCleanup: %v", err)
	}
	assertCleanupTableResult(t, result, "run_fork_delivery_event_replays", 1, 1)
	assertCleanupTableResult(t, result, "run_fork_selected_contract_bindings", 1, 1)
	assertCleanupTableResult(t, result, "run_fork_selected_contract_executions", 1, 1)
	assertCleanupTableResult(t, result, "run_fork_selected_contract_branch_divergences", 1, 1)
	assertCleanupTableResult(t, result, "run_fork_selected_contract_route_recoveries", 1, 1)
	assertCleanupTableResult(t, result, "event_deliveries", 1, 1)
	assertCleanupTableResult(t, result, "events", 2, 2)
	for _, table := range []string{
		"run_fork_delivery_event_replays",
		"run_fork_selected_contract_bindings",
		"run_fork_selected_contract_executions",
		"run_fork_selected_contract_branch_divergences",
		"run_fork_selected_contract_route_recoveries",
	} {
		if got := countRows(t, ctx, pg, table); got != 0 {
			t.Fatalf("%s rows after cleanup = %d, want 0", table, got)
		}
	}
	if got := countRowsWhere(t, ctx, pg, "runs", `run_id IN ($1::uuid, $2::uuid)`, preservedSourceRunID, preservedForkRunID); got != 2 {
		t.Fatalf("preserved run rows after cleanup = %d, want 2", got)
	}
	if got := countRowsWhere(t, ctx, pg, "events", `event_id = $1::uuid`, preservedSourceEventID); got != 1 {
		t.Fatalf("preserved source event rows after cleanup = %d, want 1", got)
	}
	if got := countRowsWhere(t, ctx, pg, "event_deliveries", `delivery_id = $1::uuid`, cleanupDeliveryID); got != 1 {
		t.Fatalf("preserved source delivery rows after cleanup = %d, want 1", got)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_RollsBackOnUnknownForeignKeyReference(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(pg.backend.ConstructionHandle())), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	if _, err := pg.backend.ExecContext(ctx, `
		CREATE TABLE cleanup_unknown_fk_probe (
			probe_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			run_id UUID NOT NULL REFERENCES runs(run_id)
		)
	`); err != nil {
		t.Fatalf("create unknown FK probe: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `INSERT INTO cleanup_unknown_fk_probe (run_id) VALUES ($1::uuid)`, runID); err != nil {
		t.Fatalf("seed unknown FK probe: %v", err)
	}

	now := time.Date(2026, 5, 16, 18, 46, 0, 0, time.UTC)
	_, err = pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
			Plan:          cleanupPlanForRunIDs(runID),
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-30 * time.Second),
		},
	})
	if err == nil {
		t.Fatal("ApplyDestructiveResetCleanup error = nil, want unknown FK rollback failure")
	}
	if got := countRowsWhere(t, ctx, pg, "runs", `run_id = $1::uuid`, runID); got != 1 {
		t.Fatalf("runs after rollback failure = %d, want 1", got)
	}
	if got := countRows(t, ctx, pg, "cleanup_unknown_fk_probe"); got != 1 {
		t.Fatalf("unknown FK probe rows after rollback failure = %d, want 1", got)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_RequiresAppliedQuiescence(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })

	now := time.Date(2026, 5, 16, 18, 50, 0, 0, time.UTC)
	_, err = pg.ApplyDestructiveResetCleanup(testAuthorActivityContext(), destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
			Plan:          cleanupPlanForRunIDs(uuid.NewString()),
		},
		Quiescence: destructivereset.QuiescenceResult{DryRun: true, AppliedAt: now.Add(-30 * time.Second)},
	})
	if err == nil {
		t.Fatal("ApplyDestructiveResetCleanup error = nil, want applied quiescence failure")
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_RejectsStaleQuiescenceEnvelope(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })

	now := time.Date(2026, 5, 16, 18, 55, 0, 0, time.UTC)
	for name, quiescence := range map[string]destructivereset.QuiescenceResult{
		"operation mismatch": {
			OperationName: "runtime.other_reset",
			AppliedAt:     now.Add(-30 * time.Second),
		},
		"predates plan": {
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-2 * time.Minute),
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := pg.ApplyDestructiveResetCleanup(testAuthorActivityContext(), destructivereset.CleanupRequest{
				ActorTokenID: "operator-token",
				RequestedAt:  now,
				Result: destructivereset.Result{
					OperationName: destructivereset.DefaultOperationName,
					PlannedAt:     now.Add(-time.Minute),
					Plan:          cleanupPlanForRunIDs(uuid.NewString()),
				},
				Quiescence: quiescence,
			})
			if err == nil {
				t.Fatal("ApplyDestructiveResetCleanup error = nil, want stale envelope failure")
			}
		})
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_RequiresPlannedCleanupRunSet(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })

	now := time.Date(2026, 5, 16, 19, 0, 0, 0, time.UTC)
	_, err = pg.ApplyDestructiveResetCleanup(testAuthorActivityContext(), destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			DryRun:        true,
			PlannedAt:     now.Add(-time.Minute),
		},
	})
	if err == nil {
		t.Fatal("ApplyDestructiveResetCleanup error = nil, want missing cleanup run set failure")
	}
}

func TestValidateDestructiveResetCleanupRequestRejectsIncludeBundlesPlanMismatch(t *testing.T) {
	now := time.Date(2026, 5, 16, 19, 2, 0, 0, time.UTC)
	_, err := validateDestructiveResetCleanupRequest(destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		Result: destructivereset.Result{
			DryRun:         true,
			IncludeBundles: true,
			PlannedAt:      now.Add(-time.Minute),
			Plan:           cleanupPlanForRunIDs(uuid.NewString()),
		},
	}, now)
	if !errors.Is(err, destructivereset.ErrInvalidRequest) {
		t.Fatalf("include_bundles plan mismatch error = %v, want ErrInvalidRequest", err)
	}
}

type destructiveResetCleanupSeed struct {
	RunA string
	RunB string
}

const (
	destructiveResetCleanupBundleHashA = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	destructiveResetCleanupBundleHashB = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type managedContainerInventoryFunc func(context.Context) ([]destructivereset.ContainerRef, error)

func (f managedContainerInventoryFunc) ManagedResetContainerInventory(ctx context.Context) ([]destructivereset.ContainerRef, error) {
	return f(ctx)
}

func cleanupPlanForRunIDs(runIDs ...string) destructivereset.Plan {
	runs := make([]destructivereset.RunRef, 0, len(runIDs))
	for _, runID := range runIDs {
		runs = append(runs, destructivereset.RunRef{RunID: runID})
	}
	return destructivereset.Plan{CleanupRuns: runs, CleanupRunSetKnown: true}
}

func cleanupPlanForRunIDsIncludingBundles(runIDs ...string) destructivereset.Plan {
	plan := cleanupPlanForRunIDs(runIDs...)
	plan.IncludeBundles = true
	return plan
}

func destructiveResetCleanupRequest(runA, runB string, requestedAt time.Time) destructivereset.CleanupRequest {
	return destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  requestedAt,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     requestedAt.Add(-time.Minute),
			Plan:          cleanupPlanForRunIDs(runA, runB),
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     requestedAt.Add(-30 * time.Second),
		},
	}
}

func destructiveResetDirectiveReservation(t *testing.T, runID, key, requestHash string, now time.Time) runtimeagentcontrol.ReserveDirectiveOperationRequest {
	t.Helper()
	operationID := uuid.NewString()
	eventID := uuid.NewString()
	request := runtimeagentcontrol.SendDirectiveRequest{
		AgentID:      "agent-a",
		FlowInstance: "cleanup",
		Directive:    "continue",
		RunID:        runID,
		Source:       runtimeagentcontrol.DirectiveSourceV1RPC,
		OperatorID:   "operator-token",
		ActorTokenID: "operator-token",
	}
	event, err := runtimeagentcontrol.NewDirectiveEvent(request, runtimeagentcontrol.RunTargetResolution{
		RunID: runID,
		Mode:  runtimeagentcontrol.RunResolutionSpecified,
	}, operationID, eventID, now, executionposture.Live)
	if err != nil {
		t.Fatalf("NewDirectiveEvent: %v", err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("AdmitForPersistence: %v", err)
	}
	return runtimeagentcontrol.ReserveDirectiveOperationRequest{
		Operation: runtimeagentcontrol.DirectiveOperation{
			OperationID:      operationID,
			Method:           runtimeagentcontrol.DirectiveOperationMethod,
			ActorTokenID:     request.ActorTokenID,
			IdempotencyKey:   key,
			RequestHash:      requestHash,
			AgentIdentity:    mustTestAgentIdentityForRun(runID, request.AgentID, request.FlowInstance),
			Directive:        request.Directive,
			RequestedRunID:   runID,
			ResolvedRunID:    runID,
			RunIDResolution:  runtimeagentcontrol.RunResolutionSpecified,
			Source:           request.Source,
			OperatorID:       request.OperatorID,
			DirectiveEventID: eventID,
			State:            runtimeagentcontrol.DirectiveOperationPrepared,
		},
		Event: admitted,
		Now:   now,
	}
}

func seedDestructiveResetCleanupRows(t *testing.T, ctx context.Context, pg *PostgresStore) destructiveResetCleanupSeed {
	t.Helper()
	runA := uuid.NewString()
	runB := uuid.NewString()
	sourceEvent := uuid.NewString()
	forkEvent := uuid.NewString()
	noRunEvent := uuid.NewString()
	sessionID := uuid.NewString()
	entityID := uuid.NewString()
	timerForkEvent := uuid.NewString()
	humanTaskCardID := uuid.NewString()
	seededAt := time.Date(2026, 5, 16, 18, 0, 0, 0, time.UTC)
	agentIdentity := mustTestAgentIdentityForRun(runA, "agent-a", "cleanup")
	agentFields, err := agentIdentityFields(agentIdentity)
	if err != nil {
		t.Fatalf("agent identity fields: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `CREATE TABLE generated_entity_fixture (entity_id UUID PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`); err != nil {
		t.Fatalf("create generated entity fixture: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `CREATE TABLE generated_node_state_fixture (entity_id UUID NOT NULL, node_id TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY(entity_id, node_id))`); err != nil {
		t.Fatalf("create generated node fixture: %v", err)
	}
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{
		RunID: runA, Origin: semanticEventRunOriginForTest(t, sourceEvent, "source.event"),
		StartedAt: seededAt.Add(-time.Hour),
	})
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{
		RunID: runB, Origin: semanticEventRunOriginForTest(t, forkEvent, "source.event"),
		StartedAt: seededAt.Add(-time.Hour),
	})
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO decision_cards (
			card_id, run_id, anchor_kind, anchor, status, snapshot, card_content_hash,
			decision_schema_hash, bundle_hash, effective_cadence, provenance, fields,
			execution_mode, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'human_task', '{}'::jsonb, 'pending', '{}'::jsonb, 'content-hash',
			'schema-hash', 'bundle-hash', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 'live', now(), now()
		)
	`, humanTaskCardID, runA); err != nil {
		t.Fatalf("seed human-task decision card: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO human_task_continuations (
			card_id, run_id, deadline_at, budget_bundle_hash, budget_limit,
			budget_window_start, budget_window_end, state, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, now() + interval '1 hour', 'bundle-hash', 5,
			now(), now() + interval '7 days', 'pending', now(), now()
		)
	`, humanTaskCardID, runA); err != nil {
		t.Fatalf("seed human-task continuation: %v", err)
	}
	entityEnvelope := events.EventEnvelope{EntityID: entityID, FlowInstance: "flow/a", Scope: events.EventScopeEntity}
	sourceSemanticEvent := eventtest.PersistedProjectionForProducer(
		sourceEvent, events.EventType("source.event"), eventtest.Producer(events.EventProducerExternal, "cleanup-ingress"), "",
		[]byte(`{}`), 0, runA, "", entityEnvelope, seededAt,
	)
	if err := insertCanonicalEventRecordFixture(ctx, pg, sourceSemanticEvent); err != nil {
		t.Fatalf("seed source event: %v", err)
	}
	for _, fixture := range []events.Event{
		eventtest.PersistedChildForProducer(
			timerForkEvent, events.EventType("timer.event"), eventtest.Producer(events.EventProducerNode, "cleanup-timer"), "",
			[]byte(`{}`), 0, runA, sourceEvent, entityEnvelope, seededAt.Add(time.Second),
		),
		eventtest.PersistedChildForProducer(
			uuid.NewString(), events.EventType("extra.event"), eventtest.Producer(events.EventProducerAgent, "cleanup-agent"), "",
			[]byte(`{}`), 0, runA, sourceEvent, entityEnvelope, seededAt.Add(2*time.Second),
		),
	} {
		if err := insertCanonicalEventRecordFixture(ctx, pg, fixture); err != nil {
			t.Fatalf("seed run-scoped event %s: %v", fixture.ID(), err)
		}
	}
	noRunSemanticEvent := eventtest.DiagnosticDirect(
		noRunEvent, events.EventTypePlatformRuntimeLog, "runtime", "", []byte(`{}`), 0,
		"", "", events.EventEnvelope{}, seededAt.Add(3*time.Second),
	)
	if err := commitDiagnosticRuntimeLogFixture(ctx, pg, noRunSemanticEvent); err != nil {
		t.Fatalf("seed preserved runtime-log event: %v", err)
	}
	sourceRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-a")}
	sourceFailure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "cleanup_source_exhausted", nil)
	sourceDeliverySnapshot := seedAgentDeliveryStateFixture(t, ctx, pg, sourceSemanticEvent, sourceRoute, "exhausted", &sourceFailure)
	var fanOutBundleHash string
	if err := pg.backend.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1::uuid`, runA).Scan(&fanOutBundleHash); err != nil {
		t.Fatalf("load destructive-reset fan-out bundle: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO fan_out_intents (
			run_id, triggering_delivery_id, package_key, element_id,
			bundle_hash, semantic_digest, source_kind, source_event_id, source_field,
			cardinality, cursor, status, next_chunk_size, capsule, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, '.', $3, $4, $5, 'event_payload_field', $6::uuid, 'items',
			1, 1, 'closed', 4, '{}'::jsonb, $7, $7
		)
	`, runA, sourceDeliverySnapshot.DeliveryID, uuid.NewString(), fanOutBundleHash, "sha256:"+strings.Repeat("2", 64), sourceEvent, seededAt); err != nil {
		t.Fatalf("seed destructive-reset fan-out intent: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO fan_out_outcomes (
			run_id, triggering_delivery_id, package_key, element_id,
			ordinal, outcome_kind, failure, created_at
		)
		SELECT run_id, triggering_delivery_id, package_key, element_id,
			0, 'semantic_rejected', '{}'::jsonb, $2
		FROM fan_out_intents WHERE run_id=$1::uuid
	`, runA, seededAt); err != nil {
		t.Fatalf("seed destructive-reset fan-out outcome: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO fan_out_obligation_barriers (
			run_id, triggering_delivery_id, package_key, element_id,
			bundle_hash, semantic_digest, target_package_key, target_flow_id,
			target_node_id, handler_event, join_id, route_scope_key,
			route_instance_id, route_instance_path, entity_id, routing_source,
			execution_mode, timer_handle, status, summary, schedule_key,
			created_at, updated_at
		)
		SELECT run_id, triggering_delivery_id, package_key, element_id,
			bundle_hash, semantic_digest, '.', '',
			'cleanup-node', 'cleanup.event', 'cleanup-join', 'cleanup-scope',
			'cleanup-instance', 'cleanup-path', $2::uuid, '{}'::jsonb,
			'live', '{}'::jsonb, 'armed', NULL, NULL,
			$3, $3
		FROM fan_out_intents WHERE run_id=$1::uuid
	`, runA, entityID, seededAt); err != nil {
		t.Fatalf("seed destructive-reset fan-out barrier: %v", err)
	}
	if err := commitDeliveryReplayEventFixture(
		ctx, pg, sourceSemanticEvent, runB, sourceDeliverySnapshot.DeliveryID, "", "agent", "agent-a", seededAt.Add(4*time.Second),
	); err != nil {
		t.Fatalf("seed delivery replay event: %v", err)
	}
	selectedLineage, err := events.NewSelectedForkLineage(
		runB, runA, sourceEvent, runfork.RunForkSelectedContractExecutionLineageOwner, "", executionmode.Live,
	)
	if err != nil {
		t.Fatalf("construct selected-fork lineage: %v", err)
	}
	selectedSemanticEvent := eventtest.SelectedForkReplay(
		forkEvent, events.EventType("source.event"), eventtest.Producer(events.EventProducerPlatform, "selected-contract-runtime"), "",
		[]byte(`{}`), 0, selectedLineage, entityEnvelope, seededAt.Add(5*time.Second),
	)
	if err := commitSelectedForkEventFixture(ctx, pg, selectedSemanticEvent, runfork.RunForkSelectedContractExecutionLineage{
		ForkRunID: runB, SourceRunID: runA, SourceEventID: sourceEvent, ForkEventID: forkEvent,
		EventName: "source.event", SelectionAuthority: runfork.RunForkSelectedContractExecutionLineageOwner,
		CreatedAt: seededAt.Add(5 * time.Second),
	}); err != nil {
		t.Fatalf("seed selected-contract fork event: %v", err)
	}
	if _, err := markRunTerminalStatusForTest(
		ctx,
		pg,
		runB,
		string(storerunlifecycle.StateCancelled),
		nil,
		seededAt.Add(6*time.Second),
	); err != nil {
		t.Fatalf("terminalize seeded fork run: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, side_effects) VALUES
			($1::uuid, 'platform', 'pipeline', 'success', '{}'::jsonb),
			($2::uuid, 'platform', 'preserved', 'success', '{}'::jsonb)
	`, timerForkEvent, noRunEvent); err != nil {
		t.Fatalf("seed receipts: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO dead_letters (original_event_id, original_event, original_payload, flow_instance, failure) VALUES
			(NULL, 'global.failure', '{}'::jsonb, 'flow/a', $1::jsonb)
	`, mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "handler_failed", nil))); err != nil {
		t.Fatalf("seed dead letters: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_bindings (fork_run_id, source_run_id, fork_event_id, mode, contracts_root, workflow_name, workflow_version)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'selected_contracts', '/contracts', 'wf', 'v1')
	`, runB, runA, forkEvent); err != nil {
		t.Fatalf("seed selected binding: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_branch_divergences (fork_run_id, source_run_id, fork_event_id, owner, policy, source_run_status_at_activation, source_run_status_after_activation)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'test', 'selected_contract_source_advanced_branch', 'running', 'completed')
	`, runB, runA, forkEvent); err != nil {
		t.Fatalf("seed selected branch divergence: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_route_recoveries (
			fork_run_id, source_run_id, fork_event_id, owner, runtime_recovery_owner, mode, contracts_root, workflow_name, workflow_version,
			route_topology_owner, recipient_planning_owner, frontier_evidence_fingerprint, route_topology_fingerprint,
			recipient_planning_fingerprint, route_topology, recipient_planning
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'test', 'test', 'selected_contracts', '/contracts', 'wf', 'v1',
			'topology', 'recipients', 'frontier', 'route', 'recipient', '{}'::jsonb, '{}'::jsonb
		)
	`, runB, runA, forkEvent); err != nil {
		t.Fatalf("seed selected route recovery: %v", err)
	}
	seedTestAgentRow(t, ctx, pg.backend.ConstructionHandle(), true, agentIdentity, "active")
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source, status
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored', 'active')
	`, sessionID, runA, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource,
		agentFields.RoutePresence, agentFields.FlowScopeKey, agentFields.FlowInstanceID, agentFields.FlowInstancePath); err != nil {
		t.Fatalf("seed agent session: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO conversation_forks (
			fork_id, source_session_id, source_run_id,
			source_agent_id, source_agent_name_owner, source_agent_name_source, source_agent_route_presence,
			source_flow_scope_key, source_flow_instance_id, source_flow_instance,
			fork_point_kind, fork_point_turn_index,
			fork_point_turn_id, fork_point_selected_at, created_by, expires_at
		) VALUES
			($1::uuid, $3::uuid, $5::uuid, $8, $9, $10, $11, $12, $13, $14, 'turn', 0, $6::uuid, now(), 'operator-token', now() + interval '1 hour'),
			($2::uuid, $4::uuid, NULL, $8, $9, $10, $11, $12, $13, $14, 'turn', 0, $7::uuid, now(), 'operator-token', now() + interval '1 hour')
	`, uuid.NewString(), uuid.NewString(), sessionID, uuid.NewString(), runA, uuid.NewString(), uuid.NewString(),
		agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource, agentFields.RoutePresence,
		agentFields.FlowScopeKey, agentFields.FlowInstanceID, agentFields.FlowInstancePath); err != nil {
		t.Fatalf("seed conversation forks: %v", err)
	}
	turnID := uuid.NewString()
	if err := persistManagedAgentTurnReadbackFixtureWithOptions(t, ctx, pg, runtimellm.AgentTurnRecord{
		AgentID: agentIdentity.AgentID(), Identity: agentIdentity,
		RunID: runA, FlowInstance: agentIdentity.FlowInstance(), Memory: agentmemory.Authored(true), SessionID: sessionID,
		EntityID:       entityID,
		RequestPayload: []byte(`{}`), ResponseRaw: []byte(`{}`), ParseOK: true,
	}, managedAgentTurnFixtureOptions{TurnID: turnID, Now: seededAt}); err != nil {
		t.Fatalf("seed agent turn: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source, status
		)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, FALSE, 'authored', 'active')
	`, runA, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource, agentFields.RoutePresence,
		agentFields.FlowScopeKey, agentFields.FlowInstanceID, agentFields.FlowInstancePath); err != nil {
		t.Fatalf("seed agent audit: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state) VALUES ($1::uuid, $2::uuid, 'flow/a', 'reset_fixture', 'active')`, runA, entityID); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `INSERT INTO entity_mutations (run_id, entity_id, domain, path, writer_type, writer_id) VALUES ($1::uuid, $2::uuid, 'authored_field', 'status', 'platform', 'test')`, runA, entityID); err != nil {
		t.Fatalf("seed entity mutation: %v", err)
	}
	admitGenericScheduleFixture(t, ctx, pg, testRootGenericScheduleCommand(
		t, runA, entityID, "destructive-reset-run", runtimegenericschedule.DelayDue(time.Hour),
	))
	forkRunTimer := admitGenericScheduleFixture(t, ctx, pg, testGlobalGenericScheduleCommand(
		"destructive-reset-fork-run", runtimegenericschedule.DelayDue(time.Hour),
	))
	forkEventTimer := admitGenericScheduleFixture(t, ctx, pg, testGlobalGenericScheduleCommand(
		"destructive-reset-fork-event", runtimegenericschedule.DelayDue(time.Hour),
	))
	admitGenericScheduleFixture(t, ctx, pg, testGlobalGenericScheduleCommand(
		"destructive-reset-global", runtimegenericschedule.EveryDue(time.Hour),
	))
	if _, err := pg.backend.ExecContext(ctx, `UPDATE timers SET forked_from_run_id = $1::uuid WHERE timer_id = $2::uuid`, runB, forkRunTimer.ID); err != nil {
		t.Fatalf("seed timer forked_from_run_id: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `UPDATE timers SET forked_from_event_id = $1::uuid WHERE timer_id = $2::uuid`, timerForkEvent, forkEventTimer.ID); err != nil {
		t.Fatalf("seed timer forked_from_event_id: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, controlled_by) VALUES ($1::uuid, 'stopped', 'test')`, runA); err != nil {
		t.Fatalf("seed run control: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO api_idempotency (method, actor_token_id, idempotency_key, request_hash, resource_id, response, expires_at)
		VALUES ('runtime.nuke', 'operator', 'idem', 'hash', 'runtime', '{}'::jsonb, now() + interval '1 hour')
	`); err != nil {
		t.Fatalf("seed api idempotency: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO runtime_ingress_state (status, controlled_by)
		VALUES ('running', 'test')
		ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, controlled_by = EXCLUDED.controlled_by
	`); err != nil {
		t.Fatalf("seed runtime ingress: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `INSERT INTO flow_instances (run_id, instance_path, flow_template, mode) VALUES ($1::uuid, 'flow/a', 'flow', 'static')`, runA); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO routing_rules (event_pattern, subscriber_type, subscriber_id, flow_instance)
		VALUES ('source.event', 'agent', 'agent-a', 'flow/a')
	`); err != nil {
		t.Fatalf("seed routing rule: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO routing_rules (run_id, event_pattern, subscriber_type, subscriber_id, flow_instance, is_materialized)
		VALUES ($1::uuid, 'source.event', 'agent', 'agent-a', 'flow/a', TRUE)
	`, runA); err != nil {
		t.Fatalf("seed materialized routing rule: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO mailbox (entity_id, flow_instance, item_type, source_event_id, from_agent, summary)
		VALUES ($1::uuid, 'flow/a', 'review_notice', $2::uuid, 'agent-a', 'preserve mailbox')
	`, entityID, sourceEvent); err != nil {
		t.Fatalf("seed mailbox: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO spend_ledger (execution_mode, run_id, entity_id, flow_instance, agent_id, model, invocation_type)
		VALUES ('live', $1::uuid, $2::uuid, 'flow/a', 'agent-a', 'model', 'agent_turn')
	`, runA, entityID); err != nil {
		t.Fatalf("seed spend ledger: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `INSERT INTO generated_entity_fixture (entity_id) VALUES ($1::uuid)`, entityID); err != nil {
		t.Fatalf("seed generated entity table: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `INSERT INTO generated_node_state_fixture (entity_id, node_id) VALUES ($1::uuid, 'node-a')`, entityID); err != nil {
		t.Fatalf("seed generated node table: %v", err)
	}
	return destructiveResetCleanupSeed{RunA: runA, RunB: runB}
}

func seedDestructiveResetBundleRows(t *testing.T, ctx context.Context, pg *PostgresStore) {
	t.Helper()
	if _, err := pg.backend.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json, metadata) VALUES
			($1, 'name: bundle-a', '{}'::jsonb, '{}'::jsonb),
			($2, 'name: bundle-b', '{}'::jsonb, '{}'::jsonb)
	`, destructiveResetCleanupBundleHashA, destructiveResetCleanupBundleHashB); err != nil {
		t.Fatalf("seed bundles: %v", err)
	}
}

func assertCleanupTableResult(t *testing.T, result destructivereset.CleanupResult, table string, matched, deleted int64) {
	t.Helper()
	for _, row := range result.Tables {
		if row.Table == table {
			if row.MatchedRows != matched || row.DeletedRows != deleted {
				t.Fatalf("cleanup table %s result = matched %d deleted %d, want %d/%d", table, row.MatchedRows, row.DeletedRows, matched, deleted)
			}
			return
		}
	}
	t.Fatalf("cleanup result missing table %s: %#v", table, result.Tables)
}

func assertCleanupTablePreserved(t *testing.T, result destructivereset.CleanupResult, table string, preserved int64) {
	t.Helper()
	for _, row := range result.Tables {
		if row.Table == table {
			if row.PreservedRows != preserved || row.MatchedRows != 0 || row.DeletedRows != 0 {
				t.Fatalf("cleanup table %s result = preserved %d matched %d deleted %d, want preserved %d only", table, row.PreservedRows, row.MatchedRows, row.DeletedRows, preserved)
			}
			return
		}
	}
	t.Fatalf("cleanup result missing table %s: %#v", table, result.Tables)
}

func countRows(t *testing.T, ctx context.Context, pg *PostgresStore, table string) int64 {
	t.Helper()
	var count int64
	if err := pg.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+quoteIdent(table)).Scan(&count); err != nil {
		if err == sql.ErrNoRows {
			return 0
		}
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func countRowsWhere(t *testing.T, ctx context.Context, pg *PostgresStore, table, where string, args ...any) int64 {
	t.Helper()
	var count int64
	if err := pg.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+quoteIdent(table)+` WHERE `+where, args...).Scan(&count); err != nil {
		t.Fatalf("count %s where %s: %v", table, where, err)
	}
	return count
}
