package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/testutil"
)

func TestSelectedContractExecutionMaterializationAllowsSelectedPendingNodeFrontier(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002400, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	_, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err == nil || !strings.Contains(err.Error(), RunForkBlockerNonAgentDeliveryReplayUnsupported) {
		t.Fatalf("MaterializeRunFork error = %v, want non-agent blocker", err)
	}

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" || materialized.SelectedContractBinding == nil || !materialized.DeliveryResumeBlocked {
		t.Fatalf("materialization = %#v", materialized)
	}
	var replayRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE fork_run_id = $1::uuid`, materialized.ForkRunID).Scan(&replayRows); err != nil {
		t.Fatalf("count replay rows: %v", err)
	}
	if replayRows != 0 {
		t.Fatalf("delivery replay rows = %d, want selected execution materialization to avoid #570 replay", replayRows)
	}
}

func TestSelectedContractExecutionMaterializationPreflightsLineageCapability(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002450, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `DROP TABLE run_fork_selected_contract_executions`); err != nil {
		t.Fatalf("drop selected execution lineage table: %v", err)
	}

	_, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "run_fork_selected_contract_executions") {
		t.Fatalf("materialization error = %v, want lineage capability failure", err)
	}
	var strayForks int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE forked_from_run_id = $1::uuid
	`, sourceRunID).Scan(&strayForks); err != nil {
		t.Fatalf("count stray forks: %v", err)
	}
	if strayForks != 0 {
		t.Fatalf("stray materialized forks = %d, want 0", strayForks)
	}
}

func TestSelectedContractExecutionMaterializationPreflightsBranchDivergenceOwnerCapability(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002475, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `ALTER TABLE run_fork_selected_contract_branch_divergences DROP COLUMN owner`); err != nil {
		t.Fatalf("drop branch divergence owner column: %v", err)
	}

	_, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "run_fork_selected_contract_branch_divergences") || !strings.Contains(err.Error(), "owner") {
		t.Fatalf("materialization error = %v, want branch divergence owner capability failure", err)
	}
	var strayForks int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE forked_from_run_id = $1::uuid
	`, sourceRunID).Scan(&strayForks); err != nil {
		t.Fatalf("count stray forks: %v", err)
	}
	if strayForks != 0 {
		t.Fatalf("stray materialized forks = %d, want 0", strayForks)
	}
}

func TestSelectedContractExecutionMaterializationPreflightsRouteRecoveryCapability(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002485, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `DROP TABLE run_fork_selected_contract_route_recoveries`); err != nil {
		t.Fatalf("drop selected route recovery table: %v", err)
	}

	_, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "run_fork_selected_contract_route_recoveries") {
		t.Fatalf("materialization error = %v, want route recovery capability failure", err)
	}
	var strayForks int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE forked_from_run_id = $1::uuid
	`, sourceRunID).Scan(&strayForks); err != nil {
		t.Fatalf("count stray forks: %v", err)
	}
	if strayForks != 0 {
		t.Fatalf("stray materialized forks = %d, want 0", strayForks)
	}
}

func TestSelectedContractExecutionMaterializationReconstructsActiveTimer(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002500, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO timers (run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload, fire_at, owner_agent, task_type, status, created_at)
		VALUES ($1::uuid, 'selected-timer', $2::uuid, 'flow-a/1', 'timer.selected', '{"source":true}'::jsonb, $3, 'agent-a', 'timer', 'active', $4)
	`, sourceRunID, entityID, at.Add(time.Hour), at); err != nil {
		t.Fatalf("seed timer: %v", err)
	}
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialized fork run_id is empty: %#v", materialized)
	}
	for _, blocker := range materialized.ReplayResumeAdmission.UnsupportedBlockers {
		if blocker.Code == RunForkBlockerTimerHistoryUnproven {
			t.Fatalf("timer blocker survived reconstruction: %#v", materialized.ReplayResumeAdmission.UnsupportedBlockers)
		}
	}
	var forkTimerCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM timers
		WHERE run_id = $1::uuid
		  AND source_timer_id IS NOT NULL
		  AND forked_from_run_id = $2::uuid
		  AND forked_from_event_id = $3::uuid
		  AND reconstruction_owner = $4
		  AND status = 'active'
	`, materialized.ForkRunID, sourceRunID, eventID, RunForkHistoricalReplayTimerReconstructionOwner).Scan(&forkTimerCount); err != nil {
		t.Fatalf("count reconstructed fork timers: %v", err)
	}
	if forkTimerCount != 1 {
		t.Fatalf("reconstructed fork timers = %d, want 1", forkTimerCount)
	}
	var sourceTimerCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM timers
		WHERE run_id = $1::uuid
		  AND source_timer_id IS NULL
		  AND status = 'active'
	`, sourceRunID).Scan(&sourceTimerCount); err != nil {
		t.Fatalf("count source timers: %v", err)
	}
	if sourceTimerCount != 1 {
		t.Fatalf("source timers = %d, want 1", sourceTimerCount)
	}
}

func TestSelectedContractExecutionMaterializationFailsClosedForUnsupportedTimerHistory(t *testing.T) {
	cases := []struct {
		name           string
		insertTimer    func(t *testing.T, db *sql.DB, sourceRunID, entityID string, at time.Time)
		expectedReason string
	}{
		{
			name: "fired timer",
			insertTimer: func(t *testing.T, db *sql.DB, sourceRunID, entityID string, at time.Time) {
				t.Helper()
				if _, err := db.ExecContext(context.Background(), `
					INSERT INTO timers (
						run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
						fire_at, owner_agent, task_type, status, fired_at, created_at
					)
					VALUES (
						$1::uuid, 'selected-fired-timer', $2::uuid, 'flow-a/1', 'timer.selected', '{"source":true}'::jsonb,
						$3, 'agent-a', 'timer', 'fired', $4, $5
					)
				`, sourceRunID, entityID, at.Add(time.Hour), at.Add(30*time.Minute), at.Add(-time.Minute)); err != nil {
					t.Fatalf("seed fired timer: %v", err)
				}
			},
			expectedReason: "source timer history is not active-at-fork only",
		},
		{
			name: "non-active timer",
			insertTimer: func(t *testing.T, db *sql.DB, sourceRunID, entityID string, at time.Time) {
				t.Helper()
				if _, err := db.ExecContext(context.Background(), `
					INSERT INTO timers (
						run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
						fire_at, owner_agent, task_type, status, created_at
					)
					VALUES (
						$1::uuid, 'selected-cancelled-timer', $2::uuid, 'flow-a/1', 'timer.selected', '{"source":true}'::jsonb,
						$3, 'agent-a', 'timer', 'cancelled', $4
					)
				`, sourceRunID, entityID, at.Add(time.Hour), at.Add(-time.Minute)); err != nil {
					t.Fatalf("seed cancelled timer: %v", err)
				}
			},
			expectedReason: "source timer history is not active-at-fork only",
		},
		{
			name: "missing executable owner",
			insertTimer: func(t *testing.T, db *sql.DB, sourceRunID, entityID string, at time.Time) {
				t.Helper()
				if _, err := db.ExecContext(context.Background(), `
					INSERT INTO timers (
						run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
						fire_at, owner_node, task_type, status, created_at
					)
					VALUES (
						$1::uuid, 'selected-ownerless-timer', $2::uuid, 'flow-a/1', 'timer.selected', '{"source":true}'::jsonb,
						$3, 'timer-node', 'timer', 'active', $4
					)
				`, sourceRunID, entityID, at.Add(time.Hour), at.Add(-time.Minute)); err != nil {
					t.Fatalf("seed ownerless timer: %v", err)
				}
			},
			expectedReason: "source timer lacks executable owner/event identity",
		},
		{
			name: "missing fire event",
			insertTimer: func(t *testing.T, db *sql.DB, sourceRunID, entityID string, at time.Time) {
				t.Helper()
				if _, err := db.ExecContext(context.Background(), `
					INSERT INTO timers (
						run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
						fire_at, owner_agent, task_type, status, created_at
					)
					VALUES (
						$1::uuid, 'selected-eventless-timer', $2::uuid, 'flow-a/1', '', '{"source":true}'::jsonb,
						$3, 'agent-a', 'timer', 'active', $4
					)
				`, sourceRunID, entityID, at.Add(time.Hour), at.Add(-time.Minute)); err != nil {
					t.Fatalf("seed eventless timer: %v", err)
				}
			},
			expectedReason: "source timer lacks executable owner/event identity",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := &PostgresStore{DB: db}
			ctx := context.Background()
			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			eventID := uuid.NewString()
			at := time.Unix(1700003525, 0).UTC()
			seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
			tc.insertTimer(t, db, sourceRunID, entityID, at)

			materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
				SourceRunID: sourceRunID,
				At:          eventID,
				ContractSelection: RunForkContractSelection{
					Mode:            "selected_contracts",
					ContractsRoot:   "/tmp/selected-contracts",
					WorkflowName:    "selected-workflow",
					WorkflowVersion: "v1",
				},
			})
			if err == nil || !strings.Contains(err.Error(), tc.expectedReason) {
				t.Fatalf("materialization error = %v, want %q", err, tc.expectedReason)
			}
			if materialized.ForkRunID != "" {
				t.Fatalf("materialized fork despite unsupported timer history: %#v", materialized)
			}
			assertNoSelectedContractForkRows(t, db, sourceRunID)
			assertNoForkTimerCopiesForSource(t, db, sourceRunID)
		})
	}
}

func TestSelectedContractTimerReconstructionFailsClosedForInvalidPayload(t *testing.T) {
	_, err := validateRunForkReconstructableSourceTimer(runForkTimerReconstructionRow{
		Status:      "active",
		OwnerAgent:  "agent-a",
		FireEvent:   "timer.selected",
		FirePayload: []byte(`{"broken"`),
	})
	if err == nil || !strings.Contains(err.Error(), "source timer payload is invalid JSON") {
		t.Fatalf("validate invalid timer payload error = %v", err)
	}
}

func TestSelectedContractTimerReconstructionFailsClosedWhenRelevantTimerDisappearsAfterPlanning(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003550, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	timerID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO timers (
			timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
			fire_at, owner_agent, task_type, status, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'selected-vanishing-timer', $3::uuid, 'flow-a/1', 'timer.selected', '{"source":true}'::jsonb,
			$4, 'agent-a', 'timer', 'active', $5
		)
	`, timerID, sourceRunID, entityID, at.Add(time.Hour), at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed timer: %v", err)
	}
	plan, err := pg.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if !runForkPlanHasTimerBlocker(plan) {
		t.Fatalf("plan missing timer blocker: %#v", plan.ReplayResumeAdmission)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM timers WHERE timer_id = $1::uuid`, timerID); err != nil {
		t.Fatalf("delete timer after planning: %v", err)
	}
	catalog, err := pg.requireRunForkSelectedContractExecutionCapabilities(ctx)
	if err != nil {
		t.Fatalf("require selected contract capabilities: %v", err)
	}
	_, err = pg.planRunForkSelectedContractTimerReconstruction(ctx, catalog, plan)
	if err == nil || !strings.Contains(err.Error(), "no reconstructable active source timers") {
		t.Fatalf("timer reconstruction error = %v, want no reconstructable timer blocker", err)
	}
}

func TestPostTSourceTimerFailsClosedForSelectedContractActivation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003600, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialized fork run_id is empty: %#v", materialized)
	}

	timerID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO timers (
			timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
			fire_at, owner_agent, task_type, status, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'post-t-source-timer', $3::uuid, 'flow-a/1', 'timer.selected', '{"source":true}'::jsonb,
			$4, 'agent-a', 'timer', 'active', $5
		)
	`, timerID, sourceRunID, entityID, at.Add(time.Hour), at.Add(time.Minute)); err != nil {
		t.Fatalf("seed post-T timer: %v", err)
	}

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err == nil || !strings.Contains(err.Error(), "source_timers_advanced_after_fork_point") {
		t.Fatalf("activation error = %v, want post-T timer blocker", err)
	}
	if activation.Activated || activation.BranchDivergence != nil {
		t.Fatalf("activation = %#v, want blocked before selected branch divergence", activation)
	}
	if !runForkTestHasActivationBlocker(activation, "source_timers_advanced_after_fork_point") {
		t.Fatalf("activation blockers = %#v, want source_timers_advanced_after_fork_point", activation.UnsupportedBlockers)
	}
	if !runForkTestHasDispositionBlocker(activation.ReplayResumeAdmission, RunForkReplayResumeFactSourceAdvanced, "source_timers_advanced_after_fork_point") {
		t.Fatalf("activation replay admission = %#v, want source advanced timer blocker", activation.ReplayResumeAdmission)
	}

	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != "running" || forkStatus != RunForkMaterializedStatus {
		t.Fatalf("run statuses source=%q fork=%q, want source running and fork materialized", sourceStatus, forkStatus)
	}

	var branchRows, forkTimerRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_branch_divergences
		WHERE fork_run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&branchRows); err != nil {
		t.Fatalf("count branch divergences: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkTimerRows); err != nil {
		t.Fatalf("count fork timers: %v", err)
	}
	if branchRows != 0 || forkTimerRows != 0 {
		t.Fatalf("branch rows=%d fork timer rows=%d, want no branch divergence and no fork timer copies", branchRows, forkTimerRows)
	}
	assertNoForkTimerCopiesForSource(t, db, sourceRunID)
}

func TestPostTSourceRouteFailsClosedForSelectedContractActivation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003610, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialized fork run_id is empty: %#v", materialized)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO routing_rules (
			event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow,
			is_materialized, status, created_at
		)
		VALUES ('item.received', 'node', 'post-t-source-route-node', 'flow-a/1', 'flow-a', true, 'active', $1)
	`, at.Add(time.Minute)); err != nil {
		t.Fatalf("seed post-T route: %v", err)
	}

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err == nil || !strings.Contains(err.Error(), "source_routes_advanced_after_fork_point") {
		t.Fatalf("activation error = %v, want post-T route blocker", err)
	}
	if activation.Activated || activation.SourceAdvancedAfterFork || activation.BranchDivergence != nil {
		t.Fatalf("activation = %#v, want blocked before selected branch divergence", activation)
	}
	if !runForkTestHasActivationBlocker(activation, "source_routes_advanced_after_fork_point") {
		t.Fatalf("activation blockers = %#v, want source_routes_advanced_after_fork_point", activation.UnsupportedBlockers)
	}
	if !runForkTestHasDispositionBlocker(activation.ReplayResumeAdmission, RunForkReplayResumeFactSourceAdvanced, "source_routes_advanced_after_fork_point") {
		t.Fatalf("activation replay admission = %#v, want source advanced route blocker", activation.ReplayResumeAdmission)
	}

	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != "running" || forkStatus != RunForkMaterializedStatus {
		t.Fatalf("run statuses source=%q fork=%q, want source running and fork materialized", sourceStatus, forkStatus)
	}

	var branchRows, forkDeliveryRows, sourceRouteRows, routeRecoveryRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_branch_divergences
		WHERE fork_run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&branchRows); err != nil {
		t.Fatalf("count branch divergences: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&forkDeliveryRows); err != nil {
		t.Fatalf("count fork deliveries: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM routing_rules
		WHERE subscriber_id = 'post-t-source-route-node'
	`).Scan(&sourceRouteRows); err != nil {
		t.Fatalf("count source route rows: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_route_recoveries
		WHERE fork_run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&routeRecoveryRows); err != nil {
		t.Fatalf("count route recovery rows: %v", err)
	}
	if branchRows != 0 || forkDeliveryRows != 0 || sourceRouteRows != 1 || routeRecoveryRows != 0 {
		t.Fatalf("branch rows=%d fork delivery rows=%d source route rows=%d route recovery rows=%d, want no divergence, no fork deliveries, one source route, no fork route recovery", branchRows, forkDeliveryRows, sourceRouteRows, routeRecoveryRows)
	}
}

func TestSelectedContractExecutionMaterializationPreservesRouteBlocker(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002525, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO routing_rules (
			event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow,
			is_materialized, status, created_at
		)
		VALUES ('item.received', 'node', 'selected-route-node', 'flow-a/2', 'flow-a', true, 'active', $1)
	`, at); err != nil {
		t.Fatalf("seed routing rule: %v", err)
	}
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), RunForkBlockerFlowRouteHistoryUnproven) {
		t.Fatalf("materialization error = %v, want route blocker", err)
	}
	if materialized.ForkRunID != "" {
		t.Fatalf("materialized fork despite route blocker: %#v", materialized)
	}
	assertNoSelectedContractForkRows(t, db, sourceRunID)
}

func seedSelectedContractExecutionStoreSource(t *testing.T, db execContextDB, sourceRunID, entityID, eventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, sourceRunID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed source run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'item.received', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, sourceRunID, eventID, entityID, at); err != nil {
		t.Fatalf("seed source event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'test-node', 'pending', $3)
	`, sourceRunID, eventID, at); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'selected-store-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"Selected Store Entity"'::jsonb, $3::uuid, 'platform', 'selected-store-test', 'seed', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'Selected Store Entity',
			'pending', '{}'::jsonb, '{"name":"Selected Store Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
}

func assertNoForkTimerCopiesForSource(t *testing.T, db *sql.DB, sourceRunID string) {
	t.Helper()
	var copied int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM timers
		WHERE forked_from_run_id = $1::uuid
		   OR source_timer_id IN (
				SELECT timer_id
				FROM timers
				WHERE run_id = $1::uuid
		   )
	`, sourceRunID).Scan(&copied); err != nil {
		t.Fatalf("count fork timer copies: %v", err)
	}
	if copied != 0 {
		t.Fatalf("fork timer copies for source run = %d, want 0", copied)
	}
}

func assertNoSelectedContractForkRows(t *testing.T, db *sql.DB, sourceRunID string) {
	t.Helper()
	ctx := context.Background()
	for name, query := range map[string]string{
		"runs": `
			SELECT COUNT(*)
			FROM runs
			WHERE forked_from_run_id = $1::uuid
		`,
		"selected_contract_bindings": `
			SELECT COUNT(*)
			FROM run_fork_selected_contract_bindings
			WHERE source_run_id = $1::uuid
		`,
		"selected_contract_executions": `
			SELECT COUNT(*)
			FROM run_fork_selected_contract_executions
			WHERE source_run_id = $1::uuid
		`,
		"selected_contract_branch_divergences": `
			SELECT COUNT(*)
			FROM run_fork_selected_contract_branch_divergences
			WHERE source_run_id = $1::uuid
		`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query, sourceRunID).Scan(&count); err != nil {
			t.Fatalf("count %s rows: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("%s rows for blocked selected-contract fork = %d, want 0", name, count)
		}
	}
}
