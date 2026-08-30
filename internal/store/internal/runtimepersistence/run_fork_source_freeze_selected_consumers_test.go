package runtimepersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type forkedSelectedExecutionSurface interface {
	selectedCompletionAuthorityStore
}

func TestForkedRunSelectedContractExecutionRefusesAdmissionAndAllowsExactFinalization(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var surface forkedSelectedExecutionSurface
			var fixture selectedCompletionFixture
			if backend == "postgres" {
				base := newForkedConsumerTestBackend(t, "postgres")
				surface = base.postgres
				fixture = newSelectedCompletionFixture(t, surface, base.db, false)
			} else {
				base := newForkedConsumerTestBackend(t, "sqlite")
				surface = base.sqlite
				fixture = newSelectedCompletionFixture(t, surface, base.db, true)
			}
			ctx := testAuthorActivitySourceArtifactContext()
			issued, err := surface.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := surface.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "selected-worker", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			markSelectedTestRunForked(t, fixture, time.Now().UTC())

			lateRequest := fixture.request
			lateRequest.Admission.ForkRunID = fixture.forkRun
			_, err = surface.IssueRunForkSelectedContractRuntimeExecution(ctx, lateRequest)
			requireForkedSourceRefusal(t, "issue selected execution", err)
			_, err = surface.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "other-worker", time.Minute)
			requireForkedSourceRefusal(t, "claim selected execution", err)
			requireForkedSourceRefusal(t, "heartbeat selected execution", surface.HeartbeatRunForkSelectedContractRuntimeExecution(ctx, authority, time.Minute))
			if err := surface.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
				t.Fatalf("quiesce accepted selected execution after source retirement: %v", err)
			}
			if err := surface.CloseRunForkSelectedContractRuntimeExecution(ctx, issued.ExecutionID); err != nil {
				t.Fatalf("close accepted selected execution after source retirement: %v", err)
			}
			if current, err := surface.IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || current {
				t.Fatalf("frozen selected authority current=%v err=%v", current, err)
			}

			var state string
			query := `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE execution_id = ?`
			if !fixture.sqlite {
				query = `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE execution_id = $1::uuid`
			}
			if err := fixture.db.QueryRowContext(ctx, query, issued.ExecutionID).Scan(&state); err != nil || state != "closed" {
				t.Fatalf("frozen selected execution state = %q, %v", state, err)
			}
		})
	}
}

func markSelectedTestRunForked(t *testing.T, fixture selectedCompletionFixture, now time.Time) {
	t.Helper()
	continuedRunID := uuid.NewString()
	ctx := testAuthorActivitySourceArtifactContext()
	var selected any
	if fixture.sqlite {
		selected = NewSQLiteRuntimeStoreForTest(fixture.db)
	} else {
		selected = newPostgresStoreWithBackend(mustPostgresBackend(fixture.db))
	}
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(),
		RunID: continuedRunID, StartedAt: now, BundleHash: authorActivityTestBundleHash,
	})
	if _, _, err := forkRunForTest(ctx, selected, storerunlifecycle.ForkSourceRequest{
		RunID: fixture.forkRun, ContinuedAsRunID: continuedRunID, EndedAt: now,
	}); err != nil {
		t.Fatalf("fork selected test run: %v", err)
	}
}

func TestForkedSourceCannotWriteSelectedContractRouteRecoveryEvidence(t *testing.T) {
	fixture := newForkedConsumerTestBackend(t, "postgres")
	eventID := uuid.NewString()
	insertForkedConsumerEvent(t, fixture, eventID, "selected.route.source", fixture.forkedAt.Add(-time.Minute))
	fixture.freeze(t)
	selection, topology, planning := testSelectedRouteRecoveryEvidence(eventID)
	_, err := fixture.postgres.RecordRunForkSelectedContractRouteRecovery(testAuthorActivitySourceArtifactContext(), runfork.RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID: fixture.continued, SourceRunID: fixture.sourceRun, ForkEventID: eventID,
		ContractSelection: selection, RouteTopology: topology, RecipientPlanning: planning,
	})
	requireForkedSourceRefusal(t, "record selected route recovery", err)

	var rows int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id = $1::uuid`, fixture.continued).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("frozen source route recovery rows = %d, %v", rows, err)
	}
}
