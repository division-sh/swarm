package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

type forkedSelectedExecutionSurface interface {
	selectedCompletionAuthorityStore
	FailRunForkSelectedContractRuntimeExecution(context.Context, runtimeeffects.Authority, json.RawMessage) error
}

func TestForkedRunSelectedContractExecutionIssueClaimHeartbeatAndTerminalMutationsRefuse(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
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
			ctx := context.Background()
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
			requireForkedSourceRefusal(t, "quiesce selected execution", surface.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority))
			requireForkedSourceRefusal(t, "fail selected execution", surface.FailRunForkSelectedContractRuntimeExecution(ctx, authority, json.RawMessage(`{"reason":"frozen"}`)))
			requireForkedSourceRefusal(t, "close selected execution", surface.CloseRunForkSelectedContractRuntimeExecution(ctx, issued.ExecutionID))
			if current, err := surface.IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || current {
				t.Fatalf("frozen selected authority current=%v err=%v", current, err)
			}

			var state string
			query := `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE execution_id = ?`
			if !fixture.sqlite {
				query = `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE execution_id = $1::uuid`
			}
			if err := fixture.db.QueryRowContext(ctx, query, issued.ExecutionID).Scan(&state); err != nil || state != "running" {
				t.Fatalf("frozen selected execution state = %q, %v", state, err)
			}
		})
	}
}

func markSelectedTestRunForked(t *testing.T, fixture selectedCompletionFixture, now time.Time) {
	t.Helper()
	continuedRunID := uuid.NewString()
	if fixture.sqlite {
		if _, err := fixture.db.ExecContext(context.Background(), `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, continuedRunID, now); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.ExecContext(context.Background(), `UPDATE runs SET status = 'forked', ended_at = ?, continued_as_run_id = ? WHERE run_id = ?`, now, continuedRunID, fixture.forkRun); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := fixture.db.ExecContext(context.Background(), `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, continuedRunID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(context.Background(), `UPDATE runs SET status = 'forked', ended_at = $2, continued_as_run_id = $3::uuid WHERE run_id = $1::uuid`, fixture.forkRun, now, continuedRunID); err != nil {
		t.Fatal(err)
	}
}

func TestForkedSourceCannotWriteSelectedContractRouteRecoveryEvidence(t *testing.T) {
	fixture := newForkedConsumerTestBackend(t, "postgres")
	eventID := uuid.NewString()
	insertForkedConsumerEvent(t, fixture, eventID, "selected.route.source", fixture.forkedAt.Add(-time.Minute))
	fixture.freeze(t)
	selection, topology, planning := testSelectedRouteRecoveryEvidence(eventID)
	_, err := fixture.postgres.RecordRunForkSelectedContractRouteRecovery(context.Background(), RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID: fixture.continued, SourceRunID: fixture.sourceRun, ForkEventID: eventID,
		ContractSelection: selection, RouteTopology: topology, RecipientPlanning: planning,
	})
	requireForkedSourceRefusal(t, "record selected route recovery", err)

	var rows int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id = $1::uuid`, fixture.continued).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("frozen source route recovery rows = %d, %v", rows, err)
	}
}
