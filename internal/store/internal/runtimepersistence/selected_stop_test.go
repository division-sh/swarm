package runtimepersistence

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedForkStopAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
			fixture := newSelectedCompletionFixture(t, selected, db, sqlite)
			ctx := testAuthorActivityContext()
			store := selected.(interface {
				RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
				StopSelectedFork(context.Context, runcontrol.SelectedStopRequest) (runcontrol.State, error)
			})
			binding, err := store.RequireRunForkSelectedContractBinding(ctx, fixture.forkRun)
			if err != nil {
				t.Fatal(err)
			}
			process, err := fixture.process.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			req := runcontrol.SelectedStopRequest{Transition: runcontrol.TransitionRequest{RunID: fixture.forkRun}, Binding: binding, Process: process}
			assertUnchanged := func() {
				t.Helper()
				var forkStatus, sourceStatus string
				var controls int
				if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1`, fixture.forkRun).Scan(&forkStatus); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1`, fixture.sourceRun).Scan(&sourceStatus); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_control_state WHERE run_id=$1`, fixture.forkRun).Scan(&controls); err != nil {
					t.Fatal(err)
				}
				if forkStatus != "paused" || sourceStatus != "running" || controls != 0 {
					t.Fatalf("refusal mutated runs or controls: fork=%s source=%s controls=%d", forkStatus, sourceStatus, controls)
				}
			}
			for _, test := range []struct {
				name   string
				change func(*runcontrol.SelectedStopRequest)
			}{
				{"run", func(r *runcontrol.SelectedStopRequest) { r.Transition.RunID = fixture.sourceRun }},
				{"binding", func(r *runcontrol.SelectedStopRequest) { r.Binding.BindingID = uuid.NewString() }},
				{"source", func(r *runcontrol.SelectedStopRequest) { r.Binding.SourceRunID = uuid.NewString() }},
				{"event", func(r *runcontrol.SelectedStopRequest) { r.Binding.ForkEventID = uuid.NewString() }},
				{"selection", func(r *runcontrol.SelectedStopRequest) {
					r.Binding.ContractSelection = runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: "bundle-v2:sha256:" + strings.Repeat("9", 64)}
				}},
				{"created", func(r *runcontrol.SelectedStopRequest) { r.Binding.CreatedAt = r.Binding.CreatedAt.Add(time.Second) }},
				{"process", func(r *runcontrol.SelectedStopRequest) { r.Process.AuthorityID = uuid.NewString() }},
				{"version", func(r *runcontrol.SelectedStopRequest) { r.Process.StateVersion++ }},
				{"generation", func(r *runcontrol.SelectedStopRequest) { r.Process.AuthorityGeneration++ }},
			} {
				t.Run(test.name, func(t *testing.T) {
					bad := req
					test.change(&bad)
					if _, err := store.StopSelectedFork(ctx, bad); err == nil {
						t.Fatal("crossed selected stop admitted")
					}
					assertUnchanged()
				})
			}
			issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "selected-stop-proof", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.StopSelectedFork(ctx, req); err == nil || !strings.Contains(err.Error(), "accepted execution settlement") {
				t.Fatalf("running work stop = %v", err)
			}
			assertUnchanged()
			if err := selected.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
				t.Fatal(err)
			}
			if err := selected.CloseRunForkSelectedContractRuntimeExecution(ctx, issued.ExecutionID); err != nil {
				t.Fatal(err)
			}
			// Failure after terminal work has been staged must roll the whole named
			// operation back, not leave a cancelled run without its control record.
			if sqlite {
				_, err = db.ExecContext(ctx, `CREATE TRIGGER selected_stop_fail BEFORE INSERT ON run_control_state BEGIN SELECT RAISE(ABORT, 'selected stop injection'); END`)
			} else {
				_, err = db.ExecContext(ctx, `CREATE FUNCTION selected_stop_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'selected stop injection'; END $$`)
				if err == nil {
					_, err = db.ExecContext(ctx, `CREATE TRIGGER selected_stop_fail BEFORE INSERT ON run_control_state FOR EACH STATEMENT EXECUTE FUNCTION selected_stop_fail()`)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.StopSelectedFork(ctx, req); err == nil || !strings.Contains(err.Error(), "selected stop injection") {
				t.Fatalf("injected stop = %v", err)
			}
			assertUnchanged()
			query := `DROP TRIGGER selected_stop_fail`
			if !sqlite {
				query += ` ON run_control_state`
			}
			if _, err := db.ExecContext(ctx, query); err != nil {
				t.Fatal(err)
			}
			state, err := store.StopSelectedFork(ctx, req)
			if err != nil || state.RunID != fixture.forkRun || state.Status != "cancelled" || state.ControlStatus != "stopped" {
				t.Fatalf("exact settled stop = %+v, %v", state, err)
			}
			if _, err := store.StopSelectedFork(ctx, req); !errors.Is(err, runcontrol.ErrAlreadyTerminal) {
				t.Fatalf("terminal repeat changed control law: %v", err)
			}
			queued := newSelectedCompletionFixtureWithProcess(t, selected, db, sqlite, fixture.process)
			queuedBinding, err := store.RequireRunForkSelectedContractBinding(ctx, queued.forkRun)
			if err != nil {
				t.Fatal(err)
			}
			queuedExecution, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, queued.request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.StopSelectedFork(ctx, runcontrol.SelectedStopRequest{Transition: runcontrol.TransitionRequest{RunID: queued.forkRun}, Binding: queuedBinding, Process: process}); err != nil {
				t.Fatal(err)
			}
			if _, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, queuedExecution, "claim-after-stop", time.Minute); err == nil {
				t.Fatal("stop-wins admitted queued execution")
			}
			var executionState, failure string
			if err := db.QueryRowContext(ctx, `SELECT state,failure FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, queuedExecution.ExecutionID).Scan(&executionState, &failure); err != nil || executionState != "closed" || !strings.Contains(failure, "stop_before_claim") {
				t.Fatalf("queued execution cancellation = %s %s %v", executionState, failure, err)
			}
		})
	}
}
