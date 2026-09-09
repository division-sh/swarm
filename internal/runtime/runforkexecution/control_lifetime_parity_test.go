package runforkexecution

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSelectedForkControlLifetimeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"executing", "retained"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				var selected any
				var db *sql.DB
				var authorityStore startupownership.Store
				var owner SelectedContractExecutionOwner
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					selected, db, authorityStore = s, storetest.Database(s), s
					owner = selectedContractSQLiteExecutionOwnerForTest(t, s)
				} else {
					_, db, _ = testutil.StartPostgres(t)
					s := storetest.AdmitPostgresRuntimeStore(t, db)
					selected, authorityStore = s, s
					owner = selectedContractExecutionOwnerForTest(t, s)
				}
				ctx := runForkTestContext(t)
				process, _ := worklifetime.ProcessFromContext(ctx)
				capability := selectedContractTestProcessCapability(t, ctx, authorityStore)
				if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
					t.Fatal(err)
				}
				root := runForkExecutionRepoRoot(t)
				loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: root, SourceRoot: filepath.Join(root, "tests/tier1-primitives/test-emits-multiple"), PlatformSpecPath: contracts.DefaultPlatformSpecFile(root)}
				loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
				if err != nil {
					t.Fatal(err)
				}
				sourceRun, eventID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
				seedSelectedOperationSource(t, ctx, backend, db, selected, loaded, sourceRun, eventID, entityID)
				request := SelectedContractExecutionRequest{SourceRunID: sourceRun, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
					ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
					AgentRuntime:      SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}}
				var forkRun string
				if phase == "retained" {
					result, err := ExecuteSelectedContractRunFork(ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					forkRun = result.Materialization.ForkRunID
					if _, err := testGatewayWorkOwner(t).RetireAndWait(context.Background()); err != nil {
						t.Fatal(err)
					}
					resultStop, selected, err := owner.StopSelectedFork(context.Background(), runcontrol.TransitionRequest{RunID: forkRun})
					if err != nil || !selected || resultStop.Status != "cancelled" {
						t.Fatalf("retained stop = %+v selected=%v err=%v", resultStop, selected, err)
					}
				} else {
					entered, cancelled, release := make(chan string, 1), make(chan struct{}), make(chan struct{})
					owner.ports.replay = selectedOperationPublicationProbe{SelectedContractReplayPersistence: owner.ports.replay, after: func(ctx context.Context) {
						occurrence, _ := worklifetime.OccurrenceFromContext(ctx)
						entered <- occurrence.(*worklifetime.SelectedForkOccurrence).Identity().RunID
						<-ctx.Done()
						close(cancelled)
						<-release
					}}
					driver, err := process.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					finished := make(chan error, 1)
					go func() {
						_, err := ExecuteSelectedContractRunFork(ctx, request)
						finished <- errors.Join(err, driver.Done())
					}()
					select {
					case forkRun = <-entered:
					case err := <-finished:
						t.Fatalf("execution did not reach held publication: %v", err)
					}
					stopDriver, err := process.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					stopped := make(chan error, 1)
					go func() {
						result, selected, err := owner.StopSelectedFork(context.Background(), runcontrol.TransitionRequest{RunID: forkRun})
						if err == nil && (!selected || result.Status != "cancelled") {
							err = errors.New("stop lost selected identity or terminal result")
						}
						stopped <- errors.Join(err, stopDriver.Done())
					}()
					select {
					case <-cancelled:
					case err := <-stopped:
						close(release)
						t.Fatalf("stop did not fence selected execution: %v", err)
					}
					select {
					case err := <-stopped:
						close(release)
						t.Fatalf("stop skipped accepted execution join: %v", err)
					default:
					}
					close(release)
					if err := <-finished; err == nil {
						t.Fatal("stopped execution reported success")
					}
					if err := <-stopped; err != nil {
						t.Fatal(err)
					}
				}
				var status, control, execution string
				if err := db.QueryRow(`SELECT r.status, c.control_status, e.state FROM runs r JOIN run_control_state c ON c.run_id=r.run_id JOIN run_fork_selected_contract_runtime_executions e ON e.fork_run_id=r.run_id WHERE r.run_id=$1`, forkRun).Scan(&status, &control, &execution); err != nil || status != "cancelled" || control != "stopped" || execution != "closed" {
					t.Fatalf("terminal readback = %s/%s/%s %v", status, control, execution, err)
				}
				var retainedEvents int
				if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1`, forkRun).Scan(&retainedEvents); err != nil || retainedEvents == 0 {
					t.Fatalf("stop discarded committed fork history: count=%d err=%v", retainedEvents, err)
				}
				if _, selected, err := owner.StopSelectedFork(ctx, runcontrol.TransitionRequest{RunID: forkRun}); !selected || !errors.Is(err, runcontrol.ErrAlreadyTerminal) {
					t.Fatalf("terminal repeat = %v %v", selected, err)
				}
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
