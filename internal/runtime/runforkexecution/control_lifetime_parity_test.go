package runforkexecution

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
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
	testSelectedForkControlLifetime(t, "")
}

func TestSelectedForkControlCommittedOutcomesBothStores(t *testing.T) {
	for _, operation := range []string{"stop", "recovery"} {
		t.Run(operation, func(t *testing.T) { testSelectedForkControlLifetime(t, operation) })
	}
}

type selectedControlOutcomeStore struct {
	SelectedContractForkLifecycle
	operation string
	cause     error
}

func (s selectedControlOutcomeStore) StopSelectedFork(ctx context.Context, req runcontrol.SelectedStopRequest) (runcontrol.State, error) {
	result, err := s.SelectedContractForkLifecycle.StopSelectedFork(ctx, req)
	if err == nil && s.operation == "stop" {
		err = s.cause
	}
	return result, err
}

func (s selectedControlOutcomeStore) RecoverSelectedFork(ctx context.Context, req runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error) {
	result, err := s.SelectedContractForkLifecycle.RecoverSelectedFork(ctx, req)
	if err == nil && s.operation == "recovery" {
		err = s.cause
	}
	return result, err
}

func testSelectedForkControlLifetime(t *testing.T, outcomeOperation string) {
	t.Helper()
	phases := []string{"executing", "executing_retirement", "retained", "reconstructed"}
	if outcomeOperation == "stop" {
		phases = []string{"retained", "reconstructed"}
	} else if outcomeOperation == "recovery" {
		phases = []string{"reconstructed"}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range phases {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				outcomeErr := errors.New("acknowledged selected control cleanup")
				var selected any
				var db *sql.DB
				var authorityStore startupownership.Store
				var owner SelectedContractExecutionOwner
				var newOwner func() SelectedContractExecutionOwner
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					selected, db, authorityStore = s, storetest.Database(s), s
					owner = selectedContractSQLiteExecutionOwnerForTest(t, s)
					newOwner = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
				} else {
					_, db, _ = testutil.StartPostgres(t)
					s := storetest.AdmitPostgresRuntimeStore(t, db)
					selected, authorityStore = s, s
					owner = selectedContractExecutionOwnerForTest(t, s)
					newOwner = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
				}
				ctx := runForkTestContext(t)
				process, _ := worklifetime.ProcessFromContext(ctx)
				capability := selectedContractTestProcessCapability(t, ctx, authorityStore)
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
				if phase != "executing" && phase != "executing_retirement" {
					result, err := ExecuteSelectedContractRunFork(ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					forkRun = result.Materialization.ForkRunID
					if phase == "reconstructed" {
						if err := owner.RetireSelectedContexts(context.Background()); err != nil {
							t.Fatal(err)
						}
						if err := capability.Release(context.Background()); err != nil {
							t.Fatal(err)
						}
						freshProcess := worklifetime.NewProcess()
						t.Cleanup(func() {
							freshProcess.Retire()
							if _, err := freshProcess.Join(context.Background()); err != nil {
								t.Error(err)
							}
						})
						capability = selectedContractTestProcessCapability(t, ctx, authorityStore)
						owner = newOwner()
						if err := owner.BindSelectedProcess(ctx, freshProcess, capability); err != nil {
							t.Fatal(err)
						}
						var beforeEvents, beforeExecutions int
						if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1`, forkRun).Scan(&beforeEvents); err != nil {
							t.Fatal(err)
						}
						if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, forkRun).Scan(&beforeExecutions); err != nil {
							t.Fatal(err)
						}
						if outcomeOperation == "recovery" {
							owner.ports.fork = selectedControlOutcomeStore{SelectedContractForkLifecycle: owner.ports.fork, operation: outcomeOperation, cause: outcomeErr}
						}
						recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly))
						if (outcomeOperation != "recovery" && err != nil) || (outcomeOperation == "recovery" && !errors.Is(err, outcomeErr)) || len(recovered) != 1 || recovered[0].RunID != forkRun || recovered[0].Disposition != runfork.SelectedForkRecoveryControlOnly {
							t.Fatalf("control reconstruction = %+v %v", recovered, err)
						}
						var afterEvents, afterExecutions int
						if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1`, forkRun).Scan(&afterEvents); err != nil {
							t.Fatal(err)
						}
						if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, forkRun).Scan(&afterExecutions); err != nil {
							t.Fatal(err)
						}
						if afterEvents != beforeEvents || afterExecutions != beforeExecutions {
							t.Fatal("control reconstruction executed selected work")
						}
						if outcomeOperation == "recovery" {
							owner.ports.contexts.mu.Lock()
							retired, admitted := owner.ports.contexts.retired, owner.ports.contexts.recovered
							owner.ports.contexts.mu.Unlock()
							if !retired || admitted {
								t.Fatal("failed recovery admitted executable work")
							}
							if err := owner.RetireSelectedContexts(context.Background()); err != nil {
								t.Fatal(err)
							}
							return
						}
					}
					if _, err := testGatewayWorkOwner(t).RetireAndWait(context.Background()); err != nil {
						t.Fatal(err)
					}
					if outcomeOperation == "stop" {
						owner.ports.fork = selectedControlOutcomeStore{SelectedContractForkLifecycle: owner.ports.fork, operation: outcomeOperation, cause: outcomeErr}
					}
					resultStop, selected, err := owner.StopSelectedFork(context.Background(), runcontrol.TransitionRequest{RunID: forkRun})
					if (outcomeOperation != "stop" && err != nil) || (outcomeOperation == "stop" && !errors.Is(err, outcomeErr)) || !selected || resultStop.RunID != forkRun || resultStop.Status != "cancelled" {
						t.Fatalf("retained stop = %+v selected=%v err=%v", resultStop, selected, err)
					}
					if outcomeOperation == "stop" && (resultStop.Recovery.Disposition != runcontrol.RecoveryFailed || !errors.Is(resultStop.Recovery.Err, outcomeErr)) {
						t.Fatalf("stop lost independent cleanup failure: %+v", resultStop.Recovery)
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
					var retired chan error
					if phase == "executing_retirement" {
						if err := owner.FenceSelectedContexts(); err != nil {
							close(release)
							t.Fatal(err)
						}
						retired = make(chan error, 1)
						go func() { retired <- owner.RetireSelectedContexts(context.Background()) }()
						select {
						case err := <-retired:
							close(release)
							t.Fatalf("retirement skipped accepted execution/control disposition: %v", err)
						default:
						}
					}
					close(release)
					if err := <-finished; err == nil {
						t.Fatal("stopped execution reported success")
					}
					if err := <-stopped; err != nil {
						t.Fatal(err)
					}
					if retired != nil {
						if err := <-retired; err != nil {
							t.Fatalf("retirement could not join settled execution/control: %v", err)
						}
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
				if phase == "executing_retirement" {
					if _, selected, err := owner.StopSelectedFork(ctx, runcontrol.TransitionRequest{RunID: forkRun}); selected || !errors.Is(err, worklifetime.ErrRetired) {
						t.Fatalf("retired owner admitted another stop: selected=%v err=%v", selected, err)
					}
				} else if _, selected, err := owner.StopSelectedFork(ctx, runcontrol.TransitionRequest{RunID: forkRun}); !selected || !errors.Is(err, runcontrol.ErrAlreadyTerminal) {
					t.Fatalf("terminal repeat = %v %v", selected, err)
				}
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
