package runforkexecution

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type selectedForkCrashCheckpoint struct {
	SourceRun, ForkRun string
}

// These are actual process deaths during the existing execution path. They do
// not prove the separate public publication or API response-reconstruction flow.
func TestSelectedForkCommittedProcessDeathBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"before_materialization", "materialized", "execution_issued", "execution_claimed", "event_committed", "before_activation", "execution_returned"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				var db *sql.DB
				var capabilityStore startupownership.Store
				var construct func() SelectedContractExecutionOwner
				var dsn string
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					db, capabilityStore = storetest.Database(s), s
					var sequence int
					var name string
					if err := db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &dsn); err != nil {
						t.Fatal(err)
					}
					construct = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
				} else {
					dsn, db, _ = testutil.StartPostgres(t)
					s := storetest.AdmitPostgresRuntimeStore(t, db)
					capabilityStore = s
					construct = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
				}
				checkpoint := killSelectedForkAtCheckpoint(t, backend, dsn, cut)
				before := selectedPreparationDatabaseSnapshot(t, db, backend)
				ctx := runForkTestContext(t)
				capability := selectedContractTestProcessCapability(t, ctx, capabilityStore)
				owner := construct()
				process, _ := worklifetime.ProcessFromContext(ctx)
				if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
					t.Fatal(err)
				}
				recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly))
				want := runfork.SelectedForkRecoveryControlOnly
				if cut == "materialized" {
					want = runfork.SelectedForkRecoveryStaged
				} else if cut == "event_committed" || cut == "execution_issued" || cut == "execution_claimed" {
					want = runfork.SelectedForkRecoveryFailed
				}
				if cut == "before_materialization" {
					if err != nil || len(recovered) != 0 {
						t.Fatalf("uncommitted preparation acquired durable recovery work: %+v %v", recovered, err)
					}
				} else {
					if err != nil || len(recovered) != 1 || recovered[0].RunID != checkpoint.ForkRun || recovered[0].Disposition != want {
						t.Fatalf("recovery after %s: %+v err=%v, want %s", cut, recovered, err, want)
					}
				}
				after := selectedPreparationDatabaseSnapshot(t, db, backend)
				for _, table := range []string{"events", "entity_state", "entity_mutations", "run_fork_selected_contract_executions"} {
					if !reflect.DeepEqual(before[table], after[table]) {
						t.Errorf("recovery changed execution/business history in %s", table)
					}
				}
				if cut == "before_materialization" || cut == "materialized" {
					var count int
					if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions`).Scan(&count); err != nil || count != 0 {
						t.Fatalf("recovery issued execution for a preparation/staged binding: count=%d err=%v", count, err)
					}
					for _, table := range []string{"runs", "run_fork_selected_contract_bindings"} {
						if !reflect.DeepEqual(before[table], after[table]) {
							t.Errorf("recovery mutated non-executable %s", table)
						}
					}
					return
				}
				var state, status string
				var generations int
				if err := db.QueryRow(`SELECT e.state,r.status FROM run_fork_selected_contract_runtime_executions e JOIN runs r ON r.run_id=e.fork_run_id WHERE r.run_id=$1`, checkpoint.ForkRun).Scan(&state, &status); err != nil {
					t.Fatal(err)
				}
				wantState := "closed"
				if cut == "before_activation" {
					wantState = "quiesced"
				}
				if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, checkpoint.ForkRun).Scan(&generations); err != nil || generations != 1 || state != wantState {
					t.Fatalf("recovery reissued or retained executable authority: generations=%d state=%s err=%v", generations, state, err)
				}
				wantStatus := "running"
				if want == runfork.SelectedForkRecoveryFailed {
					wantStatus = "failed"
				} else if cut == "before_activation" {
					wantStatus = "paused"
				}
				if status != wantStatus {
					t.Fatalf("recovery status=%s, want %s", status, wantStatus)
				}
				if err := owner.RetireSelectedContexts(ctx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func killSelectedForkAtCheckpoint(t *testing.T, backend, dsn, cut string) selectedForkCrashCheckpoint {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSelectedForkCrashProcessHelper$")
	cmd.Env = append(os.Environ(), "SWARM_SELECTED_CRASH_BACKEND="+backend, "SWARM_SELECTED_CRASH_DSN="+dsn, "SWARM_SELECTED_CRASH_CUT="+cut)
	cmd.ExtraFiles = []*os.File{writer}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	var checkpoint selectedForkCrashCheckpoint
	decodeErr := json.NewDecoder(reader).Decode(&checkpoint)
	killErr := cmd.Process.Kill()
	waitErr := cmd.Wait()
	if decodeErr != nil || killErr != nil || waitErr == nil || ctx.Err() != nil || (checkpoint.ForkRun == "" && cut != "before_materialization") || checkpoint.SourceRun == "" {
		t.Fatalf("checkpoint %s: decode=%v kill=%v wait=%v context=%v checkpoint=%+v\n%s", cut, decodeErr, killErr, waitErr, ctx.Err(), checkpoint, output.String())
	}
	var exited *exec.ExitError
	if !errors.As(waitErr, &exited) || exited.ProcessState.Success() {
		t.Fatalf("child did not die at checkpoint: %v", waitErr)
	}
	if status, ok := exited.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child did not receive SIGKILL: %v", waitErr)
	}
	return checkpoint
}

type selectedCrashMaterialization struct {
	SelectedContractForkLifecycle
	before     bool
	checkpoint func(string)
}

func (p selectedCrashMaterialization) MaterializeRunForkForSelectedContractExecution(ctx context.Context, req runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error) {
	if p.before {
		p.checkpoint("")
	}
	result, err := p.SelectedContractForkLifecycle.MaterializeRunForkForSelectedContractExecution(ctx, req)
	if err == nil {
		p.checkpoint(result.ForkRunID)
	}
	return result, err
}

type selectedCrashExecution struct {
	SelectedContractRuntimeExecutionLifecycle
	issued     bool
	checkpoint func(string)
}

func (p selectedCrashExecution) IssueRunForkSelectedContractRuntimeExecution(ctx context.Context, req runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error) {
	result, err := p.SelectedContractRuntimeExecutionLifecycle.IssueRunForkSelectedContractRuntimeExecution(ctx, req)
	if err == nil && p.issued {
		p.checkpoint(result.ForkRunID)
	}
	return result, err
}

func (p selectedCrashExecution) ClaimRunForkSelectedContractRuntimeExecution(ctx context.Context, execution runfork.SelectedContractRuntimeExecution, owner string, lease time.Duration) (effects.Authority, error) {
	result, err := p.SelectedContractRuntimeExecutionLifecycle.ClaimRunForkSelectedContractRuntimeExecution(ctx, execution, owner, lease)
	if err == nil {
		p.checkpoint(execution.ForkRunID)
	}
	return result, err
}

func TestSelectedForkCrashProcessHelper(t *testing.T) {
	backend := os.Getenv("SWARM_SELECTED_CRASH_BACKEND")
	if backend == "" {
		return
	}
	var db *sql.DB
	var selected any
	var capabilityStore startupownership.Store
	var owner SelectedContractExecutionOwner
	if backend == "sqlite" {
		var err error
		db, err = sql.Open("sqlite", os.Getenv("SWARM_SELECTED_CRASH_DSN"))
		if err != nil {
			t.Fatal(err)
		}
		s := storetest.AdmitSQLiteRuntimeStore(t, db)
		selected, capabilityStore = s, s
		owner = selectedContractSQLiteExecutionOwnerForTest(t, s)
	} else {
		var err error
		db, err = sql.Open("postgres", os.Getenv("SWARM_SELECTED_CRASH_DSN"))
		if err != nil {
			t.Fatal(err)
		}
		s := storetest.AdmitPostgresRuntimeStore(t, db)
		selected, capabilityStore = s, s
		owner = selectedContractExecutionOwnerForTest(t, s)
	}
	ctx := runForkTestContext(t)
	capability := selectedContractTestProcessCapability(t, ctx, capabilityStore)
	root := runForkExecutionRepoRoot(t)
	loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: root, SourceRoot: filepath.Join(root, "tests/tier1-primitives/test-emits-multiple"), PlatformSpecPath: contracts.DefaultPlatformSpecFile(root)}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
	if err != nil {
		t.Fatal(err)
	}
	sourceRun, eventID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	seedSelectedOperationSource(t, ctx, backend, db, selected, loaded, sourceRun, eventID, entityID)
	checkpoint := func(forkRun string) {
		pipe := os.NewFile(3, "selected-checkpoint")
		if err := json.NewEncoder(pipe).Encode(selectedForkCrashCheckpoint{SourceRun: sourceRun, ForkRun: forkRun}); err != nil {
			t.Fatal(err)
		}
		pipe.Close()
		var b [1]byte
		_, err := os.Stdin.Read(b[:])
		t.Fatalf("checkpoint returned without process death: %v", err)
	}
	cut := os.Getenv("SWARM_SELECTED_CRASH_CUT")
	if cut == "before_materialization" || cut == "materialized" {
		owner.ports.fork = selectedCrashMaterialization{SelectedContractForkLifecycle: owner.ports.fork, before: cut == "before_materialization", checkpoint: checkpoint}
	} else if cut == "execution_issued" || cut == "execution_claimed" {
		owner.ports.runtimeExecution = selectedCrashExecution{SelectedContractRuntimeExecutionLifecycle: owner.ports.runtimeExecution, issued: cut == "execution_issued", checkpoint: checkpoint}
	} else if cut == "event_committed" {
		owner.ports.replay = selectedOperationPublicationProbe{SelectedContractReplayPersistence: owner.ports.replay, after: func(ctx context.Context) {
			occurrence, _ := worklifetime.OccurrenceFromContext(ctx)
			checkpoint(occurrence.(*worklifetime.SelectedForkOccurrence).Identity().RunID)
		}}
	} else if cut == "before_activation" {
		var discards int
		owner.ports.fork = selectedOperationActivationProbe{SelectedContractForkLifecycle: owner.ports.fork, discards: &discards, before: func(ctx context.Context) {
			occurrence, _ := worklifetime.OccurrenceFromContext(ctx)
			checkpoint(occurrence.(*worklifetime.SelectedForkOccurrence).Identity().RunID)
		}}
	}
	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: sourceRun, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
		AgentRuntime:      SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}})
	if err != nil {
		t.Fatal(err)
	}
	if cut != "execution_returned" {
		t.Fatalf("execution skipped checkpoint %s", cut)
	}
	checkpoint(result.Materialization.ForkRunID)
}
