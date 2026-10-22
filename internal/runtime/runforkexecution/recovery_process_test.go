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
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type selectedForkCrashCheckpoint struct {
	SourceRun, ForkRun string
	ScheduleID         string                                `json:"schedule_id,omitempty"`
	Activity           *selectedActivityCrashCheckpoint      `json:"activity,omitempty"`
	FanOut             *selectedHandlerFanOutCrashCheckpoint `json:"fan_out,omitempty"`
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
	if strings.HasPrefix(cut, "retained_activity_") || strings.HasPrefix(cut, "retained_handler_fan_out_") {
		cmd.Args = append(cmd.Args, "-test.v")
	}
	cmd.Env = append(os.Environ(), "SWARM_SELECTED_CRASH_BACKEND="+backend, "SWARM_SELECTED_CRASH_DSN="+dsn, "SWARM_SELECTED_CRASH_CUT="+cut)
	if strings.HasPrefix(cut, "retained_timer_") || strings.HasPrefix(cut, "retained_join_") || strings.HasPrefix(cut, "retained_pending_join_") || strings.HasPrefix(cut, "retained_armed_join_") || strings.HasPrefix(cut, "retained_activity_") || strings.HasPrefix(cut, "retained_handler_fan_out_") {
		// The killed child cannot release its sealed source projections. Keep
		// every private scratch root under the parent's exact cleanup ownership.
		root := t.TempDir()
		cmd.Env = append(cmd.Env, "TMPDIR="+root)
		t.Cleanup(func() {
			if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return os.Chmod(path, 0o700)
				}
				return nil
			}); err != nil {
				t.Errorf("unseal owned crash fixture for cleanup: %v", err)
			}
		})
	}
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

type selectedCrashActivated struct {
	SelectedContractForkLifecycle
	checkpoint func(string)
}

func (p selectedCrashActivated) ActivateRunForkForSelectedContractExecution(ctx context.Context, request runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	result, err := p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, request)
	if result.Activated {
		p.checkpoint(request.ForkRunID)
	}
	return result, err
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
	if os.Getenv("SWARM_SELECTED_CRASH_CUT") == pendingReplyBeforeJournalCrashCut {
		runPendingEffectReplyCrashHelper(t, ctx, selected, owner, capability)
		return
	}
	if strings.HasPrefix(os.Getenv("SWARM_SELECTED_CRASH_CUT"), "retained_pending_join_") {
		runUnpublishedArrivalCrashHelper(t, ctx, capabilityStore, owner, capability)
		return
	}
	if strings.HasPrefix(os.Getenv("SWARM_SELECTED_CRASH_CUT"), "retained_activity_") {
		runSelectedActivityResultCrashHelper(t, ctx, selected, owner, capability)
		return
	}
	if strings.HasPrefix(os.Getenv("SWARM_SELECTED_CRASH_CUT"), "retained_handler_fan_out_") {
		runSelectedHandlerFanOutCrashHelper(t, ctx, selected, owner, capability)
		return
	}
	root := runForkExecutionRepoRoot(t)
	loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: root, SourceRoot: filepath.Join(root, "tests/tier1-primitives/test-emits-multiple")}
	cut := os.Getenv("SWARM_SELECTED_CRASH_CUT")
	timerCut := strings.HasPrefix(cut, "retained_timer_")
	joinCut := strings.HasPrefix(cut, "retained_join_")
	armedJoinCut := cut == "retained_armed_join_after_activation" || cut == "retained_armed_join_occurrence_committed"
	if timerCut {
		loader.SourceRoot = issue642TimerCrashFixture(t, root)
	} else if joinCut {
		loader.SourceRoot = publishedJoinFixture(t)
	} else if armedJoinCut {
		loader.SourceRoot = armedArrivalFixture(t)
	}
	options := SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}
	var sourceLoader SelectedContractSourceLoader = loader
	loaded, err := sourceLoader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{Selection: runfork.RunForkContractSelection{Mode: "selected_contracts"}})
	if err != nil {
		t.Fatal(err)
	}
	sourceRun, eventID := uuid.NewString(), uuid.NewString()
	if timerCut {
		eventID = seedIssue642TimerCrashSource(t, ctx, selected, owner, loaded, sourceRun)
	} else if joinCut {
		eventID, _ = seedPublishedJoinSource(t, ctx, selected, owner, loaded, sourceRun)
	} else if armedJoinCut {
		eventID = seedArmedArrivalCrashSource(t, ctx, selected, owner, loaded, sourceRun)
	} else {
		seedSelectedOperationSource(t, ctx, backend, db, selected, loaded, sourceRun, eventID)
	}
	checkpointWithSchedule := func(forkRun, scheduleID string) {
		pipe := os.NewFile(3, "selected-checkpoint")
		if err := json.NewEncoder(pipe).Encode(selectedForkCrashCheckpoint{SourceRun: sourceRun, ForkRun: forkRun, ScheduleID: scheduleID}); err != nil {
			t.Fatal(err)
		}
		pipe.Close()
		var b [1]byte
		_, err := os.Stdin.Read(b[:])
		t.Fatalf("checkpoint returned without process death: %v", err)
	}
	checkpoint := func(forkRun string) { checkpointWithSchedule(forkRun, "") }
	if cut == "before_materialization" || cut == "materialized" {
		owner.ports.fork = selectedCrashMaterialization{SelectedContractForkLifecycle: owner.ports.fork, before: cut == "before_materialization", checkpoint: checkpoint}
	} else if cut == "execution_issued" || cut == "execution_claimed" {
		owner.ports.runtimeExecution = selectedCrashExecution{SelectedContractRuntimeExecutionLifecycle: owner.ports.runtimeExecution, issued: cut == "execution_issued", checkpoint: checkpoint}
	} else if cut == "event_committed" || cut == "retained_event_committed" || cut == "retained_join_event_committed" {
		owner.ports.replay = selectedOperationPublicationProbe{SelectedContractReplayPersistence: owner.ports.replay, after: func(ctx context.Context) {
			occurrence, _ := worklifetime.OccurrenceFromContext(ctx)
			checkpoint(occurrence.(*worklifetime.SelectedForkOccurrence).Identity().RunID)
		}}
	} else if cut == "retained_armed_join_after_activation" {
		owner.ports.fork = armedArrivalCrashActivationStore{SelectedContractForkLifecycle: owner.ports.fork, schedules: owner.ports.genericSchedules, checkpoint: checkpointWithSchedule}
	} else if cut == "retained_after_activation" || cut == "retained_timer_after_activation" || cut == "retained_join_after_activation" {
		owner.ports.fork = selectedCrashActivated{SelectedContractForkLifecycle: owner.ports.fork, checkpoint: checkpoint}
	} else if cut == "retained_armed_join_occurrence_committed" {
		owner.ports.genericSchedules = newArmedArrivalCrashScheduleStore(t, owner.ports.genericSchedules, checkpointWithSchedule)
	} else if cut == "retained_timer_occurrence_committed" {
		owner.ports.workflow = newIssue642TimerCrashPersistence(t, selected, checkpoint)
	} else if cut == "before_activation" {
		var discards int
		owner.ports.fork = selectedOperationActivationProbe{SelectedContractForkLifecycle: owner.ports.fork, discards: &discards, before: func(ctx context.Context) {
			occurrence, _ := worklifetime.OccurrenceFromContext(ctx)
			checkpoint(occurrence.(*worklifetime.SelectedForkOccurrence).Identity().RunID)
		}}
	}
	request := SelectedContractExecutionRequest{SourceRunID: sourceRun, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: sourceLoader,
		ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source), AgentRuntime: options}
	if strings.HasPrefix(cut, "retained_") {
		request.ForkOperation = &runfork.ForkOperationRequest{
			OperationID: uuid.NewString(), Actor: "retained-crash", IdempotencyKey: "fixed-cut",
			TransportHash: "retained-crash-transport", SourceRunID: sourceRun, ForkEventID: eventID,
			TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: request.ContractSelection,
		}
	}
	result, err := ExecuteSelectedContractRunFork(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if cut == "retained_event_committed" || cut == "retained_timer_occurrence_committed" || cut == "retained_join_event_committed" || cut == "retained_armed_join_occurrence_committed" {
		<-ctx.Done()
		t.Fatal("retained execution stopped before its publication checkpoint")
	}
	if cut != "execution_returned" {
		t.Fatalf("execution skipped checkpoint %s", cut)
	}
	checkpoint(result.Materialization.ForkRunID)
}
