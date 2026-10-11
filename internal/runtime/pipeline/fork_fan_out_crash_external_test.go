package pipeline_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type mixedFanOutCrashFixture struct {
	backend, address string
	retained         fanoutobligation.InheritedTerminalDisposition
}

type mixedFanOutCrashCheckpoint struct {
	Proof                         storetest.SelectedForkReceiverExecutionProofRequest
	BundleHash, Token             string
	SourceEntityID, ChildEntityID string
	SourceRevision, ChildRevision int
	Authority                     *publishedBarrierCrashAuthority
}

func (f *mixedFanOutCrashFixture) publishCheckpoint(t *testing.T, checkpoint mixedFanOutCrashCheckpoint) {
	t.Helper()
	pipe := os.NewFile(3, "mixed-fanout-checkpoint")
	if err := json.NewEncoder(pipe).Encode(checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := pipe.Close(); err != nil {
		t.Fatal(err)
	}
	var input [1]byte
	_, err := os.Stdin.Read(input[:])
	t.Fatalf("claimed receiver checkpoint returned without process death: %v", err)
}

func TestIssue642MixedFanOutCrashProcessHelper(t *testing.T) {
	backend := os.Getenv("SWARM_MIXED_FANOUT_CRASH_BACKEND")
	if backend == "" {
		return
	}
	runMixedFanOutPendingMatrixWithCrash(t, false, &mixedFanOutCrashFixture{
		backend: backend, address: os.Getenv("SWARM_MIXED_FANOUT_CRASH_ADDRESS"),
		retained: fanoutobligation.InheritedTerminalDisposition(os.Getenv("SWARM_MIXED_FANOUT_CRASH_RETAINED")),
	})
}

func TestIssue642MixedFanOutCrashRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, retained := range []fanoutobligation.InheritedTerminalDisposition{fanoutobligation.InheritedSucceeded, fanoutobligation.InheritedDeadLettered} {
			t.Run(backend+"/"+string(retained), func(t *testing.T) {
				address := filepath.Join(t.TempDir(), "crashed-runtime.db")
				selection := storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: address}
				if backend == "postgres" {
					address = testutil.StartPostgresDSN(t)
					selection = storebackend.Selection{Backend: storebackend.BackendPostgres}
				}
				checkpoint := killMixedFanOutAtClaim(t, backend, address, retained)
				storeRequest := storeselected.RuntimeRequest{Selection: selection}
				if backend == "postgres" {
					storeRequest.PostgresDSN = address
				}
				selected, err := storeselected.OpenRuntime(t.Context(), storeRequest)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = selected.CloseUnactivated() })
				repo, err := filepath.Abs(filepath.Join("..", "..", ".."))
				if err != nil {
					t.Fatal(err)
				}
				_, bundle, err := cliapp.NewSwarmWorkflowModule(repo, canonicalrouting.CopyForkFanOutReceiverOwnership(t), filepath.Join(repo, "platform-spec.yaml"))
				if err != nil || bundle.SourceArtifact.BundleHash() != checkpoint.BundleHash {
					t.Fatalf("restart schema must use the exact original artifact: %v", err)
				}
				plans, err := cliapp.StateStoreSchemaPlans(bundle)
				if err != nil {
					t.Fatal(err)
				}
				if err := selected.Schema().BootstrapSchema(t.Context(), store.SchemaBootstrapRequest{
					PlatformPlans: plans.Platform, StatePlans: plans.State,
					Origin: store.RuntimeStoreOrigin{SwarmVersion: "fork-engine-test", PlatformVersion: bundle.Platform.Platform.Version, CreatedAt: time.Now().UTC()},
				}); err != nil {
					t.Fatal(err)
				}
				runtimeID := uuid.NewString()
				fact, err := correlation.NewSourceArtifactFact(checkpoint.BundleHash)
				if err != nil {
					t.Fatal(err)
				}
				ctx := correlation.WithSourceArtifactFact(correlation.WithRuntimeInstanceID(t.Context(), runtimeID), fact)
				ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, fact.BundleHash()))
				process := worklifetime.NewProcess()
				ctx = worklifetime.WithProcess(ctx, process)
				capability, err := selected.StartupOwnership().AcquireProcessCapability(ctx, startupownership.AcquireRequest{
					OwnerID: "mixed-fanout-restart", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID,
				})
				if err != nil {
					t.Fatal(err)
				}
				family, ok := selected.RunFork()
				if !ok {
					t.Fatal("restart requires the canonical selected fork owner")
				}
				t.Cleanup(func() {
					cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := family.RetireSelectedContexts(cleanup); err != nil {
						t.Error(err)
					}
					process.Retire()
					if _, err := process.Join(cleanup); err != nil {
						t.Error(err)
					}
					if err := capability.Release(cleanup); err != nil {
						t.Error(err)
					}
				})
				if err := family.BindSelectedProcess(ctx, process, capability); err != nil {
					t.Fatal(err)
				}
				baseline := process.ActiveCount()
				beforeSource, err := selected.Entities().LoadOperatorEntity(ctx, checkpoint.SourceEntityID, checkpoint.Proof.SourceRunID)
				if err != nil || beforeSource.Entity.Revision != checkpoint.SourceRevision || beforeSource.Fields["processed_token"] != "seeded" {
					t.Fatalf("crash changed the frozen source receiver: %+v err=%v", beforeSource, err)
				}
				original, found, err := family.Operations().LoadForkOperation(ctx, "mixed-fanout-crash", "pending-cut", "mixed-fanout-crash-transport")
				if err != nil || !found || original.Status != runfork.ForkOperationActivated || original.ForkRunID != checkpoint.Proof.ChildRunID || original.Result == nil {
					t.Fatalf("crash lost exact activation acknowledgment: %+v found=%t err=%v", original, found, err)
				}
				probe := &forkEngineProbe{claimed: make(chan forkEngineObservation, 1), settled: make(chan lifecycleprobe.Signal, 1),
					resume: make(chan struct{}), resumeSettled: make(chan struct{})}
				var release, releaseSettlement sync.Once
				t.Cleanup(func() {
					release.Do(func() { close(probe.resume) })
					releaseSettlement.Do(func() { close(probe.resumeSettled) })
				})
				options := runforkexecution.SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: capability}
				options.AgentManagerOptions.TestLifecycleProbe = probe
				wait, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				recovered, err := family.RecoverSelectedForkContexts(wait, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live), runforkexecution.SelectedForkRecoveryEnvironment{
					SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: selected.SourceArtifactStore()}, AgentRuntime: options,
				})
				if err != nil || len(recovered) != 1 || recovered[0].RunID != checkpoint.Proof.ChildRunID || recovered[0].ExecutionID != checkpoint.Proof.ExecutionID || recovered[0].Disposition != runfork.SelectedForkRecoveryResume {
					t.Fatalf("mixed handler recovery: %+v err=%v", recovered, err)
				}
				var observed forkEngineObservation
				select {
				case observed = <-probe.claimed:
				case <-wait.Done():
					t.Fatal("recovery did not claim the exact pending receiver")
				}
				claim, claimed := deliverylifecycle.ClaimFromContext(observed.ctx)
				admission, admitted := managedexecution.FromContext(observed.ctx)
				if !claimed || !admitted || claim.DeliveryID() != checkpoint.Proof.DeliveryID || observed.id != checkpoint.Proof.ChildEventID ||
					uint64(claim.Version()) <= checkpoint.Proof.ClaimVersion || admission.Generation <= checkpoint.Proof.Generation ||
					admission.ExecutionAuthorityID == checkpoint.Proof.ExecutionID ||
					!admission.AuthorizesSelected(admission.ExecutionAuthorityID, checkpoint.Proof.ChildRunID, admission.Generation) {
					t.Fatalf("recovery reused a historical claim/grant or changed owed identity: checkpoint=%+v event=%s claim=%+v admission=%+v", checkpoint, observed.id, claim, admission)
				}
				request := checkpoint.Proof
				request.ExecutionID = admission.ExecutionAuthorityID
				request.Generation, request.ClaimVersion = admission.Generation, uint64(claim.Version())
				physical := readForkEngineExecutionProof(t, wait, selected, request)
				if physical.ExecutionCount != 1 || physical.PublicationCount != 1 || physical.BindingID != request.BindingID {
					t.Fatalf("fresh successor grant lacks its exact native execution/publication/binding: %+v", physical)
				}
				release.Do(func() { close(probe.resume) })
				select {
				case signal := <-probe.settled:
					if signal.EventID != observed.id || signal.Status != "delivered" {
						t.Fatalf("recovered receiver changed its disposition: %+v", signal)
					}
				case <-wait.Done():
					t.Fatal(wait.Err())
				}
				settlement, err := selected.RuntimeDeps().DeliveryStore.Snapshot(wait, claim.DeliveryID())
				if err != nil || settlement.Status != deliverylifecycle.StatusDelivered || !settlement.MatchesSettlementClaim(claim) {
					t.Fatalf("recovered settlement lost its current claim: %+v err=%v", settlement, err)
				}
				releaseSettlement.Do(func() { close(probe.resumeSettled) })
				waitForkEnginePipelineSettlement(t, wait, selected, request, settlement)
				child, err := selected.Entities().LoadOperatorEntity(wait, checkpoint.ChildEntityID, checkpoint.Proof.ChildRunID)
				if err != nil || child.Fields["processed_token"] != checkpoint.Token || child.Entity.Revision != checkpoint.ChildRevision+1 {
					t.Fatalf("crash/restart lost once-only child business mutation: %+v err=%v", child, err)
				}
				report, err := selected.Runs().LoadRunDebugReport(wait, checkpoint.Proof.ChildRunID, operatorread.RunDebugQueryOptions{})
				if err != nil || report.FanOut.Intents != 1 || report.FanOut.Cardinality != 1 || report.FanOut.Cursor != 1 || report.FanOut.Committed != 1 ||
					report.FanOut.Settled != 1 || report.FanOut.Unsettled != 0 || report.FanOut.Owed != 0 ||
					len(report.Deliveries) != 1 || report.Deliveries[0].SubscriberID != claim.SubscriberID() || report.Deliveries[0].Status != "delivered" || report.Deliveries[0].Count != 1 {
					t.Fatalf("restart duplicated settled siblings or lost the continued ordinal: %+v err=%v", report, err)
				}
				outcomes, err := storetest.ReadFanOutPublishedOutcomeStorage(wait, selected.RuntimeDeps().DeliveryStore, checkpoint.Proof.ChildRunID)
				if err != nil || len(outcomes) != 1 || outcomes[0].Ordinal != 0 || outcomes[0].EventID != checkpoint.Proof.ChildEventID || outcomes[0].Kind != string(fanoutobligation.OutcomeContinued) {
					t.Fatalf("restart replaced or duplicated the original continued publication: %+v err=%v", outcomes, err)
				}
				cut, err := family.Plan(wait, runfork.RunForkPlanRequest{SourceRunID: checkpoint.Proof.ChildRunID, At: checkpoint.Proof.ChildEventID})
				if err != nil {
					t.Fatal(err)
				}
				assertMixedFanOutPendingPlan(t, cut, checkpoint.Proof.ChildEventID, retained)
				afterSource, err := selected.Entities().LoadOperatorEntity(wait, checkpoint.SourceEntityID, checkpoint.Proof.SourceRunID)
				if err != nil || afterSource.Entity.Revision != beforeSource.Entity.Revision || !reflect.DeepEqual(afterSource.Fields, beforeSource.Fields) {
					t.Fatalf("restart changed source business state: %+v err=%v", afterSource, err)
				}
				again, found, err := family.Operations().LoadForkOperation(wait, original.Request.Actor, original.Request.IdempotencyKey, original.Request.TransportHash)
				if err != nil || !found || !reflect.DeepEqual(original, again) {
					t.Fatalf("restart changed the permanent acknowledgment: %+v err=%v", again, err)
				}
				cleanup, cleanupCancel := context.WithTimeout(context.WithoutCancel(wait), 10*time.Second)
				defer cleanupCancel()
				if err := family.RetireSelectedContexts(cleanup); err != nil || process.ActiveCount() != baseline {
					t.Fatalf("recovered selected work did not join retirement: work=%d baseline=%d err=%v", process.ActiveCount(), baseline, err)
				}
			})
		}
	}
}

func killMixedFanOutAtClaim(t *testing.T, backend, address string, retained fanoutobligation.InheritedTerminalDisposition) mixedFanOutCrashCheckpoint {
	t.Helper()
	return killSelectedReceiverAtClaim(t, "TestIssue642MixedFanOutCrashProcessHelper", []string{
		"SWARM_MIXED_FANOUT_CRASH_BACKEND=" + backend, "SWARM_MIXED_FANOUT_CRASH_ADDRESS=" + address,
		"SWARM_MIXED_FANOUT_CRASH_RETAINED=" + string(retained),
	})
}

func killSelectedReceiverAtClaim(t *testing.T, helper string, environment []string) mixedFanOutCrashCheckpoint {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	root := t.TempDir()
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
			t.Errorf("unseal only the owned crash fixture for cleanup: %v", err)
		}
	})
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+helper+"$")
	cmd.Env = append(append(os.Environ(), "TMPDIR="+root), environment...)
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
	var checkpoint mixedFanOutCrashCheckpoint
	decodeErr := json.NewDecoder(reader).Decode(&checkpoint)
	killErr := cmd.Process.Kill()
	waitErr := cmd.Wait()
	var exited *exec.ExitError
	if decodeErr != nil || killErr != nil || ctx.Err() != nil || !errors.As(waitErr, &exited) {
		t.Fatalf("mixed claim checkpoint: decode=%v kill=%v wait=%v context=%v\n%s", decodeErr, killErr, waitErr, ctx.Err(), output.String())
	}
	if status, ok := exited.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("mixed receiver process did not die by SIGKILL: %v", waitErr)
	}
	return checkpoint
}
