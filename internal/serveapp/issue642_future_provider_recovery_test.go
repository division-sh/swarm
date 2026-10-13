package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

const issue642FutureRecoveryHelperEnv = "SWARM_TEST_ISSUE642_FUTURE_RECOVERY"

// H retains the real authored provider, gateway, native source prefix and serve
// recovery composition. The outer process kills an actual selected agent claim,
// not a predicted destination, materialization checkpoint or synthetic receipt.
func TestIssue642FutureProviderProcessDeathRecoveryBothStores(t *testing.T) {
	for _, minted := range []bool{false, true} {
		name := "keyed"
		if minted {
			name = "minted"
		}
		for _, backend := range []string{"sqlite", "postgres"} {
			t.Run(name+"/"+backend, func(t *testing.T) {
				var root string
				if minted {
					root = canonicalrouting.CopySelectedFutureMintedProviderInput(t)
				} else {
					root = canonicalrouting.CopySelectedFutureKeyedProviderInput(t)
				}
				location := filepath.Join(t.TempDir(), "future-recovery.sqlite")
				if backend == "postgres" {
					location = storetest.PostgresFixtureLocation(t)
				}
				environment := []string{
					issue642FutureRecoveryHelperEnv + "=1", "TEST_642_BACKEND=" + backend,
					"TEST_642_SOURCE=" + root, "TEST_642_LOCATION=" + location,
					"TEST_642_MINTED=" + strconv.FormatBool(minted), "TMPDIR=" + ownedMockLifecycleRoot(t),
				}
				first, frames := issue642StartFutureRecoveryProcess(t, environment, "crash", nil)
				issue642ReadFutureRecoveryFrame(t, first, frames, "ready", serveRuntimeReadyTimeout)
				issue642ReadFutureRecoveryFrame(t, first, frames, "source", 30*time.Second)
				cut := issue642ReadFutureRecoveryFrame(t, first, frames, "cut", 30*time.Second).Proof
				if cut == nil {
					t.Fatal("claim cut omitted its native checkpoint")
				}
				if err := first.kill(); err != nil {
					t.Fatal(err)
				}
				var exit *exec.ExitError
				if !errors.As(first.waitError(), &exit) {
					t.Fatalf("claim cut did not die by signal: %v\n%s", first.waitError(), first.output.String())
				}
				status, ok := exit.Sys().(syscall.WaitStatus)
				if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
					t.Fatalf("claim cut exit=%v, want SIGKILL", exit.Sys())
				}
				for _, mode := range []string{"resume", "terminal"} {
					process, frames := issue642StartFutureRecoveryProcess(t, environment, mode, cut)
					issue642ReadFutureRecoveryFrame(t, process, frames, "ready", serveRuntimeReadyTimeout)
					result := issue642ReadFutureRecoveryFrame(t, process, frames, "done", 30*time.Second).Proof
					select {
					case <-process.exited:
					case <-time.After(10 * time.Second):
						t.Fatalf("%s helper did not join within cleanup bound\n%s", mode, process.output.String())
					}
					if process.waitError() != nil || result == nil {
						t.Fatalf("%s native recovery failed: %v\n%s", mode, process.waitError(), process.output.String())
					}
					cut = result
				}
			})
		}
	}
}

type issue642FutureClaimFacts struct {
	RunID, EventID, DeliveryID, AgentID, RouteIdentity, ConstructedKey string
	ClaimVersion                                                       int64
	Target                                                             operatorread.OperatorDeliveryTarget
	Scope                                                              authoractivity.Scope
	ExecutionID                                                        string
	Generation, Fence                                                  uint64
	Admission                                                          managedexecution.Admission
	Grant                                                              startupownership.GrantEvidence
}

type issue642FutureExecutionRow struct {
	ExecutionID, BindingID, State string
	Generation, Fence             uint64
	Terminal, LeaseEmpty, Clean   bool
}

type issue642FutureRecoveryProof struct {
	SourceRunID, SourceKey string
	ProcessBaseline        uint64
	ACK                    runfork.ForkOperationRecord
	Claim                  issue642FutureClaimFacts
	RecoveredClaim         *issue642FutureClaimFacts
	SourceDomain           map[string]storetest.SelectedForkStorageTableSnapshot
	Executions             []issue642FutureExecutionRow
}

type issue642FutureRecoveryFrame struct {
	Phase string
	Proof *issue642FutureRecoveryProof
	Err   string
}

func issue642StartFutureRecoveryProcess(t *testing.T, environment []string, mode string, proof *issue642FutureRecoveryProof) (*channelOnboardingCrashServeProcess, <-chan issue642FutureRecoveryFrame) {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	input, send, err := os.Pipe()
	if err != nil {
		read.Close()
		write.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { read.Close(); send.Close() })
	process := startServedCrashProcessBeforeReadiness(t, "TestIssue642FutureProviderRecoveryProcessHelper", append(environment, "TEST_642_PHASE="+mode), write, input)
	write.Close()
	input.Close()
	// Bound failed helper cleanup before the older generic stop cleanup runs.
	t.Cleanup(func() { _ = process.kill() })
	frames := make(chan issue642FutureRecoveryFrame, 4)
	go func() {
		defer close(frames)
		decoder := json.NewDecoder(read)
		for {
			var frame issue642FutureRecoveryFrame
			if err := decoder.Decode(&frame); err != nil {
				frames <- issue642FutureRecoveryFrame{Err: err.Error()}
				return
			}
			frames <- frame
		}
	}()
	encoded := make(chan error, 1)
	go func() {
		encoded <- json.NewEncoder(send).Encode(proof)
		send.Close()
	}()
	select {
	case err := <-encoded:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("native checkpoint handoff exceeded request bound")
	}
	return process, frames
}

func issue642ReadFutureRecoveryFrame(t *testing.T, process *channelOnboardingCrashServeProcess, frames <-chan issue642FutureRecoveryFrame, phase string, bound time.Duration) issue642FutureRecoveryFrame {
	t.Helper()
	select {
	case frame, ok := <-frames:
		if !ok || frame.Err != "" || frame.Phase != phase {
			t.Fatalf("native recovery phase %s: %+v\n%s", phase, frame, process.output.String())
		}
		return frame
	case <-time.After(bound):
		t.Fatalf("native recovery phase %s exceeded %s\n%s", phase, bound, process.output.String())
		return issue642FutureRecoveryFrame{}
	}
}

func TestIssue642FutureProviderRecoveryProcessHelper(t *testing.T) {
	if os.Getenv(issue642FutureRecoveryHelperEnv) != "1" {
		t.Skip("subprocess helper")
	}
	out, input := os.NewFile(3, "native-proof"), os.NewFile(4, "prior-native-proof")
	defer out.Close()
	defer input.Close()
	var proof *issue642FutureRecoveryProof
	if err := json.NewDecoder(input).Decode(&proof); err != nil {
		t.Fatal(err)
	}
	emit := func(phase string) {
		if err := json.NewEncoder(out).Encode(issue642FutureRecoveryFrame{Phase: phase, Proof: proof}); err != nil {
			t.Fatal(err)
		}
	}
	mode := os.Getenv("TEST_642_PHASE")
	minted := os.Getenv("TEST_642_MINTED") == "true"
	probe := issue642InstallFutureRecoveryProbe(t, mode, minted, proof)
	_, start := issue2564ServeHarnessAtLocation(t, os.Getenv("TEST_642_BACKEND"), os.Getenv("TEST_642_SOURCE"), true, os.Getenv("TEST_642_LOCATION"))
	process, rt := start()
	t.Cleanup(func() {
		probe.release()
		process.cancel()
		if code, joined := process.waitForExit(10 * time.Second); !joined || code != 0 {
			t.Errorf("native future-provider cleanup code=%d joined=%t\n%s", code, joined, process.outputString())
		}
	})
	emit("ready")
	if mode == "crash" {
		response := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "event.publish", map[string]any{
			"event_name": "start.requested", "bundle_hash": rt.BundleHash,
			"payload": map[string]any{"work_id": "one"}, "idempotency_key": "future-recovery-source",
		}, 30*time.Second)
		if response.Error != nil {
			t.Fatalf("native source prefix: %+v", response.Error)
		}
		var seed servedEventPublishRPCResult
		if err := json.Unmarshal(response.Result, &seed); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		err := issue642WaitRecoveryBusiness(ctx, rt, seed.RunID)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		rt.waitDeliveries(t, seed.RunID)
		issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
		issue642RequireKeyedProviderResult(t, rt, seed.RunID)
		proof = &issue642FutureRecoveryProof{SourceRunID: seed.RunID, ProcessBaseline: rt.Runtime.Options.ProcessWorkOwner.ActiveCount()}
		if minted {
			proof.SourceKey = issue642RequireMintedProviderIdentity(t, rt, seed.RunID)
		}
		proof.SourceDomain, err = storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
		if err != nil {
			t.Fatal(err)
		}
		emit("source")
		fork := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, map[string]any{
			"source_run_id": seed.RunID, "at_start": true, "allow_source_freeze": true,
			"idempotency_key": "future-recovery-start-cut",
		})
		if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID ||
			fork.ForkPointKind != string(runfork.RunForkPointRunStart) || fork.ForkEventID != "" || fork.ForkRunStatus != runfork.RunForkActivatedStatus {
			t.Fatalf("native start-cut ACK: %+v", fork)
		}
		observation := probe.waitClaim(t)
		if observation.err != nil {
			t.Fatal(observation.err)
		}
		proof.Claim = observation.facts
		if proof.Claim.RunID != fork.ForkRunID || (minted && proof.Claim.ConstructedKey == proof.SourceKey) {
			t.Fatalf("native construction escaped its child: %+v", proof.Claim)
		}
		probe.mu.Lock()
		operationID, operations, preparation := probe.operationID, probe.operations, probe.preparation
		probe.mu.Unlock()
		if operationID == "" || preparation == nil || preparation.MaterializationRequired || !preparation.MaterializationSupported ||
			len(preparation.ConfiguredAgentPlans) != 0 || len(preparation.AgentRecipientPlans) != 0 {
			t.Fatalf("start-cut preparation invented a future actor: operation=%s preparation=%+v", operationID, preparation)
		}
		var found bool
		proof.ACK, found, err = operations.LoadForkOperationByID(t.Context(), operationID)
		if err != nil || !found || proof.ACK.Validate() != nil || proof.ACK.Status != runfork.ForkOperationActivated ||
			proof.ACK.ForkRunID != fork.ForkRunID || proof.ACK.Result.ForkRunStatus != fork.ForkRunStatus {
			t.Fatalf("activation did not retain its permanent ACK: %+v err=%v", proof.ACK, err)
		}
		issue642RequireFutureClaimCut(t, rt, proof.Claim)
		proof.Executions, err = issue642ReadFutureExecutions(t.Context(), rt.selected, fork.ForkRunID)
		if err != nil || len(proof.Executions) != 1 || proof.Executions[0].ExecutionID != proof.Claim.ExecutionID ||
			proof.Executions[0].BindingID != proof.ACK.BindingID || proof.Executions[0].Generation != proof.Claim.Generation ||
			proof.Executions[0].Fence != proof.Claim.Fence || proof.Executions[0].State != "running" || !proof.Executions[0].Clean {
			t.Fatalf("claim cut lost its one running native executor: %+v err=%v", proof.Executions, err)
		}
		emit("cut")
		// Only the outer SIGKILL ends the successful cut; cleanup never owns it.
		<-t.Context().Done()
		t.Fatal("claim cut canceled instead of SIGKILL")
	}
	if proof == nil || (mode != "resume" && mode != "terminal") {
		t.Fatalf("invalid native recovery phase %q", mode)
	}
	probe.mu.Lock()
	recovered := probe.recovered
	probe.mu.Unlock()
	want := runfork.SelectedForkRecoveryResume
	if mode == "terminal" {
		want = runfork.SelectedForkRecoveryTerminal
	}
	if len(recovered) != 1 || recovered[0].RunID != proof.Claim.RunID || recovered[0].Disposition != want ||
		!reflect.DeepEqual(recovered[0].Operation, &proof.ACK) {
		t.Fatalf("actual native recovery lost ACK/disposition: %+v", recovered)
	}
	phase, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	issue642RequireFutureSourceStartupDiagnostic(t, phase, rt, proof)
	if mode == "resume" {
		observation := probe.waitClaim(t)
		if observation.err != nil {
			t.Fatal(observation.err)
		}
		proof.RecoveredClaim = &observation.facts
		if err := observation.occurrence.Wait(phase); err != nil {
			t.Fatalf("successor native Manager/selected companion join: %v", err)
		}
		select {
		case <-observation.grant.Done():
		case <-phase.Done():
			t.Fatalf("successor native grant did not retire: %v", phase.Err())
		}
	}
	probe.mu.Lock()
	grants, claims := len(probe.grants), probe.claims
	probe.mu.Unlock()
	if mode == "resume" && (grants != 1 || claims != 1) {
		t.Fatalf("successor admission grants=%d claims=%d, want one each", grants, claims)
	}
	if mode == "terminal" && (grants != 0 || claims != 0) {
		t.Fatalf("terminal recovery issued a third executor: grants=%d claims=%d", grants, claims)
	}
	for work := rt.Runtime.Options.ProcessWorkOwner.ActiveCount(); work != proof.ProcessBaseline; work = rt.Runtime.Options.ProcessWorkOwner.ActiveCount() {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-phase.Done():
			t.Fatalf("native recovery process work=%d baseline=%d: %v", work, proof.ProcessBaseline, phase.Err())
		}
	}
	ack, found, err := probe.operations.LoadForkOperationByID(t.Context(), proof.ACK.Request.OperationID)
	if err != nil || !found || !reflect.DeepEqual(ack, proof.ACK) {
		t.Fatalf("terminal child recast the permanent running ACK: %+v err=%v", ack, err)
	}
	issue642RequireKeyedProviderResult(t, rt, proof.Claim.RunID)
	issue642RequireFutureRecoveredOutcome(t, rt, proof)
	rows, err := issue642ReadFutureExecutions(t.Context(), rt.selected, proof.Claim.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "terminal" && !reflect.DeepEqual(rows, proof.Executions) {
		t.Fatalf("terminal re-recovery changed its exact two executor rows: before=%+v after=%+v", proof.Executions, rows)
	}
	proof.Executions = rows
	process.cancel()
	if code, joined := process.waitForExit(10 * time.Second); !joined || code != 0 || rt.Runtime.Options.ProcessWorkOwner.ActiveCount() != 0 {
		t.Fatalf("native owner shutdown code=%d joined=%t process_work=%d\n%s", code, joined, rt.Runtime.Options.ProcessWorkOwner.ActiveCount(), process.outputString())
	}
	emit("done")
}

type issue642FutureClaimObservation struct {
	facts      issue642FutureClaimFacts
	occurrence *worklifetime.ManagerWorkOccurrence
	grant      startupownership.GenerationGrant
	err        error
}

type issue642FutureRecoveryProbe struct {
	mu          sync.Mutex
	mode        string
	minted      bool
	expected    *issue642FutureRecoveryProof
	fixture     issue2564ServedFixture
	operations  apiv1.RunForkOperationReader
	retire      func(context.Context) error
	operationID string
	preparation *runforkexecution.SelectedContractAgentRuntimeMaterialization
	grants      map[string]startupownership.GenerationGrant
	claims      int
	observed    chan issue642FutureClaimObservation
	recovered   []runfork.SelectedForkRecoveryResult
	gate        chan struct{}
	releaseOnce sync.Once
}

func (p *issue642FutureRecoveryProbe) release() { p.releaseOnce.Do(func() { close(p.gate) }) }

func (p *issue642FutureRecoveryProbe) waitClaim(t *testing.T) issue642FutureClaimObservation {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	diagnostic := time.NewTimer(15 * time.Second)
	defer diagnostic.Stop()
	for {
		select {
		case observation := <-p.observed:
			return observation
		case <-diagnostic.C:
			if p.expected != nil {
				claim := p.expected.Claim
				snapshot, snapshotErr := p.fixture.persistence.deps.DeliveryStore.Snapshot(t.Context(), claim.DeliveryID)
				header, headerErr := p.fixture.selected.LoadRunHeader(t.Context(), claim.RunID)
				debug, debugErr := p.fixture.selected.LoadRunDebugReport(t.Context(), claim.RunID, operatorread.RunDebugQueryOptions{})
				failure, failureErr := json.Marshal(header.Failure)
				if header.Failure != nil {
					cleanup, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
					retireErr := p.retire(cleanup)
					cancel()
					t.Fatalf("native successor failed before provider claim: failure=%s; exact retirement diagnostics: %v", failure, retireErr)
				}
				fmt.Printf("future_recovery_wait_before_cancellation mode=%s failure=%s failure_encode_error=%v snapshot=%+v snapshot_error=%v header=%+v header_error=%v debug=%+v debug_error=%v\n", p.mode, failure, failureErr, snapshot, snapshotErr, header, headerErr, debug, debugErr)
			}
		case <-deadline.C:
			t.Fatal("selected native agent claim never reached its pre-provider probe")
			return issue642FutureClaimObservation{}
		}
	}
}

// The capability delegates issuance and returns the original native grant.
// Neither captured evidence nor materialization is substituted for currentness.
type issue642FutureGrantCapture struct {
	startupownership.ProcessCapability
	probe *issue642FutureRecoveryProbe
}

func (c issue642FutureGrantCapture) IssueSelectedForkGenerationGrant(ctx context.Context, request startupownership.SelectedForkGrantRequest) (startupownership.GenerationGrant, error) {
	grant, err := c.ProcessCapability.IssueSelectedForkGenerationGrant(ctx, request)
	if grant != nil {
		c.probe.mu.Lock()
		c.probe.grants[request.Binding.ExecutionID] = grant
		c.probe.mu.Unlock()
	}
	return grant, err
}

func issue642InstallFutureRecoveryProbe(t *testing.T, mode string, minted bool, expected *issue642FutureRecoveryProof) *issue642FutureRecoveryProbe {
	t.Helper()
	probe := &issue642FutureRecoveryProbe{mode: mode, minted: minted, expected: expected,
		grants: make(map[string]startupownership.GenerationGrant), observed: make(chan issue642FutureClaimObservation, 2), gate: make(chan struct{})}
	previous := buildSelectedAPICapabilities
	buildSelectedAPICapabilities = func(owner *selectedStoreOwner, request selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
		caps, err := previous(owner, request)
		if err != nil {
			return caps, err
		}
		executor, ok := caps.RunFork.(apiv1.SelectedContractRunForkExecutor)
		if !ok || request.ProcessCapability == nil || caps.SelectedForkProcess == nil {
			return caps, fmt.Errorf("future recovery requires native selected executor/process, got %T", caps.RunFork)
		}
		deps := owner.RuntimeDeps()
		reader, ok := deps.EventStore.(issue2564ServedObservationOwner)
		if !ok {
			return caps, fmt.Errorf("future recovery lacks native observation owner: %T", deps.EventStore)
		}
		probe.mu.Lock()
		probe.fixture = issue2564ServedFixture{selected: reader, persistence: projectServeRuntimePersistence(owner)}
		probe.operations = caps.RunForkOperations
		probe.retire = caps.SelectedForkRetirement.RetireSelectedContexts
		probe.mu.Unlock()
		executor.AgentRuntime.ProcessCapability = issue642FutureGrantCapture{ProcessCapability: request.ProcessCapability, probe: probe}
		executor.AgentRuntime.AgentManagerOptions.TestLifecycleProbe = probe
		caps.SelectedForkRecovery.AgentRuntime = executor.AgentRuntime
		execute := executor.ExecuteSelectedContractRunFork
		executor.ExecuteSelectedContractRunFork = func(ctx context.Context, req runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
			if req.ForkOperation != nil {
				probe.mu.Lock()
				probe.operationID = req.ForkOperation.OperationID
				probe.mu.Unlock()
			}
			result, err := execute(ctx, req)
			probe.mu.Lock()
			probe.preparation = result.AgentRuntimeMaterialization
			probe.mu.Unlock()
			return result, err
		}
		caps.RunFork = executor
		caps.SelectedForkProcess = &issue642FutureRecoveryOwner{selectedForkProcessOwner: caps.SelectedForkProcess, probe: probe}
		return caps, nil
	}
	t.Cleanup(func() { probe.release(); buildSelectedAPICapabilities = previous })
	return probe
}

func (p *issue642FutureRecoveryProbe) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	authority, selected := effects.AuthorityFromContext(ctx)
	if !selected || authority.Kind != effects.AuthoritySelectedContractFork || signal.Kind != lifecycleprobe.DeliveryStatusChanged ||
		signal.EventType != "work.assigned" || signal.SubscriberType != "agent" || signal.Status != "in_progress" {
		return
	}
	p.mu.Lock()
	p.claims++
	fixture, grant, count := p.fixture, p.grants[authority.SelectedFork.ExecutionID], p.claims
	p.mu.Unlock()
	observation := issue642FutureClaimObservation{grant: grant}
	observation.facts, observation.occurrence, observation.err = issue642ObserveFutureClaim(ctx, fixture, signal, authority, grant, p.minted)
	if count != 1 {
		observation.err = errors.Join(observation.err, fmt.Errorf("selected assignment claimed %d times in this boot", count))
	}
	if p.mode == "resume" && observation.err == nil {
		old, next := p.expected.Claim, observation.facts
		if next.RunID != old.RunID || next.EventID != old.EventID || next.DeliveryID != old.DeliveryID ||
			next.AgentID != old.AgentID || next.RouteIdentity != old.RouteIdentity || next.Target != old.Target ||
			next.ConstructedKey != old.ConstructedKey || next.Scope.BundleHash != old.Scope.BundleHash || next.Scope.RuntimeInstanceID == old.Scope.RuntimeInstanceID || next.ClaimVersion != old.ClaimVersion+1 ||
			next.ExecutionID == old.ExecutionID || next.Generation != old.Generation+1 || next.Fence != 1 ||
			next.Grant.SelectedFork.BindingID != old.Grant.SelectedFork.BindingID || next.Grant.GrantID == old.Grant.GrantID ||
			next.Grant.ProcessBootID == old.Grant.ProcessBootID || next.Grant.ProcessAuthorityID == old.Grant.ProcessAuthorityID {
			observation.err = fmt.Errorf("recovered claim changed assignment/construction or lost successor fence: old=%+v next=%+v", old, next)
		}
	}
	if observation.err != nil {
		fmt.Printf("future_recovery_claim mode=%s error=%v\n", p.mode, observation.err)
	}
	p.observed <- observation
	if p.mode == "crash" || observation.err != nil {
		select {
		case <-p.gate:
		case <-ctx.Done():
		}
	}
}

func issue642ObserveFutureClaim(ctx context.Context, fixture issue2564ServedFixture, signal lifecycleprobe.Signal, authority effects.Authority, grant startupownership.GenerationGrant, minted bool) (issue642FutureClaimFacts, *worklifetime.ManagerWorkOccurrence, error) {
	var facts issue642FutureClaimFacts
	claim, claimed := deliverylifecycle.ClaimFromContext(ctx)
	route, routed := deliverylifecycle.RouteFromContext(ctx)
	admission, admitted := managedexecution.FromContext(ctx)
	scope, scoped := authoractivity.ScopeFromContext(ctx)
	work, owned := worklifetime.OccurrenceFromContext(ctx)
	// Manager work composes its own occurrence with the selected companion.
	// Its native Wait joins both; the grant, not that lifetime, admits execution.
	occurrence, exact := work.(*worklifetime.ManagerWorkOccurrence)
	if !claimed || claim.Validate() != nil || !routed || !admitted || !scoped || !owned || !exact || !authority.Valid() || grant == nil {
		return facts, occurrence, fmt.Errorf("native claim context: claimed=%t valid_claim=%t routed=%t admitted=%t scoped=%t owned=%t owner=%T exact_manager=%t valid_authority=%t grant=%t", claimed, claim.Validate() == nil, routed, admitted, scoped, owned, work, exact, authority.Valid(), grant != nil)
	}
	if err := grant.ProveCurrent(ctx); err != nil {
		return facts, occurrence, fmt.Errorf("native grant is not current at actual selected claim: %w", err)
	}
	evidence, err := grant.Evidence()
	if err != nil || evidence.Validate() != nil || evidence.SelectedFork == nil || evidence.State != startupownership.GrantAdmitted {
		return facts, occurrence, fmt.Errorf("actual claim lacks admitted native selected grant: %+v err=%v", evidence, err)
	}
	binding := evidence.SelectedFork
	identity, err := route.Identity()
	if err != nil || claim.RouteIdentity() != events.EncodeDeliveryRouteIdentity(identity) || claim.SubscriberClass() != deliverylifecycle.SubscriberAgent ||
		claim.SubscriberID() != signal.SubscriberID || claim.RunID() != authority.SelectedFork.ForkRunID ||
		binding.ExecutionID != authority.SelectedFork.ExecutionID || binding.ExecutionGeneration != authority.SelectedFork.Generation ||
		binding.ForkRunID != claim.RunID() || binding.FenceGeneration != authority.FenceGeneration || binding.ExecutionOwner != authority.ExecutionOwner ||
		binding.AdmissionFingerprint != authority.SelectedFork.AdmissionFingerprint || binding.ActorCensusFingerprint != authority.SelectedFork.ActorCensusFingerprint ||
		!admission.AuthorizesSelected(binding.ExecutionID, binding.ForkRunID, binding.ExecutionGeneration) ||
		admission.ActorCensusFingerprint != binding.ActorCensusFingerprint || admission.BundleHash != evidence.BundleHash ||
		scope != authoractivity.BundleScope(evidence.RuntimeInstanceID, evidence.BundleHash) {
		return facts, occurrence, fmt.Errorf("native grant/claim/route/managed admission disagree: grant=%+v authority=%+v admission=%+v", evidence, authority, admission)
	}
	page, err := fixture.selected.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: claim.RunID(), EventName: "work.assigned"}, Limit: 2})
	if err != nil || page.NextCursor != "" || len(page.Events) != 1 || page.Events[0].EventID != signal.EventID || len(page.Events[0].Deliveries) != 1 {
		return facts, occurrence, fmt.Errorf("native claim lacks its one canonical assigned event: %+v err=%v", page, err)
	}
	delivery := page.Events[0].Deliveries[0]
	snapshot, err := fixture.persistence.deps.DeliveryStore.Snapshot(ctx, claim.DeliveryID())
	if err != nil || snapshot.EventID != signal.EventID || snapshot.RunID != claim.RunID() || snapshot.ClaimVersion != claim.Version() ||
		snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.SubscriberID != signal.SubscriberID ||
		delivery.DeliveryID != claim.DeliveryID() || !reflect.DeepEqual(delivery.Route, route) || delivery.Target.FlowID != "work" || delivery.Target.EntityID == "" ||
		snapshot.RouteIdentity != identity || !reflect.DeepEqual(snapshot.Route, route) || snapshot.Authority.ExecutionID() != binding.ExecutionID || snapshot.Authority.Generation() != binding.ExecutionGeneration {
		return facts, occurrence, fmt.Errorf("native claim readback disagrees with assigned route: snapshot=%+v delivery=%+v err=%v", snapshot, delivery, err)
	}
	worker, err := fixture.selected.LoadOperatorEntity(ctx, delivery.Target.EntityID, claim.RunID())
	keyField := "work_id"
	if minted {
		keyField = "case_id"
	}
	key, ok := worker.Fields[keyField].(string)
	if err != nil || !ok || key == "" || worker.Entity.CurrentState != "pending" || worker.Entity.FlowInstance != delivery.Target.FlowInstance || page.Events[0].Payload["work_id"] != "one" {
		return facts, occurrence, fmt.Errorf("claim precedes or escapes actual native construction: worker=%+v event=%+v err=%v", worker, page.Events[0], err)
	}
	if minted {
		if _, err := uuid.Parse(key); err != nil {
			return facts, occurrence, fmt.Errorf("native minted constructor key: %w", err)
		}
	} else if key != "one" {
		return facts, occurrence, fmt.Errorf("native keyed constructor key=%q, want one", key)
	}
	facts = issue642FutureClaimFacts{RunID: claim.RunID(), EventID: signal.EventID, DeliveryID: claim.DeliveryID(),
		AgentID: claim.SubscriberID(), RouteIdentity: claim.RouteIdentity(), ConstructedKey: key, ClaimVersion: claim.Version(),
		Target: delivery.Target, Scope: scope, ExecutionID: binding.ExecutionID, Generation: binding.ExecutionGeneration,
		Fence: binding.FenceGeneration, Admission: admission, Grant: evidence}
	return facts, occurrence, nil
}

type issue642FutureRecoveryOwner struct {
	selectedForkProcessOwner
	probe   *issue642FutureRecoveryProbe
	process *worklifetime.Process
}

func (o *issue642FutureRecoveryOwner) BindSelectedProcess(ctx context.Context, process *worklifetime.Process, capability startupownership.ProcessCapability) error {
	if err := o.selectedForkProcessOwner.BindSelectedProcess(ctx, process, capability); err != nil {
		return err
	}
	o.process = process
	return nil
}

func (o *issue642FutureRecoveryOwner) RecoverSelectedForkContexts(ctx context.Context, request effects.RecoveryRequest, environment runforkexecution.SelectedForkRecoveryEnvironment) ([]runfork.SelectedForkRecoveryResult, error) {
	baseline := o.process.ActiveCount()
	fmt.Printf("future_recovery_enter mode=%s process_work=%d\n", o.probe.mode, baseline)
	results, err := o.selectedForkProcessOwner.RecoverSelectedForkContexts(ctx, request, environment)
	fmt.Printf("future_recovery_native_return mode=%s results=%+v error=%v\n", o.probe.mode, results, err)
	if err != nil || o.probe.mode == "crash" {
		return results, err
	}
	p := o.probe
	if p.expected == nil || len(results) != 1 || results[0].RunID != p.expected.Claim.RunID || !reflect.DeepEqual(results[0].Operation, &p.expected.ACK) {
		return results, fmt.Errorf("native recovery omitted the exact permanent selected ACK: %+v", results)
	}
	// Observe the native acknowledgment without delaying ordinary startup.
	// Pending dispatch and readiness consumers must start before we await work.
	if p.mode == "resume" {
		if results[0].Disposition != runfork.SelectedForkRecoveryResume || results[0].ExecutionID != p.expected.Claim.ExecutionID {
			return results, fmt.Errorf("selected claim cut was not resumed by its native predecessor: %+v", results)
		}
	} else if results[0].Disposition != runfork.SelectedForkRecoveryTerminal {
		return results, fmt.Errorf("terminal re-recovery was executable: %+v", results)
	}
	p.mu.Lock()
	p.recovered = results
	p.mu.Unlock()
	return results, nil
}

func issue642WaitRecoveryBusiness(ctx context.Context, fixture issue2564ServedFixture, runID string) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		header, err := fixture.selected.LoadRunHeader(ctx, runID)
		if err != nil {
			return err
		}
		summary, err := fixture.selected.SummarizeRun(ctx, runID)
		if err != nil {
			return err
		}
		if header.Status == "completed" && header.EndedAt != nil && header.Failure == nil && summary.Settled() && summary.DeadLetter+summary.Canceled == 0 {
			return nil
		}
		if header.Failure != nil || summary.DeadLetter+summary.Canceled != 0 || (header.Status != "running" && header.Status != "completed") {
			return fmt.Errorf("native future provider cannot complete: header=%+v deliveries=%+v", header, summary)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("native future provider business bound: header=%+v deliveries=%+v: %w", header, summary, ctx.Err())
		}
	}
}

func issue642RequireFutureClaimCut(t *testing.T, fixture issue2564ServedFixture, claim issue642FutureClaimFacts) {
	t.Helper()
	turns, err := storetest.ReadManagedAgentTurnStorage(t.Context(), fixture.selected, claim.RunID, claim.AgentID)
	if err != nil || len(turns) != 0 {
		t.Fatalf("claim cut launched a business turn: %+v err=%v", turns, err)
	}
	effects, err := storetest.ReadManagedTurnEffectStorage(t.Context(), fixture.selected, claim.RunID, claim.AgentID)
	if err != nil || effects != (storetest.ManagedTurnEffectStorage{}) {
		t.Fatalf("claim cut launched a business provider/effect: %+v err=%v", effects, err)
	}
	if finished := fixture.events(t, claim.RunID, claim.Target.FlowInstance+"/work.finished"); len(finished) != 0 {
		t.Fatalf("claim cut already published a result: %+v", finished)
	}
	root, err := fixture.selected.LoadOperatorEntity(t.Context(), claim.RunID, claim.RunID)
	if err != nil || root.Entity.CurrentState != "pending" || root.Fields["counter"] != float64(0) {
		t.Fatalf("claim cut already changed the root result: %+v err=%v", root, err)
	}
	outcomes, err := fixture.persistence.deps.DeliveryStore.Outcomes(t.Context(), claim.DeliveryID)
	if err != nil || len(outcomes) != 0 {
		t.Fatalf("held native claim already settled: %+v err=%v", outcomes, err)
	}
}

// Ordinary process startup rebinds the source actor and appends its own exact
// diagnostic. Validate that append before measuring selected-child isolation.
func issue642RequireFutureSourceStartupDiagnostic(t *testing.T, ctx context.Context, fixture issue2564ServedFixture, proof *issue642FutureRecoveryProof) {
	t.Helper()
	grant, err := fixture.Runtime.CurrentStartupGrantEvidence()
	if err != nil || grant.Validate() != nil || grant.State != startupownership.GrantAdmitted || grant.SelectedFork != nil {
		t.Fatalf("source startup has no current normal grant: %+v err=%v", grant, err)
	}
	for {
		after, err := storetest.ReadSelectedForkSourceDomain(ctx, fixture.selected, proof.SourceRunID)
		if err != nil {
			t.Fatal(err)
		}
		before := proof.SourceDomain["events"]
		for table, original := range proof.SourceDomain {
			if table != "events" && !reflect.DeepEqual(original, after[table]) {
				t.Fatalf("normal startup changed source business family %s", table)
			}
		}
		current := after["events"]
		if !reflect.DeepEqual(before.Columns, current.Columns) {
			t.Fatal("source startup changed the physical event column inventory")
		}
		originals := make(map[string]bool, len(before.Rows))
		for _, row := range before.Rows {
			originals[row] = true
		}
		var added []string
		for _, row := range current.Rows {
			if originals[row] {
				delete(originals, row)
			} else {
				added = append(added, row)
			}
		}
		if len(originals) != 0 || len(added) > 1 {
			t.Fatalf("normal startup rewrote source events: removed=%d added=%d", len(originals), len(added))
		}
		if len(added) == 0 {
			select {
			case <-time.After(10 * time.Millisecond):
				continue
			case <-ctx.Done():
				t.Fatalf("source actor startup diagnostic never settled: %v", ctx.Err())
			}
		}
		rows, err := storetest.DecodeSelectedForkSnapshotRows(storetest.SelectedForkStorageTableSnapshot{Columns: current.Columns, Rows: added})
		if err != nil || len(rows) != 1 {
			t.Fatalf("decode exact source startup append: %v", err)
		}
		row := rows[0]
		for column, want := range map[string]string{
			"run_id": proof.SourceRunID, "event_name": "platform.runtime_log", "event_class": "diagnostic_direct",
			"produced_by": "runtime", "produced_by_type": "platform", "routing_source_kind": "absent",
			"source_route": "{}", "target_route": "{}", "target_set": "[]",
		} {
			value, err := row[column].Text()
			if err != nil || value != want {
				t.Fatalf("source startup diagnostic %s=%q, want %q: %v", column, value, want, err)
			}
		}
		payload, err := row["payload_bytes"].Text()
		if err != nil {
			t.Fatal(err)
		}
		var log struct {
			Details struct {
				Action  string `json:"action"`
				RunID   string `json:"run_id"`
				Binding struct {
					GrantID           string `json:"generation_grant_id"`
					ProcessAuthority  string `json:"process_authority_id"`
					ProcessOwner      string `json:"process_owner_id"`
					ProcessBoot       string `json:"process_boot_id"`
					BundleHash        string `json:"bundle_hash"`
					RuntimeInstance   string `json:"runtime_instance_id"`
					RuntimeGeneration uint64 `json:"runtime_generation"`
				} `json:"process_binding"`
				Provenance struct {
					Origin struct{ Owner, Causality string }
				} `json:"diagnostic_provenance"`
			} `json:"details"`
		}
		if err := json.Unmarshal([]byte(payload), &log); err != nil {
			t.Fatal(err)
		}
		binding := log.Details.Binding
		if log.Details.Action != "platform.agent_lifecycle_transition" || log.Details.RunID != proof.SourceRunID ||
			log.Details.Provenance.Origin.Owner != "normal" || log.Details.Provenance.Origin.Causality != "observation" ||
			binding.GrantID != grant.GrantID || binding.ProcessAuthority != grant.ProcessAuthorityID ||
			binding.ProcessOwner != grant.ProcessOwnerID || binding.ProcessBoot != grant.ProcessBootID ||
			binding.BundleHash != grant.BundleHash || binding.RuntimeInstance != grant.RuntimeInstanceID || binding.RuntimeGeneration != grant.RuntimeGeneration {
			t.Fatalf("source append did not come from this normal startup grant: log=%+v grant=%+v", log, grant)
		}
		t.Logf("source_startup_diagnostic run=%s grant=%s original_rows_unchanged=%d append=1", proof.SourceRunID, grant.GrantID, len(before.Rows))
		proof.SourceDomain = after
		return
	}
}

func issue642RequireFutureRecoveredOutcome(t *testing.T, fixture issue2564ServedFixture, proof *issue642FutureRecoveryProof) {
	t.Helper()
	if proof.RecoveredClaim == nil {
		t.Fatal("successor lost its exact native recovered claim")
	}
	next := *proof.RecoveredClaim
	assigned := fixture.events(t, next.RunID, "work.assigned")
	if len(assigned) != 1 || assigned[0].EventID != next.EventID || len(assigned[0].Deliveries) != 1 ||
		assigned[0].Deliveries[0].DeliveryID != next.DeliveryID || assigned[0].Deliveries[0].Target != next.Target {
		t.Fatalf("recovery reconstructed the canonical assignment/target: %+v", assigned)
	}
	worker, err := fixture.selected.LoadOperatorEntity(t.Context(), next.Target.EntityID, next.RunID)
	keyField := "work_id"
	if proof.SourceKey != "" {
		keyField = "case_id"
	}
	if err != nil || worker.Fields[keyField] != next.ConstructedKey {
		t.Fatalf("recovery reconstructed the child key: %+v err=%v", worker, err)
	}
	finished := fixture.events(t, next.RunID, next.Target.FlowInstance+"/work.finished")
	deliveries := append([]operatorread.OperatorEventDelivery(nil), assigned[0].Deliveries...)
	deliveries = append(deliveries, finished[0].Deliveries...)
	for _, delivery := range deliveries {
		version := int64(1)
		if delivery.DeliveryID == next.DeliveryID {
			version = next.ClaimVersion
		}
		outcomes, err := fixture.persistence.deps.DeliveryStore.Outcomes(t.Context(), delivery.DeliveryID)
		if err != nil || len(outcomes) != 1 || outcomes[0].ClaimVersion != version || outcomes[0].Outcome != "delivered" || outcomes[0].Failure != nil ||
			!delivery.Terminal || delivery.Status != "delivered" || delivery.Failure != nil {
			t.Fatalf("canonical assigned/final consumer did not settle exactly once: delivery=%+v outcomes=%+v err=%v", delivery, outcomes, err)
		}
	}
	effects, err := storetest.ReadManagedTurnEffectStorage(t.Context(), fixture.selected, next.RunID, next.AgentID)
	if err != nil || effects.Settled != 1 || effects.Projected != 0 || effects.ProviderAttempts != 1 {
		t.Fatalf("recovered provider did not respond exactly once: %+v err=%v", effects, err)
	}
	rows, err := issue642ReadFutureExecutions(t.Context(), fixture.selected, next.RunID)
	if err != nil || len(rows) != 2 {
		t.Fatalf("recovery did not retain exactly two native executors: %+v err=%v", rows, err)
	}
	for _, row := range rows {
		wantFence, wantGeneration := next.Fence, next.Generation
		if row.ExecutionID == proof.Claim.ExecutionID {
			wantFence, wantGeneration = proof.Claim.Fence+1, proof.Claim.Generation
		} else if row.ExecutionID != next.ExecutionID {
			t.Fatalf("recovery admitted an unobserved executor: %+v", row)
		}
		if row.BindingID != proof.ACK.BindingID || row.Generation != wantGeneration || row.Fence != wantFence || row.State != "closed" || !row.Terminal || !row.LeaseEmpty || !row.Clean {
			t.Fatalf("native executor did not fence and close cleanly: %+v", row)
		}
	}
	state, err := storetest.ReadSelectedExecutionStorage(t.Context(), fixture.selected, next.RunID)
	if err != nil || state.State != "closed" || state.RunStatus != "completed" || state.Occurrences != 2 {
		t.Fatalf("native recovered terminal state: %+v err=%v", state, err)
	}
	after, err := storetest.ReadSelectedForkSourceDomain(t.Context(), fixture.selected, proof.SourceRunID)
	if err != nil || !reflect.DeepEqual(after, proof.SourceDomain) {
		for table, before := range proof.SourceDomain {
			current := after[table]
			if reflect.DeepEqual(before, current) {
				continue
			}
			beforeRows := make(map[string]bool, len(before.Rows))
			for _, row := range before.Rows {
				beforeRows[row] = true
			}
			t.Logf("source_domain_delta table=%s before_count=%d after_count=%d columns_before=%v columns_after=%v", table, len(before.Rows), len(current.Rows), before.Columns, current.Columns)
			for _, row := range current.Rows {
				if beforeRows[row] {
					delete(beforeRows, row)
				} else {
					t.Logf("source_domain_added table=%s row=%s", table, row)
				}
			}
			for row := range beforeRows {
				t.Logf("source_domain_removed table=%s row=%s", table, row)
			}
		}
		t.Fatalf("selected recovery changed the source domain: %v", err)
	}
	issue642RequireKeyedProviderResult(t, fixture, proof.SourceRunID)
}

// Decode the existing whole-store witness, never a caller-authored raw query.
func issue642ReadFutureExecutions(ctx context.Context, selected any, runID string) ([]issue642FutureExecutionRow, error) {
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil {
		return nil, err
	}
	rows, err := storetest.DecodeSelectedForkSnapshotRows(snapshot["run_fork_selected_contract_runtime_executions"])
	if err != nil {
		return nil, err
	}
	var out []issue642FutureExecutionRow
	for _, row := range rows {
		text := func(column string) (string, error) {
			cell, ok := row[column]
			if !ok {
				return "", fmt.Errorf("native execution snapshot omitted %s", column)
			}
			return cell.Text()
		}
		run, err := text("fork_run_id")
		if err != nil {
			return nil, err
		}
		if run != runID {
			continue
		}
		var result issue642FutureExecutionRow
		result.ExecutionID, err = text("execution_id")
		if err != nil {
			return nil, err
		}
		result.BindingID, err = text("binding_id")
		if err != nil {
			return nil, err
		}
		result.State, err = text("state")
		if err != nil {
			return nil, err
		}
		for column, target := range map[string]*uint64{"generation": &result.Generation, "fence_generation": &result.Fence} {
			cell, ok := row[column]
			if !ok || cell.Type != "int64" {
				return nil, fmt.Errorf("native execution snapshot has invalid %s: %+v", column, cell)
			}
			if err := json.Unmarshal(cell.Value, target); err != nil || *target == 0 {
				return nil, fmt.Errorf("native execution snapshot %s: %s err=%v", column, cell.Value, err)
			}
		}
		for _, column := range []string{"terminal_at", "lease_expires_at", "failure"} {
			if _, ok := row[column]; !ok {
				return nil, fmt.Errorf("native execution snapshot omitted %s", column)
			}
		}
		result.Terminal = string(row["terminal_at"].Value) != "null"
		result.LeaseEmpty = string(row["lease_expires_at"].Value) == "null"
		result.Clean = string(row["failure"].Value) == "null"
		out = append(out, result)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Generation < out[j].Generation })
	return out, nil
}
