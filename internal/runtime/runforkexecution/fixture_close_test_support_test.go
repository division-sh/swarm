package runforkexecution

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type fixtureCloseGrantProbe struct {
	startupownership.GenerationGrant
	evidence startupownership.GrantEvidence
	onRead   func()
	refusal  error
	proofs   int
}

func (g *fixtureCloseGrantProbe) Evidence() (startupownership.GrantEvidence, error) {
	if g.onRead != nil {
		g.onRead()
	}
	return g.evidence, nil
}

func (g *fixtureCloseGrantProbe) ProveCurrent(context.Context) error {
	g.proofs++
	return g.refusal
}

func TestSelectedFixtureCloseRequiresCurrentRetainedOccurrence(t *testing.T) {
	for _, phase := range []string{"current_join", "missing_binding", "wrong_run", "unavailable", "retired", "unreconciled", "ambiguous", "retiring", "closed", "stale_binding", "unready", "ordinary_occurrence", "stale_generation", "canceled", "retirement_race", "native_fence"} {
		t.Run(phase, func(t *testing.T) {
			runID, executionID := uuid.NewString(), uuid.NewString()
			binding := testSelectedContractBinding(runID)
			binding.BindingID = uuid.NewString()
			fingerprint := "sha256:" + strings.Repeat("a", 64)
			admission, err := managedexecution.New(managedexecution.KindSelectedContractFork, executionID, 1, runID, fingerprint, runForkTestBundleHash, nil)
			if err != nil {
				t.Fatal(err)
			}
			authority := effects.Authority{Kind: effects.AuthoritySelectedContractFork, ID: executionID,
				ExecutionOwner: "fixture-close-owner", LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: 3, ExecutionMode: executionmode.Mock,
				SelectedFork: effects.SelectedContractForkAuthority{ExecutionID: executionID, ForkRunID: runID, Generation: 1,
					AdmissionFingerprint: fingerprint, ContainerPlanFingerprint: fingerprint, ActorCensusFingerprint: fingerprint, EffectiveConfigFingerprint: fingerprint}}
			process := worklifetime.NewProcess()
			occurrence, err := process.NewSelectedFork(context.Background(), worklifetime.SelectedForkIdentity{ExecutionID: executionID, RunID: runID, Generation: 1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := occurrence.RetireAndWait(context.Background()); err != nil {
					t.Error(err)
				}
				if process.ActiveCount() != 0 {
					t.Errorf("fixture close leaked joined work: %d", process.ActiveCount())
				}
			})
			owned := worklifetime.WithOccurrence(managedexecution.WithAdmission(effects.WithAuthority(context.Background(), authority), admission), occurrence)
			fence := errors.New("native selected generation fenced")
			grant := &fixtureCloseGrantProbe{refusal: fence, evidence: startupownership.GrantEvidence{
				GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "fixture-process", ProcessBootID: uuid.NewString(),
				BundleHash: runForkTestBundleHash, RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1, StateVersion: 3, State: startupownership.GrantAdmitted,
				SelectedFork: &startupownership.SelectedForkGrantBinding{BindingID: binding.BindingID, ForkRunID: runID,
					ExecutionID: executionID, ExecutionGeneration: 1, FenceGeneration: 3, ExecutionOwner: authority.ExecutionOwner,
					AdmissionFingerprint: fingerprint, ContainerPlanFingerprint: fingerprint, ActorCensusFingerprint: fingerprint,
					EffectiveConfigFingerprint: fingerprint, DeclarationPlanFingerprint: fingerprint, PreparationFingerprint: fingerprint}}}
			if err := grant.evidence.Validate(); err != nil {
				t.Fatal(err)
			}
			runtime := &selectedContractAgentRuntime{controlContext: owned, bus: &bus.EventBus{}, generationGrant: grant, timerLifecycle: &pipeline.PipelineCoordinator{}}
			entry := &selectedForkContext{binding: binding, done: make(chan struct{}), retained: &PreparedSelectedFork{retainedRuntime: runtime}}
			contexts := &selectedForkContexts{recovered: true, entries: map[*selectedContractOperation]*selectedForkContext{{}: entry}}
			store := &selectedControlBindingFixture{binding: binding, exists: true}
			owner := SelectedContractExecutionOwner{ports: &selectedContractExecutionPorts{fork: store, contexts: contexts}}
			caller, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			before := process.ActiveCount()
			switch phase {
			case "current_join":
				use, err := owner.acquireSelectedFixtureCloseForTest(caller, binding)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = use.lease.Done() }()
				defer use.cancel()
				if use.result.RunID != runID || use.result.ExecutionID != executionID || use.result.BindingID != binding.BindingID || use.result.Generation != 1 {
					t.Fatal("fixture close did not join the exact retained execution")
				}
				canceled, stop := context.WithCancel(context.Background())
				stop()
				if err := occurrence.WaitForQuiescence(canceled); !errors.Is(err, context.Canceled) {
					t.Fatalf("fixture close lacks a live joined input: %v", err)
				}
				wantDeadline, _ := caller.Deadline()
				if deadline, bounded := use.ctx.Deadline(); !bounded || deadline != wantDeadline {
					t.Fatal("fixture close changed the caller deadline")
				}
				cancel()
				<-use.ctx.Done()
				if owned.Err() != nil {
					t.Fatal("fixture caller cancellation retired the child")
				}
				if err := use.lease.Done(); err != nil {
					t.Fatal(err)
				}
				joinCtx, stopJoin := context.WithTimeout(context.Background(), time.Second)
				defer stopJoin()
				if err := occurrence.WaitForQuiescence(joinCtx); err != nil {
					t.Fatalf("released fixture input remains active: %v", err)
				}
				if process.ActiveCount() != before {
					t.Fatal("completed fixture input retained a work lease")
				}
				return
			case "missing_binding":
				store.exists = false
			case "wrong_run":
				runID = uuid.NewString()
			case "unavailable":
				contexts.entries = nil
			case "retired":
				contexts.retired = true
			case "unreconciled":
				contexts.recovered = false
			case "ambiguous":
				other := *entry
				contexts.entries[&selectedContractOperation{}] = &other
			case "retiring":
				entry.retiring = true
			case "closed":
				entry.closed = true
			case "stale_binding":
				entry.binding.BindingID = uuid.NewString()
			case "unready":
				runtime.timerLifecycle = nil
			case "ordinary_occurrence":
				ordinary, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{RuntimeInstanceID: uuid.NewString(), BundleHash: runForkTestBundleHash})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if _, err := ordinary.RetireAndWait(context.Background()); err != nil {
						t.Error(err)
					}
				})
				runtime.controlContext = worklifetime.WithOccurrence(owned, ordinary)
			case "stale_generation":
				grant.evidence.SelectedFork.ExecutionGeneration++
				grant.evidence.RuntimeGeneration++
			case "canceled":
				cancel()
			case "retirement_race":
				grant.onRead = occurrence.Retire
			}
			before = process.ActiveCount()
			result, err := owner.PublishSelectedFixtureCloseForTest(caller, runID, SelectedFixtureCloseInputForTest{AccountID: "changed"})
			if err == nil || result != (SelectedFixtureCloseResultForTest{}) || process.ActiveCount() != before {
				t.Fatalf("non-current fixture input admitted or leaked work: result=%+v error=%v work=%d/%d", result, err, process.ActiveCount(), before)
			}
			if phase == "native_fence" {
				if !errors.Is(err, fence) || grant.proofs != 1 {
					t.Fatalf("lost native fence after joining: %v proofs=%d", err, grant.proofs)
				}
			} else if grant.proofs != 0 {
				t.Fatal("fixture input bypassed the intended retained-owner refusal")
			}
			if phase == "retirement_race" && !errors.Is(err, worklifetime.ErrRetired) {
				t.Fatalf("retirement failed outside the existing occurrence fence: %v", err)
			}
			if phase == "canceled" && (!errors.Is(err, context.Canceled) || store.calls != 0) {
				t.Fatalf("canceled caller performed work or lost its cause: %v calls=%d", err, store.calls)
			}
		})
	}
}
