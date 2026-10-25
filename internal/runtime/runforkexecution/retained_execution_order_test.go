package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type retainedExecutionOrderProbe struct {
	SelectedContractForkLifecycle
	SelectedContractReplayPersistence
	mu                 sync.Mutex
	activated          bool
	violation          bool
	started            chan struct{}
	release            chan struct{}
	cleanup            error
	publicationError   error
	publicationPanic   bool
	startFailure       bool
	startedOnce        sync.Once
	publicationContext context.Context
}

func (p *retainedExecutionOrderProbe) awaitFailure(ctx context.Context) error {
	p.startedOnce.Do(func() { close(p.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.release:
		return errors.New("injected post-acknowledgment startup failure")
	}
}

type retainedStartupProbe struct {
	runlifecycle.OperationOwner
	probe *retainedExecutionOrderProbe
}

func (p retainedStartupProbe) RequireActiveRunSource(ctx context.Context, runID string) (correlation.SourceArtifactFact, error) {
	p.probe.mu.Lock()
	fail := p.probe.startFailure
	p.probe.mu.Unlock()
	if fail {
		return correlation.SourceArtifactFact{}, p.probe.awaitFailure(ctx)
	}
	return p.OperationOwner.RequireActiveRunSource(ctx, runID)
}

type retainedFailureSettlementProbe struct {
	SelectedContractRuntimeExecutionLifecycle
	mu           sync.Mutex
	calls        int
	refuseOnce   bool
	staleOnce    bool
	staleRefused bool
	selected     any
	cleanup      error
}

type retainedContinuationProbe struct {
	deliverylifecycle.Store
	probe *retainedExecutionOrderProbe
}

func (p retainedContinuationProbe) ScanDeliveryContinuations(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, cursor deliverylifecycle.ContinuationCursor, limit int) (deliverylifecycle.ContinuationPage, error) {
	return deliverylifecycle.ContinuationPage{}, p.probe.awaitFailure(ctx)
}

func (p *retainedFailureSettlementProbe) FailActivatedRunForkSelectedContractRuntimeExecution(ctx context.Context, authority effects.Authority, raw json.RawMessage) (bool, error) {
	p.mu.Lock()
	p.calls++
	refuse := p.refuseOnce && p.calls == 1
	stale := p.staleOnce && p.calls == 1
	p.mu.Unlock()
	if stale {
		before, err := storetest.ReadSelectedForkSourceDomain(ctx, p.selected, authority.SelectedFork.ForkRunID)
		if err != nil {
			return false, err
		}
		wrong := authority
		wrong.FenceGeneration++
		acknowledged, rejection := p.SelectedContractRuntimeExecutionLifecycle.FailActivatedRunForkSelectedContractRuntimeExecution(ctx, wrong, raw)
		after, err := storetest.ReadSelectedForkSourceDomain(ctx, p.selected, authority.SelectedFork.ForkRunID)
		refused := !acknowledged && rejection != nil && err == nil && reflect.DeepEqual(before, after)
		p.mu.Lock()
		p.staleRefused = refused
		p.mu.Unlock()
		return false, errors.Join(errors.New("injected stale failure-disposition fence"), rejection, err)
	}
	if refuse {
		return false, errors.New("injected unacknowledged failure disposition")
	}
	acknowledged, err := p.SelectedContractRuntimeExecutionLifecycle.FailActivatedRunForkSelectedContractRuntimeExecution(ctx, authority, raw)
	if acknowledged {
		err = errors.Join(err, p.cleanup)
	}
	return acknowledged, err
}

func (p *retainedExecutionOrderProbe) ActivateRunForkForSelectedContractExecution(ctx context.Context, request runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	activation, err := p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, request)
	if activation.Activated {
		p.mu.Lock()
		p.activated = true
		p.mu.Unlock()
		return activation, errors.Join(err, p.cleanup)
	}
	return activation, err
}

func (p *retainedExecutionOrderProbe) CommitSelectedForkEvent(ctx context.Context, request bus.CommitSelectedForkEventRequest) (bus.CommittedSelectedForkEvent, error) {
	p.mu.Lock()
	p.publicationContext = context.WithoutCancel(ctx)
	p.violation = !p.activated
	p.startedOnce.Do(func() { close(p.started) })
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return bus.CommittedSelectedForkEvent{}, ctx.Err()
	case <-p.release:
		if p.publicationPanic {
			panic("injected post-acknowledgment publication panic")
		}
		if p.publicationError != nil {
			return bus.CommittedSelectedForkEvent{}, p.publicationError
		}
		return p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, request)
	}
}

func TestSelectedForkAcknowledgmentPrecedesBusinessDrainBothStores(t *testing.T) {
	// W4 reverses the legacy ordering: business settlement precedes live ACK.
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newActivityForkFixture(t, backend)
			owner, selected := fixture.owner, fixture.owner.ports.fork
			ctx, request, _ := q1GateComponentSource(t, owner, false, true, true)
			loaded, err := request.SourceLoader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
				SourceRunID: request.SourceRunID, Selection: request.ContractSelection,
			})
			if err != nil {
				t.Fatal(err)
			}
			ordinary, contexts := liveForkRuntimeFixture(t, ctx, selected, owner, loaded, request.SourceRunID, executionposture.Live)
			probe := &retainedExecutionOrderProbe{SelectedContractForkLifecycle: owner.ports.fork, SelectedContractReplayPersistence: owner.ports.replay,
				started: make(chan struct{}), release: make(chan struct{})}
			owner.ports.fork, owner.ports.replay = probe, probe
			request.Owner, request.LiveRuntime, request.LiveContexts, request.LiveRuns = owner, ordinary, contexts, selected.(runlifecycle.LiveRunReader)
			type outcome struct {
				result SelectedContractExecutionResult
				err    error
			}
			done := make(chan outcome, 1)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(probe.release) }) }
			joined := false
			defer func() {
				release()
				if !joined {
					select {
					case result := <-done:
						if result.err != nil {
							t.Error(result.err)
						}
					case <-time.After(5 * time.Second):
						t.Error("selected publication did not join")
					}
				}
			}()
			go func() {
				result, err := ExecuteSelectedContractRunFork(ctx, request)
				done <- outcome{result, err}
			}()
			select {
			case <-probe.started:
			case result := <-done:
				joined = true
				t.Fatalf("selected publication missed its native cut: %+v err=%v", result.result, result.err)
			case <-time.After(5 * time.Second):
				t.Fatal("selected publication never reached its native cut")
			}
			operation := request.ForkOperation
			beforeACK, found, err := selected.LoadForkOperationByID(ctx, operation.OperationID)
			if err != nil || !found || beforeACK.Status != runfork.ForkOperationMaterialized || beforeACK.Result != nil || beforeACK.ForkRunID == "" {
				t.Fatalf("unsettled business publication acquired a live ACK: %+v err=%v", beforeACK, err)
			}
			storage, err := storetest.ReadSelectedExecutionStorage(ctx, selected, beforeACK.ForkRunID)
			if err != nil || storage.State != "running" || storage.RunStatus != "paused" || storage.Occurrences != 1 {
				t.Fatalf("unsettled publication lost its exact private selected executor: %+v err=%v", storage, err)
			}
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, request.SourceRunID)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-done:
				joined = true
				t.Fatalf("unsettled business work returned a public ACK: %+v err=%v", result.result, result.err)
			default:
			}
			release()
			select {
			case result := <-done:
				joined = true
				if result.err != nil || !result.result.Activation.Activated || result.result.Materialization.ForkRunID != beforeACK.ForkRunID || result.result.ExecutedEventCount != 1 {
					t.Fatalf("settled publication lost its exact live ACK: %+v err=%v", result.result, result.err)
				}
				for _, event := range result.result.ForkEvents {
					if receipt := storetest.ObservePipelineReceipt(t, ctx, selected, event.ForkEventID); receipt.Count != 1 || receipt.Outcome != "success" {
						t.Fatalf("live ACK preceded canonical business settlement: %+v", receipt)
					}
				}
			case <-time.After(5 * time.Second):
				t.Fatal("settled selected publication did not return its live ACK")
			}
			afterACK, found, err := selected.LoadForkOperationByID(ctx, operation.OperationID)
			if err != nil || !found || afterACK.Status != runfork.ForkOperationActivated || afterACK.Result == nil || afterACK.ForkRunID != beforeACK.ForkRunID || afterACK.BindingID != beforeACK.BindingID || afterACK.SemanticHash != beforeACK.SemanticHash || !reflect.DeepEqual(afterACK.Request, beforeACK.Request) {
				t.Fatalf("live ACK changed its original operation or child: %+v err=%v", afterACK, err)
			}
			if sourceAfter, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, request.SourceRunID); err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("selected publication changed source business state: %v", err)
			}
		})
	}
}

func TestSelectedCompletionActualScopeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newActivityForkFixture(t, backend)
			owner, selected := fixture.owner, fixture.owner.ports.fork
			ctx, request, _ := q1GateComponentSource(t, owner, false, true, false)
			attachment := q1StageBranchPointComponent(t, ctx, request)
			selectedCtx := attachment.container.attachment.ctx
			if _, err := owner.ports.busDurable.RunLifecycle.TransitionActiveRun(selectedCtx, runlifecycle.ActiveTransitionRequest{
				RunID: attachment.materialized.ForkRunID, State: runlifecycle.StateRunning,
			}); err != nil {
				t.Fatal(err)
			}
			if err := attachment.container.attachment.runtime.startExecution(selectedCtx, attachment.container.attachment.pipeline, attachment.container.diagnostics, false); err != nil {
				t.Fatal(err)
			}
			requester := selected.(interface {
				RequestCompletionCandidate(context.Context, runlifecycle.CandidateRequest) (runlifecycle.CandidateRequestDisposition, error)
			})
			if _, err := requester.RequestCompletionCandidate(selectedCtx, runlifecycle.CandidateAtTime(attachment.materialized.ForkRunID, time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			proveSelectedCompletionScope(t, ctx, selectedCtx, selected, owner.ports.candidates, attachment.prepared.loadedSource.Source, attachment.materialized.ForkRunID)
		})
	}
}

func TestReviewerSelectedAsyncFailureDisposesResources(t *testing.T) {
	for _, fault := range []string{"publication", "execution-scope", "panic", "cancellation", "startup", "continuation", "settlement-retry", "settlement-cleanup", "stale-settlement", "cleanup-retry", "cleanup-panic", "operator-stop"} {
		t.Run(fault, func(t *testing.T) { proveRetainedAcknowledgment(t, fault) })
	}
}

func proveRetainedAcknowledgment(t *testing.T, fault string) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newActivityForkFixture(t, backend)
			owner, selected := fixture.owner, fixture.owner.ports.fork
			ctx, request, _ := q1GateComponentSource(t, owner, false, true, false)
			sourceID := request.SourceRunID
			// This is private selected failure/cleanup, not a public handoff ACK.
			operation := *request.ForkOperation
			request.ForkOperation = nil
			probe := &retainedExecutionOrderProbe{
				SelectedContractForkLifecycle: owner.ports.fork, SelectedContractReplayPersistence: owner.ports.replay,
				started: make(chan struct{}), release: make(chan struct{}),
			}
			if fault == "publication" || fault == "execution-scope" || strings.HasPrefix(fault, "settlement-") || strings.HasPrefix(fault, "cleanup-") || fault == "stale-settlement" {
				probe.publicationError = errors.New("reviewer injected publication failure")
			} else if fault == "cancellation" {
				probe.publicationError = context.Canceled
			} else if fault == "panic" {
				probe.publicationPanic = true
			}
			owner.ports.busDurable.RunLifecycle = retainedStartupProbe{OperationOwner: owner.ports.busDurable.RunLifecycle, probe: probe}
			if fault == "continuation" {
				owner.ports.busDurable.DeliveryLifecycle = retainedContinuationProbe{Store: owner.ports.busDurable.DeliveryLifecycle, probe: probe}
			}
			settlement := &retainedFailureSettlementProbe{SelectedContractRuntimeExecutionLifecycle: owner.ports.runtimeExecution, selected: selected, refuseOnce: fault == "settlement-retry", staleOnce: fault == "stale-settlement"}
			if fault == "settlement-cleanup" {
				settlement.cleanup = errors.New("acknowledged failure disposition cleanup")
			}
			owner.ports.runtimeExecution = settlement
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(probe.release) }) }
			defer release()
			owner.ports.fork, owner.ports.replay = probe, probe
			request.Owner = owner
			process, _ := worklifetime.ProcessFromContext(ctx)
			baselineLeases := process.ActiveCount()
			attachment := q1StageBranchPointComponent(t, ctx, request)
			if err := owner.retainPrepared(attachment.prepared); err != nil {
				t.Fatal(err)
			}
			probe.startFailure = fault == "startup"
			if _, err := owner.ports.busDurable.RunLifecycle.TransitionActiveRun(attachment.container.attachment.ctx, runlifecycle.ActiveTransitionRequest{
				RunID: attachment.materialized.ForkRunID, State: runlifecycle.StateRunning,
			}); err != nil {
				t.Fatal(err)
			}
			if err := attachment.container.Serve(); err != nil {
				t.Fatal(err)
			}
			result := SelectedContractExecutionResult{Materialization: attachment.materialized}
			operations, ok := selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			})
			if !ok {
				t.Fatal("selected store lacks its permanent operation reader")
			}
			record, found, err := operations.LoadForkOperation(context.Background(), operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || found {
				t.Fatalf("private selected component fabricated a public acknowledgment: record=%+v found=%v error=%v", record, found, err)
			}
			select {
			case <-probe.started:
			case <-time.After(5 * time.Second):
				t.Fatal("retained executor never began ordinary publication")
			}
			probe.mu.Lock()
			violation := probe.violation
			probe.mu.Unlock()
			if !violation && fault != "startup" && fault != "continuation" {
				t.Fatal("selected failure cut unexpectedly required a public activation ACK")
			}
			storage, err := storetest.ReadSelectedExecutionStorage(context.Background(), selected, result.Materialization.ForkRunID)
			if err != nil || storage.State != "running" || storage.RunStatus != "running" || storage.Occurrences != 1 {
				t.Fatalf("component did not retain one current executor: %+v %v", storage, err)
			}
			if fault == "" {
				return
			}
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(context.Background(), selected, sourceID)
			if err != nil {
				t.Fatal(err)
			}
			if fault == "execution-scope" {
				// Orchestration's source is not the bound executable scope. Failure
				// must still consume the attachment's exact selected source fact.
				otherFact, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("f", 64))
				if err != nil {
					t.Fatal(err)
				}
				owner.ports.contexts.mu.Lock()
				for _, entry := range owner.ports.contexts.entries {
					entry.operation.mu.Lock()
					entry.operation.owned = correlation.WithSourceArtifactFact(entry.operation.owned, otherFact)
					entry.operation.mu.Unlock()
				}
				owner.ports.contexts.mu.Unlock()
			}
			if strings.HasPrefix(fault, "cleanup-") {
				owner.ports.contexts.mu.Lock()
				var prepared *PreparedSelectedFork
				for _, entry := range owner.ports.contexts.entries {
					prepared = entry.retained
				}
				owner.ports.contexts.mu.Unlock()
				if prepared == nil {
					t.Fatal("acknowledged execution lacks its retained preparation")
				}
				prepared.bindMu.Lock()
				originalRelease, first := prepared.loadedSource.Cleanup, true
				prepared.loadedSource.Cleanup = func() error {
					if first {
						first = false
						if fault == "cleanup-panic" {
							panic("injected selected-source resource release panic")
						}
						return errors.New("injected selected-source resource release failure")
					}
					if originalRelease != nil {
						return originalRelease()
					}
					return nil
				}
				prepared.bindMu.Unlock()
			}
			wantStatus := "failed"
			if fault == "operator-stop" {
				wait, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				stopped, selected, err := owner.StopSelectedFork(wait, runcontrol.TransitionRequest{RunID: result.Materialization.ForkRunID, Now: time.Now().UTC()})
				cancel()
				if err != nil || !selected || stopped.Status != "cancelled" {
					t.Fatalf("exact operator stop did not own terminal disposition: %+v %v", stopped, err)
				}
				wantStatus = "cancelled"
			} else {
				release()
			}
			if fault == "settlement-retry" || fault == "stale-settlement" || strings.HasPrefix(fault, "cleanup-") {
				deadline := time.Now().Add(5 * time.Second)
				for {
					owner.ports.contexts.mu.Lock()
					var retained *PreparedSelectedFork
					for _, entry := range owner.ports.contexts.entries {
						if entry.closed {
							retained = entry.retained
						}
					}
					owner.ports.contexts.mu.Unlock()
					if retained != nil {
						if process.ActiveCount() <= baselineLeases {
							t.Fatal("unacknowledged failure lost process possession")
						}
						if err := retained.Close(); err != nil {
							t.Fatalf("retry exact failure disposition: %v", err)
						}
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("unacknowledged disposition did not retain its exact preparation")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			wait, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for {
				storage, err = storetest.ReadSelectedExecutionStorage(wait, selected, result.Materialization.ForkRunID)
				if err != nil {
					t.Fatal(err)
				}
				owner.ports.contexts.mu.Lock()
				retained := len(owner.ports.contexts.entries)
				owner.ports.contexts.mu.Unlock()
				if storage.State == "closed" && storage.RunStatus == wantStatus && retained == 0 && process.ActiveCount() == baselineLeases {
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("failed serving was not settled and retired: %+v preparations=%d leases=%d (baseline=%d)", storage, retained, process.ActiveCount(), baselineLeases)
				case <-time.After(10 * time.Millisecond):
				}
			}
			after, found, err := operations.LoadForkOperation(wait, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || found || !reflect.DeepEqual(record, after) {
				t.Fatalf("private failure fabricated an acknowledged creation: %+v %v", after, err)
			}
			cardinality, err := storetest.ReadLifecycleEventCardinality(wait, selected, result.Materialization.ForkRunID, "item.received")
			if err != nil || cardinality != 0 {
				t.Fatalf("failed publication leaked or duplicated business events: count=%d %v", cardinality, err)
			}
			sourceAfter, err := storetest.ReadSelectedForkSourceDomain(wait, selected, sourceID)
			if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("child failure changed source business state: %v", err)
			}
			if err := owner.RetireSelectedContexts(wait); err != nil {
				t.Fatalf("successful resource release was mistaken for business failure: %v", err)
			}
			settlement.mu.Lock()
			calls := settlement.calls
			staleRefused := settlement.staleRefused
			settlement.mu.Unlock()
			if fault == "stale-settlement" && !staleRefused {
				t.Fatal("stale failure fence mutated the child or historical facts")
			}
			want := 1
			if fault == "operator-stop" {
				want = 0
			}
			if fault == "settlement-retry" || fault == "stale-settlement" {
				want = 2
			}
			if calls != want {
				t.Fatalf("failure disposition calls=%d, want %d", calls, want)
			}
		})
	}
}
