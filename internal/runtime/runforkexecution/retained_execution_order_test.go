package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type retainedExecutionOrderProbe struct {
	SelectedContractForkLifecycle
	SelectedContractReplayPersistence
	mu               sync.Mutex
	activated        bool
	violation        bool
	started          chan struct{}
	release          chan struct{}
	cleanup          error
	publicationError error
	publicationPanic bool
	startFailure     bool
	startedOnce      sync.Once
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
	fail := p.probe.activated && p.probe.startFailure
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
	proveRetainedAcknowledgment(t, "")
}

func TestReviewerSelectedAsyncFailureDisposesResources(t *testing.T) {
	for _, fault := range []string{"publication", "panic", "cancellation", "startup", "continuation", "settlement-retry", "settlement-cleanup", "stale-settlement", "cleanup-retry", "cleanup-panic", "operator-stop", "staged-publication"} {
		t.Run(fault, func(t *testing.T) { proveRetainedAcknowledgment(t, fault) })
	}
}

func proveRetainedAcknowledgment(t *testing.T, fault string) {
	t.Helper()
	staged := strings.HasPrefix(fault, "staged-")
	fault = strings.TrimPrefix(fault, "staged-")
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected SelectedContractForkLifecycle
			var owner SelectedContractExecutionOwner
			var db *sql.DB
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				pg := storetest.AdmitPostgresRuntimeStore(t, db)
				selected, owner = pg, selectedContractExecutionOwnerForTest(t, pg)
			} else {
				sqlite := storetest.StartSQLiteRuntimeStore(t)
				db = storetest.DatabaseForTest(sqlite)
				selected, owner = sqlite, selectedRuntimeOutcomeSQLiteOwner(t, sqlite)
			}
			ctx := runForkTestContext(t)
			process, _ := worklifetime.ProcessFromContext(ctx)
			baselineLeases := process.ActiveCount()
			repo := runForkExecutionRepoRoot(t)
			loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple"), PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}
			loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts})
			if err != nil {
				t.Fatal(err)
			}
			ctx = correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact)
			scope, err := authoractivity.BundleScopeForTarget(ctx, loaded.SourceArtifactFact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			ctx = authoractivity.WithScope(ctx, scope)
			descriptors, err := rootruntime.AuthorActivityEventDescriptors(loaded.Source)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := selected.RegisterAuthorActivityEventCatalog(scope, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(catalog.Release)
			sourceID, eventID := uuid.NewString(), uuid.NewString()
			route := selectedExecutionEntitylessNodeRoute("source-only-node")
			if staged {
				route = selectedExecutionTestAgentRoute(t, sourceID, "source-only-agent", "")
				route.Target = events.MustExistingEntityTarget(events.RouteIdentity{
					FlowID: semanticview.RootExecutionFlowID(loaded.Source), FlowInstance: sourceID, EntityID: sourceID,
				})
			}
			seedSelectedRuntimeOutcomeSource(t, ctx, backend, db, selected, loaded, sourceID, eventID, route, time.Unix(1700002200, 0).UTC())
			probe := &retainedExecutionOrderProbe{
				SelectedContractForkLifecycle: owner.ports.fork, SelectedContractReplayPersistence: owner.ports.replay,
				started: make(chan struct{}), release: make(chan struct{}), cleanup: errors.New("activation cleanup diagnostic"),
			}
			if fault == "publication" || strings.HasPrefix(fault, "settlement-") || strings.HasPrefix(fault, "cleanup-") || fault == "stale-settlement" {
				probe.publicationError = errors.New("reviewer injected publication failure")
			} else if fault == "cancellation" {
				probe.publicationError = context.Canceled
			} else if fault == "panic" {
				probe.publicationPanic = true
			} else if fault == "startup" {
				probe.startFailure = true
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
			selection := runforkadmission.SelectedContractSelection(loaded.Source)
			operation := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "retention-proof", IdempotencyKey: "fixed-cut",
				TransportHash: "retention-proof-transport", SourceRunID: sourceID, ForkEventID: eventID,
				TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: selection,
			}
			request := SelectedContractExecutionRequest{
				SourceRunID: sourceID, At: eventID, AllowSourceFreeze: true, Owner: owner, ForkOperation: &operation,
				SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability},
			}
			var result SelectedContractExecutionResult
			if staged {
				prepared, err := owner.Prepare(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				// The supported staged activation surface is unkeyed. Keyed
				// request/acknowledgment preservation is proved by direct execution.
				materialized, acknowledged, err := owner.materializePrepared(prepared.operation.PreparationContext(), prepared)
				if err != nil || !acknowledged {
					t.Fatalf("staged materialization: %+v %v", materialized, err)
				}
				if err := prepared.Close(); err != nil {
					t.Fatal(err)
				}
				activation, err := ActivateSelectedContractRunFork(ctx, SelectedContractActivationGateRequest{
					ForkRunID: materialized.ForkRunID, AllowSourceFreeze: true, Store: selected.(SelectedContractActivationStore),
					ExecutionOwner: owner, SourceLoader: loader, AgentRuntime: request.AgentRuntime,
				})
				if err != nil {
					t.Fatal(err)
				}
				result = SelectedContractExecutionResult{Materialization: materialized, Activation: activation.RunForkActivation, ExecutedEventCount: activation.ExecutedEventCount}
			} else {
				result, err = ExecuteSelectedContractRunFork(ctx, request)
			}
			if err != nil || !result.Activation.Activated || result.ExecutedEventCount != 0 {
				t.Fatalf("activation was confused with drain or cleanup: result=%+v error=%v", result, err)
			}
			operations, ok := selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			})
			if !ok {
				t.Fatal("selected store lacks its permanent operation reader")
			}
			record, found, err := operations.LoadForkOperation(context.Background(), operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || (!staged && (!found || record.Status != runfork.ForkOperationActivated || record.ForkRunID != result.Materialization.ForkRunID || record.Result == nil || record.Result.ExecutedEventCount != 0)) || (staged && found) {
				t.Fatalf("activation lacked durable acknowledgment before drain: record=%+v found=%v error=%v", record, found, err)
			}
			select {
			case <-probe.started:
			case <-time.After(5 * time.Second):
				t.Fatal("retained executor never began ordinary publication")
			}
			probe.mu.Lock()
			violation := probe.violation
			probe.mu.Unlock()
			if violation {
				t.Fatal("business publication preceded acknowledged activation")
			}
			storage, err := storetest.ReadSelectedExecutionStorage(context.Background(), selected, result.Materialization.ForkRunID)
			if err != nil || storage.State != "running" || storage.RunStatus != "running" || storage.Occurrences != 1 {
				t.Fatalf("acknowledgment did not retain one current executor: %+v %v", storage, err)
			}
			if fault == "" {
				return
			}
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(context.Background(), selected, sourceID)
			if err != nil {
				t.Fatal(err)
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
			if err != nil || found == staged || !reflect.DeepEqual(record, after) {
				t.Fatalf("business failure changed the acknowledged creation: %+v %v", after, err)
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
